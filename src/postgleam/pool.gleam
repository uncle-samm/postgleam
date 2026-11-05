//// A pool of PostgreSQL connections for running queries concurrently.
////
//// Each connection is its own actor, so queries on different connections
//// run in parallel. A caller checks a connection out, uses it, and checks it
//// back in; the pool then rolls back anything the caller left open before
//// handing the connection to the next caller. If no connection is free the
//// caller waits up to `config.queue_timeout` and then gets `PoolTimeout`.
////
//// ```gleam
//// import postgleam
//// import postgleam/config
//// import postgleam/decode
//// import postgleam/pool
////
//// pub fn main() {
////   let assert Ok(cfg) = config.from_url("postgres://app:secret@localhost/app")
////   let assert Ok(db) = pool.start(cfg, size: 10)
////   let assert Ok(response) =
////     pool.query_with(db, "SELECT $1::int4 + 1", [postgleam.int(41)], {
////       use n <- decode.element(0, decode.int)
////       decode.success(n)
////     })
////   pool.shutdown(db)
//// }
//// ```
////
//// The pool also:
//// - pings connections that have been idle for `config.idle_interval` ms,
////   reconnecting broken ones before a caller needs them;
//// - when it finds a broken connection, checks every idle connection at
////   once, so sockets that died together (for example while the host was
////   suspended) are replaced in one sweep instead of one failed query at a
////   time;
//// - replaces a connection whose actor crashed;
//// - takes a connection back if the caller holding it exits.
////
//// An application that knows its sockets are dead, for example right after
//// the machine resumed from a suspend, can call `reconnect_all`.

import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{
  type ExitMessage, type Monitor, type Name, type Pid, type Subject,
}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import postgleam.{type Connection, type Param, type Response}
import postgleam/config.{type Config}
import postgleam/connection.{type ExtendedQueryResult, type SimpleQueryResult}
import postgleam/decode.{type RowDecoder}
import postgleam/error.{type Error}
import postgleam/internal/call
import postgleam/internal/connection_actor

/// A handle to a running pool.
pub opaque type Pool {
  Pool(subject: Subject(PoolMessage))
}

/// Messages the pool process handles. Only needed to name a pool (see
/// `supervised`).
pub opaque type PoolMessage {
  Checkout(caller: Pid, reply: Subject(Result(#(Int, Connection), Error)))
  Checkin(id: Int)
  ResetDone(id: Int, healthy: Bool)
  CheckDone(id: Int, healthy: Bool, sweep: Bool)
  ReconnectAll(reply: Subject(Result(Nil, Error)))
  CallerDown(process.Down)
  ConnectionExit(ExitMessage)
  ExpireWaiters
  PingIdle
  Replenish
  GetStats(reply: Subject(Result(Stats, Error)))
  Shutdown(reply: Subject(Nil))
}

type Slot {
  Slot(
    conn: Connection,
    pid: Pid,
    status: SlotStatus,
    /// Reconnect this connection when it is next free (set by
    /// `reconnect_all` while the connection was in use).
    stale: Bool,
  )
}

type SlotStatus {
  /// Free since the given monotonic time (ms).
  Idle(since: Int)
  /// Checked out by the process behind `monitor`.
  Busy(monitor: Monitor)
  /// Checked in; waiting for the connection to roll back leftovers.
  Resetting
  /// Being pinged or reconnected; not handed out until it reports back.
  Checking
}

type Waiter {
  Waiter(
    caller: Pid,
    monitor: Monitor,
    reply: Subject(Result(#(Int, Connection), Error)),
    deadline: Int,
  )
}

type State {
  State(
    config: Config,
    size: Int,
    self: Subject(PoolMessage),
    slots: Dict(Int, Slot),
    /// Waiting callers, oldest first.
    waiters: List(Waiter),
    next_id: Int,
    /// Number of connections that crashed and could not be replaced yet.
    missing: Int,
    /// When the last sweep started (monotonic ms), to stop a server that is
    /// down from triggering one sweep per failed query.
    last_sweep: Int,
  )
}

/// Minimum time between two sweeps.
const sweep_interval = 1000

/// Start a pool of `size` connections. All connections are opened before
/// this returns, so a bad config or an unreachable server fails here. The
/// pool is linked to the calling process.
pub fn start(config: Config, size size: Int) -> Result(Pool, Error) {
  // A failed initialiser can only return a string; the typed error comes
  // back on this subject.
  let errors = process.new_subject()
  case builder(config, size, Some(errors)) |> actor.start {
    Ok(started) -> Ok(started.data)
    Error(actor.InitFailed(reason)) ->
      case process.receive(errors, 0) {
        Ok(e) -> Error(e)
        Error(Nil) -> Error(error.ConnectionError(reason))
      }
    Error(e) -> Error(start_error(e))
  }
}

/// A child specification for running the pool under a supervisor, registered
/// under `name`. Get a handle with `named(name)`.
///
/// ```gleam
/// let name = process.new_name("db")
/// let assert Ok(_) =
///   static_supervisor.new(static_supervisor.OneForOne)
///   |> static_supervisor.add(pool.supervised(cfg, 10, name))
///   |> static_supervisor.start
/// let db = pool.named(name)
/// ```
pub fn supervised(
  config: Config,
  size: Int,
  name: Name(PoolMessage),
) -> supervision.ChildSpecification(Pool) {
  supervision.worker(fn() {
    builder(config, size, None)
    |> actor.named(name)
    |> actor.start
  })
}

/// A handle to the pool registered under `name` by `supervised`.
pub fn named(name: Name(PoolMessage)) -> Pool {
  Pool(process.named_subject(name))
}

fn builder(
  config: Config,
  size: Int,
  errors: Option(Subject(Error)),
) -> actor.Builder(State, PoolMessage, Pool) {
  let init_timeout = size * { config.connect_timeout + 1000 } + 1000
  actor.new_with_initialiser(init_timeout, fn(subject) {
    use config <- result.try(
      config.validate(config)
      |> result.map_error(fn(reason) {
        report(errors, error.ConnectionError(reason))
      }),
    )
    use <- bool.lazy_guard(size < 1, fn() {
      Error(report(
        errors,
        error.ConnectionError("Pool size must be at least 1"),
      ))
    })
    // Connections are linked to the pool; trap their exits so one crashing
    // connection is replaced instead of taking the pool down.
    process.trap_exits(True)
    use slots <- result.try(
      open_slots(config, size, 0, dict.new())
      |> result.map_error(report(errors, _)),
    )
    // Internal messages (timers, reset callbacks) use a pid-based subject:
    // sending to a named subject panics once the name is unregistered.
    let internal = process.new_subject()
    let state =
      State(
        config:,
        size:,
        self: internal,
        slots:,
        waiters: [],
        next_id: size,
        missing: 0,
        last_sweep: now() - sweep_interval,
      )
    process.send_after(internal, config.idle_interval, PingIdle)
    let selector =
      process.new_selector()
      |> process.select(subject)
      |> process.select(internal)
      |> process.select_monitors(CallerDown)
      |> process.select_trapped_exits(ConnectionExit)
    actor.initialised(state)
    |> actor.selecting(selector)
    |> actor.returning(Pool(subject))
    |> Ok
  })
  |> actor.on_message(handle_message)
}

/// Hand a start failure to `start` (if it is waiting) and describe it for
/// the actor API.
fn report(errors: Option(Subject(Error)), e: Error) -> String {
  case errors {
    Some(errors) -> process.send(errors, e)
    None -> Nil
  }
  error.to_string(e)
}

fn open_slots(
  config: Config,
  remaining: Int,
  id: Int,
  slots: Dict(Int, Slot),
) -> Result(Dict(Int, Slot), Error) {
  case remaining {
    0 -> Ok(slots)
    _ ->
      case connection_actor.start(config) {
        Ok(started) -> {
          let slot = new_slot(started, config)
          open_slots(
            config,
            remaining - 1,
            id + 1,
            dict.insert(slots, id, slot),
          )
        }
        Error(e) -> {
          dict.each(slots, fn(_, slot) { stop_connection(slot) })
          Error(e)
        }
      }
  }
}

fn new_slot(
  started: actor.Started(Subject(connection_actor.Message)),
  config: Config,
) -> Slot {
  Slot(
    conn: postgleam.from_actor(started.data, started.pid, config),
    pid: started.pid,
    status: Idle(since: now()),
    stale: False,
  )
}

fn handle_message(
  state: State,
  msg: PoolMessage,
) -> actor.Next(State, PoolMessage) {
  case msg {
    Checkout(caller, reply) -> {
      let monitor = process.monitor(caller)
      let waiter =
        Waiter(
          caller:,
          monitor:,
          reply:,
          deadline: now() + state.config.queue_timeout,
        )
      case take_idle(state) {
        Ok(#(id, slot)) -> actor.continue(grant(state, id, slot, waiter))
        Error(Nil) -> {
          process.send_after(
            state.self,
            state.config.queue_timeout,
            ExpireWaiters,
          )
          actor.continue(
            State(..state, waiters: list.append(state.waiters, [waiter])),
          )
        }
      }
    }

    Checkin(id) ->
      case dict.get(state.slots, id) {
        Ok(Slot(status: Busy(monitor), ..) as slot) -> {
          process.demonitor_process(monitor)
          actor.continue(begin_reset(state, id, slot))
        }
        // Unknown id (the connection crashed meanwhile) or a double
        // check-in: nothing to do.
        _ -> actor.continue(state)
      }

    ResetDone(id, healthy) ->
      case dict.get(state.slots, id) {
        Ok(Slot(status: Resetting, stale: True, ..) as slot) ->
          actor.continue(recycle(state, id, slot))
        Ok(Slot(status: Resetting, ..) as slot) -> {
          let state = release(state, id, slot)
          // The last user lost the connection: others may be gone too.
          actor.continue(case healthy {
            True -> state
            False -> sweep(state)
          })
        }
        _ -> actor.continue(state)
      }

    CheckDone(id, healthy, from_sweep) ->
      case dict.get(state.slots, id) {
        Ok(Slot(status: Checking, stale: True, ..) as slot) ->
          actor.continue(recycle(state, id, slot))
        Ok(Slot(status: Checking, ..) as slot) -> {
          let state = release(state, id, slot)
          actor.continue(case healthy || from_sweep {
            True -> state
            False -> sweep(state)
          })
        }
        _ -> actor.continue(state)
      }

    ReconnectAll(reply) -> {
      process.send(reply, Ok(Nil))
      actor.continue(reconnect_all_slots(state))
    }

    CallerDown(process.ProcessDown(monitor:, ..)) ->
      actor.continue(caller_down(state, monitor))
    CallerDown(process.PortDown(..)) -> actor.continue(state)

    ConnectionExit(process.ExitMessage(pid, reason)) ->
      case find_slot_by_pid(state, pid) {
        Ok(id) -> actor.continue(replace_connection(state, id))
        Error(Nil) ->
          case reason {
            // A linked process (such as the one that started the pool)
            // finished normally: links ignore that, so do we.
            process.Normal -> actor.continue(state)
            _ -> {
              stop_all(state)
              actor.stop_abnormal("pool: linked process exited")
            }
          }
      }

    ExpireWaiters -> actor.continue(expire_waiters(state))

    PingIdle -> {
      let cutoff = now() - state.config.idle_interval
      let state =
        check_idle(state, fn(since) { since <= cutoff }, ping(state, _, False))
      process.send_after(state.self, state.config.idle_interval, PingIdle)
      actor.continue(state)
    }

    Replenish -> actor.continue(replenish(state))

    GetStats(reply) -> {
      let count = fn(pred) {
        dict.values(state.slots) |> list.count(fn(slot) { pred(slot.status) })
      }
      process.send(
        reply,
        Ok(Stats(
          size: state.size,
          idle: count(is_idle),
          busy: count(fn(s) { !is_idle(s) }),
          waiting: list.length(state.waiters),
        )),
      )
      actor.continue(state)
    }

    Shutdown(reply) -> {
      stop_all(state)
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

/// Take the free connection that has been idle longest.
fn take_idle(state: State) -> Result(#(Int, Slot), Nil) {
  dict.fold(state.slots, Error(Nil), fn(best, id, slot) {
    case slot.status, best {
      Idle(since), Ok(#(_, Slot(status: Idle(best_since), ..)))
        if since < best_since
      -> Ok(#(id, slot))
      Idle(_), Error(Nil) -> Ok(#(id, slot))
      _, _ -> best
    }
  })
}

/// Mark the idle connections whose idle-since time passes `pick` as
/// `Checking` and run `check` on each, in parallel (each connection is its
/// own actor). `check` must make the connection report back with
/// `CheckDone`.
fn check_idle(
  state: State,
  pick: fn(Int) -> Bool,
  check: fn(#(Int, Slot)) -> Nil,
) -> State {
  let picked =
    dict.to_list(state.slots)
    |> list.filter(fn(entry) {
      case { entry.1 }.status {
        Idle(since) -> pick(since)
        _ -> False
      }
    })
  list.each(picked, check)
  let slots =
    list.fold(picked, state.slots, fn(slots, entry) {
      dict.insert(slots, entry.0, Slot(..entry.1, status: Checking))
    })
  State(..state, slots:)
}

fn ping(state: State, entry: #(Int, Slot), from_sweep: Bool) -> Nil {
  let pool = state.self
  let id = entry.0
  process.send(
    postgleam.subject({ entry.1 }.conn),
    connection_actor.Ping(fn(healthy) {
      process.send(pool, CheckDone(id, healthy, from_sweep))
    }),
  )
}

fn send_recycle(state: State, id: Int, slot: Slot) -> Nil {
  let pool = state.self
  process.send(
    postgleam.subject(slot.conn),
    connection_actor.Recycle(fn(healthy) {
      process.send(pool, CheckDone(id, healthy, True))
    }),
  )
}

/// A connection was found broken. Ping every idle connection now instead
/// of waiting for each to fail a query. Failures found by the sweep do not
/// start another sweep.
fn sweep(state: State) -> State {
  let now = now()
  use <- bool.guard(now - state.last_sweep < sweep_interval, state)
  check_idle(State(..state, last_sweep: now), fn(_) { True }, ping(
    state,
    _,
    True,
  ))
}

/// Reconnect one connection that is free, then release it.
fn recycle(state: State, id: Int, slot: Slot) -> State {
  send_recycle(state, id, slot)
  let slot = Slot(..slot, status: Checking, stale: False)
  State(..state, slots: dict.insert(state.slots, id, slot))
}

/// Reconnect idle connections now, and the rest as soon as they are free.
fn reconnect_all_slots(state: State) -> State {
  let state =
    State(
      ..state,
      slots: dict.map_values(state.slots, fn(_, slot) {
        case slot.status {
          Idle(_) -> slot
          _ -> Slot(..slot, stale: True)
        }
      }),
    )
  check_idle(state, fn(_) { True }, fn(entry) {
    send_recycle(state, entry.0, entry.1)
  })
}

fn grant(state: State, id: Int, slot: Slot, waiter: Waiter) -> State {
  process.send(waiter.reply, Ok(#(id, slot.conn)))
  let slot = Slot(..slot, status: Busy(waiter.monitor))
  State(..state, slots: dict.insert(state.slots, id, slot))
}

/// Ask the connection to roll back whatever its last user left open; it
/// reports back with `ResetDone`.
fn begin_reset(state: State, id: Int, slot: Slot) -> State {
  use <- bool.lazy_guard(slot.stale, fn() { recycle(state, id, slot) })
  let pool = state.self
  process.send(
    postgleam.subject(slot.conn),
    connection_actor.Reset(fn(healthy) {
      process.send(pool, ResetDone(id, healthy))
    }),
  )
  let slot = Slot(..slot, status: Resetting)
  State(..state, slots: dict.insert(state.slots, id, slot))
}

/// A connection is free again: give it to the oldest waiter, if any.
fn release(state: State, id: Int, slot: Slot) -> State {
  case state.waiters {
    [waiter, ..rest] -> grant(State(..state, waiters: rest), id, slot, waiter)
    [] -> {
      let slot = Slot(..slot, status: Idle(since: now()))
      State(..state, slots: dict.insert(state.slots, id, slot))
    }
  }
}

fn caller_down(state: State, monitor: Monitor) -> State {
  let holder =
    dict.to_list(state.slots)
    |> list.find(fn(entry) {
      case entry.1.status {
        Busy(m) -> m == monitor
        _ -> False
      }
    })
  case holder {
    Ok(#(id, slot)) -> begin_reset(state, id, slot)
    Error(Nil) ->
      State(
        ..state,
        waiters: list.filter(state.waiters, fn(w) { w.monitor != monitor }),
      )
  }
}

fn expire_waiters(state: State) -> State {
  let now = now()
  let #(expired, waiting) =
    list.partition(state.waiters, fn(w) { w.deadline <= now })
  list.each(expired, fn(w) {
    process.demonitor_process(w.monitor)
    process.send(w.reply, Error(error.PoolTimeout))
  })
  State(..state, waiters: waiting)
}

fn find_slot_by_pid(state: State, pid: Pid) -> Result(Int, Nil) {
  dict.to_list(state.slots)
  |> list.find(fn(entry) { { entry.1 }.pid == pid })
  |> result.map(fn(entry) { entry.0 })
}

/// A connection actor crashed. Its holder (if any) sees errors on its next
/// call; the pool forgets it and starts a replacement.
fn replace_connection(state: State, id: Int) -> State {
  case dict.get(state.slots, id) {
    Ok(Slot(status: Busy(monitor), ..)) -> process.demonitor_process(monitor)
    _ -> Nil
  }
  State(
    ..state,
    slots: dict.delete(state.slots, id),
    missing: state.missing + 1,
  )
  |> replenish
}

/// Start replacements for crashed connections. They connect on first use,
/// so this never blocks the pool on the network.
fn replenish(state: State) -> State {
  case state.missing {
    0 -> state
    _ ->
      case connection_actor.start_lazy(state.config) {
        Ok(started) -> {
          let id = state.next_id
          let slot = new_slot(started, state.config)
          let state =
            State(
              ..state,
              slots: dict.insert(state.slots, id, slot),
              next_id: id + 1,
              missing: state.missing - 1,
            )
          replenish(release(state, id, slot))
        }
        Error(_) -> {
          process.send_after(state.self, 1000, Replenish)
          state
        }
      }
  }
}

fn stop_all(state: State) -> Nil {
  list.each(state.waiters, fn(w) {
    process.send(
      w.reply,
      Error(error.ConnectionError("The pool is shutting down")),
    )
  })
  dict.each(state.slots, fn(_, slot) { stop_connection(slot) })
}

fn stop_connection(slot: Slot) -> Nil {
  process.send(
    postgleam.subject(slot.conn),
    connection_actor.Disconnect(process.new_subject()),
  )
}

// =============================================================================
// Using the pool
// =============================================================================

/// Check out a connection, run `f` with it, and check it back in. The
/// connection must not be used after `f` returns. Anything `f` leaves open
/// (such as a transaction) is rolled back before the next caller gets the
/// connection. If the calling process exits while holding the connection,
/// the pool takes it back.
pub fn with_connection(
  pool: Pool,
  f: fn(Connection) -> Result(a, Error),
) -> Result(a, Error) {
  use #(id, conn) <- result.try(checkout(pool))
  let result = f(conn)
  process.send(pool.subject, Checkin(id))
  result
}

fn checkout(pool: Pool) -> Result(#(Int, Connection), Error) {
  // The pool always answers within queue_timeout, so there is no deadline
  // here; the monitor covers the pool going away.
  call.call(
    pool.subject,
    Checkout(process.self(), _),
    call.UntilReplyOrExit,
    Error(not_running()),
    Error(error.PoolTimeout),
  )
}

fn not_running() -> Error {
  error.ConnectionError("The pool is not running")
}

/// Run a parameterized query on a pooled connection.
pub fn query(
  pool: Pool,
  sql: String,
  params: List(Param),
) -> Result(ExtendedQueryResult, Error) {
  with_connection(pool, postgleam.query(_, sql, params))
}

/// Run a parameterized query on a pooled connection and decode each row.
pub fn query_with(
  pool: Pool,
  sql: String,
  params: List(Param),
  decoder: RowDecoder(a),
) -> Result(Response(a), Error) {
  with_connection(pool, postgleam.query_with(_, sql, params, decoder))
}

/// Run a query and decode the first row. Errors if there are no rows.
pub fn query_one(
  pool: Pool,
  sql: String,
  params: List(Param),
  decoder: RowDecoder(a),
) -> Result(a, Error) {
  with_connection(pool, postgleam.query_one(_, sql, params, decoder))
}

/// Run one statement with several parameter sets in one round trip.
pub fn query_batch(
  pool: Pool,
  sql: String,
  param_sets: List(List(Param)),
) -> Result(List(ExtendedQueryResult), Error) {
  with_connection(pool, postgleam.query_batch(_, sql, param_sets))
}

/// Run a simple (text protocol) query on a pooled connection.
pub fn simple_query(
  pool: Pool,
  sql: String,
) -> Result(List(SimpleQueryResult), Error) {
  with_connection(pool, postgleam.simple_query(_, sql))
}

/// Run `f` in a transaction on one pooled connection. See
/// `postgleam.transaction`.
pub fn transaction(
  pool: Pool,
  f: fn(Connection) -> Result(a, e),
) -> Result(a, postgleam.TransactionError(e)) {
  case checkout(pool) {
    Error(e) -> Error(postgleam.TransactionFailed(e))
    Ok(#(id, conn)) -> {
      let result = postgleam.transaction(conn, f)
      process.send(pool.subject, Checkin(id))
      result
    }
  }
}

/// Reconnect every connection in the pool: idle ones now, and ones in use
/// as soon as they are checked back in. The old sockets are closed without
/// waiting for the server, so this is quick even when they are dead.
///
/// Use it when the application knows its connections cannot have survived,
/// for example right after the machine resumed from a suspend. Returns once
/// the pool has started the reconnects; callers that check out a connection
/// meanwhile get one that has been reconnected.
pub fn reconnect_all(pool: Pool) -> Result(Nil, Error) {
  call.call(
    pool.subject,
    ReconnectAll,
    call.UntilReplyOrExit,
    Error(not_running()),
    Error(error.PoolTimeout),
  )
}

/// Stop the pool and close all its connections. Callers still holding a
/// connection get errors on their next call.
pub fn shutdown(pool: Pool) -> Nil {
  call.call(pool.subject, Shutdown, call.Within(10_000), Nil, Nil)
}

/// A snapshot of the pool, for monitoring.
pub type Stats {
  Stats(
    /// Configured number of connections.
    size: Int,
    /// Connections free right now.
    idle: Int,
    /// Connections checked out, being reset after check-in, or being
    /// pinged or reconnected.
    busy: Int,
    /// Callers waiting for a connection.
    waiting: Int,
  )
}

/// Get a snapshot of the pool's state.
pub fn stats(pool: Pool) -> Result(Stats, Error) {
  call.call(
    pool.subject,
    GetStats,
    call.UntilReplyOrExit,
    Error(not_running()),
    Error(error.PoolTimeout),
  )
}

fn is_idle(status: SlotStatus) -> Bool {
  case status {
    Idle(_) -> True
    _ -> False
  }
}

// =============================================================================
// Helpers
// =============================================================================

fn start_error(e: actor.StartError) -> Error {
  error.ConnectionError(start_error_message(e))
}

fn start_error_message(e: actor.StartError) -> String {
  case e {
    actor.InitTimeout -> "Initialization timed out"
    actor.InitFailed(reason) -> reason
    actor.InitExited(_) -> "Process exited during initialization"
  }
}

fn now() -> Int {
  monotonic_time(atom.create("millisecond"))
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: Atom) -> Int
