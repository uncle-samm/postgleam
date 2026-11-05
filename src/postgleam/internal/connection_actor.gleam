//// OTP actor that owns one PostgreSQL connection, runs requests one at a
//// time, caches prepared statements, and recovers from broken connections.
////
//// Recovery rules:
//// - A server error (`PgError`), encode or decode error leaves the
////   connection synchronised; the actor keeps using it.
//// - A timeout, socket or protocol error leaves the connection in an unknown
////   state. The actor cancels the running query (on timeout), closes the
////   socket, drops the statement cache, and reconnects on the next request.
//// - If that happens inside a transaction, the transaction is gone. Rather
////   than silently running the rest of it in autocommit mode on a new
////   connection, every request fails until the caller ends the transaction
////   (`ROLLBACK`, `COMMIT`, `ABORT` or `END` as a simple query, or a pool
////   reset). `COMMIT` then reports the failure.

import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import postgleam/codec/defaults
import postgleam/codec/registry.{type Registry}
import postgleam/config.{type Config}
import postgleam/connection.{
  type ConnectionState, type ExtendedQueryResult, type Failure,
  type PreparedStatement, Failure,
}
import postgleam/copy
import postgleam/error.{type Error}
import postgleam/message
import postgleam/stream
import postgleam/value.{type Value}

/// Messages the connection actor handles.
pub type Message {
  Query(
    sql: String,
    params: List(Option(Value)),
    reply: Subject(Result(ExtendedQueryResult, Error)),
  )
  SimpleQuery(
    sql: String,
    reply: Subject(Result(List(connection.SimpleQueryResult), Error)),
  )
  Prepare(
    name: String,
    sql: String,
    reply: Subject(Result(PreparedStatement, Error)),
  )
  ExecutePrepared(
    prepared: PreparedStatement,
    params: List(Option(Value)),
    reply: Subject(Result(ExtendedQueryResult, Error)),
  )
  BatchQuery(
    sql: String,
    param_sets: List(List(Option(Value))),
    reply: Subject(Result(List(ExtendedQueryResult), Error)),
  )
  CloseStatement(name: String, reply: Subject(Result(Nil, Error)))
  CopyIn(
    sql: String,
    data: List(BitArray),
    reply: Subject(Result(String, Error)),
  )
  CopyOut(sql: String, reply: Subject(Result(List(BitArray), Error)))
  /// Run `run` over the rows, `chunk_size` rows at a time, inside the actor.
  Fold(
    sql: String,
    run: fn(ConnectionState, PreparedStatement, Registry) ->
      Result(#(Nil, ConnectionState), Failure),
    reply: Subject(Result(Nil, Error)),
  )
  /// Pool check-in: roll back any open transaction, then call `done`
  /// (from the actor) with whether the connection is still open.
  Reset(done: fn(Bool) -> Nil)
  /// Pool health check: run `SELECT 1`, waiting at most the connect
  /// timeout (capped by the query timeout). If that fails, reconnect at
  /// once. `done` is called (from the actor) with whether the ping
  /// succeeded.
  Ping(done: fn(Bool) -> Nil)
  /// Close the connection without asking the server, then open a new one.
  /// For sockets that are presumed dead, such as after the host was
  /// suspended. `done` is called with whether the new connection is open.
  Recycle(done: fn(Bool) -> Nil)
  Disconnect(reply: Subject(Nil))
}

/// Maximum number of cached prepared statements per connection.
const max_cache_size = 256

/// Actor state.
pub type ActorState {
  ActorState(
    /// `None` while disconnected; the next request reconnects.
    conn: Option(ConnectionState),
    config: Config,
    registry: Registry,
    /// SQL -> prepared statement.
    cache: Dict(String, PreparedStatement),
    /// Cached SQL, newest first (eviction takes the last).
    cache_order: List(String),
    next_id: Int,
    /// The connection was lost inside a transaction; see the module docs.
    lost_transaction: Bool,
  )
}

/// Start the connection actor. Fails if the first connection fails, with
/// the connection error itself.
pub fn start(config: Config) -> Result(actor.Started(Subject(Message)), Error) {
  // The actor API only carries a string out of a failed initialiser, so the
  // typed error travels back on its own subject.
  let errors = process.new_subject()
  let started =
    actor.new_with_initialiser(config.connect_timeout + 1000, fn(subject) {
      case connection.connect(config) {
        Ok(conn) ->
          initial_state(config, Some(conn))
          |> actor.initialised
          |> actor.returning(subject)
          |> Ok
        Error(e) -> {
          process.send(errors, e)
          Error(error.to_string(e))
        }
      }
    })
    |> actor.on_message(handle_message)
    |> actor.start()
  case started {
    Ok(started) -> Ok(started)
    Error(actor.InitFailed(reason)) ->
      case process.receive(errors, 0) {
        Ok(e) -> Error(e)
        Error(Nil) -> Error(error.ConnectionError(reason))
      }
    Error(actor.InitTimeout) ->
      Error(error.ConnectionError("Connection initialization timed out"))
    Error(actor.InitExited(_)) ->
      Error(error.ConnectionError("Connection process exited during init"))
  }
}

/// Start the connection actor without connecting; the first request
/// connects. Used by the pool to replace a crashed connection without
/// blocking.
pub fn start_lazy(
  config: Config,
) -> Result(actor.Started(Subject(Message)), actor.StartError) {
  actor.new_with_initialiser(1000, fn(subject) {
    initial_state(config, None)
    |> actor.initialised
    |> actor.returning(subject)
    |> Ok
  })
  |> actor.on_message(handle_message)
  |> actor.start()
}

fn initial_state(config: Config, conn: Option(ConnectionState)) -> ActorState {
  ActorState(
    conn: conn,
    config: config,
    registry: defaults.build_registry(),
    cache: dict.new(),
    cache_order: [],
    next_id: 0,
    lost_transaction: False,
  )
}

fn handle_message(
  state: ActorState,
  msg: Message,
) -> actor.Next(ActorState, Message) {
  case msg {
    Query(sql, params, reply) -> {
      let #(result, state) = case state.config.pgbouncer {
        True ->
          run(state, fn(conn) {
            connection.extended_query_unnamed_f(
              conn,
              sql,
              params,
              state.registry,
              state.config.timeout,
            )
          })
        False ->
          with_statement(state, sql, True, fn(conn, prepared) {
            connection.execute_prepared_f(
              conn,
              prepared,
              params,
              state.registry,
              state.config.timeout,
            )
          })
      }
      process.send(reply, result)
      actor.continue(state)
    }

    BatchQuery(sql, param_sets, reply) -> {
      let #(result, state) = case state.config.pgbouncer {
        True ->
          run(state, fn(conn) {
            connection.extended_query_pipeline_unnamed_f(
              conn,
              sql,
              param_sets,
              state.registry,
              state.config.timeout,
            )
          })
        False ->
          with_statement(state, sql, True, fn(conn, prepared) {
            connection.execute_pipeline_f(
              conn,
              prepared,
              param_sets,
              state.registry,
              state.config.timeout,
            )
          })
      }
      process.send(reply, result)
      actor.continue(state)
    }

    Fold(sql, fold, reply) -> {
      // The unnamed statement, described without a Sync, keeps the whole
      // fold in one protocol cycle: PgBouncer keeps the same backend.
      let #(result, state) =
        run(state, fn(conn) {
          use #(prepared, conn) <- result.try(connection.describe_unnamed_f(
            conn,
            sql,
            state.config.timeout,
          ))
          fold(conn, prepared, state.registry)
        })
      process.send(reply, result)
      actor.continue(state)
    }

    SimpleQuery(sql, reply) -> {
      let #(result, state) = case state.lost_transaction, transaction_end(sql) {
        True, Some(Rollback) -> #(
          Ok([
            connection.SimpleQueryResult(tag: "ROLLBACK", columns: [], rows: []),
          ]),
          ActorState(..state, lost_transaction: False),
        )
        True, Some(Commit) -> #(
          Error(lost_transaction_error()),
          ActorState(..state, lost_transaction: False),
        )
        _, _ ->
          run(state, connection.simple_query_f(_, sql, state.config.timeout))
      }
      process.send(reply, result)
      actor.continue(state)
    }

    Prepare(name, sql, reply) -> {
      let #(result, state) =
        run(state, connection.prepare_f(_, name, sql, [], state.config.timeout))
      process.send(reply, result)
      actor.continue(state)
    }

    ExecutePrepared(prepared, params, reply) -> {
      let #(result, state) =
        run(state, fn(conn) {
          connection.execute_prepared_f(
            conn,
            prepared,
            params,
            state.registry,
            state.config.timeout,
          )
        })
      process.send(reply, result)
      actor.continue(state)
    }

    CloseStatement(name, reply) -> {
      let #(result, state) =
        run(state, fn(conn) {
          connection.close_statement_f(conn, name, state.config.timeout)
          |> result.map(fn(c) { #(Nil, c) })
        })
      process.send(reply, result)
      actor.continue(state)
    }

    CopyIn(sql, data, reply) -> {
      let #(result, state) =
        run(state, copy.copy_in_f(_, sql, data, state.config.timeout))
      process.send(reply, result)
      actor.continue(state)
    }

    CopyOut(sql, reply) -> {
      let #(result, state) =
        run(state, copy.copy_out_f(_, sql, state.config.timeout))
      process.send(reply, result)
      actor.continue(state)
    }

    Reset(done) -> {
      let state = reset(state)
      done(option.is_some(state.conn))
      actor.continue(state)
    }

    Ping(done) -> {
      let timeout = int.min(state.config.timeout, state.config.connect_timeout)
      let #(result, state) =
        run(state, connection.simple_query_f(_, "SELECT 1", timeout))
      let state = case result {
        Ok(_) -> state
        // The failed ping dropped the connection; reconnect now so the
        // next caller does not pay for it.
        Error(_) -> reconnect(state)
      }
      done(result.is_ok(result))
      actor.continue(state)
    }

    Recycle(done) -> {
      let state = case state.conn {
        Some(conn) -> {
          connection.disconnect(conn)
          ActorState(
            ..state,
            conn: None,
            cache: dict.new(),
            cache_order: [],
            lost_transaction: False,
          )
        }
        None -> ActorState(..state, lost_transaction: False)
      }
      let state = reconnect(state)
      done(option.is_some(state.conn))
      actor.continue(state)
    }

    Disconnect(reply) -> {
      case state.conn {
        Some(conn) -> connection.disconnect(conn)
        None -> Nil
      }
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

type TransactionEnd {
  Commit
  Rollback
}

/// Recognise a statement that ends a transaction.
fn transaction_end(sql: String) -> Option(TransactionEnd) {
  let words =
    sql
    |> string.trim
    |> string.replace(";", "")
    |> string.lowercase
    |> string.split(" ")
    |> list.filter(fn(w) { w != "" })
  case words {
    ["rollback", ..] | ["abort", ..] -> Some(Rollback)
    ["commit", ..] | ["end", ..] -> Some(Commit)
    _ -> None
  }
}

fn lost_transaction_error() -> Error {
  error.ConnectionError(
    "The connection was lost inside a transaction, so the transaction was rolled back",
  )
}

/// Roll back an open or failed transaction so the next user starts clean.
fn reset(state: ActorState) -> ActorState {
  let state = ActorState(..state, lost_transaction: False)
  case state.conn {
    Some(conn) ->
      case conn.transaction_status {
        message.Idle -> state
        message.InTransaction | message.FailedTransaction -> {
          let #(_, state) =
            run(state, connection.simple_query_f(
              _,
              "ROLLBACK",
              state.config.timeout,
            ))
          state
        }
      }
    None -> state
  }
}

/// Run `f` on the connection (reconnecting first if needed) and apply the
/// recovery rules to the outcome.
fn run(
  state: ActorState,
  f: fn(ConnectionState) -> Result(#(a, ConnectionState), Failure),
) -> #(Result(a, Error), ActorState) {
  use <- bool.guard(state.lost_transaction, #(
    Error(lost_transaction_error()),
    state,
  ))
  case ensure_connected(state) {
    Error(#(e, state)) -> #(Error(e), state)
    Ok(#(conn, state)) ->
      case f(conn) {
        Ok(#(value, conn)) -> #(
          Ok(value),
          ActorState(..state, conn: Some(conn)),
        )
        Error(Failure(error: e, state: Some(conn))) -> #(
          Error(e),
          ActorState(..state, conn: Some(conn)),
        )
        Error(Failure(error: e, state: None)) -> #(
          Error(e),
          drop_connection(state, conn, e),
        )
      }
  }
}

/// Open a connection if there is none. A failure leaves the actor
/// disconnected; the next request tries again and reports the error.
fn reconnect(state: ActorState) -> ActorState {
  case state.lost_transaction, ensure_connected(state) {
    // Keep the failure visible to the transaction's owner.
    True, _ -> state
    False, Ok(#(_, state)) -> state
    False, Error(#(_, state)) -> state
  }
}

fn ensure_connected(
  state: ActorState,
) -> Result(#(ConnectionState, ActorState), #(Error, ActorState)) {
  case state.conn {
    Some(conn) -> Ok(#(conn, state))
    None ->
      case connection.connect(state.config) {
        Ok(conn) -> Ok(#(conn, ActorState(..state, conn: Some(conn))))
        Error(e) -> Error(#(e, state))
      }
  }
}

/// The connection is in an unknown state: cancel whatever the server is
/// still running (after a timeout), close it and forget its statements.
fn drop_connection(
  state: ActorState,
  conn: ConnectionState,
  reason: Error,
) -> ActorState {
  case reason {
    error.TimeoutError -> {
      let _ = connection.cancel(conn, state.config)
      Nil
    }
    _ -> Nil
  }
  connection.disconnect(conn)
  ActorState(
    ..state,
    conn: None,
    cache: dict.new(),
    cache_order: [],
    lost_transaction: conn.transaction_status != message.Idle,
  )
}

/// Run `f` with a cached prepared statement for `sql`, preparing (and
/// caching) it on a miss. If the server says the cached statement is stale
/// (the table changed shape, or DISCARD/DEALLOCATE dropped it), re-prepare
/// and retry once when outside a transaction.
fn with_statement(
  state: ActorState,
  sql: String,
  retry: Bool,
  f: fn(ConnectionState, PreparedStatement) ->
    Result(#(a, ConnectionState), Failure),
) -> #(Result(a, Error), ActorState) {
  let #(prepared, state) = case dict.get(state.cache, sql) {
    Ok(prepared) -> #(Ok(prepared), state)
    Error(_) -> prepare_and_cache(state, sql)
  }
  case prepared {
    Error(e) -> #(Error(e), state)
    Ok(prepared) -> {
      let #(result, state) = run(state, f(_, prepared))
      case result, retry {
        Error(error.PgError(fields:, ..)), True ->
          case is_stale_statement(fields.code), state.conn {
            True, Some(conn) -> {
              let state = forget(state, sql)
              // Best effort: the statement may already be gone.
              let #(_, state) =
                run(state, fn(c) {
                  connection.close_statement_f(
                    c,
                    prepared.name,
                    state.config.timeout,
                  )
                  |> result.map(fn(c) { #(Nil, c) })
                })
              case conn.transaction_status {
                message.Idle -> with_statement(state, sql, False, f)
                _ -> #(result, state)
              }
            }
            _, _ -> #(result, state)
          }
        _, _ -> #(result, state)
      }
    }
  }
}

/// 0A000: "cached plan must not change result type";
/// 26000: "prepared statement does not exist".
fn is_stale_statement(code: String) -> Bool {
  code == "0A000" || code == "26000"
}

fn prepare_and_cache(
  state: ActorState,
  sql: String,
) -> #(Result(PreparedStatement, Error), ActorState) {
  let state = evict_if_needed(state)
  let name = "_pg_" <> int.to_string(state.next_id)
  let state = ActorState(..state, next_id: state.next_id + 1)
  let #(result, state) =
    run(state, connection.prepare_f(_, name, sql, [], state.config.timeout))
  case result {
    Ok(prepared) -> #(
      Ok(prepared),
      ActorState(
        ..state,
        cache: dict.insert(state.cache, sql, prepared),
        cache_order: [sql, ..state.cache_order],
      ),
    )
    Error(e) -> #(Error(e), state)
  }
}

fn forget(state: ActorState, sql: String) -> ActorState {
  ActorState(
    ..state,
    cache: dict.delete(state.cache, sql),
    cache_order: list.filter(state.cache_order, fn(s) { s != sql }),
  )
}

/// Evict (and close on the server) the oldest statement when the cache is full.
fn evict_if_needed(state: ActorState) -> ActorState {
  case dict.size(state.cache) >= max_cache_size, list.last(state.cache_order) {
    True, Ok(oldest) -> {
      let name = dict.get(state.cache, oldest) |> result.map(fn(p) { p.name })
      let state = forget(state, oldest)
      case name {
        Ok(name) -> {
          let #(_, state) =
            run(state, fn(c) {
              connection.close_statement_f(c, name, state.config.timeout)
              |> result.map(fn(c) { #(Nil, c) })
            })
          state
        }
        Error(_) -> state
      }
    }
    _, _ -> state
  }
}

/// Build the function a `Fold` message runs inside the actor. `f` gets the
/// accumulator, the column names and the row.
pub fn fold_runner(
  params: List(Option(Value)),
  chunk_size: Int,
  timeout: Int,
  initial: acc,
  f: fn(acc, List(String), List(Option(Value))) -> acc,
  out: Subject(acc),
) -> fn(ConnectionState, PreparedStatement, Registry) ->
  Result(#(Nil, ConnectionState), Failure) {
  fn(conn, prepared: PreparedStatement, registry) {
    let columns = list.map(prepared.result_fields, fn(field) { field.name })
    use #(#(acc, _tag), conn) <- result.try(
      stream.fold_f(
        conn,
        prepared,
        params,
        registry,
        chunk_size,
        timeout,
        initial,
        fn(acc, row) { f(acc, columns, row) },
      ),
    )
    process.send(out, acc)
    Ok(#(Nil, conn))
  }
}
