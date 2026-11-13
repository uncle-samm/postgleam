import gleam/erlang/atom
import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/decode
import postgleam/error
import postgleam/pool
import postgleam/value

fn test_config() {
  config.default()
  |> config.password("postgres")
  |> config.database("postgleam_test")
}

fn now_ms() -> Int {
  monotonic_time(atom.create("millisecond"))
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: atom.Atom) -> Int

pub fn pool_start_and_shutdown_test() {
  let assert Ok(p) = pool.start(test_config(), size: 2)
  pool.shutdown(p)
  // A stopped pool reports an error instead of crashing the caller.
  let assert Error(error.ConnectionError(_)) = pool.query(p, "SELECT 1", [])
}

pub fn pool_rejects_size_zero_test() {
  let assert Error(_) = pool.start(test_config(), size: 0)
}

pub fn pool_query_test() {
  let assert Ok(p) = pool.start(test_config(), size: 2)
  let assert Ok(result) = pool.query(p, "SELECT 42::int4 AS num", [])
  should.equal(result.rows, [[Some(value.Integer(42))]])
  pool.shutdown(p)
}

pub fn pool_query_with_and_one_test() {
  let assert Ok(p) = pool.start(test_config(), size: 2)
  let decoder = {
    use n <- decode.field("n", decode.int)
    decode.success(n)
  }
  let assert Ok(response) =
    pool.query_with(p, "SELECT generate_series(1, 3) AS n", [], decoder)
  should.equal(response.rows, [1, 2, 3])
  let assert Ok(41) =
    pool.query_one(p, "SELECT $1::int4 - 1 AS n", [postgleam.int(42)], decoder)
  pool.shutdown(p)
}

pub fn pool_simple_query_test() {
  let assert Ok(p) = pool.start(test_config(), size: 2)
  let assert Ok([result]) = pool.simple_query(p, "SELECT 1 AS num")
  should.equal(result.tag, "SELECT 1")
  pool.shutdown(p)
}

pub fn pool_query_batch_test() {
  let assert Ok(p) = pool.start(test_config(), size: 1)
  let assert Ok(results) =
    pool.query_batch(p, "SELECT $1::int4", [
      [postgleam.int(1)],
      [postgleam.int(2)],
    ])
  should.equal(list.map(results, fn(r) { r.rows }), [
    [[Some(value.Integer(1))]],
    [[Some(value.Integer(2))]],
  ])
  pool.shutdown(p)
}

pub fn pool_error_recovery_test() {
  let assert Ok(p) = pool.start(test_config(), size: 1)
  let assert Error(error.PgError(..)) =
    pool.query(p, "SELECT * FROM nonexistent_xyz", [])
  let assert Ok(result) = pool.query(p, "SELECT 1::int4 AS val", [])
  should.equal(result.rows, [[Some(value.Integer(1))]])
  pool.shutdown(p)
}

pub fn pool_wrong_password_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("wrong_password")
  let assert Error(_) = pool.start(cfg, size: 1)
}

/// Regression: the old pool ran every query inside the pool process, one at
/// a time. Ten 200 ms queries on a pool of ten must overlap.
pub fn pool_runs_queries_concurrently_test() {
  let assert Ok(p) = pool.start(test_config(), size: 10)
  let done = process.new_subject()
  let started = now_ms()
  list.each(list.repeat(Nil, 10), fn(_) {
    process.spawn(fn() {
      process.send(done, pool.query(p, "SELECT pg_sleep(0.2)", []))
    })
  })
  list.each(list.repeat(Nil, 10), fn(_) {
    let assert Ok(Ok(_)) = process.receive(done, 5000)
  })
  let elapsed = now_ms() - started
  // Serial execution would take 2000 ms.
  should.be_true(elapsed < 1000)
  pool.shutdown(p)
}

pub fn pool_queue_timeout_test() {
  let cfg = test_config() |> config.queue_timeout(100)
  let assert Ok(p) = pool.start(cfg, size: 1)
  let holder_ready = process.new_subject()
  process.spawn(fn() {
    pool.with_connection(p, fn(_conn) {
      process.send(holder_ready, Nil)
      process.sleep(500)
      Ok(Nil)
    })
  })
  let assert Ok(Nil) = process.receive(holder_ready, 2000)
  let assert Error(error.PoolTimeout) = pool.query(p, "SELECT 1", [])
  pool.shutdown(p)
}

pub fn pool_waiter_gets_connection_when_freed_test() {
  let cfg = test_config() |> config.queue_timeout(2000)
  let assert Ok(p) = pool.start(cfg, size: 1)
  let holder_ready = process.new_subject()
  process.spawn(fn() {
    pool.with_connection(p, fn(_conn) {
      process.send(holder_ready, Nil)
      process.sleep(200)
      Ok(Nil)
    })
  })
  let assert Ok(Nil) = process.receive(holder_ready, 2000)
  let assert Ok(_) = pool.query(p, "SELECT 1", [])
  pool.shutdown(p)
}

/// Whether the next pooled connection is inside a transaction left open by
/// an earlier caller. In a simple query outside a transaction, now() is the
/// statement's start time; inside one it is the (earlier) BEGIN time.
fn in_transaction(p: pool.Pool) -> Bool {
  let assert Ok([result]) =
    pool.simple_query(p, "SELECT now() = statement_timestamp()")
  result.rows != [[Some("t")]]
}

/// A caller that dies holding a connection inside a transaction must not
/// leak the connection or the open transaction to the next caller.
pub fn pool_reclaims_connection_from_dead_caller_test() {
  let assert Ok(p) = pool.start(test_config(), size: 1)
  let ready = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      pool.with_connection(p, fn(conn) {
        let assert Ok(_) = postgleam.simple_query(conn, "BEGIN")
        process.send(ready, Nil)
        process.sleep(10_000)
        Ok(Nil)
      })
    })
  let assert Ok(Nil) = process.receive(ready, 2000)
  process.sleep(20)
  process.kill(pid)
  should.equal(in_transaction(p), False)
  // Check-in resets asynchronously; give it a moment.
  process.sleep(20)
  let assert Ok(stats) = pool.stats(p)
  should.equal(stats, pool.Stats(size: 1, idle: 1, busy: 0, waiting: 0))
  pool.shutdown(p)
}

pub fn pool_resets_open_transaction_on_checkin_test() {
  let assert Ok(p) = pool.start(test_config(), size: 1)
  let assert Ok(Nil) =
    pool.with_connection(p, fn(conn) {
      let assert Ok(_) = postgleam.simple_query(conn, "BEGIN")
      Ok(Nil)
    })
  process.sleep(20)
  should.equal(in_transaction(p), False)
  pool.shutdown(p)
}

pub fn pool_transaction_test() {
  let assert Ok(p) = pool.start(test_config(), size: 2)
  let assert Ok(_) =
    pool.simple_query(
      p,
      "DROP TABLE IF EXISTS _pool_tx; CREATE TABLE _pool_tx (n int4)",
    )
  let assert Ok(Nil) =
    pool.transaction(p, fn(conn) {
      let assert Ok(_) =
        postgleam.query(conn, "INSERT INTO _pool_tx VALUES (1)", [])
      Ok(Nil)
    })
  let assert Error(postgleam.RolledBack("nope")) =
    pool.transaction(p, fn(conn) {
      let assert Ok(_) =
        postgleam.query(conn, "INSERT INTO _pool_tx VALUES (2)", [])
      Error("nope")
    })
  let assert Ok(r) = pool.query(p, "SELECT count(*)::int4 FROM _pool_tx", [])
  should.equal(r.rows, [[Some(value.Integer(1))]])
  let assert Ok(_) = pool.simple_query(p, "DROP TABLE _pool_tx")
  pool.shutdown(p)
}

pub fn pool_health_check_test() {
  let cfg = test_config() |> config.idle_interval(100)
  let assert Ok(p) = pool.start(cfg, size: 2)
  let assert Ok(_) = pool.query(p, "SELECT 1::int4", [])
  process.sleep(350)
  let assert Ok(result) = pool.query(p, "SELECT 2::int4", [])
  should.equal(result.rows, [[Some(value.Integer(2))]])
  pool.shutdown(p)
}

/// A connection the server killed is replaced transparently: the next
/// query reconnects.
pub fn pool_recovers_from_terminated_backend_test() {
  let assert Ok(p) = pool.start(test_config(), size: 1)
  let assert Ok(r) = pool.query(p, "SELECT pg_backend_pid()", [])
  let assert [[Some(value.Integer(backend))]] = r.rows
  let assert Ok(admin) = postgleam.connect(test_config())
  let assert Ok(_) =
    postgleam.query(admin, "SELECT pg_terminate_backend($1)", [
      postgleam.int(backend),
    ])
  postgleam.disconnect(admin)
  process.sleep(100)
  // The first query may fail (the socket is dead); the next must succeed.
  let _ = pool.query(p, "SELECT 1", [])
  let assert Ok(r) = pool.query(p, "SELECT pg_backend_pid()", [])
  let assert [[Some(value.Integer(new_backend))]] = r.rows
  should.not_equal(new_backend, backend)
  pool.shutdown(p)
}

pub fn pool_pgbouncer_mode_test() {
  let cfg = test_config() |> config.pgbouncer(True)
  let assert Ok(p) = pool.start(cfg, size: 2)
  let assert Ok(result) = pool.query(p, "SELECT $1::int4", [postgleam.int(42)])
  should.equal(result.rows, [[Some(value.Integer(42))]])
  pool.shutdown(p)
}

pub fn pool_named_supervised_test() {
  let name = process.new_name("postgleam_pool_test")
  let spec = pool.supervised(test_config(), 2, name)
  let assert Ok(_) = { spec.start }()
  let p = pool.named(name)
  let assert Ok(r) = pool.query(p, "SELECT 7::int4", [])
  should.equal(r.rows, [[Some(value.Integer(7))]])
  pool.shutdown(p)
}

pub fn pool_stats_test() {
  let assert Ok(p) = pool.start(test_config(), size: 3)
  let assert Ok(stats) = pool.stats(p)
  should.equal(stats, pool.Stats(size: 3, idle: 3, busy: 0, waiting: 0))
  let assert Ok(Nil) =
    pool.with_connection(p, fn(_conn) {
      let assert Ok(inside) = pool.stats(p)
      should.equal(inside.busy, 1)
      Ok(Nil)
    })
  pool.shutdown(p)
}

@external(erlang, "postgleam_test_ffi", "rescue")
fn rescue(f: fn() -> a) -> Result(Nil, Nil)

/// Bug: a panic inside `with_connection` or `transaction` skipped the
/// check-in. A process that survived the panic (a web handler that rescues
/// crashes) kept the connection for ever, and the pool ran dry.
pub fn pool_checks_in_when_the_callback_panics_test() {
  let cfg = test_config() |> config.queue_timeout(500)
  let assert Ok(p) = pool.start(cfg, size: 2)
  list.each([1, 2, 3], fn(_) {
    let assert Error(Nil) =
      rescue(fn() { pool.with_connection(p, fn(_conn) { panic as "boom" }) })
    let assert Error(Nil) =
      rescue(fn() { pool.transaction(p, fn(_conn) { panic as "boom" }) })
  })
  // Both connections are back: one caller can hold one while it uses the
  // other.
  let assert Ok(_) =
    pool.with_connection(p, fn(_conn) { pool.query(p, "SELECT 1", []) })
  pool.shutdown(p)
}

/// A panic inside a pooled transaction rolls the transaction back.
pub fn pool_transaction_rolls_back_on_panic_test() {
  let assert Ok(p) = pool.start(test_config(), size: 1)
  let assert Ok(_) =
    pool.simple_query(
      p,
      "DROP TABLE IF EXISTS _panic_tx; CREATE TABLE _panic_tx (n int4)",
    )
  let assert Error(Nil) =
    rescue(fn() {
      pool.transaction(p, fn(conn) {
        let assert Ok(_) =
          postgleam.query(conn, "INSERT INTO _panic_tx VALUES (1)", [])
        panic as "boom"
      })
    })
  let assert Ok(r) = pool.query(p, "SELECT count(*)::int4 FROM _panic_tx", [])
  should.equal(r.rows, [[Some(value.Integer(0))]])
  let assert Ok(_) = pool.simple_query(p, "DROP TABLE _panic_tx")
  pool.shutdown(p)
}
