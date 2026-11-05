//// Tests against a real PgBouncer in transaction pooling mode. They run
//// only when PgBouncer answers on localhost:6432 (database postgleam_test,
//// user postgres/postgres); otherwise each test returns early.
////
//// With few server connections shared by many clients, the backend behind
//// a client changes between transactions, which breaks anything that
//// relies on server-side state across a Sync.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/decode
import postgleam/error
import postgleam/pool
import postgleam/value

fn bouncer_config() {
  config.default()
  |> config.host("127.0.0.1")
  |> config.port(6432)
  |> config.password("postgres")
  |> config.database("postgleam_test")
  |> config.pgbouncer(True)
}

fn with_bouncer(test_body: fn() -> Nil) -> Nil {
  case postgleam.connect(bouncer_config() |> config.connect_timeout(500)) {
    Ok(conn) -> {
      postgleam.disconnect(conn)
      test_body()
    }
    Error(_) -> Nil
  }
}

pub fn pgbouncer_concurrent_mixed_workload_test() {
  use <- with_bouncer
  let assert Ok(p) = pool.start(bouncer_config(), size: 8)
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }
  let done = process.new_subject()
  list.each(int.range(0, 8, [], fn(acc, i) { [i, ..acc] }), fn(worker) {
    process.spawn(fn() {
      list.each(int.range(1, 51, [], fn(acc, i) { [i, ..acc] }), fn(i) {
        let n = worker * 1000 + i
        let assert Ok(r) =
          pool.query(p, "SELECT $1::int4 + 1", [postgleam.int(n)])
        should.equal(r.rows, [[Some(value.Integer(n + 1))]])
        let assert Ok(rs) =
          pool.query_batch(p, "SELECT $1::int4", [
            [postgleam.int(n)],
            [postgleam.int(n + 1)],
          ])
        should.equal(list.length(rs), 2)
        let assert Error(error.PgError(..)) = pool.query(p, "SELEC 1", [])
        let assert Ok(sum) =
          pool.with_connection(p, fn(conn) {
            postgleam.query_fold(
              conn,
              "SELECT generate_series(1, $1::int4)",
              [postgleam.int(10)],
              3,
              decoder,
              0,
              int.add,
            )
          })
        should.equal(sum, 55)
        let assert Ok(Nil) =
          pool.transaction(p, fn(conn) {
            let assert Ok(_) = postgleam.query(conn, "SELECT 1", [])
            Ok(Nil)
          })
      })
      process.send(done, Nil)
    })
  })
  list.each(int.range(0, 8, [], fn(acc, i) { [i, ..acc] }), fn(_) {
    let assert Ok(Nil) = process.receive(done, 60_000)
  })
  pool.shutdown(p)
}
