//// Queries per second for the common ways of using postgleam, sequential
//// and concurrent. Every query is `SELECT $1::int4` (or `SELECT 1` for
//// simple_query), so this measures driver and round-trip overhead, not
//// PostgreSQL's work.
////
//// Run with `make bench-throughput`.

import gleam/erlang/atom
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import postgleam
import postgleam/config.{type Config}
import postgleam/pool

const queries = 20_000

const callers = 10

pub fn run(cfg: Config) -> Nil {
  io.println(
    "Throughput: "
    <> int.to_string(queries)
    <> " queries per scenario (SELECT $1::int4)",
  )
  io.println("")
  io.println(string.pad_end("Scenario", 44, " ") <> "queries/s   us/query")
  io.println(string.repeat("-", 66))

  let assert Ok(conn) = postgleam.connect(cfg)
  warm(conn)
  measure("Connection.query (cached statement)", fn() {
    repeat(queries, fn() { postgleam.query(conn, "SELECT $1::int4", [one()]) })
  })
  measure("Connection.simple_query", fn() {
    repeat(queries, fn() { postgleam.simple_query(conn, "SELECT 1") })
  })
  postgleam.disconnect(conn)

  let assert Ok(bouncer) = postgleam.connect(cfg |> config.pgbouncer(True))
  measure("Connection.query, pgbouncer mode", fn() {
    repeat(queries, fn() {
      postgleam.query(bouncer, "SELECT $1::int4", [one()])
    })
  })
  postgleam.disconnect(bouncer)

  let assert Ok(p) = pool.start(cfg, size: callers)
  measure("pool.query, 1 caller", fn() {
    repeat(queries, fn() { pool.query(p, "SELECT $1::int4", [one()]) })
  })
  measure("pool.query, 10 callers, pool of 10", fn() {
    concurrently(fn() {
      repeat(queries / callers, fn() {
        pool.query(p, "SELECT $1::int4", [one()])
      })
    })
  })
  pool.shutdown(p)

  measure("10 callers, own Connection each", fn() {
    concurrently(fn() {
      let assert Ok(c) = postgleam.connect(cfg)
      repeat(queries / callers, fn() {
        postgleam.query(c, "SELECT $1::int4", [one()])
      })
      postgleam.disconnect(c)
    })
  })
  io.println("")
}

fn one() {
  postgleam.int(1)
}

fn warm(conn) {
  let assert Ok(_) = postgleam.query(conn, "SELECT $1::int4", [one()])
  Nil
}

fn measure(label: String, f: fn() -> Nil) -> Nil {
  let started = now_us()
  f()
  let elapsed = int.max(now_us() - started, 1)
  io.println(
    string.pad_end(label, 44, " ")
    <> string.pad_start(int.to_string(queries * 1_000_000 / elapsed), 9, " ")
    <> string.pad_start(
      int.to_string(elapsed * 10 / queries)
        |> fn(s) { string.drop_end(s, 1) <> "." <> string.slice(s, -1, 1) },
      11,
      " ",
    ),
  )
}

fn repeat(n: Int, f: fn() -> Result(a, e)) -> Nil {
  case n {
    0 -> Nil
    _ -> {
      let assert Ok(_) = f()
      repeat(n - 1, f)
    }
  }
}

fn concurrently(f: fn() -> Nil) -> Nil {
  let done = process.new_subject()
  list.each(list.repeat(Nil, callers), fn(_) {
    process.spawn(fn() {
      f()
      process.send(done, Nil)
    })
  })
  list.each(list.repeat(Nil, callers), fn(_) {
    let assert Ok(Nil) = process.receive(done, 600_000)
  })
}

fn now_us() -> Int {
  monotonic_time(atom.create("microsecond"))
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: atom.Atom) -> Int
