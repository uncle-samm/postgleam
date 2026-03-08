/// Integration benchmarks — live database operations (requires Docker PostgreSQL).

import bench/runner.{type BenchmarkResult}
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleamy/bench
import postgleam
import postgleam/config
import postgleam/decode
import postgleam/pool
import postgleam/value

const duration = 5000

const warmup = 50

pub fn run() -> List(BenchmarkResult) {
  let cfg = config.default() |> config.database("postgleam_test")

  // Connect for the main benchmarks
  let assert Ok(conn) = postgleam.connect(cfg)

  // Setup: create temp table for insert benchmarks
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TEMP TABLE IF NOT EXISTS bench_insert (id serial PRIMARY KEY, val int4, name text)",
    )

  io.println("Running integration benchmarks...")

  let results = [
    bench_connect_disconnect(cfg),
    bench_simple_query_select_1(conn),
    bench_query_select_param(conn),
    bench_query_1000_rows(conn),
    bench_single_insert(conn),
    bench_query_with_decoder(conn),
    bench_prepare_execute_close(conn),
    bench_transaction(conn),
    bench_batch_insert_100(conn),
    bench_pool_throughput(cfg),
  ]

  postgleam.disconnect(conn)

  list.flat_map(results, fn(r) { runner.extract_results(r) })
}

fn bench_connect_disconnect(cfg: config.Config) -> bench.BenchResults {
  bench.run(
    [bench.Input("tcp", cfg)],
    [
      bench.Function("connect+disconnect", fn(c) {
        let assert Ok(conn) = postgleam.connect(c)
        postgleam.disconnect(conn)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_simple_query_select_1(
  conn: postgleam.Connection,
) -> bench.BenchResults {
  bench.run(
    [bench.Input("select_1", conn)],
    [
      bench.Function("simple_query", fn(c) {
        let assert Ok(_) = postgleam.simple_query(c, "SELECT 1")
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_query_select_param(conn: postgleam.Connection) -> bench.BenchResults {
  bench.run(
    [bench.Input("select_$1", conn)],
    [
      bench.Function("query", fn(c) {
        let assert Ok(_) =
          postgleam.query(c, "SELECT $1::int4", [postgleam.int(42)])
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_query_1000_rows(conn: postgleam.Connection) -> bench.BenchResults {
  bench.run(
    [bench.Input("1000_rows", conn)],
    [
      bench.Function("query", fn(c) {
        let assert Ok(_) =
          postgleam.query(
            c,
            "SELECT generate_series(1,1000)::int4",
            [],
          )
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_single_insert(conn: postgleam.Connection) -> bench.BenchResults {
  bench.run(
    [bench.Input("1_row", conn)],
    [
      bench.Function("insert", fn(c) {
        let assert Ok(_) =
          postgleam.query(
            c,
            "INSERT INTO bench_insert (val, name) VALUES ($1, $2)",
            [postgleam.int(42), postgleam.text("bench")],
          )
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_query_with_decoder(conn: postgleam.Connection) -> bench.BenchResults {
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }
  bench.run(
    [bench.Input("100_rows", conn)],
    [
      bench.Function("query_with", fn(c) {
        let assert Ok(_) =
          postgleam.query_with(
            c,
            "SELECT generate_series(1,100)::int4",
            [],
            decoder,
          )
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_prepare_execute_close(
  conn: postgleam.Connection,
) -> bench.BenchResults {
  bench.run(
    [bench.Input("lifecycle", conn)],
    [
      bench.Function("prepare+execute+close", fn(c) {
        let assert Ok(stmt) =
          postgleam.prepare(c, "bench_stmt", "SELECT $1::int4")
        let assert Ok(_) =
          postgleam.execute(c, stmt, [Some(value.Integer(1))])
        let assert Ok(_) = postgleam.close(c, "bench_stmt")
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_transaction(conn: postgleam.Connection) -> bench.BenchResults {
  bench.run(
    [bench.Input("select_1", conn)],
    [
      bench.Function("transaction", fn(c) {
        let assert Ok(_) =
          postgleam.transaction(c, fn(tc) {
            let assert Ok(_) =
              postgleam.query(tc, "SELECT $1::int4", [postgleam.int(1)])
            Ok(Nil)
          })
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_batch_insert_100(conn: postgleam.Connection) -> bench.BenchResults {
  bench.run(
    [bench.Input("100_rows", conn)],
    [
      bench.Function("batch_insert_tx", fn(c) {
        let assert Ok(_) =
          postgleam.transaction(c, fn(tc) {
            int.range(1, 100, Nil, fn(_, i) {
              let assert Ok(_) =
                postgleam.query(
                  tc,
                  "INSERT INTO bench_insert (val, name) VALUES ($1, $2)",
                  [postgleam.int(i), postgleam.text("batch")],
                )
              Nil
            })
            Ok(Nil)
          })
        Nil
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_pool_throughput(cfg: config.Config) -> bench.BenchResults {
  let assert Ok(started) = pool.start(cfg, 5)
  let pool_subject = started.data

  let results =
    bench.run(
      [bench.Input("10_queries", pool_subject)],
      [
        bench.Function("pool.query", fn(p) {
          int.range(1, 10, Nil, fn(_, _) {
            let assert Ok(_) =
              pool.query(p, "SELECT $1::int4", [Some(value.Integer(1))], 5000)
            Nil
          })
        }),
      ],
      [bench.Duration(duration), bench.Warmup(warmup)],
    )

  pool.shutdown(pool_subject, 5000)
  results
}
