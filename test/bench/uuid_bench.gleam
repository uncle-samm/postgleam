/// UUID string parsing/formatting benchmarks — pure Gleam hot paths.

import bench/runner.{type BenchmarkResult}
import gleam/list
import gleamy/bench
import postgleam/value

const duration = 2000

const warmup = 100

pub fn run() -> List(BenchmarkResult) {
  let results = [
    bench_from_string_hyphenated(),
    bench_from_string_no_dashes(),
    bench_to_string(),
    bench_roundtrip(),
  ]
  list.flat_map(results, fn(r) { runner.extract_results(r) })
}

fn bench_from_string_hyphenated() -> bench.BenchResults {
  bench.run(
    [bench.Input("hyphenated", "550e8400-e29b-41d4-a716-446655440000")],
    [bench.Function("uuid_from_string", fn(s) { value.uuid_from_string(s) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_from_string_no_dashes() -> bench.BenchResults {
  bench.run(
    [bench.Input("no_dashes", "550e8400e29b41d4a716446655440000")],
    [bench.Function("uuid_from_string", fn(s) { value.uuid_from_string(s) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_to_string() -> bench.BenchResults {
  let uuid_val =
    value.Uuid(<<0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7, 0x16,
      0x44, 0x66, 0x55, 0x44, 0x00, 0x00>>)
  bench.run(
    [bench.Input("16_bytes", uuid_val)],
    [bench.Function("uuid_to_string", fn(v) { value.uuid_to_string(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_roundtrip() -> bench.BenchResults {
  bench.run(
    [bench.Input("hyphenated", "550e8400-e29b-41d4-a716-446655440000")],
    [
      bench.Function("uuid_roundtrip", fn(s) {
        let assert Ok(v) = value.uuid_from_string(s)
        value.uuid_to_string(v)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}
