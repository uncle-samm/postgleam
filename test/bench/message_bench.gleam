/// Wire protocol message encode/decode benchmarks.

import bench/runner.{type BenchmarkResult}
import gleam/list
import gleam/option.{Some}
import gleamy/bench
import postgleam/message

const duration = 2000

const warmup = 100

pub fn run() -> List(BenchmarkResult) {
  let results = [
    bench_encode_parse(),
    bench_encode_bind(),
    bench_encode_execute(),
    bench_encode_simple_query(),
    bench_decode_data_row_1col(),
    bench_decode_data_row_5col(),
    bench_extract_row_values_1col(),
    bench_extract_row_values_5col(),
    bench_extract_row_values_10col(),
  ]
  list.flat_map(results, fn(r) { runner.extract_results(r) })
}

// =============================================================================
// Encode benchmarks
// =============================================================================

fn bench_encode_parse() -> bench.BenchResults {
  let msg = message.Parse("", "SELECT $1::int4", [23])
  bench.run(
    [bench.Input("parse_1param", msg)],
    [bench.Function("encode_frontend", fn(m) { message.encode_frontend(m) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_encode_bind() -> bench.BenchResults {
  let msg =
    message.Bind(
      "",
      "",
      [message.BinaryFormat],
      [Some(<<0, 0, 0, 42>>)],
      [message.BinaryFormat],
    )
  bench.run(
    [bench.Input("bind_1param", msg)],
    [bench.Function("encode_frontend", fn(m) { message.encode_frontend(m) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_encode_execute() -> bench.BenchResults {
  let msg = message.Execute("", 0)
  bench.run(
    [bench.Input("execute", msg)],
    [bench.Function("encode_frontend", fn(m) { message.encode_frontend(m) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_encode_simple_query() -> bench.BenchResults {
  let msg = message.SimpleQuery("SELECT 1")
  bench.run(
    [bench.Input("simple_query", msg)],
    [bench.Function("encode_frontend", fn(m) { message.encode_frontend(m) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

// =============================================================================
// Decode benchmarks
// =============================================================================

fn bench_decode_data_row_1col() -> bench.BenchResults {
  // DataRow with 1 int4 column: type='D' (0x44), length includes itself
  // Payload: num_cols(2) + col_len(4) + data(4) = 10, length = 10 + 4 = 14
  let wire = <<0x44, 14:32-big, 1:16-big, 4:32-big, 0, 0, 0, 42>>
  bench.run(
    [bench.Input("1_col_int4", wire)],
    [bench.Function("decode_backend", fn(b) { message.decode_backend(b) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_decode_data_row_5col() -> bench.BenchResults {
  // DataRow with 5 int4 columns
  // Payload: num_cols:16 + 5 * (col_len:32 + 4 bytes) = 2 + 5*8 = 42
  // length = 42 + 4 = 46
  let wire = <<
    0x44, 46:32-big, 5:16-big,
    // Col 1
    4:32-big, 0, 0, 0, 1,
    // Col 2
    4:32-big, 0, 0, 0, 2,
    // Col 3
    4:32-big, 0, 0, 0, 3,
    // Col 4
    4:32-big, 0, 0, 0, 4,
    // Col 5
    4:32-big, 0, 0, 0, 5,
  >>
  bench.run(
    [bench.Input("5_col_int4", wire)],
    [bench.Function("decode_backend", fn(b) { message.decode_backend(b) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_extract_row_values_1col() -> bench.BenchResults {
  // Just the column data portion (after num_cols)
  let payload = <<4:32-big, 0, 0, 0, 42>>
  bench.run(
    [bench.Input("1_col", payload)],
    [
      bench.Function("extract_row_values", fn(p) {
        message.extract_row_values(p)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_extract_row_values_5col() -> bench.BenchResults {
  let payload = <<
    4:32-big, 0, 0, 0, 1,
    4:32-big, 0, 0, 0, 2,
    4:32-big, 0, 0, 0, 3,
    4:32-big, 0, 0, 0, 4,
    4:32-big, 0, 0, 0, 5,
  >>
  bench.run(
    [bench.Input("5_col", payload)],
    [
      bench.Function("extract_row_values", fn(p) {
        message.extract_row_values(p)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_extract_row_values_10col() -> bench.BenchResults {
  // 10 columns with NULL interspersed (NULL = 0xFFFFFFFF = -1 as int32)
  let payload = <<
    4:32-big, 0, 0, 0, 1,
    4:32-big, 0, 0, 0, 2,
    255, 255, 255, 255,
    4:32-big, 0, 0, 0, 4,
    4:32-big, 0, 0, 0, 5,
    255, 255, 255, 255,
    4:32-big, 0, 0, 0, 7,
    4:32-big, 0, 0, 0, 8,
    4:32-big, 0, 0, 0, 9,
    4:32-big, 0, 0, 0, 10,
  >>
  bench.run(
    [bench.Input("10_col_mixed", payload)],
    [
      bench.Function("extract_row_values", fn(p) {
        message.extract_row_values(p)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}
