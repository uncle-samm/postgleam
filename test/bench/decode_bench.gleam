/// Row decoder combinator benchmarks — measures overhead of the use-based decoder chain.

import bench/runner.{type BenchmarkResult}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleamy/bench
import postgleam/decode
import postgleam/value.{type Value}

const duration = 2000

const warmup = 100

pub fn run() -> List(BenchmarkResult) {
  let results = [
    bench_decode_1_element(),
    bench_decode_3_elements(),
    bench_decode_5_elements(),
    bench_decode_10_elements(),
    bench_decode_with_optional(),
  ]
  list.flat_map(results, fn(r) { runner.extract_results(r) })
}

fn bench_decode_1_element() -> bench.BenchResults {
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }
  let row = [Some(value.Integer(42))]
  bench.run(
    [bench.Input("1_elem", row)],
    [
      bench.Function("decode.run", fn(r: List(Option(Value))) {
        decode.run(decoder, r)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_decode_3_elements() -> bench.BenchResults {
  let decoder = {
    use a <- decode.element(0, decode.int)
    use b <- decode.element(1, decode.text)
    use c <- decode.element(2, decode.bool)
    decode.success(#(a, b, c))
  }
  let row = [
    Some(value.Integer(1)),
    Some(value.Text("hello")),
    Some(value.Boolean(True)),
  ]
  bench.run(
    [bench.Input("3_elem", row)],
    [
      bench.Function("decode.run", fn(r: List(Option(Value))) {
        decode.run(decoder, r)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_decode_5_elements() -> bench.BenchResults {
  let decoder = {
    use a <- decode.element(0, decode.int)
    use b <- decode.element(1, decode.text)
    use c <- decode.element(2, decode.bool)
    use d <- decode.element(3, decode.float)
    use e <- decode.element(4, decode.int)
    decode.success(#(a, b, c, d, e))
  }
  let row = [
    Some(value.Integer(1)),
    Some(value.Text("hello")),
    Some(value.Boolean(True)),
    Some(value.Float(3.14)),
    Some(value.Integer(99)),
  ]
  bench.run(
    [bench.Input("5_elem", row)],
    [
      bench.Function("decode.run", fn(r: List(Option(Value))) {
        decode.run(decoder, r)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_decode_10_elements() -> bench.BenchResults {
  let decoder = {
    use a <- decode.element(0, decode.int)
    use b <- decode.element(1, decode.text)
    use c <- decode.element(2, decode.bool)
    use d <- decode.element(3, decode.float)
    use e <- decode.element(4, decode.int)
    use f <- decode.element(5, decode.text)
    use g <- decode.element(6, decode.bool)
    use h <- decode.element(7, decode.float)
    use i <- decode.element(8, decode.int)
    use j <- decode.element(9, decode.text)
    decode.success(#(a, b, c, d, e, f, g, h, i, j))
  }
  let row = [
    Some(value.Integer(1)),
    Some(value.Text("hello")),
    Some(value.Boolean(True)),
    Some(value.Float(3.14)),
    Some(value.Integer(99)),
    Some(value.Text("world")),
    Some(value.Boolean(False)),
    Some(value.Float(2.71)),
    Some(value.Integer(7)),
    Some(value.Text("gleam")),
  ]
  bench.run(
    [bench.Input("10_elem", row)],
    [
      bench.Function("decode.run", fn(r: List(Option(Value))) {
        decode.run(decoder, r)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_decode_with_optional() -> bench.BenchResults {
  let decoder = {
    use a <- decode.element(0, decode.int)
    use b <- decode.element(1, decode.optional(decode.text))
    use c <- decode.element(2, decode.optional(decode.int))
    decode.success(#(a, b, c))
  }
  let row = [Some(value.Integer(1)), Some(value.Text("hi")), None]
  bench.run(
    [bench.Input("3_elem_optional", row)],
    [
      bench.Function("decode.run", fn(r: List(Option(Value))) {
        decode.run(decoder, r)
      }),
    ],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}
