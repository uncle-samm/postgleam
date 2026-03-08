/// Codec registry benchmarks — Dict lookup is on every query's hot path.

import bench/runner.{type BenchmarkResult}
import gleam/list
import gleamy/bench
import postgleam/codec/defaults
import postgleam/codec/registry

const duration = 2000

const warmup = 100

pub fn run() -> List(BenchmarkResult) {
  let reg = registry.build(defaults.matchers())
  let results = [
    bench_lookup_int4(reg),
    bench_lookup_text(reg),
    bench_lookup_uuid(reg),
    bench_lookup_numeric(reg),
    bench_lookup_miss(reg),
    bench_build_registry(),
  ]
  list.flat_map(results, fn(r) { runner.extract_results(r) })
}

fn bench_lookup_int4(reg: registry.Registry) -> bench.BenchResults {
  bench.run(
    [bench.Input("int4_oid_23", reg)],
    [bench.Function("registry.lookup", fn(r) { registry.lookup(r, 23) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_lookup_text(reg: registry.Registry) -> bench.BenchResults {
  bench.run(
    [bench.Input("text_oid_25", reg)],
    [bench.Function("registry.lookup", fn(r) { registry.lookup(r, 25) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_lookup_uuid(reg: registry.Registry) -> bench.BenchResults {
  bench.run(
    [bench.Input("uuid_oid_2950", reg)],
    [bench.Function("registry.lookup", fn(r) { registry.lookup(r, 2950) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_lookup_numeric(reg: registry.Registry) -> bench.BenchResults {
  bench.run(
    [bench.Input("numeric_oid_1700", reg)],
    [bench.Function("registry.lookup", fn(r) { registry.lookup(r, 1700) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_lookup_miss(reg: registry.Registry) -> bench.BenchResults {
  bench.run(
    [bench.Input("miss_oid_99999", reg)],
    [bench.Function("registry.lookup", fn(r) { registry.lookup(r, 99_999) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_build_registry() -> bench.BenchResults {
  let matchers = defaults.matchers()
  bench.run(
    [bench.Input("24_matchers", matchers)],
    [bench.Function("registry.build", fn(m) { registry.build(m) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}
