/// Benchmark runner entry point.
/// Invoke via: gleam build && erl -pa build/dev/erlang/*/ebin -noshell -run bench_runner main -- <args>

import bench/codec_bench
import bench/compare
import bench/decode_bench
import bench/integration_bench
import bench/message_bench
import bench/registry_bench
import bench/runner
import bench/uuid_bench
import gleam/io
import gleam/list

pub fn main() {
  let args = get_args()

  case args {
    ["pure"] -> run_pure()
    ["integration"] -> run_integration()
    ["compare", baseline, current] -> compare.run(baseline, current)
    ["all"] -> {
      run_pure()
      run_integration()
    }
    _ -> {
      io.println("Usage:")
      io.println("  make bench-pure          Run pure Gleam benchmarks")
      io.println("  make bench-integration   Run database benchmarks")
      io.println("  make bench               Run all benchmarks")
      io.println(
        "  make bench-compare BASELINE=... CURRENT=...  Compare results",
      )
    }
  }
}

fn run_pure() -> Nil {
  io.println("Running pure Gleam benchmarks...")
  io.println("")

  let codec_results = codec_bench.run()
  let uuid_results = uuid_bench.run()
  let message_results = message_bench.run()
  let decode_results = decode_bench.run()
  let registry_results = registry_bench.run()

  let all =
    list.flatten([
      codec_results,
      uuid_results,
      message_results,
      decode_results,
      registry_results,
    ])

  let report = runner.make_report("pure", all)
  runner.print_table(report)

  case runner.write_report(report, "bench/results/pure.json") {
    Ok(_) -> io.println("Results written to bench/results/pure.json")
    Error(e) -> io.println("Failed to write results: " <> e)
  }
}

fn run_integration() -> Nil {
  io.println("Running integration benchmarks...")
  io.println("")

  let results = integration_bench.run()
  let report = runner.make_report("integration", results)
  runner.print_table(report)

  case runner.write_report(report, "bench/results/integration.json") {
    Ok(_) -> io.println("Results written to bench/results/integration.json")
    Error(e) -> io.println("Failed to write results: " <> e)
  }
}

@external(erlang, "bench_ffi", "get_args")
fn get_args() -> List(String)
