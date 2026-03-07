import gleam/option.{Some}
import gleeunit/should
import postgleam/codec/defaults
import postgleam/codec/registry as codec_registry
import postgleam/config
import postgleam/connection
import postgleam/stream
import postgleam/value

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

fn build_registry() {
  codec_registry.build(defaults.matchers())
}

pub fn stream_all_rows_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  // Prepare a query that returns 3 rows
  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, 3) AS s(num)",
      [],
      cfg.timeout,
    )

  // Stream with max_rows=1 to get 3 chunks
  let assert Ok(#(result, state)) =
    stream.stream_query(state, prepared, [], reg, 1, cfg.timeout)

  should.equal(list_length(result.rows), 3)
  should.equal(result.rows, [
    [Some(value.Integer(1))],
    [Some(value.Integer(2))],
    [Some(value.Integer(3))],
  ])

  connection.disconnect(state)
}

pub fn stream_chunks_of_2_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, 5) AS s(num)",
      [],
      cfg.timeout,
    )

  // Stream with max_rows=2: should get [1,2], [3,4], [5]
  let assert Ok(#(result, state)) =
    stream.stream_query(state, prepared, [], reg, 2, cfg.timeout)

  should.equal(list_length(result.rows), 5)
  should.equal(result.rows, [
    [Some(value.Integer(1))],
    [Some(value.Integer(2))],
    [Some(value.Integer(3))],
    [Some(value.Integer(4))],
    [Some(value.Integer(5))],
  ])

  connection.disconnect(state)
}

pub fn stream_large_max_rows_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, 2) AS s(num)",
      [],
      cfg.timeout,
    )

  // max_rows=100 is larger than result set - should get all in one chunk
  let assert Ok(#(result, state)) =
    stream.stream_query(state, prepared, [], reg, 100, cfg.timeout)

  should.equal(result.rows, [
    [Some(value.Integer(1))],
    [Some(value.Integer(2))],
  ])

  connection.disconnect(state)
}

pub fn stream_with_params_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, $1::int4) AS s(num)",
      [],
      cfg.timeout,
    )

  let assert Ok(#(result, state)) =
    stream.stream_query(
      state,
      prepared,
      [Some(value.Integer(4))],
      reg,
      2,
      cfg.timeout,
    )

  should.equal(list_length(result.rows), 4)

  connection.disconnect(state)
}

pub fn stream_fetch_chunk_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, 3) AS s(num)",
      [],
      cfg.timeout,
    )

  // Bind and execute first chunk (max_rows=2)
  let assert Ok(#(chunk1, state)) =
    connection.bind_and_execute_portal(state, prepared, [], reg, 2, cfg.timeout)

  case chunk1 {
    connection.StreamMore(rows) -> should.equal(list_length(rows), 2)
    _ -> should.fail()
  }

  // Fetch next chunk
  let assert Ok(#(chunk2, state)) =
    stream.fetch_chunk(state, prepared, reg, 2, cfg.timeout)

  case chunk2 {
    connection.StreamDone(_, rows) -> should.equal(list_length(rows), 1)
    _ -> should.fail()
  }

  // Sync to finalize
  let assert Ok(state) = connection.sync_portal(state, cfg.timeout)

  connection.disconnect(state)
}

pub fn stream_empty_result_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, 0) AS s(num)",
      [],
      cfg.timeout,
    )

  let assert Ok(#(result, state)) =
    stream.stream_query(state, prepared, [], reg, 10, cfg.timeout)

  should.equal(result.rows, [])

  connection.disconnect(state)
}

pub fn stream_then_regular_query_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, 3) AS s(num)",
      [],
      cfg.timeout,
    )

  // Stream query
  let assert Ok(#(_, state)) =
    stream.stream_query(state, prepared, [], reg, 1, cfg.timeout)

  // Regular query should still work after streaming
  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT 42::int4 AS num",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Integer(42))]])

  connection.disconnect(state)
}

fn list_length(l: List(a)) -> Int {
  case l {
    [] -> 0
    [_, ..rest] -> 1 + list_length(rest)
  }
}
