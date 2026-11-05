//// Stream query results in chunks using portal suspension (Execute with a
//// row limit), so large result sets never sit in memory all at once.

import gleam/list
import gleam/option.{type Option}
import gleam/result
import postgleam/codec/registry.{type Registry}
import postgleam/connection.{
  type ConnectionState, type ExtendedQueryResult, type Failure,
  type PreparedStatement, type StreamChunk, ExtendedQueryResult, StreamDone,
  StreamMore,
}
import postgleam/error.{type Error}
import postgleam/value.{type Value}

/// Fold over a query's rows, fetching `max_rows` at a time. Only one chunk is
/// held in memory. Returns the final accumulator and the command tag.
pub fn fold(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  max_rows: Int,
  timeout: Int,
  initial: acc,
  f: fn(acc, List(Option(Value))) -> acc,
) -> Result(#(#(acc, String), ConnectionState), Error) {
  fold_f(state, prepared, params, registry, max_rows, timeout, initial, f)
  |> result.map_error(fn(e) { e.error })
}

@internal
pub fn fold_f(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  max_rows: Int,
  timeout: Int,
  initial: acc,
  f: fn(acc, List(Option(Value))) -> acc,
) -> Result(#(#(acc, String), ConnectionState), Failure) {
  use #(chunk, state) <- result.try(connection.bind_and_execute_portal_f(
    state,
    prepared,
    params,
    registry,
    max_rows,
    timeout,
  ))
  fold_loop(state, prepared, registry, max_rows, timeout, chunk, initial, f)
}

fn fold_loop(
  state: ConnectionState,
  prepared: PreparedStatement,
  registry: Registry,
  max_rows: Int,
  timeout: Int,
  chunk: StreamChunk,
  acc: acc,
  f: fn(acc, List(Option(Value))) -> acc,
) -> Result(#(#(acc, String), ConnectionState), Failure) {
  case chunk {
    StreamMore(rows) -> {
      let acc = list.fold(rows, acc, f)
      use #(chunk, state) <- result.try(connection.execute_portal_f(
        state,
        prepared,
        registry,
        max_rows,
        timeout,
      ))
      fold_loop(state, prepared, registry, max_rows, timeout, chunk, acc, f)
    }
    StreamDone(tag, rows) -> {
      let acc = list.fold(rows, acc, f)
      use state <- result.try(connection.sync_portal_f(state, timeout))
      Ok(#(#(acc, tag), state))
    }
  }
}

/// Fetch all rows in chunks of `max_rows` and return them together.
/// Prefer `fold` for large results; this collects everything in memory.
pub fn stream_query(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  use #(#(rows, tag), state) <- result.try(
    fold(state, prepared, params, registry, max_rows, timeout, [], fn(acc, row) {
      [row, ..acc]
    }),
  )
  Ok(#(
    ExtendedQueryResult(
      tag: tag,
      columns: prepared.result_fields,
      rows: list.reverse(rows),
    ),
    state,
  ))
}

/// Fetch a single chunk of rows from an already-bound portal.
pub fn fetch_chunk(
  state: ConnectionState,
  prepared: PreparedStatement,
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Error) {
  connection.execute_portal(state, prepared, registry, max_rows, timeout)
}
