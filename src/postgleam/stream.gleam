/// Streaming query results using portal suspension.
/// Fetches rows in chunks using Execute with max_rows.

import gleam/option.{type Option}
import postgleam/codec/registry.{type Registry}
import postgleam/connection.{
  type ConnectionState, type ExtendedQueryResult, type PreparedStatement,
  type StreamChunk, ExtendedQueryResult, StreamDone, StreamMore,
}
import postgleam/error.{type Error}
import postgleam/value.{type Value}

/// Fetch all rows from a query in chunks of max_rows.
/// Returns the complete result after all chunks have been collected.
pub fn stream_query(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  // Bind + Execute first chunk
  use #(chunk, state) <- ok(connection.bind_and_execute_portal(
    state,
    prepared,
    params,
    registry,
    max_rows,
    timeout,
  ))
  collect_chunks(state, prepared, registry, max_rows, timeout, chunk, [])
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

// --- Internal helpers ---

fn collect_chunks(
  state: ConnectionState,
  prepared: PreparedStatement,
  registry: Registry,
  max_rows: Int,
  timeout: Int,
  chunk: StreamChunk,
  acc: List(List(Option(Value))),
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  case chunk {
    StreamMore(rows) -> {
      let new_acc = append_rows(acc, rows)
      use #(next_chunk, state) <- ok(
        connection.execute_portal(state, prepared, registry, max_rows, timeout),
      )
      collect_chunks(
        state,
        prepared,
        registry,
        max_rows,
        timeout,
        next_chunk,
        new_acc,
      )
    }
    StreamDone(tag, rows) -> {
      let all_rows = append_rows(acc, rows)
      // Sync to finalize
      use state <- ok(connection.sync_portal(state, timeout))
      Ok(#(
        ExtendedQueryResult(
          tag: tag,
          columns: prepared.result_fields,
          rows: all_rows,
        ),
        state,
      ))
    }
  }
}

fn append_rows(
  acc: List(List(Option(Value))),
  new: List(List(Option(Value))),
) -> List(List(Option(Value))) {
  case new {
    [] -> acc
    _ -> list_append(acc, new)
  }
}

fn list_append(a: List(a), b: List(a)) -> List(a) {
  case a {
    [] -> b
    [x, ..rest] -> [x, ..list_append(rest, b)]
  }
}

fn ok(
  result: Result(a, Error),
  next: fn(a) -> Result(b, Error),
) -> Result(b, Error) {
  case result {
    Ok(v) -> next(v)
    Error(e) -> Error(e)
  }
}
