//// COPY protocol support for bulk data transfer.

import gleam/list
import gleam/option
import gleam/result
import postgleam/connection.{type ConnectionState, type Failure, Failure}
import postgleam/error.{type Error}
import postgleam/message

/// COPY rows into a table. `sql` must be a `COPY ... FROM STDIN` statement and
/// `data` the rows in the COPY format (text format: tab-separated, each row
/// ending with `\n`). Returns the command tag, e.g. `"COPY 1000"`.
pub fn copy_in(
  state: ConnectionState,
  sql: String,
  data: List(BitArray),
  timeout: Int,
) -> Result(#(String, ConnectionState), Error) {
  copy_in_f(state, sql, data, timeout) |> result.map_error(fn(f) { f.error })
}

@internal
pub fn copy_in_f(
  state: ConnectionState,
  sql: String,
  data: List(BitArray),
  timeout: Int,
) -> Result(#(String, ConnectionState), Failure) {
  use state <- result.try(
    connection.send_message(state, message.SimpleQuery(sql)) |> lift,
  )
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    message.CopyInResponse(..) -> {
      // All CopyData chunks and CopyDone go out in one write.
      let msgs =
        list.map(data, message.CopyDataMsg)
        |> list.append([message.CopyDoneMsg])
      use state <- result.try(connection.send_messages(state, msgs) |> lift)
      recv_complete(state, sql, timeout, "")
    }
    message.ErrorResponse(fields) ->
      Error(connection.fail_to_ready(state, fields, sql, timeout))
    other -> Error(connection.protocol_failure("CopyInResponse", other))
  }
}

/// COPY rows out of a table. `sql` must be a `COPY ... TO STDOUT` statement.
/// Returns the raw data chunks (one per row in text format).
pub fn copy_out(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(BitArray), ConnectionState), Error) {
  copy_out_f(state, sql, timeout) |> result.map_error(fn(f) { f.error })
}

@internal
pub fn copy_out_f(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(BitArray), ConnectionState), Failure) {
  use state <- result.try(
    connection.send_message(state, message.SimpleQuery(sql)) |> lift,
  )
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    message.CopyOutResponse(..) -> recv_copy_data(state, sql, timeout, [])
    message.ErrorResponse(fields) ->
      Error(connection.fail_to_ready(state, fields, sql, timeout))
    other -> Error(connection.protocol_failure("CopyOutResponse", other))
  }
}

fn recv_copy_data(
  state: ConnectionState,
  sql: String,
  timeout: Int,
  acc: List(BitArray),
) -> Result(#(List(BitArray), ConnectionState), Failure) {
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    message.CopyData(data) -> recv_copy_data(state, sql, timeout, [data, ..acc])
    message.CopyDone -> {
      use #(_, state) <- result.try(recv_complete(state, sql, timeout, ""))
      Ok(#(list.reverse(acc), state))
    }
    message.ErrorResponse(fields) ->
      Error(connection.fail_to_ready(state, fields, sql, timeout))
    other -> Error(connection.protocol_failure("CopyData or CopyDone", other))
  }
}

/// Read CommandComplete then ReadyForQuery.
fn recv_complete(
  state: ConnectionState,
  sql: String,
  timeout: Int,
  tag: String,
) -> Result(#(String, ConnectionState), Failure) {
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    message.CommandComplete(tag) -> recv_complete(state, sql, timeout, tag)
    message.ReadyForQuery(status) ->
      Ok(#(tag, connection.ConnectionState(..state, transaction_status: status)))
    message.ErrorResponse(fields) ->
      Error(connection.fail_to_ready(state, fields, sql, timeout))
    other ->
      Error(connection.protocol_failure(
        "CommandComplete or ReadyForQuery",
        other,
      ))
  }
}

fn next(state: ConnectionState, timeout: Int) {
  connection.receive_message(state, timeout) |> lift
}

fn lift(r: Result(a, Error)) -> Result(a, Failure) {
  result.map_error(r, fn(e) { Failure(error: e, state: option.None) })
}
