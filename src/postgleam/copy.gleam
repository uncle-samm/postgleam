/// COPY protocol support for bulk data transfer.

import gleam/option.{None}
import gleam/result
import postgleam/connection.{type ConnectionState}
import postgleam/error.{type Error}
import postgleam/message

/// COPY data to a table from text-format data rows
pub fn copy_in(
  state: ConnectionState,
  sql: String,
  data: List(BitArray),
  timeout: Int,
) -> Result(#(String, ConnectionState), Error) {
  // Send the COPY command via simple query
  use state <- result.try(
    connection.send_message(state, message.SimpleQuery(sql)),
  )
  // Receive CopyInResponse
  use state <- result.try(recv_copy_in_response(state, timeout))
  // Send CopyData messages
  use state <- result.try(send_copy_data(state, data))
  // Send CopyDone
  use state <- result.try(
    connection.send_message(state, message.CopyDoneMsg),
  )
  // Receive CommandComplete + ReadyForQuery
  recv_copy_complete(state, timeout)
}

/// COPY data from a table to the client
pub fn copy_out(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(BitArray), ConnectionState), Error) {
  use state <- result.try(
    connection.send_message(state, message.SimpleQuery(sql)),
  )
  use state <- result.try(recv_copy_out_response(state, timeout))
  recv_copy_out_data(state, timeout, [])
}

fn recv_copy_in_response(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.CopyInResponse(_, _) -> Ok(state)
    message.ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(fields: pg_fields, connection_id: state.connection_id, query: None))
    }
    message.NoticeResponse(_) -> recv_copy_in_response(state, timeout)
    _ -> Error(error.ProtocolError("Expected CopyInResponse"))
  }
}

fn recv_copy_out_response(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.CopyOutResponse(_, _) -> Ok(state)
    message.ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(fields: pg_fields, connection_id: state.connection_id, query: None))
    }
    message.NoticeResponse(_) -> recv_copy_out_response(state, timeout)
    _ -> Error(error.ProtocolError("Expected CopyOutResponse"))
  }
}

fn send_copy_data(
  state: ConnectionState,
  data: List(BitArray),
) -> Result(ConnectionState, Error) {
  case data {
    [] -> Ok(state)
    [chunk, ..rest] -> {
      use state <- result.try(
        connection.send_message(state, message.CopyDataMsg(chunk)),
      )
      send_copy_data(state, rest)
    }
  }
}

fn recv_copy_complete(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(String, ConnectionState), Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.CommandComplete(tag) -> {
      // Receive ReadyForQuery
      use #(msg2, state) <- result.try(connection.receive_message(state, timeout))
      case msg2 {
        message.ReadyForQuery(status) ->
          Ok(#(tag, connection.ConnectionState(..state, transaction_status: status)))
        _ -> Ok(#(tag, state))
      }
    }
    message.ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(fields: pg_fields, connection_id: state.connection_id, query: None))
    }
    message.NoticeResponse(_) -> recv_copy_complete(state, timeout)
    _ -> Error(error.ProtocolError("Expected CommandComplete after COPY"))
  }
}

fn recv_copy_out_data(
  state: ConnectionState,
  timeout: Int,
  acc: List(BitArray),
) -> Result(#(List(BitArray), ConnectionState), Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.CopyData(data) ->
      recv_copy_out_data(state, timeout, [data, ..acc])
    message.CopyDone -> {
      // Receive CommandComplete + ReadyForQuery
      use #(msg2, state) <- result.try(connection.receive_message(state, timeout))
      case msg2 {
        message.CommandComplete(_) -> {
          use #(msg3, state) <- result.try(connection.receive_message(state, timeout))
          case msg3 {
            message.ReadyForQuery(status) ->
              Ok(#(list_reverse(acc), connection.ConnectionState(..state, transaction_status: status)))
            _ -> Ok(#(list_reverse(acc), state))
          }
        }
        _ -> Ok(#(list_reverse(acc), state))
      }
    }
    message.ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(fields: pg_fields, connection_id: state.connection_id, query: None))
    }
    _ -> Error(error.ProtocolError("Expected CopyData or CopyDone"))
  }
}

fn list_reverse(l: List(a)) -> List(a) {
  list_reverse_loop(l, [])
}

fn list_reverse_loop(l: List(a), acc: List(a)) -> List(a) {
  case l {
    [] -> acc
    [x, ..rest] -> list_reverse_loop(rest, [x, ..acc])
  }
}
