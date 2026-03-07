/// LISTEN/NOTIFY support for PostgreSQL pub/sub notifications.
/// Uses a dedicated connection that listens for notifications.

import gleam/result
import postgleam/connection.{type ConnectionState, ConnectionState}
import postgleam/error.{type Error}
import postgleam/message

/// A notification received from PostgreSQL
pub type Notification {
  Notification(channel: String, payload: String, pg_pid: Int)
}

/// Listen on a channel
pub fn listen(
  state: ConnectionState,
  channel: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(_, state) <- result.try(
    connection.simple_query(state, "LISTEN " <> quote_identifier(channel), timeout),
  )
  Ok(state)
}

/// Stop listening on a channel
pub fn unlisten(
  state: ConnectionState,
  channel: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(_, state) <- result.try(
    connection.simple_query(state, "UNLISTEN " <> quote_identifier(channel), timeout),
  )
  Ok(state)
}

/// Send a notification on a channel
pub fn notify(
  state: ConnectionState,
  channel: String,
  payload: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  let sql =
    "SELECT pg_notify("
    <> quote_literal(channel)
    <> ", "
    <> quote_literal(payload)
    <> ")"
  use #(_, state) <- result.try(connection.simple_query(state, sql, timeout))
  Ok(state)
}

/// Check for pending notifications.
/// Sends a lightweight query to flush any queued notifications from the server.
/// Returns a list of notifications that arrived, plus the updated state.
pub fn receive_notifications(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(List(Notification), ConnectionState), Error) {
  // Send a simple SELECT to trigger the server to send any queued notifications
  use #(_, state) <- result.try(
    connection.simple_query(state, "SELECT 1", timeout),
  )
  // Check the buffer for any NotificationResponse messages
  collect_notifications(state, timeout, [])
}

fn collect_notifications(
  state: ConnectionState,
  _timeout: Int,
  acc: List(Notification),
) -> Result(#(List(Notification), ConnectionState), Error) {
  // Check buffer for notifications without blocking
  case message.decode_backend(state.buffer) {
    message.Decoded(message.NotificationResponse(pg_pid, channel, payload), _rest) -> {
      let notif = Notification(channel: channel, payload: payload, pg_pid: pg_pid)
      let state = ConnectionState(..state, buffer: <<>>)
      collect_notifications(state, 0, [notif, ..acc])
    }
    _ -> Ok(#(list_reverse(acc), state))
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

fn quote_identifier(s: String) -> String {
  "\"" <> s <> "\""
}

fn quote_literal(s: String) -> String {
  "'" <> s <> "'"
}
