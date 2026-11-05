//// LISTEN/NOTIFY on a low-level `ConnectionState`.
////
//// For most applications `postgleam/listener` is easier: it runs a
//// dedicated connection and sends each notification to a `Subject`.

import gleam/option.{Some}
import gleam/result
import gleam/string
import postgleam/codec/defaults
import postgleam/connection.{type ConnectionState}
import postgleam/error.{type Error}
import postgleam/value

/// A notification received from PostgreSQL.
pub type Notification =
  connection.Notification

/// Listen on a channel.
pub fn listen(
  state: ConnectionState,
  channel: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(_, state) <- result.try(connection.simple_query(
    state,
    "LISTEN " <> quote_identifier(channel),
    timeout,
  ))
  Ok(state)
}

/// Stop listening on a channel.
pub fn unlisten(
  state: ConnectionState,
  channel: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(_, state) <- result.try(connection.simple_query(
    state,
    "UNLISTEN " <> quote_identifier(channel),
    timeout,
  ))
  Ok(state)
}

/// Send a notification on a channel (parameterised `pg_notify`).
pub fn notify(
  state: ConnectionState,
  channel: String,
  payload: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(_, state) <- result.try(connection.extended_query(
    state,
    "SELECT pg_notify($1, $2)",
    [Some(value.Text(channel)), Some(value.Text(payload))],
    defaults.build_registry(),
    timeout,
  ))
  Ok(state)
}

/// Return the notifications received so far, oldest first. If there are
/// none, wait up to `timeout` milliseconds for one to arrive. Returns an
/// empty list when nothing arrives in time.
pub fn receive_notifications(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(List(Notification), ConnectionState), Error) {
  connection.wait_for_notifications(state, timeout)
}

/// Quote an identifier for LISTEN/UNLISTEN (which take no parameters).
@internal
pub fn quote_identifier(s: String) -> String {
  "\"" <> string.replace(s, "\"", "\"\"") <> "\""
}
