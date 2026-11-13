import gleam/dict.{type Dict}
import gleam/option.{type Option}

/// PostgreSQL error/notice fields from the wire protocol
pub type PgErrorFields {
  PgErrorFields(
    severity: String,
    code: String,
    message: String,
    detail: Option(String),
    hint: Option(String),
    position: Option(String),
    internal_position: Option(String),
    internal_query: Option(String),
    where_: Option(String),
    schema: Option(String),
    table: Option(String),
    column: Option(String),
    data_type: Option(String),
    constraint: Option(String),
    file: Option(String),
    line: Option(String),
    routine: Option(String),
  )
}

/// Errors that can occur during postgleam operations
pub type Error {
  /// A PostgreSQL error response
  PgError(
    fields: PgErrorFields,
    connection_id: Option(Int),
    query: Option(String),
  )
  /// Connection-level error
  ConnectionError(message: String)
  /// Authentication failed
  AuthenticationError(message: String)
  /// Error encoding parameters
  EncodeError(message: String)
  /// Error decoding results
  DecodeError(message: String)
  /// Protocol violation or unexpected message
  ProtocolError(message: String)
  /// TCP/socket error
  SocketError(message: String)
  /// Operation timed out
  TimeoutError
  /// No pool connection became free within `queue_timeout`
  PoolTimeout
}

/// A one-line human-readable description of an error.
pub fn to_string(err: Error) -> String {
  case err {
    PgError(fields, _, _) ->
      "PostgreSQL error " <> fields.code <> ": " <> fields.message
    ConnectionError(msg) -> "Connection error: " <> msg
    AuthenticationError(msg) -> "Authentication error: " <> msg
    EncodeError(msg) -> "Encode error: " <> msg
    DecodeError(msg) -> "Decode error: " <> msg
    ProtocolError(msg) -> "Protocol error: " <> msg
    SocketError(msg) -> "Socket error: " <> msg
    TimeoutError -> "Timed out"
    PoolTimeout -> "Timed out waiting for a pool connection"
  }
}

/// The five-character SQLSTATE code of an error the server reported, such as
/// `"23505"`. `Error(Nil)` for errors that didn't come from the server.
pub fn sqlstate(err: Error) -> Result(String, Nil) {
  case err {
    PgError(fields:, ..) -> Ok(fields.code)
    _ -> Error(Nil)
  }
}

/// Whether the server refused a row because it would break a unique
/// constraint or primary key (SQLSTATE 23505).
pub fn is_unique_violation(err: Error) -> Bool {
  sqlstate(err) == Ok("23505")
}

/// Whether the server refused a change because of a foreign key
/// (SQLSTATE 23503).
pub fn is_foreign_key_violation(err: Error) -> Bool {
  sqlstate(err) == Ok("23503")
}

/// Whether the transaction lost to a concurrent one and is worth running
/// again: a serialization failure (40001) or a deadlock (40P01).
pub fn is_serialization_failure(err: Error) -> Bool {
  case sqlstate(err) {
    Ok("40001") | Ok("40P01") -> True
    _ -> False
  }
}

/// Parse a raw error field dict (from wire protocol) into structured PgErrorFields
pub fn parse_error_fields(fields: Dict(String, String)) -> PgErrorFields {
  PgErrorFields(
    severity: field(fields, "severity"),
    code: field(fields, "code"),
    message: field(fields, "message"),
    detail: opt_field(fields, "detail"),
    hint: opt_field(fields, "hint"),
    position: opt_field(fields, "position"),
    internal_position: opt_field(fields, "internal_position"),
    internal_query: opt_field(fields, "internal_query"),
    where_: opt_field(fields, "where"),
    schema: opt_field(fields, "schema"),
    table: opt_field(fields, "table"),
    column: opt_field(fields, "column"),
    data_type: opt_field(fields, "data_type"),
    constraint: opt_field(fields, "constraint"),
    file: opt_field(fields, "file"),
    line: opt_field(fields, "line"),
    routine: opt_field(fields, "routine"),
  )
}

fn field(fields: Dict(String, String), key: String) -> String {
  case dict.get(fields, key) {
    Ok(v) -> v
    Error(_) -> ""
  }
}

fn opt_field(fields: Dict(String, String), key: String) -> Option(String) {
  case dict.get(fields, key) {
    Ok(v) -> option.Some(v)
    Error(_) -> option.None
  }
}
