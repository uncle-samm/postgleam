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
  PgError(fields: PgErrorFields, connection_id: Option(Int), query: Option(String))
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
