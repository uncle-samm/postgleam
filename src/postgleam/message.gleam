import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}

// Protocol version 3.0
const protocol_version_major = 3

const protocol_version_minor = 0

// --- Backend Messages (Server → Client) ---

/// Transaction status reported by ReadyForQuery
pub type TransactionStatus {
  Idle
  InTransaction
  FailedTransaction
}

/// Format code for column data
pub type Format {
  TextFormat
  BinaryFormat
}

/// A field descriptor in a RowDescription
pub type RowField {
  RowField(
    name: String,
    table_oid: Int,
    column: Int,
    type_oid: Int,
    type_size: Int,
    type_mod: Int,
    format: Format,
  )
}

/// Authentication request types
pub type AuthType {
  AuthOk
  AuthKerberos
  AuthCleartext
  AuthMd5(salt: BitArray)
  AuthScm
  AuthGss
  AuthGssContinue(data: BitArray)
  AuthSspi
  AuthSasl(mechanisms: List(String))
  AuthSaslContinue(data: BitArray)
  AuthSaslFinal(data: BitArray)
}

/// Describe target type
pub type DescribeType {
  DescribeStatement
  DescribePortal
}

/// Messages sent from PostgreSQL backend to client
pub type BackendMessage {
  AuthenticationMsg(auth: AuthType)
  BackendKeyData(pid: Int, key: Int)
  ReadyForQuery(status: TransactionStatus)
  ParameterStatus(name: String, value: String)
  RowDescription(fields: List(RowField))
  DataRow(values: BitArray)
  CommandComplete(tag: String)
  ParseComplete
  BindComplete
  CloseComplete
  NoData
  PortalSuspended
  EmptyQueryResponse
  ErrorResponse(fields: Dict(String, String))
  NoticeResponse(fields: Dict(String, String))
  NotificationResponse(pg_pid: Int, channel: String, payload: String)
  ParameterDescription(type_oids: List(Int))
  CopyInResponse(format: Format, columns: List(Format))
  CopyOutResponse(format: Format, columns: List(Format))
  CopyBothResponse(format: Format, columns: List(Format))
  CopyData(data: BitArray)
  CopyDone
}

/// Result of trying to decode a message from a byte buffer
pub type DecodeResult {
  /// Successfully decoded a message, with remaining bytes
  Decoded(message: BackendMessage, rest: BitArray)
  /// Not enough data yet — need more bytes
  Incomplete
  /// Unrecoverable decode error
  DecodeFailed(reason: String)
}

// --- Frontend Messages (Client → Server) ---

/// Messages sent from client to PostgreSQL backend
pub type FrontendMessage {
  StartupMessage(params: List(#(String, String)))
  PasswordMessage(password: String)
  SASLInitialResponse(mechanism: String, data: BitArray)
  SASLResponse(data: BitArray)
  SimpleQuery(statement: String)
  Parse(name: String, statement: String, type_oids: List(Int))
  Describe(type_: DescribeType, name: String)
  Bind(
    portal: String,
    statement: String,
    param_formats: List(Format),
    params: List(Option(BitArray)),
    result_formats: List(Format),
  )
  Execute(portal: String, max_rows: Int)
  Close(type_: DescribeType, name: String)
  Sync
  Flush
  Terminate
  SSLRequest
  CancelRequest(pid: Int, key: Int)
  CopyDataMsg(data: BitArray)
  CopyDoneMsg
  CopyFail(message: String)
}

// =============================================================================
// ENCODING (Frontend → bytes)
// =============================================================================

/// Encode a frontend message to bytes ready to send on the wire
pub fn encode_frontend(msg: FrontendMessage) -> BitArray {
  case msg {
    StartupMessage(params) -> encode_startup(params)
    PasswordMessage(password) -> encode_with_type("p", <<password_bytes(password):bits>>)
    SASLInitialResponse(mechanism, data) -> encode_sasl_initial(mechanism, data)
    SASLResponse(data) -> encode_with_type("p", data)
    SimpleQuery(statement) -> encode_with_type("Q", <<string_bytes(statement):bits, 0>>)
    Parse(name, statement, oids) -> encode_parse(name, statement, oids)
    Describe(type_, name) -> encode_with_type("D", <<describe_byte(type_), string_bytes(name):bits, 0>>)
    Bind(portal, statement, pf, params, rf) -> encode_bind(portal, statement, pf, params, rf)
    Execute(portal, max_rows) -> encode_with_type("E", <<string_bytes(portal):bits, 0, max_rows:32-big>>)
    Close(type_, name) -> encode_with_type("C", <<describe_byte(type_), string_bytes(name):bits, 0>>)
    Sync -> encode_with_type("S", <<>>)
    Flush -> encode_with_type("H", <<>>)
    Terminate -> encode_with_type("X", <<>>)
    SSLRequest -> encode_no_type(<<1234:16-big, 5679:16-big>>)
    CancelRequest(pid, key) -> encode_no_type(<<1234:16-big, 5678:16-big, pid:32-big, key:32-big>>)
    CopyDataMsg(data) -> encode_with_type("d", data)
    CopyDoneMsg -> encode_with_type("c", <<>>)
    CopyFail(message) -> encode_with_type("f", <<string_bytes(message):bits, 0>>)
  }
}

fn encode_startup(params: List(#(String, String))) -> BitArray {
  let vsn = <<protocol_version_major:16-big, protocol_version_minor:16-big>>
  let param_bytes = encode_startup_params(params, <<>>)
  let data = <<vsn:bits, param_bytes:bits, 0>>
  let size = bit_array.byte_size(data) + 4
  <<size:32-big, data:bits>>
}

fn encode_startup_params(params: List(#(String, String)), acc: BitArray) -> BitArray {
  case params {
    [] -> acc
    [#(key, value), ..rest] -> {
      let entry = <<string_bytes(key):bits, 0, string_bytes(value):bits, 0>>
      encode_startup_params(rest, <<acc:bits, entry:bits>>)
    }
  }
}

fn encode_parse(name: String, statement: String, oids: List(Int)) -> BitArray {
  let num_oids = list.length(oids)
  let oid_bytes = encode_oids(oids, <<>>)
  let data = <<string_bytes(name):bits, 0, string_bytes(statement):bits, 0, num_oids:16-big, oid_bytes:bits>>
  encode_with_type("P", data)
}

fn encode_oids(oids: List(Int), acc: BitArray) -> BitArray {
  case oids {
    [] -> acc
    [oid, ..rest] -> encode_oids(rest, <<acc:bits, oid:32-big>>)
  }
}

fn encode_bind(
  portal: String,
  statement: String,
  param_formats: List(Format),
  params: List(Option(BitArray)),
  result_formats: List(Format),
) -> BitArray {
  let pf_bytes = encode_formats(param_formats, <<>>)
  let rf_bytes = encode_formats(result_formats, <<>>)
  let num_pf = list.length(param_formats)
  let num_rf = list.length(result_formats)
  let num_params = list.length(params)
  let param_bytes = encode_params(params, <<>>)
  let data = <<
    string_bytes(portal):bits, 0,
    string_bytes(statement):bits, 0,
    num_pf:16-big, pf_bytes:bits,
    num_params:16-big, param_bytes:bits,
    num_rf:16-big, rf_bytes:bits,
  >>
  encode_with_type("B", data)
}

fn encode_formats(formats: List(Format), acc: BitArray) -> BitArray {
  case formats {
    [] -> acc
    [f, ..rest] -> encode_formats(rest, <<acc:bits, format_to_int(f):16-big>>)
  }
}

fn encode_params(params: List(Option(BitArray)), acc: BitArray) -> BitArray {
  case params {
    [] -> acc
    [Some(data), ..rest] -> {
      let len = bit_array.byte_size(data)
      encode_params(rest, <<acc:bits, len:32-big, data:bits>>)
    }
    [None, ..rest] -> {
      // -1 means NULL
      // -1 as int32 signals NULL in the wire protocol
      let null_marker = <<255, 255, 255, 255>>
      encode_params(rest, <<acc:bits, null_marker:bits>>)
    }
  }
}

fn encode_sasl_initial(mechanism: String, data: BitArray) -> BitArray {
  let data_len = bit_array.byte_size(data)
  let payload = <<string_bytes(mechanism):bits, 0, data_len:32-big, data:bits>>
  encode_with_type("p", payload)
}

fn encode_with_type(type_str: String, data: BitArray) -> BitArray {
  let assert Ok(type_byte) = bit_array.slice(string_bytes(type_str), 0, 1)
  let size = bit_array.byte_size(data) + 4
  <<type_byte:bits, size:32-big, data:bits>>
}

fn encode_no_type(data: BitArray) -> BitArray {
  let size = bit_array.byte_size(data) + 4
  <<size:32-big, data:bits>>
}

fn describe_byte(type_: DescribeType) -> Int {
  case type_ {
    DescribeStatement -> 0x53  // 'S'
    DescribePortal -> 0x50     // 'P'
  }
}

fn format_to_int(f: Format) -> Int {
  case f {
    TextFormat -> 0
    BinaryFormat -> 1
  }
}

fn password_bytes(password: String) -> BitArray {
  <<string_bytes(password):bits, 0>>
}

fn string_bytes(s: String) -> BitArray {
  bit_array.from_string(s)
}

// =============================================================================
// DECODING (bytes → Backend Messages)
// =============================================================================

/// Try to decode one backend message from a byte buffer.
/// Returns Decoded(msg, remaining_bytes), Incomplete, or DecodeFailed.
pub fn decode_backend(buffer: BitArray) -> DecodeResult {
  let buf_size = bit_array.byte_size(buffer)
  // Need at least 5 bytes: 1 type + 4 length
  case buf_size < 5 {
    True -> Incomplete
    False -> {
      let assert Ok(<<type_byte:8-unsigned, length:32-big-signed, _rest:bits>>) =
        Ok(buffer)
      // length includes the 4-byte length field itself
      let payload_size = length - 4
      let total_size = 1 + length
      case buf_size < total_size {
        True -> Incomplete
        False -> {
          let assert Ok(payload) = bit_array.slice(buffer, 5, payload_size)
          let assert Ok(rest) = bit_array.slice(buffer, total_size, buf_size - total_size)
          decode_message(type_byte, payload, payload_size, rest)
        }
      }
    }
  }
}

fn decode_message(
  type_byte: Int,
  payload: BitArray,
  size: Int,
  rest: BitArray,
) -> DecodeResult {
  case type_byte {
    // 'R' - Authentication
    0x52 -> decode_auth(payload, size, rest)
    // 'K' - BackendKeyData
    0x4B -> decode_backend_key(payload, rest)
    // 'Z' - ReadyForQuery
    0x5A -> decode_ready(payload, rest)
    // 'S' - ParameterStatus
    0x53 -> decode_parameter_status(payload, rest)
    // 'T' - RowDescription
    0x54 -> decode_row_description(payload, rest)
    // 'D' - DataRow
    0x44 -> decode_data_row(payload, rest)
    // 'C' - CommandComplete
    0x43 -> decode_command_complete(payload, rest)
    // '1' - ParseComplete
    0x31 -> Decoded(ParseComplete, rest)
    // '2' - BindComplete
    0x32 -> Decoded(BindComplete, rest)
    // '3' - CloseComplete
    0x33 -> Decoded(CloseComplete, rest)
    // 'n' - NoData
    0x6E -> Decoded(NoData, rest)
    // 's' - PortalSuspended
    0x73 -> Decoded(PortalSuspended, rest)
    // 'I' - EmptyQueryResponse
    0x49 -> Decoded(EmptyQueryResponse, rest)
    // 'E' - ErrorResponse
    0x45 -> decode_error_notice(payload, True, rest)
    // 'N' - NoticeResponse
    0x4E -> decode_error_notice(payload, False, rest)
    // 'A' - NotificationResponse
    0x41 -> decode_notification(payload, rest)
    // 't' - ParameterDescription
    0x74 -> decode_parameter_desc(payload, rest)
    // 'G' - CopyInResponse
    0x47 -> decode_copy_response(payload, CopyInResponse, rest)
    // 'H' - CopyOutResponse
    0x48 -> decode_copy_response(payload, CopyOutResponse, rest)
    // 'W' - CopyBothResponse
    0x57 -> decode_copy_response(payload, CopyBothResponse, rest)
    // 'd' - CopyData
    0x64 -> Decoded(CopyData(data: payload), rest)
    // 'c' - CopyDone
    0x63 -> Decoded(CopyDone, rest)
    _ -> DecodeFailed("Unknown message type byte: " <> int_to_hex(type_byte))
  }
}

// --- Decode helpers ---

fn decode_auth(payload: BitArray, _size: Int, rest: BitArray) -> DecodeResult {
  case payload {
    <<0:32-big-signed>> -> Decoded(AuthenticationMsg(AuthOk), rest)
    <<2:32-big-signed>> -> Decoded(AuthenticationMsg(AuthKerberos), rest)
    <<3:32-big-signed>> -> Decoded(AuthenticationMsg(AuthCleartext), rest)
    <<5:32-big-signed, salt:bits>> -> {
      case bit_array.byte_size(salt) >= 4 {
        True -> {
          let assert Ok(salt4) = bit_array.slice(salt, 0, 4)
          Decoded(AuthenticationMsg(AuthMd5(salt: salt4)), rest)
        }
        False -> DecodeFailed("MD5 auth missing salt")
      }
    }
    <<7:32-big-signed>> -> Decoded(AuthenticationMsg(AuthGss), rest)
    <<8:32-big-signed, data:bits>> -> Decoded(AuthenticationMsg(AuthGssContinue(data: data)), rest)
    <<9:32-big-signed>> -> Decoded(AuthenticationMsg(AuthSspi), rest)
    <<10:32-big-signed, mechanisms_data:bits>> -> {
      let mechanisms = decode_sasl_mechanisms(mechanisms_data, [])
      Decoded(AuthenticationMsg(AuthSasl(mechanisms: mechanisms)), rest)
    }
    <<11:32-big-signed, data:bits>> -> Decoded(AuthenticationMsg(AuthSaslContinue(data: data)), rest)
    <<12:32-big-signed, data:bits>> -> Decoded(AuthenticationMsg(AuthSaslFinal(data: data)), rest)
    _ -> DecodeFailed("Unknown auth type")
  }
}

fn decode_sasl_mechanisms(data: BitArray, acc: List(String)) -> List(String) {
  case data {
    <<0>> -> list.reverse(acc)
    <<>> -> list.reverse(acc)
    _ -> {
      case decode_cstring(data) {
        Ok(#(mechanism, remaining)) -> decode_sasl_mechanisms(remaining, [mechanism, ..acc])
        Error(_) -> list.reverse(acc)
      }
    }
  }
}

fn decode_backend_key(payload: BitArray, rest: BitArray) -> DecodeResult {
  case payload {
    <<pid:32-big-signed, key:32-big-signed>> ->
      Decoded(BackendKeyData(pid: pid, key: key), rest)
    _ -> DecodeFailed("Invalid BackendKeyData")
  }
}

fn decode_ready(payload: BitArray, rest: BitArray) -> DecodeResult {
  case payload {
    <<0x49>> -> Decoded(ReadyForQuery(Idle), rest)          // 'I'
    <<0x54>> -> Decoded(ReadyForQuery(InTransaction), rest)  // 'T'
    <<0x45>> -> Decoded(ReadyForQuery(FailedTransaction), rest) // 'E'
    _ -> DecodeFailed("Invalid ReadyForQuery status")
  }
}

fn decode_parameter_status(payload: BitArray, rest: BitArray) -> DecodeResult {
  case decode_cstring(payload) {
    Ok(#(name, remaining)) -> {
      case decode_cstring(remaining) {
        Ok(#(value, _)) -> Decoded(ParameterStatus(name: name, value: value), rest)
        Error(_) -> DecodeFailed("Invalid ParameterStatus value")
      }
    }
    Error(_) -> DecodeFailed("Invalid ParameterStatus name")
  }
}

fn decode_row_description(payload: BitArray, rest: BitArray) -> DecodeResult {
  case payload {
    <<num_fields:16-big-unsigned, fields_data:bits>> -> {
      case decode_row_fields(fields_data, num_fields, []) {
        Ok(fields) -> Decoded(RowDescription(fields: fields), rest)
        Error(e) -> DecodeFailed(e)
      }
    }
    _ -> DecodeFailed("Invalid RowDescription")
  }
}

fn decode_row_fields(
  data: BitArray,
  count: Int,
  acc: List(RowField),
) -> Result(List(RowField), String) {
  case count {
    0 -> Ok(list.reverse(acc))
    _ -> {
      case decode_cstring(data) {
        Ok(#(name, remaining)) -> {
          case remaining {
            <<table_oid:32-big-unsigned, column:16-big-signed,
              type_oid:32-big-unsigned, type_size:16-big-signed,
              type_mod:32-big-signed, format_code:16-big-signed,
              field_rest:bits>> -> {
              let format = case format_code {
                1 -> BinaryFormat
                _ -> TextFormat
              }
              let field = RowField(
                name: name,
                table_oid: table_oid,
                column: column,
                type_oid: type_oid,
                type_size: type_size,
                type_mod: type_mod,
                format: format,
              )
              decode_row_fields(field_rest, count - 1, [field, ..acc])
            }
            _ -> Error("Invalid row field data")
          }
        }
        Error(_) -> Error("Invalid row field name")
      }
    }
  }
}

fn decode_data_row(payload: BitArray, rest: BitArray) -> DecodeResult {
  // DataRow: num_columns(int16) + column data
  // We keep the raw payload (including the column count) for later decoding
  case payload {
    <<_num_columns:16-big-unsigned, values:bits>> ->
      Decoded(DataRow(values: values), rest)
    _ -> DecodeFailed("Invalid DataRow")
  }
}

fn decode_command_complete(payload: BitArray, rest: BitArray) -> DecodeResult {
  case decode_cstring(payload) {
    Ok(#(tag, _)) -> Decoded(CommandComplete(tag: tag), rest)
    Error(_) -> DecodeFailed("Invalid CommandComplete")
  }
}

fn decode_error_notice(
  payload: BitArray,
  is_error: Bool,
  rest: BitArray,
) -> DecodeResult {
  let fields = decode_error_fields(payload, dict.new())
  case is_error {
    True -> Decoded(ErrorResponse(fields: fields), rest)
    False -> Decoded(NoticeResponse(fields: fields), rest)
  }
}

fn decode_error_fields(data: BitArray, acc: Dict(String, String)) -> Dict(String, String) {
  case data {
    <<0>> -> acc
    <<>> -> acc
    <<field_type:8-unsigned, remaining:bits>> -> {
      case decode_cstring(remaining) {
        Ok(#(value, next)) -> {
          let key = decode_field_type(field_type)
          decode_error_fields(next, dict.insert(acc, key, value))
        }
        Error(_) -> acc
      }
    }
    _ -> acc
  }
}

fn decode_field_type(byte: Int) -> String {
  case byte {
    0x53 -> "severity"     // 'S'
    0x56 -> "severity_v"   // 'V' (non-localized severity, PG 9.6+)
    0x43 -> "code"         // 'C'
    0x4D -> "message"      // 'M'
    0x44 -> "detail"       // 'D'
    0x48 -> "hint"         // 'H'
    0x50 -> "position"     // 'P'
    0x70 -> "internal_position" // 'p'
    0x71 -> "internal_query"    // 'q'
    0x57 -> "where"        // 'W'
    0x73 -> "schema"       // 's'
    0x74 -> "table"        // 't'
    0x63 -> "column"       // 'c'
    0x64 -> "data_type"    // 'd'
    0x6E -> "constraint"   // 'n'
    0x46 -> "file"         // 'F'
    0x4C -> "line"         // 'L'
    0x52 -> "routine"      // 'R'
    _ -> "unknown"
  }
}

fn decode_notification(payload: BitArray, rest: BitArray) -> DecodeResult {
  case payload {
    <<pg_pid:32-big-signed, remaining:bits>> -> {
      case decode_cstring(remaining) {
        Ok(#(channel, remaining2)) -> {
          case decode_cstring(remaining2) {
            Ok(#(payload_str, _)) ->
              Decoded(NotificationResponse(
                pg_pid: pg_pid,
                channel: channel,
                payload: payload_str,
              ), rest)
            Error(_) -> DecodeFailed("Invalid notification payload")
          }
        }
        Error(_) -> DecodeFailed("Invalid notification channel")
      }
    }
    _ -> DecodeFailed("Invalid NotificationResponse")
  }
}

fn decode_parameter_desc(payload: BitArray, rest: BitArray) -> DecodeResult {
  case payload {
    <<num_params:16-big-unsigned, oids_data:bits>> -> {
      let oids = decode_oid_list(oids_data, num_params, [])
      Decoded(ParameterDescription(type_oids: oids), rest)
    }
    _ -> DecodeFailed("Invalid ParameterDescription")
  }
}

fn decode_oid_list(data: BitArray, count: Int, acc: List(Int)) -> List(Int) {
  case count {
    0 -> list.reverse(acc)
    _ -> {
      case data {
        <<oid:32-big-unsigned, remaining:bits>> ->
          decode_oid_list(remaining, count - 1, [oid, ..acc])
        _ -> list.reverse(acc)
      }
    }
  }
}

fn decode_copy_response(
  payload: BitArray,
  constructor: fn(Format, List(Format)) -> BackendMessage,
  rest: BitArray,
) -> DecodeResult {
  case payload {
    <<format_byte:8-unsigned, num_cols:16-big-unsigned, col_data:bits>> -> {
      let format = int_to_format(format_byte)
      let columns = decode_column_formats(col_data, num_cols, [])
      Decoded(constructor(format, columns), rest)
    }
    _ -> DecodeFailed("Invalid CopyResponse")
  }
}

fn decode_column_formats(data: BitArray, count: Int, acc: List(Format)) -> List(Format) {
  case count {
    0 -> list.reverse(acc)
    _ -> {
      case data {
        <<f:16-big-unsigned, remaining:bits>> ->
          decode_column_formats(remaining, count - 1, [int_to_format(f), ..acc])
        _ -> list.reverse(acc)
      }
    }
  }
}

fn int_to_format(n: Int) -> Format {
  case n {
    1 -> BinaryFormat
    _ -> TextFormat
  }
}

// =============================================================================
// Utility: C-string decoding (null-terminated)
// =============================================================================

/// Decode a null-terminated C string from a BitArray.
/// Returns the string and the remaining bytes after the null terminator.
pub fn decode_cstring(data: BitArray) -> Result(#(String, BitArray), Nil) {
  decode_cstring_loop(data, <<>>)
}

fn decode_cstring_loop(data: BitArray, acc: BitArray) -> Result(#(String, BitArray), Nil) {
  case data {
    <<0, remaining:bits>> -> {
      case bit_array.to_string(acc) {
        Ok(s) -> Ok(#(s, remaining))
        Error(_) -> Error(Nil)
      }
    }
    <<byte:8, remaining:bits>> ->
      decode_cstring_loop(remaining, <<acc:bits, byte>>)
    _ -> Error(Nil)
  }
}

// =============================================================================
// DataRow value extraction helpers
// =============================================================================

/// Extract column values from a DataRow's raw value bytes.
/// Returns a list of Option(BitArray) — None for NULL columns.
pub fn extract_row_values(values: BitArray) -> List(Option(BitArray)) {
  extract_row_values_loop(values, [])
  |> list.reverse
}

fn extract_row_values_loop(
  data: BitArray,
  acc: List(Option(BitArray)),
) -> List(Option(BitArray)) {
  case data {
    <<>> -> acc
    <<-1:32-big-signed, remaining:bits>> ->
      extract_row_values_loop(remaining, [None, ..acc])
    <<len:32-big-signed, value:bytes-size(len), remaining:bits>> ->
      extract_row_values_loop(remaining, [Some(value), ..acc])
    _ -> acc
  }
}

// --- Utility ---

fn int_to_hex(n: Int) -> String {
  case n {
    _ if n < 16 -> {
      let assert Ok(c) = hex_char(n)
      "0x0" <> c
    }
    _ -> {
      let hi = n / 16
      let lo = n % 16
      let assert Ok(h) = hex_char(hi)
      let assert Ok(l) = hex_char(lo)
      "0x" <> h <> l
    }
  }
}

fn hex_char(n: Int) -> Result(String, Nil) {
  case n {
    0 -> Ok("0")
    1 -> Ok("1")
    2 -> Ok("2")
    3 -> Ok("3")
    4 -> Ok("4")
    5 -> Ok("5")
    6 -> Ok("6")
    7 -> Ok("7")
    8 -> Ok("8")
    9 -> Ok("9")
    10 -> Ok("A")
    11 -> Ok("B")
    12 -> Ok("C")
    13 -> Ok("D")
    14 -> Ok("E")
    15 -> Ok("F")
    _ -> Error(Nil)
  }
}
