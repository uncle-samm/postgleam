import gleam/bit_array
import gleam/dict
import gleam/option.{None, Some}
import gleeunit/should
import postgleam/message.{
  AuthCleartext, AuthMd5, AuthOk, AuthSasl, AuthSaslContinue,
  AuthenticationMsg, BackendKeyData, BinaryFormat, Bind, BindComplete,
  CancelRequest, Close, CloseComplete, CommandComplete, CopyData, CopyDone,
  CopyInResponse, CopyOutResponse, DataRow, Decoded,
  Describe, DescribePortal, DescribeStatement, EmptyQueryResponse,
  ErrorResponse, Execute, Flush, Incomplete, NoData, NoticeResponse,
  NotificationResponse, ParameterDescription, ParameterStatus, Parse,
  ParseComplete, PasswordMessage, PortalSuspended, ReadyForQuery,
  RowDescription, SSLRequest, SimpleQuery, StartupMessage, Sync,
  Terminate, TextFormat,
}

// =============================================================================
// Frontend message encoding tests
// =============================================================================

pub fn encode_startup_message_test() {
  let msg = StartupMessage(params: [#("user", "postgres"), #("database", "mydb")])
  let bytes = message.encode_frontend(msg)
  // Should start with length (4 bytes), then version 3.0 (4 bytes)
  let assert <<_size:32-big-unsigned, 0, 3, 0, 0, rest:bits>> = bytes
  // Rest should contain "user\0postgres\0database\0mydb\0\0"
  should.be_true(cstring_found_in(rest, "user"))
}

pub fn encode_password_message_test() {
  let bytes = message.encode_frontend(PasswordMessage("secret"))
  // Type 'p' (0x70), then length, then "secret\0"
  let assert <<0x70, _size:32-big-unsigned, rest:bits>> = bytes
  // Should end with null terminator
  let last_byte = bit_array.byte_size(rest) - 1
  let assert Ok(<<0>>) = bit_array.slice(rest, last_byte, 1)
}

pub fn encode_simple_query_test() {
  let bytes = message.encode_frontend(SimpleQuery("SELECT 1"))
  let assert <<0x51, _size:32-big-unsigned, rest:bits>> = bytes
  let assert Ok(#("SELECT 1", <<>>)) = message.decode_cstring(rest)
}

pub fn encode_parse_test() {
  let bytes = message.encode_frontend(Parse(name: "", statement: "SELECT $1", type_oids: [23]))
  let assert <<0x50, _size:32-big-unsigned, rest:bits>> = bytes
  let assert Ok(#("", rest2)) = message.decode_cstring(rest)
  let assert Ok(#("SELECT $1", rest3)) = message.decode_cstring(rest2)
  let assert <<1:16-big-unsigned, 23:32-big-unsigned>> = rest3
}

pub fn encode_parse_no_oids_test() {
  let bytes = message.encode_frontend(Parse(name: "stmt1", statement: "SELECT 1", type_oids: []))
  let assert <<0x50, _size:32-big-unsigned, rest:bits>> = bytes
  let assert Ok(#("stmt1", rest2)) = message.decode_cstring(rest)
  let assert Ok(#("SELECT 1", rest3)) = message.decode_cstring(rest2)
  let assert <<0:16-big-unsigned>> = rest3
}

pub fn encode_describe_statement_test() {
  let bytes = message.encode_frontend(Describe(type_: DescribeStatement, name: ""))
  let assert <<0x44, _size:32-big-unsigned, 0x53, rest:bits>> = bytes
  let assert Ok(#("", <<>>)) = message.decode_cstring(rest)
}

pub fn encode_describe_portal_test() {
  let bytes = message.encode_frontend(Describe(type_: DescribePortal, name: "p1"))
  let assert <<0x44, _size:32-big-unsigned, 0x50, rest:bits>> = bytes
  let assert Ok(#("p1", <<>>)) = message.decode_cstring(rest)
}

pub fn encode_bind_test() {
  let params = [Some(<<0, 0, 0, 42>>), None]
  let bytes = message.encode_frontend(Bind(
    portal: "",
    statement: "",
    param_formats: [BinaryFormat],
    params: params,
    result_formats: [BinaryFormat],
  ))
  let assert <<0x42, _size:32-big-unsigned, _rest:bits>> = bytes
}

pub fn encode_execute_test() {
  let bytes = message.encode_frontend(Execute(portal: "", max_rows: 0))
  let assert <<0x45, _size:32-big-unsigned, 0, 0:32-big-unsigned>> = bytes
}

pub fn encode_sync_test() {
  let bytes = message.encode_frontend(Sync)
  should.equal(bytes, <<0x53, 0, 0, 0, 4>>)
}

pub fn encode_flush_test() {
  let bytes = message.encode_frontend(Flush)
  should.equal(bytes, <<0x48, 0, 0, 0, 4>>)
}

pub fn encode_terminate_test() {
  let bytes = message.encode_frontend(Terminate)
  should.equal(bytes, <<0x58, 0, 0, 0, 4>>)
}

pub fn encode_close_statement_test() {
  let bytes = message.encode_frontend(Close(type_: DescribeStatement, name: "stmt1"))
  let assert <<0x43, _size:32-big-unsigned, 0x53, rest:bits>> = bytes
  let assert Ok(#("stmt1", <<>>)) = message.decode_cstring(rest)
}

pub fn encode_ssl_request_test() {
  let bytes = message.encode_frontend(SSLRequest)
  let assert <<8:32-big-unsigned, 1234:16-big-unsigned, 5679:16-big-unsigned>> = bytes
}

pub fn encode_cancel_request_test() {
  let bytes = message.encode_frontend(CancelRequest(pid: 12345, key: 67890))
  let assert <<16:32-big-unsigned, 1234:16-big-unsigned, 5678:16-big-unsigned,
    12_345:32-big-unsigned, 67_890:32-big-unsigned>> = bytes
}

// =============================================================================
// Backend message decoding tests
// =============================================================================

pub fn decode_auth_ok_test() {
  let data = <<0x52, 0, 0, 0, 8, 0, 0, 0, 0>>
  let assert Decoded(AuthenticationMsg(AuthOk), <<>>) = message.decode_backend(data)
}

pub fn decode_auth_cleartext_test() {
  let data = <<0x52, 0, 0, 0, 8, 0, 0, 0, 3>>
  let assert Decoded(AuthenticationMsg(AuthCleartext), <<>>) = message.decode_backend(data)
}

pub fn decode_auth_md5_test() {
  let salt = <<1, 2, 3, 4>>
  let data = <<0x52, 0, 0, 0, 12, 0, 0, 0, 5, salt:bits>>
  let assert Decoded(AuthenticationMsg(AuthMd5(salt: decoded_salt)), <<>>) =
    message.decode_backend(data)
  should.equal(decoded_salt, salt)
}

pub fn decode_auth_sasl_test() {
  // Auth SASL: type=10, then "SCRAM-SHA-256\0\0"
  let mechanism = <<"SCRAM-SHA-256":utf8, 0, 0>>
  let auth_type = <<0, 0, 0, 10>>
  let payload = <<auth_type:bits, mechanism:bits>>
  let payload_size = bit_array.byte_size(payload) + 4
  let data = <<0x52, payload_size:32-big, payload:bits>>
  let assert Decoded(AuthenticationMsg(AuthSasl(mechanisms: mechs)), <<>>) =
    message.decode_backend(data)
  should.equal(mechs, ["SCRAM-SHA-256"])
}

pub fn decode_auth_sasl_continue_test() {
  let challenge = <<"r=nonce,s=salt,i=4096":utf8>>
  let auth_type = <<0, 0, 0, 11>>
  let payload = <<auth_type:bits, challenge:bits>>
  let payload_size = bit_array.byte_size(payload) + 4
  let data = <<0x52, payload_size:32-big, payload:bits>>
  let assert Decoded(AuthenticationMsg(AuthSaslContinue(data: decoded)), <<>>) =
    message.decode_backend(data)
  should.equal(decoded, challenge)
}

pub fn decode_backend_key_data_test() {
  // pid=12345 (0x00003039), key=43981 (0x0000ABCD)
  let data = <<0x4B, 0, 0, 0, 12, 0, 0, 48, 57, 0, 0, 171, 205>>
  let assert Decoded(BackendKeyData(pid: pid, key: key), <<>>) =
    message.decode_backend(data)
  should.equal(pid, 12345)
  should.equal(key, 43981)
}

pub fn decode_ready_for_query_idle_test() {
  let data = <<0x5A, 0, 0, 0, 5, 0x49>>
  let assert Decoded(ReadyForQuery(message.Idle), <<>>) = message.decode_backend(data)
}

pub fn decode_ready_for_query_transaction_test() {
  let data = <<0x5A, 0, 0, 0, 5, 0x54>>
  let assert Decoded(ReadyForQuery(message.InTransaction), <<>>) = message.decode_backend(data)
}

pub fn decode_ready_for_query_error_test() {
  let data = <<0x5A, 0, 0, 0, 5, 0x45>>
  let assert Decoded(ReadyForQuery(message.FailedTransaction), <<>>) = message.decode_backend(data)
}

pub fn decode_parameter_status_test() {
  let payload = <<"server_version":utf8, 0, "16.1":utf8, 0>>
  let payload_size = bit_array.byte_size(payload) + 4
  let data = <<0x53, payload_size:32-big, payload:bits>>
  let assert Decoded(ParameterStatus(name: n, value: v), <<>>) =
    message.decode_backend(data)
  should.equal(n, "server_version")
  should.equal(v, "16.1")
}

pub fn decode_parse_complete_test() {
  let assert Decoded(ParseComplete, <<>>) = message.decode_backend(<<0x31, 0, 0, 0, 4>>)
}

pub fn decode_bind_complete_test() {
  let assert Decoded(BindComplete, <<>>) = message.decode_backend(<<0x32, 0, 0, 0, 4>>)
}

pub fn decode_close_complete_test() {
  let assert Decoded(CloseComplete, <<>>) = message.decode_backend(<<0x33, 0, 0, 0, 4>>)
}

pub fn decode_no_data_test() {
  let assert Decoded(NoData, <<>>) = message.decode_backend(<<0x6E, 0, 0, 0, 4>>)
}

pub fn decode_portal_suspended_test() {
  let assert Decoded(PortalSuspended, <<>>) = message.decode_backend(<<0x73, 0, 0, 0, 4>>)
}

pub fn decode_empty_query_test() {
  let assert Decoded(EmptyQueryResponse, <<>>) = message.decode_backend(<<0x49, 0, 0, 0, 4>>)
}

pub fn decode_command_complete_test() {
  let tag = <<"SELECT 1":utf8, 0>>
  let payload_size = bit_array.byte_size(tag) + 4
  let data = <<0x43, payload_size:32-big, tag:bits>>
  let assert Decoded(CommandComplete(tag: t), <<>>) = message.decode_backend(data)
  should.equal(t, "SELECT 1")
}

pub fn decode_command_complete_insert_test() {
  let tag = <<"INSERT 0 1":utf8, 0>>
  let payload_size = bit_array.byte_size(tag) + 4
  let data = <<0x43, payload_size:32-big, tag:bits>>
  let assert Decoded(CommandComplete(tag: t), <<>>) = message.decode_backend(data)
  should.equal(t, "INSERT 0 1")
}

pub fn decode_row_description_test() {
  // One field: "id", type int4 (oid=23), size=4, text format
  let name = <<"id":utf8, 0>>
  let field_data = <<
    name:bits,
    0:32,     // table_oid
    1:16,     // column
    23:32,    // type_oid (int4)
    4:16,     // type_size
    0:32,     // type_mod
    0:16,     // format (text)
  >>
  let payload = <<1:16, field_data:bits>>
  let payload_size = bit_array.byte_size(payload) + 4
  let data = <<0x54, payload_size:32-big, payload:bits>>
  let assert Decoded(RowDescription(fields: [field]), <<>>) =
    message.decode_backend(data)
  should.equal(field.name, "id")
  should.equal(field.type_oid, 23)
  should.equal(field.type_size, 4)
  should.equal(field.format, TextFormat)
}

pub fn decode_data_row_test() {
  // 2 columns: "42" (text, len=2) and NULL (-1)
  let col1 = <<"42":utf8>>
  let payload = <<2:16, 2:32, col1:bits, 255, 255, 255, 255>>
  let payload_size = bit_array.byte_size(payload) + 4
  let data = <<0x44, payload_size:32-big, payload:bits>>
  let assert Decoded(DataRow(values: values), <<>>) = message.decode_backend(data)
  let extracted = message.extract_row_values(values)
  should.equal(extracted, [Some(col1), None])
}

pub fn decode_error_response_test() {
  let payload = <<
    0x53, "ERROR":utf8, 0,
    0x43, "42P01":utf8, 0,
    0x4D, "relation \"foo\" does not exist":utf8, 0,
    0,
  >>
  let payload_size = bit_array.byte_size(payload) + 4
  let data = <<0x45, payload_size:32-big, payload:bits>>
  let assert Decoded(ErrorResponse(fields: fields), <<>>) =
    message.decode_backend(data)
  let assert Ok("ERROR") = dict.get(fields, "severity")
  let assert Ok("42P01") = dict.get(fields, "code")
  let assert Ok("relation \"foo\" does not exist") = dict.get(fields, "message")
}

pub fn decode_notice_response_test() {
  let payload = <<
    0x53, "NOTICE":utf8, 0,
    0x4D, "test notice":utf8, 0,
    0x43, "00000":utf8, 0,
    0,
  >>
  let payload_size = bit_array.byte_size(payload) + 4
  let data = <<0x4E, payload_size:32-big, payload:bits>>
  let assert Decoded(NoticeResponse(fields: fields), <<>>) =
    message.decode_backend(data)
  let assert Ok("NOTICE") = dict.get(fields, "severity")
  let assert Ok("test notice") = dict.get(fields, "message")
}

pub fn decode_notification_test() {
  // pid=12345, channel="my_channel", payload="hello"
  let inner = <<0, 0, 48, 57, "my_channel":utf8, 0, "hello":utf8, 0>>
  let payload_size = bit_array.byte_size(inner) + 4
  let data = <<0x41, payload_size:32-big, inner:bits>>
  let assert Decoded(NotificationResponse(pg_pid: pid, channel: ch, payload: p), <<>>) =
    message.decode_backend(data)
  should.equal(pid, 12345)
  should.equal(ch, "my_channel")
  should.equal(p, "hello")
}

pub fn decode_parameter_description_test() {
  // 2 params: oid 23 (int4) and oid 25 (text)
  let inner = <<0, 2, 0, 0, 0, 23, 0, 0, 0, 25>>
  let payload_size = bit_array.byte_size(inner) + 4
  let data = <<0x74, payload_size:32-big, inner:bits>>
  let assert Decoded(ParameterDescription(type_oids: oids), <<>>) =
    message.decode_backend(data)
  should.equal(oids, [23, 25])
}

pub fn decode_copy_in_response_test() {
  // Overall text format, 2 columns: text, binary
  let inner = <<0, 0, 2, 0, 0, 0, 1>>
  let payload_size = bit_array.byte_size(inner) + 4
  let data = <<0x47, payload_size:32-big, inner:bits>>
  let assert Decoded(CopyInResponse(format: TextFormat, columns: cols), <<>>) =
    message.decode_backend(data)
  should.equal(cols, [TextFormat, BinaryFormat])
}

pub fn decode_copy_out_response_test() {
  // Overall binary format, 1 binary column
  let inner = <<1, 0, 1, 0, 1>>
  let payload_size = bit_array.byte_size(inner) + 4
  let data = <<0x48, payload_size:32-big, inner:bits>>
  let assert Decoded(CopyOutResponse(format: BinaryFormat, columns: cols), <<>>) =
    message.decode_backend(data)
  should.equal(cols, [BinaryFormat])
}

pub fn decode_copy_data_test() {
  let copy_payload = <<"hello world":utf8>>
  let payload_size = bit_array.byte_size(copy_payload) + 4
  let data = <<0x64, payload_size:32-big, copy_payload:bits>>
  let assert Decoded(CopyData(data: d), <<>>) = message.decode_backend(data)
  should.equal(d, copy_payload)
}

pub fn decode_copy_done_test() {
  let assert Decoded(CopyDone, <<>>) = message.decode_backend(<<0x63, 0, 0, 0, 4>>)
}

// =============================================================================
// Incomplete / multi-message buffer tests
// =============================================================================

pub fn decode_incomplete_test() {
  let assert Incomplete = message.decode_backend(<<0x52, 0, 0>>)
}

pub fn decode_incomplete_partial_payload_test() {
  let assert Incomplete = message.decode_backend(<<0x52, 0, 0, 0, 8, 0, 0, 0>>)
}

pub fn decode_remaining_bytes_test() {
  // Two messages in buffer: AuthOk + ReadyForQuery(Idle)
  let data = <<0x52, 0, 0, 0, 8, 0, 0, 0, 0, 0x5A, 0, 0, 0, 5, 0x49>>
  let assert Decoded(AuthenticationMsg(AuthOk), rest) = message.decode_backend(data)
  should.equal(bit_array.byte_size(rest), 6)
  let assert Decoded(ReadyForQuery(message.Idle), <<>>) = message.decode_backend(rest)
}

pub fn decode_empty_buffer_test() {
  let assert Incomplete = message.decode_backend(<<>>)
}

// =============================================================================
// C-string decode utility tests
// =============================================================================

pub fn decode_cstring_test() {
  let data = <<"hello":utf8, 0, "world":utf8>>
  let assert Ok(#("hello", rest)) = message.decode_cstring(data)
  let assert Ok("world") = bit_array.to_string(rest)
}

pub fn decode_cstring_empty_test() {
  let data = <<0, "rest":utf8>>
  let assert Ok(#("", rest)) = message.decode_cstring(data)
  let assert Ok("rest") = bit_array.to_string(rest)
}

pub fn decode_cstring_no_null_test() {
  let assert Error(Nil) = message.decode_cstring(<<"no null":utf8>>)
}

// =============================================================================
// DataRow value extraction tests
// =============================================================================

pub fn extract_row_values_empty_test() {
  should.equal(message.extract_row_values(<<>>), [])
}

pub fn extract_row_values_with_null_test() {
  // -1 as int32 = 0xFFFFFFFF
  should.equal(message.extract_row_values(<<255, 255, 255, 255>>), [None])
}

pub fn extract_row_values_mixed_test() {
  let col1 = <<"hello":utf8>>
  // col1: len=5, data="hello"; col2: NULL; col3: len=2, data="42"
  let data = <<0, 0, 0, 5, col1:bits, 255, 255, 255, 255, 0, 0, 0, 2, "42":utf8>>
  let result = message.extract_row_values(data)
  should.equal(result, [Some(col1), None, Some(<<"42":utf8>>)])
}

// --- Helpers ---

fn cstring_found_in(data: BitArray, target: String) -> Bool {
  case message.decode_cstring(data) {
    Ok(#(s, _)) if s == target -> True
    Ok(#(_, rest)) -> cstring_found_in(rest, target)
    Error(_) -> False
  }
}
