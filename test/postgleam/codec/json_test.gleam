import gleeunit/should
import postgleam/codec/json
import postgleam/codec/jsonb
import postgleam/value

pub fn json_encode_test() {
  let assert Ok(encoded) = json.encode(value.Json("{\"key\":\"value\"}"))
  should.equal(encoded, <<"{\"key\":\"value\"}":utf8>>)
}

pub fn json_decode_test() {
  json.decode(<<"{\"key\":\"value\"}":utf8>>)
  |> should.equal(Ok(value.Json("{\"key\":\"value\"}")))
}

pub fn json_roundtrip_test() {
  let s = "[1,2,3]"
  let assert Ok(encoded) = json.encode(value.Json(s))
  json.decode(encoded)
  |> should.equal(Ok(value.Json(s)))
}

pub fn json_empty_test() {
  let assert Ok(encoded) = json.encode(value.Json("null"))
  json.decode(encoded)
  |> should.equal(Ok(value.Json("null")))
}

pub fn json_wrong_type_test() {
  json.encode(value.Integer(1))
  |> should.be_error()
}

// --- jsonb ---

pub fn jsonb_encode_test() {
  let assert Ok(encoded) = jsonb.encode(value.Jsonb("{\"a\":1}"))
  // Should have version byte 0x01 prefix
  should.equal(encoded, <<1, "{\"a\":1}":utf8>>)
}

pub fn jsonb_decode_test() {
  jsonb.decode(<<1, "{\"a\":1}":utf8>>)
  |> should.equal(Ok(value.Jsonb("{\"a\":1}")))
}

pub fn jsonb_decode_missing_version_test() {
  jsonb.decode(<<"{\"a\":1}":utf8>>)
  |> should.be_error()
}

pub fn jsonb_roundtrip_test() {
  let s = "[true, false, null]"
  let assert Ok(encoded) = jsonb.encode(value.Jsonb(s))
  jsonb.decode(encoded)
  |> should.equal(Ok(value.Jsonb(s)))
}

pub fn jsonb_wrong_type_test() {
  jsonb.encode(value.Integer(1))
  |> should.be_error()
}
