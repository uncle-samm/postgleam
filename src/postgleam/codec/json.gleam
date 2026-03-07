/// PostgreSQL json codec - binary format
/// Wire: raw UTF-8 JSON string (no prefix)

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Json}

pub const oid = 114

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "json",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "json", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Json(s) -> Ok(<<s:utf8>>)
    _ -> Error("json codec: expected Json value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case bit_array.to_string(data) {
    Ok(s) -> Ok(Json(s))
    Error(_) -> Error("json codec: invalid UTF-8")
  }
}
