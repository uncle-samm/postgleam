/// PostgreSQL jsonb codec - binary format
/// Wire: 1 byte version (0x01) + raw UTF-8 JSON string

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Jsonb}

pub const oid = 3802

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "jsonb",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "jsonb", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Jsonb(s) -> Ok(<<1, s:utf8>>)
    _ -> Error("jsonb codec: expected Jsonb value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<1, json_data:bits>> ->
      case bit_array.to_string(json_data) {
        Ok(s) -> Ok(Jsonb(s))
        Error(_) -> Error("jsonb codec: invalid UTF-8")
      }
    _ -> Error("jsonb codec: missing version byte or invalid data")
  }
}
