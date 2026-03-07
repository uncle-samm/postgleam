/// PostgreSQL text/varchar/char/bpchar/citext/unknown/enum codec - binary format
/// Wire: variable length raw bytes (UTF-8 string)

import gleam/bit_array
import gleam/option.{Some}
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Text}

/// OIDs for text types
pub const text_oid = 25

pub const varchar_oid = 1043

pub const char_oid = 18

pub const bpchar_oid = 1042

pub const unknown_oid = 705

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "text",
    oids: [text_oid, varchar_oid, char_oid, bpchar_oid, unknown_oid],
    send: Some("textsend"),
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "text",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Text(s) -> Ok(<<s:utf8>>)
    _ -> Error("text codec: expected Text value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case bit_array.to_string(data) {
    Ok(s) -> Ok(Text(s))
    Error(_) -> Error("text codec: invalid UTF-8")
  }
}
