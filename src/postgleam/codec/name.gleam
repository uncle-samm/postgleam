/// PostgreSQL name codec - binary format
/// Wire: variable length UTF-8, max 63 bytes

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Text}

/// OID for name
pub const oid = 19

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "name",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "name",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Text(s) -> {
      let bytes = <<s:utf8>>
      case bit_array.byte_size(bytes) < 64 {
        True -> Ok(bytes)
        False -> Error("name codec: value must be < 64 bytes")
      }
    }
    _ -> Error("name codec: expected Text value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case bit_array.to_string(data) {
    Ok(s) -> Ok(Text(s))
    Error(_) -> Error("name codec: invalid UTF-8")
  }
}
