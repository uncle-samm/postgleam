/// PostgreSQL int2 (smallint) codec - binary format
/// Wire: 2 bytes signed big-endian

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Integer}

/// OID for int2
pub const oid = 21

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "int2",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "int2",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Integer(n) ->
      case n >= -32_768 && n <= 32_767 {
        True -> Ok(<<n:16-big>>)
        False -> Error("int2 codec: value out of range (-32768..32767)")
      }
    _ -> Error("int2 codec: expected Integer value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<n:16-big-signed>> -> Ok(Integer(n))
    _ -> Error("int2 codec: expected 2 bytes")
  }
}
