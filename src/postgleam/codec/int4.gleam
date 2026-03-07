/// PostgreSQL int4 (integer) codec - binary format
/// Wire: 4 bytes signed big-endian

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Integer}

/// OID for int4
pub const oid = 23

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "int4",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "int4",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Integer(n) ->
      case n >= -2_147_483_648 && n <= 2_147_483_647 {
        True -> Ok(<<n:32-big>>)
        False -> Error("int4 codec: value out of range (-2147483648..2147483647)")
      }
    _ -> Error("int4 codec: expected Integer value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<n:32-big-signed>> -> Ok(Integer(n))
    _ -> Error("int4 codec: expected 4 bytes")
  }
}
