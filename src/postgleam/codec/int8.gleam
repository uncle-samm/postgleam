/// PostgreSQL int8 (bigint) codec - binary format
/// Wire: 8 bytes signed big-endian

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Integer}

/// OID for int8
pub const oid = 20

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "int8",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "int8",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Integer(n) -> Ok(<<n:64-big>>)
    _ -> Error("int8 codec: expected Integer value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<n:64-big-signed>> -> Ok(Integer(n))
    _ -> Error("int8 codec: expected 8 bytes")
  }
}
