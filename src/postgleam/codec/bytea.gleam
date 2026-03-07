/// PostgreSQL bytea codec - binary format
/// Wire: variable length raw bytes

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Bytea}

/// OID for bytea
pub const oid = 17

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "bytea",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "bytea",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Bytea(data) -> Ok(data)
    _ -> Error("bytea codec: expected Bytea value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  Ok(Bytea(data))
}
