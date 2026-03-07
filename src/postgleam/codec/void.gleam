/// PostgreSQL void codec - binary format
/// Wire: 0 bytes (empty payload)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Void}

/// OID for void
pub const oid = 2278

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "void",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "void",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Void -> Ok(<<>>)
    _ -> Error("void codec: expected Void value")
  }
}

pub fn decode(_data: BitArray) -> Result(Value, String) {
  Ok(Void)
}
