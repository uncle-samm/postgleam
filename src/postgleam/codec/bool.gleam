/// PostgreSQL bool codec - binary format
/// Wire: 1 byte (0x01 = true, 0x00 = false)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Boolean}

/// OID for bool
pub const oid = 16

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "bool",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "bool",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Boolean(True) -> Ok(<<1>>)
    Boolean(False) -> Ok(<<0>>)
    _ -> Error("bool codec: expected Boolean value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<1>> -> Ok(Boolean(True))
    <<0>> -> Ok(Boolean(False))
    _ -> Error("bool codec: invalid data")
  }
}
