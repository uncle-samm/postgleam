/// PostgreSQL uuid codec - binary format
/// Wire: 16 bytes raw UUID (RFC 4122)

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Uuid}

/// OID for uuid
pub const oid = 2950

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "uuid",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "uuid",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Uuid(data) ->
      case bit_array.byte_size(data) {
        16 -> Ok(data)
        _ -> Error("uuid codec: expected 16-byte UUID")
      }
    _ -> Error("uuid codec: expected Uuid value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case bit_array.byte_size(data) {
    16 -> Ok(Uuid(data))
    _ -> Error("uuid codec: expected 16 bytes")
  }
}
