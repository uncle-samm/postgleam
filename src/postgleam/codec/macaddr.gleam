/// PostgreSQL macaddr codec - binary format
/// Wire: 6 bytes

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Macaddr}

pub const oid = 829

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "macaddr",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "macaddr", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Macaddr(data) ->
      case bit_array.byte_size(data) {
        6 -> Ok(data)
        _ -> Error("macaddr codec: expected 6-byte MAC address")
      }
    _ -> Error("macaddr codec: expected Macaddr value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case bit_array.byte_size(data) {
    6 -> Ok(Macaddr(data))
    _ -> Error("macaddr codec: expected 6 bytes")
  }
}
