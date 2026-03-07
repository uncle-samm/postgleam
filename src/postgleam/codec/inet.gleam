/// PostgreSQL inet/cidr codec - binary format
/// Wire: family(1) + netmask(1) + is_cidr(1) + addr_len(1) + address(4 or 16)

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Inet}

pub const inet_oid = 869

pub const cidr_oid = 650

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "inet",
    oids: [inet_oid, cidr_oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "inet", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Inet(family, address, netmask) -> {
      let addr_len = bit_array.byte_size(address)
      let is_cidr = case family {
        2 ->
          case netmask == 32 {
            True -> 0
            False -> 1
          }
        3 ->
          case netmask == 128 {
            True -> 0
            False -> 1
          }
        _ -> 0
      }
      Ok(<<family, netmask, is_cidr, addr_len, address:bits>>)
    }
    _ -> Error("inet codec: expected Inet value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<family, netmask, _is_cidr, addr_len, address:bits>> -> {
      case bit_array.byte_size(address) >= addr_len {
        True -> {
          let assert Ok(addr) = bit_array.slice(address, 0, addr_len)
          Ok(Inet(family, addr, netmask))
        }
        False -> Error("inet codec: address too short")
      }
    }
    _ -> Error("inet codec: invalid data")
  }
}
