/// PostgreSQL OID codec - binary format
/// Wire: 4 bytes unsigned big-endian
/// Handles: oid, regclass, regtype, regproc, xid, cid, etc.

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Oid}

/// OIDs for OID-like types
pub const oid_oid = 26

pub const regproc_oid = 24

pub const regclass_oid = 2205

pub const regtype_oid = 2206

pub const xid_oid = 28

pub const cid_oid = 29

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "oid",
    oids: [oid_oid, regproc_oid, regclass_oid, regtype_oid, xid_oid, cid_oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "oid",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Oid(n) ->
      case n >= 0 && n <= 4_294_967_295 {
        True -> Ok(<<n:32-big>>)
        False -> Error("oid codec: value out of range (0..4294967295)")
      }
    _ -> Error("oid codec: expected Oid value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<n:32-big>> -> Ok(Oid(n))
    _ -> Error("oid codec: expected 4 bytes")
  }
}
