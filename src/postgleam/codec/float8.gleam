/// PostgreSQL float8 (double precision) codec - binary format
/// Wire: 8 bytes IEEE 754 double-precision
/// Special values: NaN, +Infinity, -Infinity

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Float, NaN, NegInfinity, PosInfinity}

/// OID for float8
pub const oid = 701

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "float8",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "float8",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Float(f) -> Ok(<<f:float-64-big>>)
    NaN -> Ok(<<0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
    PosInfinity -> Ok(<<0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
    NegInfinity -> Ok(<<0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
    value.Integer(n) -> Ok(<<int_to_float(n):float-64-big>>)
    _ -> Error("float8 codec: expected Float, NaN, PosInfinity, or NegInfinity")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    // +Infinity
    <<0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>> -> Ok(PosInfinity)
    // -Infinity
    <<0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>> -> Ok(NegInfinity)
    // NaN patterns (exponent bits all 1s, non-zero mantissa)
    <<0x7F, 0xF8, _:48>> -> Ok(NaN)
    <<0xFF, 0xF8, _:48>> -> Ok(NaN)
    <<0x7F, 0xF0, a, b, c, d, e, f>>
      if a > 0 || b > 0 || c > 0 || d > 0 || e > 0 || f > 0
    -> Ok(NaN)
    <<0xFF, 0xF0, a, b, c, d, e, f>>
      if a > 0 || b > 0 || c > 0 || d > 0 || e > 0 || f > 0
    -> Ok(NaN)
    <<f:float-64-big>> -> Ok(Float(f))
    _ -> Error("float8 codec: expected 8 bytes")
  }
}

@external(erlang, "erlang", "float")
fn int_to_float(n: Int) -> Float
