/// PostgreSQL float4 (real) codec - binary format
/// Wire: 4 bytes IEEE 754 single-precision
/// Special values: NaN, +Infinity, -Infinity
import gleam/int
import gleam/option
import postgleam/codec.{
  type Codec, type CodecMatcher, Binary, Codec, CodecMatcher,
}
import postgleam/value.{type Value, Float, NaN, NegInfinity, PosInfinity}

/// OID for float4
pub const oid = 700

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "float4",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "float4",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Float(f) -> encode_checked(f)
    NaN -> Ok(<<0x7F, 0xC0, 0x00, 0x00>>)
    PosInfinity -> Ok(<<0x7F, 0x80, 0x00, 0x00>>)
    NegInfinity -> Ok(<<0xFF, 0x80, 0x00, 0x00>>)
    value.Integer(n) -> encode_checked(int.to_float(n))
    _ -> Error("float4 codec: expected Float, NaN, PosInfinity, or NegInfinity")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    // +Infinity: sign=0, exponent=0xFF, mantissa=0
    <<0x7F, 0x80, 0x00, 0x00>> -> Ok(PosInfinity)
    // -Infinity: sign=1, exponent=0xFF, mantissa=0
    <<0xFF, 0x80, 0x00, 0x00>> -> Ok(NegInfinity)
    // NaN: exponent=0xFF, non-zero mantissa
    <<0x7F, hi, _:16>> if hi > 0x80 -> Ok(NaN)
    <<0xFF, hi, _:16>> if hi > 0x80 -> Ok(NaN)
    <<0x7F, 0x80, a, b>> if a > 0 || b > 0 -> Ok(NaN)
    <<0xFF, 0x80, a, b>> if a > 0 || b > 0 -> Ok(NaN)
    _ -> {
      case decode_float32(data) {
        Ok(f) -> Ok(Float(f))
        Error(e) -> Error(e)
      }
    }
  }
}

const float4_max = 3.4028234663852886e38

fn encode_checked(f: Float) -> Result(BitArray, String) {
  case f >. float4_max || f <. 0.0 -. float4_max {
    True -> Error("float4 codec: value out of range for real")
    False -> Ok(encode_float32(f))
  }
}

fn encode_float32(f: Float) -> BitArray {
  <<f:float-size(32)>>
}

fn decode_float32(data: BitArray) -> Result(Float, String) {
  case data {
    <<f:float-size(32)>> -> Ok(f)
    _ -> Error("float4 codec: expected 4 bytes")
  }
}
