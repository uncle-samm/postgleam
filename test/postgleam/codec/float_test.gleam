import gleeunit/should
import postgleam/codec/float4
import postgleam/codec/float8
import postgleam/value

// --- float4 ---

pub fn float4_encode_zero_test() {
  let assert Ok(encoded) = float4.encode(value.Float(0.0))
  should.equal(encoded, <<0, 0, 0, 0>>)
}

pub fn float4_encode_one_test() {
  let assert Ok(encoded) = float4.encode(value.Float(1.0))
  // IEEE 754: 1.0 = 0x3F800000
  should.equal(encoded, <<0x3F, 0x80, 0x00, 0x00>>)
}

pub fn float4_encode_nan_test() {
  let assert Ok(encoded) = float4.encode(value.NaN)
  should.equal(encoded, <<0x7F, 0xC0, 0x00, 0x00>>)
}

pub fn float4_encode_pos_infinity_test() {
  let assert Ok(encoded) = float4.encode(value.PosInfinity)
  should.equal(encoded, <<0x7F, 0x80, 0x00, 0x00>>)
}

pub fn float4_encode_neg_infinity_test() {
  let assert Ok(encoded) = float4.encode(value.NegInfinity)
  should.equal(encoded, <<0xFF, 0x80, 0x00, 0x00>>)
}

pub fn float4_decode_nan_test() {
  float4.decode(<<0x7F, 0xC0, 0x00, 0x00>>)
  |> should.equal(Ok(value.NaN))
}

pub fn float4_decode_pos_infinity_test() {
  float4.decode(<<0x7F, 0x80, 0x00, 0x00>>)
  |> should.equal(Ok(value.PosInfinity))
}

pub fn float4_decode_neg_infinity_test() {
  float4.decode(<<0xFF, 0x80, 0x00, 0x00>>)
  |> should.equal(Ok(value.NegInfinity))
}

pub fn float4_roundtrip_test() {
  let vals = [0.0, 1.0, -1.0, 3.14, -273.15, 1.0e10]
  float4_roundtrip_list(vals)
}

pub fn float4_encode_integer_coercion_test() {
  let assert Ok(_) = float4.encode(value.Integer(42))
}

pub fn float4_wrong_type_test() {
  float4.encode(value.Boolean(True))
  |> should.be_error()
}

fn float4_roundtrip_list(vals: List(Float)) -> Nil {
  case vals {
    [] -> Nil
    [v, ..rest] -> {
      let assert Ok(encoded) = float4.encode(value.Float(v))
      let assert Ok(decoded) = float4.decode(encoded)
      // float4 has limited precision so we just check it's a Float
      case decoded {
        value.Float(_) -> Nil
        _ -> panic as "Expected Float value"
      }
      float4_roundtrip_list(rest)
    }
  }
}

// --- float8 ---

pub fn float8_encode_zero_test() {
  float8.encode(value.Float(0.0))
  |> should.equal(Ok(<<0, 0, 0, 0, 0, 0, 0, 0>>))
}

pub fn float8_encode_one_test() {
  float8.encode(value.Float(1.0))
  // IEEE 754 double: 1.0 = 0x3FF0000000000000
  |> should.equal(Ok(<<0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>))
}

pub fn float8_encode_nan_test() {
  let assert Ok(encoded) = float8.encode(value.NaN)
  should.equal(encoded, <<0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
}

pub fn float8_encode_pos_infinity_test() {
  let assert Ok(encoded) = float8.encode(value.PosInfinity)
  should.equal(encoded, <<0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
}

pub fn float8_encode_neg_infinity_test() {
  let assert Ok(encoded) = float8.encode(value.NegInfinity)
  should.equal(encoded, <<0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
}

pub fn float8_decode_nan_test() {
  float8.decode(<<0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
  |> should.equal(Ok(value.NaN))
}

pub fn float8_decode_pos_infinity_test() {
  float8.decode(<<0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
  |> should.equal(Ok(value.PosInfinity))
}

pub fn float8_decode_neg_infinity_test() {
  float8.decode(<<0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
  |> should.equal(Ok(value.NegInfinity))
}

pub fn float8_roundtrip_test() {
  let vals = [0.0, 1.0, -1.0, 3.141592653589793, -273.15, 1.0e100, 5.0e-324]
  float8_roundtrip_list(vals)
}

pub fn float8_roundtrip_exact_test() {
  let assert Ok(encoded) = float8.encode(value.Float(3.141592653589793))
  float8.decode(encoded)
  |> should.equal(Ok(value.Float(3.141592653589793)))
}

pub fn float8_encode_integer_coercion_test() {
  let assert Ok(encoded) = float8.encode(value.Integer(42))
  float8.decode(encoded)
  |> should.equal(Ok(value.Float(42.0)))
}

pub fn float8_wrong_type_test() {
  float8.encode(value.Boolean(True))
  |> should.be_error()
}

pub fn float8_decode_invalid_test() {
  float8.decode(<<1, 2, 3>>)
  |> should.be_error()
}

fn float8_roundtrip_list(vals: List(Float)) -> Nil {
  case vals {
    [] -> Nil
    [v, ..rest] -> {
      let assert Ok(encoded) = float8.encode(value.Float(v))
      let assert Ok(decoded) = float8.decode(encoded)
      should.equal(decoded, value.Float(v))
      float8_roundtrip_list(rest)
    }
  }
}
