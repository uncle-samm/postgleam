import gleeunit/should
import postgleam/codec/int2
import postgleam/codec/int4
import postgleam/codec/int8
import postgleam/value

// --- int2 ---

pub fn int2_encode_zero_test() {
  int2.encode(value.Integer(0))
  |> should.equal(Ok(<<0, 0>>))
}

pub fn int2_encode_positive_test() {
  int2.encode(value.Integer(42))
  |> should.equal(Ok(<<0, 42>>))
}

pub fn int2_encode_negative_test() {
  let assert Ok(encoded) = int2.encode(value.Integer(-1))
  // -1 in 16-bit two's complement = 0xFFFF
  should.equal(encoded, <<255, 255>>)
}

pub fn int2_encode_max_test() {
  int2.encode(value.Integer(32_767))
  |> should.be_ok()
}

pub fn int2_encode_min_test() {
  int2.encode(value.Integer(-32_768))
  |> should.be_ok()
}

pub fn int2_encode_overflow_test() {
  int2.encode(value.Integer(32_768))
  |> should.be_error()
}

pub fn int2_encode_underflow_test() {
  int2.encode(value.Integer(-32_769))
  |> should.be_error()
}

pub fn int2_decode_zero_test() {
  int2.decode(<<0, 0>>)
  |> should.equal(Ok(value.Integer(0)))
}

pub fn int2_decode_positive_test() {
  int2.decode(<<0, 42>>)
  |> should.equal(Ok(value.Integer(42)))
}

pub fn int2_decode_negative_test() {
  int2.decode(<<255, 255>>)
  |> should.equal(Ok(value.Integer(-1)))
}

pub fn int2_decode_invalid_test() {
  int2.decode(<<0>>)
  |> should.be_error()
}

pub fn int2_roundtrip_test() {
  let vals = [0, 1, -1, 32_767, -32_768, 256, -256]
  roundtrip_list(vals, int2.encode, int2.decode)
}

// --- int4 ---

pub fn int4_encode_zero_test() {
  int4.encode(value.Integer(0))
  |> should.equal(Ok(<<0, 0, 0, 0>>))
}

pub fn int4_encode_positive_test() {
  int4.encode(value.Integer(1))
  |> should.equal(Ok(<<0, 0, 0, 1>>))
}

pub fn int4_encode_large_test() {
  int4.encode(value.Integer(100_000))
  |> should.be_ok()
}

pub fn int4_encode_max_test() {
  int4.encode(value.Integer(2_147_483_647))
  |> should.be_ok()
}

pub fn int4_encode_min_test() {
  int4.encode(value.Integer(-2_147_483_648))
  |> should.be_ok()
}

pub fn int4_encode_overflow_test() {
  int4.encode(value.Integer(2_147_483_648))
  |> should.be_error()
}

pub fn int4_decode_zero_test() {
  int4.decode(<<0, 0, 0, 0>>)
  |> should.equal(Ok(value.Integer(0)))
}

pub fn int4_decode_negative_test() {
  int4.decode(<<255, 255, 255, 255>>)
  |> should.equal(Ok(value.Integer(-1)))
}

pub fn int4_roundtrip_test() {
  let vals = [0, 1, -1, 2_147_483_647, -2_147_483_648, 100_000, -100_000]
  roundtrip_list(vals, int4.encode, int4.decode)
}

// --- int8 ---

pub fn int8_encode_zero_test() {
  int8.encode(value.Integer(0))
  |> should.equal(Ok(<<0, 0, 0, 0, 0, 0, 0, 0>>))
}

pub fn int8_encode_positive_test() {
  int8.encode(value.Integer(1))
  |> should.equal(Ok(<<0, 0, 0, 0, 0, 0, 0, 1>>))
}

pub fn int8_encode_large_test() {
  // 2^40 = 1099511627776
  int8.encode(value.Integer(1_099_511_627_776))
  |> should.be_ok()
}

pub fn int8_decode_zero_test() {
  int8.decode(<<0, 0, 0, 0, 0, 0, 0, 0>>)
  |> should.equal(Ok(value.Integer(0)))
}

pub fn int8_decode_negative_test() {
  int8.decode(<<255, 255, 255, 255, 255, 255, 255, 255>>)
  |> should.equal(Ok(value.Integer(-1)))
}

pub fn int8_roundtrip_test() {
  let vals = [0, 1, -1, 1_099_511_627_776, -1_099_511_627_776]
  roundtrip_list(vals, int8.encode, int8.decode)
}

pub fn int8_wrong_type_test() {
  int8.encode(value.Boolean(True))
  |> should.be_error()
}

// --- helper ---

fn roundtrip_list(
  vals: List(Int),
  encode: fn(value.Value) -> Result(BitArray, String),
  decode: fn(BitArray) -> Result(value.Value, String),
) -> Nil {
  case vals {
    [] -> Nil
    [v, ..rest] -> {
      let assert Ok(encoded) = encode(value.Integer(v))
      let assert Ok(decoded) = decode(encoded)
      should.equal(decoded, value.Integer(v))
      roundtrip_list(rest, encode, decode)
    }
  }
}
