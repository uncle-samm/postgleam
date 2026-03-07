import gleeunit/should
import postgleam/codec/bool
import postgleam/value

pub fn encode_true_test() {
  bool.encode(value.Boolean(True))
  |> should.equal(Ok(<<1>>))
}

pub fn encode_false_test() {
  bool.encode(value.Boolean(False))
  |> should.equal(Ok(<<0>>))
}

pub fn decode_true_test() {
  bool.decode(<<1>>)
  |> should.equal(Ok(value.Boolean(True)))
}

pub fn decode_false_test() {
  bool.decode(<<0>>)
  |> should.equal(Ok(value.Boolean(False)))
}

pub fn decode_invalid_test() {
  bool.decode(<<2>>)
  |> should.be_error()
}

pub fn encode_wrong_type_test() {
  bool.encode(value.Integer(1))
  |> should.be_error()
}

pub fn roundtrip_true_test() {
  let assert Ok(encoded) = bool.encode(value.Boolean(True))
  bool.decode(encoded)
  |> should.equal(Ok(value.Boolean(True)))
}

pub fn roundtrip_false_test() {
  let assert Ok(encoded) = bool.encode(value.Boolean(False))
  bool.decode(encoded)
  |> should.equal(Ok(value.Boolean(False)))
}
