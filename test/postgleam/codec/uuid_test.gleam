import gleam/bit_array
import gleeunit/should
import postgleam/codec/uuid

import postgleam/value

pub fn encode_test() {
  let data =
    <<0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66,
      0x55, 0x44, 0x00, 0x00>>
  uuid.encode(value.Uuid(data))
  |> should.equal(Ok(data))
}

pub fn encode_wrong_size_test() {
  uuid.encode(value.Uuid(<<1, 2, 3>>))
  |> should.be_error()
}

pub fn decode_test() {
  let data =
    <<0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66,
      0x55, 0x44, 0x00, 0x00>>
  uuid.decode(data)
  |> should.equal(Ok(value.Uuid(data)))
}

pub fn decode_wrong_size_test() {
  uuid.decode(<<1, 2, 3>>)
  |> should.be_error()
}

pub fn roundtrip_test() {
  let data =
    <<0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9,
      0xbd, 0x38, 0x0a, 0x11>>
  let assert Ok(encoded) = uuid.encode(value.Uuid(data))
  uuid.decode(encoded)
  |> should.equal(Ok(value.Uuid(data)))
}

pub fn all_zeros_test() {
  let data = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
  let assert Ok(encoded) = uuid.encode(value.Uuid(data))
  should.equal(bit_array.byte_size(encoded), 16)
}

pub fn wrong_type_test() {
  uuid.encode(value.Integer(1))
  |> should.be_error()
}
