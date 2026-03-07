import gleam/dict
import gleeunit/should
import postgleam/codec/defaults
import postgleam/codec/oid_codec
import postgleam/codec/registry
import postgleam/codec/void
import postgleam/value

// --- oid ---

pub fn oid_encode_zero_test() {
  oid_codec.encode(value.Oid(0))
  |> should.equal(Ok(<<0, 0, 0, 0>>))
}

pub fn oid_encode_one_test() {
  oid_codec.encode(value.Oid(1))
  |> should.equal(Ok(<<0, 0, 0, 1>>))
}

pub fn oid_encode_max_test() {
  oid_codec.encode(value.Oid(4_294_967_295))
  |> should.equal(Ok(<<255, 255, 255, 255>>))
}

pub fn oid_encode_overflow_test() {
  oid_codec.encode(value.Oid(4_294_967_296))
  |> should.be_error()
}

pub fn oid_encode_negative_test() {
  oid_codec.encode(value.Oid(-1))
  |> should.be_error()
}

pub fn oid_decode_zero_test() {
  oid_codec.decode(<<0, 0, 0, 0>>)
  |> should.equal(Ok(value.Oid(0)))
}

pub fn oid_decode_max_test() {
  oid_codec.decode(<<255, 255, 255, 255>>)
  |> should.equal(Ok(value.Oid(4_294_967_295)))
}

pub fn oid_roundtrip_test() {
  let assert Ok(encoded) = oid_codec.encode(value.Oid(12_345))
  oid_codec.decode(encoded)
  |> should.equal(Ok(value.Oid(12_345)))
}

pub fn oid_wrong_type_test() {
  oid_codec.encode(value.Integer(1))
  |> should.be_error()
}

// --- void ---

pub fn void_encode_test() {
  void.encode(value.Void)
  |> should.equal(Ok(<<>>))
}

pub fn void_decode_test() {
  void.decode(<<>>)
  |> should.equal(Ok(value.Void))
}

pub fn void_wrong_type_test() {
  void.encode(value.Integer(1))
  |> should.be_error()
}

// --- registry ---

pub fn registry_build_test() {
  let reg = registry.build(defaults.matchers())
  // Should have entries for all registered OIDs
  // bool=16, int2=21, int4=23, int8=20, float4=700, float8=701,
  // text=25, varchar=1043, char=18, bpchar=1042, unknown=705,
  // bytea=17, uuid=2950, oid=26, regproc=24, regclass=2205,
  // regtype=2206, xid=28, cid=29, name=19, void=2278
  should.be_true(dict.size(reg) >= 20)
}

pub fn registry_lookup_bool_test() {
  let reg = registry.build(defaults.matchers())
  let assert Ok(codec) = registry.lookup(reg, 16)
  should.equal(codec.type_name, "bool")
}

pub fn registry_lookup_int4_test() {
  let reg = registry.build(defaults.matchers())
  let assert Ok(codec) = registry.lookup(reg, 23)
  should.equal(codec.type_name, "int4")
}

pub fn registry_lookup_missing_test() {
  let reg = registry.build(defaults.matchers())
  registry.lookup(reg, 99_999)
  |> should.be_error()
}

pub fn registry_encode_decode_via_lookup_test() {
  let reg = registry.build(defaults.matchers())
  let assert Ok(codec) = registry.lookup(reg, 23)
  let assert Ok(encoded) = codec.encode(value.Integer(42))
  let assert Ok(decoded) = codec.decode(encoded)
  should.equal(decoded, value.Integer(42))
}
