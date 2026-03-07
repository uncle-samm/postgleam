import gleeunit/should
import postgleam/codec/inet
import postgleam/codec/macaddr
import postgleam/codec/point
import postgleam/value

// --- point ---

pub fn point_encode_test() {
  let assert Ok(encoded) = point.encode(value.Point(1.0, 2.0))
  should.equal(encoded, <<1.0:float-64-big, 2.0:float-64-big>>)
}

pub fn point_decode_test() {
  point.decode(<<1.0:float-64-big, 2.0:float-64-big>>)
  |> should.equal(Ok(value.Point(1.0, 2.0)))
}

pub fn point_roundtrip_test() {
  let assert Ok(encoded) = point.encode(value.Point(-3.5, 7.25))
  point.decode(encoded)
  |> should.equal(Ok(value.Point(-3.5, 7.25)))
}

pub fn point_wrong_type_test() {
  point.encode(value.Integer(1))
  |> should.be_error()
}

pub fn point_origin_test() {
  let assert Ok(encoded) = point.encode(value.Point(0.0, 0.0))
  point.decode(encoded)
  |> should.equal(Ok(value.Point(0.0, 0.0)))
}

// --- inet ---

pub fn inet_encode_ipv4_test() {
  let addr = <<192, 168, 1, 1>>
  let assert Ok(encoded) = inet.encode(value.Inet(2, addr, 32))
  should.equal(encoded, <<2, 32, 0, 4, 192, 168, 1, 1>>)
}

pub fn inet_decode_ipv4_test() {
  let assert Ok(value.Inet(family, addr, netmask)) =
    inet.decode(<<2, 24, 0, 4, 10, 0, 0, 0>>)
  should.equal(family, 2)
  should.equal(addr, <<10, 0, 0, 0>>)
  should.equal(netmask, 24)
}

pub fn inet_roundtrip_ipv4_test() {
  let addr = <<172, 16, 0, 1>>
  let assert Ok(encoded) = inet.encode(value.Inet(2, addr, 16))
  let assert Ok(value.Inet(family, decoded_addr, netmask)) = inet.decode(encoded)
  should.equal(family, 2)
  should.equal(decoded_addr, addr)
  should.equal(netmask, 16)
}

pub fn inet_ipv6_test() {
  // ::1 (loopback)
  let addr = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>
  let assert Ok(encoded) = inet.encode(value.Inet(3, addr, 128))
  let assert Ok(value.Inet(family, decoded_addr, netmask)) = inet.decode(encoded)
  should.equal(family, 3)
  should.equal(decoded_addr, addr)
  should.equal(netmask, 128)
}

pub fn inet_wrong_type_test() {
  inet.encode(value.Integer(1))
  |> should.be_error()
}

// --- macaddr ---

pub fn macaddr_encode_test() {
  let addr = <<0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF>>
  macaddr.encode(value.Macaddr(addr))
  |> should.equal(Ok(addr))
}

pub fn macaddr_decode_test() {
  let addr = <<0x00, 0x11, 0x22, 0x33, 0x44, 0x55>>
  macaddr.decode(addr)
  |> should.equal(Ok(value.Macaddr(addr)))
}

pub fn macaddr_roundtrip_test() {
  let addr = <<0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01>>
  let assert Ok(encoded) = macaddr.encode(value.Macaddr(addr))
  macaddr.decode(encoded)
  |> should.equal(Ok(value.Macaddr(addr)))
}

pub fn macaddr_wrong_size_test() {
  macaddr.encode(value.Macaddr(<<1, 2, 3>>))
  |> should.be_error()
}

pub fn macaddr_wrong_type_test() {
  macaddr.encode(value.Integer(1))
  |> should.be_error()
}
