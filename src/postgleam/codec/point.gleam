/// PostgreSQL point codec - binary format
/// Wire: 16 bytes = float64 x + float64 y

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Point}

pub const oid = 600

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "point",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "point", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Point(x, y) -> Ok(<<x:float-64-big, y:float-64-big>>)
    _ -> Error("point codec: expected Point value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<x:float-64-big, y:float-64-big>> -> Ok(Point(x, y))
    _ -> Error("point codec: expected 16 bytes")
  }
}
