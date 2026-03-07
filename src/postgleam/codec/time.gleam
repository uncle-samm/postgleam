/// PostgreSQL time codec - binary format
/// Wire: 8 bytes int64 = microseconds since midnight

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Time}

pub const oid = 1083

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "time",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "time", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Time(usec) -> Ok(<<usec:64-big>>)
    _ -> Error("time codec: expected Time value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<usec:64-big-signed>> -> Ok(Time(usec))
    _ -> Error("time codec: expected 8 bytes")
  }
}
