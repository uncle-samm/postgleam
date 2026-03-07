/// PostgreSQL timetz codec - binary format
/// Wire: 8 bytes int64 (microseconds) + 4 bytes int32 (tz offset seconds)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, TimeTz}

pub const oid = 1266

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "timetz",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "timetz", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    TimeTz(usec, tz_offset) -> Ok(<<usec:64-big, tz_offset:32-big>>)
    _ -> Error("timetz codec: expected TimeTz value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<usec:64-big-signed, tz_offset:32-big-signed>> -> Ok(TimeTz(usec, tz_offset))
    _ -> Error("timetz codec: expected 12 bytes")
  }
}
