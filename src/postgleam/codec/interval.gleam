/// PostgreSQL interval codec - binary format
/// Wire: 8 bytes int64 (microseconds) + 4 bytes int32 (days) + 4 bytes int32 (months)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Interval}

pub const oid = 1186

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "interval",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "interval", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Interval(usec, days, months) ->
      Ok(<<usec:64-big, days:32-big, months:32-big>>)
    _ -> Error("interval codec: expected Interval value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<usec:64-big-signed, days:32-big-signed, months:32-big-signed>> ->
      Ok(Interval(usec, days, months))
    _ -> Error("interval codec: expected 16 bytes")
  }
}
