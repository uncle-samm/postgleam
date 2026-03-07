/// PostgreSQL date codec - binary format
/// Wire: 4 bytes int32 = days since 2000-01-01

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Date}

pub const oid = 1082

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "date",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "date", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Date(days) -> Ok(<<days:32-big>>)
    _ -> Error("date codec: expected Date value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<days:32-big-signed>> -> Ok(Date(days))
    _ -> Error("date codec: expected 4 bytes")
  }
}
