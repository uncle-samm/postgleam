/// PostgreSQL time codec - binary format
/// Wire: 8 bytes int64 = microseconds since midnight
import gleam/option
import postgleam/codec.{
  type Codec, type CodecMatcher, Binary, Codec, CodecMatcher,
}
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
  Codec(
    type_name: "time",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Time(usec) ->
      case usec >= 0 && usec <= 86_400_000_000 {
        True -> Ok(<<usec:64-big>>)
        False ->
          Error("time codec: value out of range (0..86400000000 microseconds)")
      }
    _ -> Error("time codec: expected Time value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<usec:64-big-signed>> -> Ok(Time(usec))
    _ -> Error("time codec: expected 8 bytes")
  }
}
