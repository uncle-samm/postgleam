/// PostgreSQL timestamp codec - binary format
/// Wire: 8 bytes int64 = microseconds since 2000-01-01 00:00:00
/// Special: int64 max = +infinity, int64 min = -infinity
import gleam/option
import postgleam/codec.{
  type Codec, type CodecMatcher, Binary, Codec, CodecMatcher,
}
import postgleam/value.{type Value, NegInfinity, PosInfinity, Timestamp}

pub const oid = 1114

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "timestamp",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "timestamp",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    // The two ends of the range are 'infinity' and '-infinity' on the wire,
    // so a finite timestamp can't use them.
    Timestamp(usec) ->
      case
        usec > -9_223_372_036_854_775_808 && usec < 9_223_372_036_854_775_807
      {
        True -> Ok(<<usec:64-big>>)
        False ->
          Error("timestamp codec: value out of range for a 64-bit integer")
      }
    PosInfinity -> Ok(<<0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>)
    NegInfinity -> Ok(<<0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>)
    _ -> Error("timestamp codec: expected Timestamp value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>> -> Ok(PosInfinity)
    <<0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>> -> Ok(NegInfinity)
    <<usec:64-big-signed>> -> Ok(Timestamp(usec))
    _ -> Error("timestamp codec: expected 8 bytes")
  }
}
