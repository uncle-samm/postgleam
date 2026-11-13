/// PostgreSQL date codec - binary format
/// Wire: 4 bytes int32 = days since 2000-01-01
import gleam/option
import postgleam/codec.{
  type Codec, type CodecMatcher, Binary, Codec, CodecMatcher,
}
import postgleam/value.{type Value, Date, NegInfinity, PosInfinity}

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
  Codec(
    type_name: "date",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    // The two ends of the range are 'infinity' and '-infinity' on the wire,
    // so a finite date can't use them.
    Date(days) ->
      case days > -2_147_483_648 && days < 2_147_483_647 {
        True -> Ok(<<days:32-big>>)
        False ->
          Error("date codec: value out of range (-2147483647..2147483646 days)")
      }
    PosInfinity -> Ok(<<0x7F, 0xFF, 0xFF, 0xFF>>)
    NegInfinity -> Ok(<<0x80, 0x00, 0x00, 0x00>>)
    _ -> Error("date codec: expected Date value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<0x7F, 0xFF, 0xFF, 0xFF>> -> Ok(PosInfinity)
    <<0x80, 0x00, 0x00, 0x00>> -> Ok(NegInfinity)
    <<days:32-big-signed>> -> Ok(Date(days))
    _ -> Error("date codec: expected 4 bytes")
  }
}
