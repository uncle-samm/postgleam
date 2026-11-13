//// PostgreSQL numeric codec (binary format).
////
//// Wire: ndigits(int16) weight(int16) sign(uint16) dscale(int16)
//// digits(uint16[ndigits]), base-10000 digits. Special signs: NaN 0xC000,
//// +Infinity 0xD000, -Infinity 0xF000.
////
//// Values are represented as decimal strings to keep arbitrary precision.

import gleam/float
import gleam/int
import gleam/list
import gleam/option
import gleam/string
import postgleam/codec.{
  type Codec, type CodecMatcher, Binary, Codec, CodecMatcher,
}
import postgleam/value.{type Value, NaN, NegInfinity, Numeric, PosInfinity}

pub const oid = 1700

const sign_pos = 0x0000

const sign_neg = 0x4000

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "numeric",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "numeric",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    NaN -> Ok(<<0:16, 0:16, 0xC000:16, 0:16>>)
    PosInfinity -> Ok(<<0:16, 0:16, 0xD000:16, 0:16>>)
    NegInfinity -> Ok(<<0:16, 0:16, 0xF000:16, 0:16>>)
    Numeric(s) -> encode_string(s)
    // Integers and finite floats convert to numeric without surprises.
    value.Integer(n) -> encode_string(int.to_string(n))
    value.Float(f) -> encode_string(float.to_string(f))
    _ ->
      Error(
        "numeric codec: expected Numeric, Integer, Float, NaN, PosInfinity, or NegInfinity",
      )
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<_:16, _:16-signed, 0xC000:16, _:16>> -> Ok(NaN)
    <<_:16, _:16-signed, 0xD000:16, _:16>> -> Ok(PosInfinity)
    <<_:16, _:16-signed, 0xF000:16, _:16>> -> Ok(NegInfinity)
    <<ndigits:16, weight:16-signed, sign:16, dscale:16, rest:bits>> ->
      case read_digits(rest, ndigits, []) {
        Ok(digits) ->
          Ok(Numeric(digits_to_string(digits, weight, sign, dscale)))
        Error(e) -> Error(e)
      }
    _ -> Error("numeric codec: invalid data")
  }
}

// --- Encoding ---

/// Parse a decimal string such as `-12.345`, `1e5` or `.5E-3` and encode it.
/// Anything else is an error rather than a silently wrong number.
fn encode_string(input: String) -> Result(BitArray, String) {
  let invalid =
    Error("numeric codec: invalid numeric string " <> string.inspect(input))
  let s = string.trim(input)
  case string.lowercase(s) {
    "nan" -> encode(NaN)
    "infinity" | "+infinity" | "inf" | "+inf" -> encode(PosInfinity)
    "-infinity" | "-inf" -> encode(NegInfinity)
    _ -> {
      let #(sign, unsigned) = case s {
        "-" <> rest -> #(sign_neg, rest)
        "+" <> rest -> #(sign_pos, rest)
        _ -> #(sign_pos, s)
      }
      let #(mantissa, exponent) = case
        string.split_once(string.lowercase(unsigned), "e")
      {
        Ok(#(m, e)) -> #(m, parse_exponent(e))
        Error(_) -> #(unsigned, Ok(0))
      }
      let #(int_part, frac_part) = case string.split_once(mantissa, ".") {
        Ok(#(i, f)) -> #(i, f)
        Error(_) -> #(mantissa, "")
      }
      case exponent, all_digits(int_part), all_digits(frac_part) {
        Ok(exp), True, True if int_part != "" || frac_part != "" ->
          Ok(encode_parts(
            sign,
            int_part <> frac_part,
            string.length(frac_part) - exp,
          ))
        _, _, _ -> invalid
      }
    }
  }
}

fn parse_exponent(e: String) -> Result(Int, Nil) {
  let digits = case e {
    "+" <> rest -> rest
    "-" <> rest -> rest
    _ -> e
  }
  case all_digits(digits) && digits != "" {
    True -> int.parse(e)
    False -> Error(Nil)
  }
}

fn all_digits(s: String) -> Bool {
  string.to_utf_codepoints(s)
  |> list.all(fn(c) {
    let n = string.utf_codepoint_to_int(c)
    n >= 48 && n <= 57
  })
}

/// Encode value = digits × 10^(-scale).
fn encode_parts(sign: Int, digits: String, scale: Int) -> BitArray {
  // A negative scale means trailing zeros: 1e5 = "1" with scale -5.
  let #(digits, scale) = case scale < 0 {
    True -> #(digits <> string.repeat("0", -scale), 0)
    False -> #(digits, scale)
  }
  // Pad so there is at least one integer digit.
  let digits = case string.length(digits) <= scale {
    True -> string.repeat("0", scale - string.length(digits) + 1) <> digits
    False -> digits
  }
  let int_len = string.length(digits) - scale
  let int_str = string.slice(digits, 0, int_len)
  let frac_str = string.slice(digits, int_len, scale)
  // Group into base-10000 digits aligned on the decimal point.
  let int_pad = { 4 - int_len % 4 } % 4
  let frac_pad = { 4 - scale % 4 } % 4
  let int_groups = groups(string.repeat("0", int_pad) <> int_str)
  let frac_groups = groups(frac_str <> string.repeat("0", frac_pad))
  // Strip leading zero groups (adjusting weight) and trailing zero groups.
  let weight = list.length(int_groups) - 1
  let #(all, weight) =
    strip_leading(list.append(int_groups, frac_groups), weight)
  let all =
    list.reverse(all) |> list.drop_while(fn(d) { d == 0 }) |> list.reverse
  let #(sign, weight) = case all {
    // Zero: no digits, weight 0, never negative.
    [] -> #(sign_pos, 0)
    _ -> #(sign, weight)
  }
  let body = list.fold(all, <<>>, fn(acc, d) { <<acc:bits, d:16>> })
  <<list.length(all):16, weight:16, sign:16, scale:16, body:bits>>
}

fn strip_leading(digits: List(Int), weight: Int) -> #(List(Int), Int) {
  case digits {
    [0, ..rest] -> strip_leading(rest, weight - 1)
    _ -> #(digits, weight)
  }
}

fn groups(s: String) -> List(Int) {
  case string.length(s) {
    0 -> []
    _ -> {
      let assert Ok(n) = int.parse(string.slice(s, 0, 4))
      [n, ..groups(string.drop_start(s, 4))]
    }
  }
}

// --- Decoding ---

fn read_digits(
  data: BitArray,
  count: Int,
  acc: List(Int),
) -> Result(List(Int), String) {
  case count, data {
    0, _ -> Ok(list.reverse(acc))
    _, <<d:16, rest:bits>> if d < 10_000 ->
      read_digits(rest, count - 1, [d, ..acc])
    _, _ -> Error("numeric codec: truncated or invalid digits")
  }
}

fn digits_to_string(
  digits: List(Int),
  weight: Int,
  sign: Int,
  dscale: Int,
) -> String {
  let prefix = case sign == sign_neg {
    True -> "-"
    False -> ""
  }
  // Digit i (0-based) has value d × 10000^(weight - i).
  let int_count = weight + 1
  let int_digits = case int_count > 0 {
    True -> {
      let taken = list.take(digits, int_count)
      list.append(taken, list.repeat(0, int_count - list.length(taken)))
    }
    False -> []
  }
  let frac_digits = case int_count >= 0 {
    True -> list.drop(digits, int_count)
    False -> list.append(list.repeat(0, -int_count), digits)
  }
  let int_str = case int_digits {
    [] -> "0"
    [first, ..rest] ->
      int.to_string(first) <> string.concat(list.map(rest, pad4))
  }
  let frac_str =
    string.concat(list.map(frac_digits, pad4))
    |> fit(dscale)
  case dscale > 0 {
    True -> prefix <> int_str <> "." <> frac_str
    False -> prefix <> int_str
  }
}

fn pad4(d: Int) -> String {
  string.pad_start(int.to_string(d), 4, "0")
}

fn fit(s: String, len: Int) -> String {
  case string.length(s) >= len {
    True -> string.slice(s, 0, len)
    False -> s <> string.repeat("0", len - string.length(s))
  }
}
