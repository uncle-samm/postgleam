/// Dynamic value type representing PostgreSQL values in binary wire format.
/// Users pattern-match on these to extract typed values.

import gleam/bit_array
import gleam/option.{type Option}
import gleam/string

/// A PostgreSQL value
pub type Value {
  /// NULL
  Null
  /// Boolean
  Boolean(Bool)
  /// Integer (int2, int4, int8)
  Integer(Int)
  /// Float (float4, float8) - normal finite values
  Float(Float)
  /// Positive infinity (float4, float8, timestamp, timestamptz)
  PosInfinity
  /// Negative infinity (float4, float8, timestamp, timestamptz)
  NegInfinity
  /// NaN (float4, float8, numeric)
  NaN
  /// Text string (text, varchar, char, bpchar, name, citext, unknown, enum)
  Text(String)
  /// Raw bytes (bytea)
  Bytea(BitArray)
  /// UUID as 16-byte binary
  Uuid(BitArray)
  /// OID (oid, regclass, regtype, xid, cid, etc.)
  Oid(Int)
  /// Void (void type, no data)
  Void
  /// Array of values
  Array(List(Option(Value)))
  /// Date as days since 2000-01-01 (negative for dates before)
  Date(Int)
  /// Time as microseconds since midnight
  Time(Int)
  /// TimeTZ as microseconds since midnight + timezone offset in seconds
  TimeTz(microseconds: Int, tz_offset: Int)
  /// Timestamp as microseconds since 2000-01-01 00:00:00
  Timestamp(Int)
  /// Timestamptz as microseconds since 2000-01-01 00:00:00 UTC
  Timestamptz(Int)
  /// Interval: microseconds, days, months
  Interval(microseconds: Int, days: Int, months: Int)
  /// JSON string (already serialized)
  Json(String)
  /// JSONB string (already serialized)
  Jsonb(String)
  /// Numeric as string representation (preserving precision)
  Numeric(String)
  /// Point (x, y)
  Point(x: Float, y: Float)
  /// Inet/CIDR address: family (2=ipv4, 3=ipv6), address bytes, netmask
  Inet(family: Int, address: BitArray, netmask: Int)
  /// MAC address (6 bytes)
  Macaddr(BitArray)
}

/// Parse a UUID string into a Uuid value.
/// Accepts formats: "550e8400-e29b-41d4-a716-446655440000"
/// or "550e8400e29b41d4a716446655440000" (with or without dashes).
pub fn uuid_from_string(s: String) -> Result(Value, Nil) {
  let bytes = bit_array.from_string(s)
  case bytes {
    // Hyphenated: 8-4-4-4-12 (36 chars)
    <<
      a1, a2, a3, a4, a5, a6, a7, a8, 0x2D,
      b1, b2, b3, b4, 0x2D,
      c1, c2, c3, c4, 0x2D,
      d1, d2, d3, d4, 0x2D,
      e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12,
    >> ->
      decode_hex_bytes(<<
        a1, a2, a3, a4, a5, a6, a7, a8,
        b1, b2, b3, b4,
        c1, c2, c3, c4,
        d1, d2, d3, d4,
        e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12,
      >>, <<>>)
    _ ->
      // Try without dashes (32 chars)
      case bit_array.byte_size(bytes) {
        32 -> decode_hex_bytes(bytes, <<>>)
        _ -> Error(Nil)
      }
  }
}

/// Format a Uuid value as a hyphenated string.
/// Returns "550e8400-e29b-41d4-a716-446655440000" format.
pub fn uuid_to_string(val: Value) -> Result(String, Nil) {
  case val {
    Uuid(<<a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p>>) -> {
      Ok(
        byte_to_hex(a)
        <> byte_to_hex(b)
        <> byte_to_hex(c)
        <> byte_to_hex(d)
        <> "-"
        <> byte_to_hex(e)
        <> byte_to_hex(f)
        <> "-"
        <> byte_to_hex(g)
        <> byte_to_hex(h)
        <> "-"
        <> byte_to_hex(i)
        <> byte_to_hex(j)
        <> "-"
        <> byte_to_hex(k)
        <> byte_to_hex(l)
        <> byte_to_hex(m)
        <> byte_to_hex(n)
        <> byte_to_hex(o)
        <> byte_to_hex(p),
      )
    }
    _ -> Error(Nil)
  }
}

fn decode_hex_bytes(
  input: BitArray,
  acc: BitArray,
) -> Result(Value, Nil) {
  case input {
    <<>> ->
      case bit_array.byte_size(acc) {
        16 -> Ok(Uuid(acc))
        _ -> Error(Nil)
      }
    <<hi, lo, rest:bytes>> ->
      case hex_byte(hi), hex_byte(lo) {
        Ok(h), Ok(l) -> decode_hex_bytes(rest, <<acc:bits, { h * 16 + l }>>)
        _, _ -> Error(Nil)
      }
    _ -> Error(Nil)
  }
}

fn hex_byte(b: Int) -> Result(Int, Nil) {
  case b {
    _ if b >= 0x30 && b <= 0x39 -> Ok(b - 0x30)
    _ if b >= 0x61 && b <= 0x66 -> Ok(b - 0x61 + 10)
    _ if b >= 0x41 && b <= 0x46 -> Ok(b - 0x41 + 10)
    _ -> Error(Nil)
  }
}

const hex_chars = "0123456789abcdef"

fn byte_to_hex(b: Int) -> String {
  let hi = string.slice(hex_chars, b / 16, 1)
  let lo = string.slice(hex_chars, b % 16, 1)
  hi <> lo
}
