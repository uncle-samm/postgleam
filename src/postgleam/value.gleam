/// Dynamic value type representing PostgreSQL values in binary wire format.
/// Users pattern-match on these to extract typed values.

import gleam/option.{type Option}

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
