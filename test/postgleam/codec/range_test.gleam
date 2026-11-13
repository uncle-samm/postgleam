//// Values that don't fit their wire type are refused. A bit array segment
//// wraps silently, so without these checks `Integer(2^63)` was stored as
//// the most negative int8.

import gleeunit/should
import postgleam/codec/date
import postgleam/codec/int8
import postgleam/codec/interval
import postgleam/codec/money
import postgleam/codec/time
import postgleam/codec/timestamp
import postgleam/codec/timestamptz
import postgleam/codec/timetz
import postgleam/value

const int64_max = 9_223_372_036_854_775_807

const int64_min = -9_223_372_036_854_775_808

pub fn int8_range_test() {
  let assert Ok(_) = int8.encode(value.Integer(int64_max))
  let assert Ok(_) = int8.encode(value.Integer(int64_min))
  let assert Error(_) = int8.encode(value.Integer(int64_max + 1))
  let assert Error(_) = int8.encode(value.Integer(int64_min - 1))
}

pub fn money_range_test() {
  let assert Ok(_) = money.encode(value.Money(int64_max))
  let assert Error(_) = money.encode(value.Money(int64_max + 1))
  let assert Error(_) = money.encode(value.Money(int64_min - 1))
}

pub fn date_range_test() {
  let assert Ok(_) = date.encode(value.Date(2_147_483_646))
  let assert Ok(_) = date.encode(value.Date(-2_147_483_647))
  // The two ends are infinity on the wire, not dates.
  let assert Error(_) = date.encode(value.Date(2_147_483_647))
  let assert Error(_) = date.encode(value.Date(-2_147_483_648))
  let assert Error(_) = date.encode(value.Date(4_294_967_296))
}

pub fn date_infinity_round_trip_test() {
  let assert Ok(encoded) = date.encode(value.PosInfinity)
  date.decode(encoded) |> should.equal(Ok(value.PosInfinity))
  let assert Ok(encoded) = date.encode(value.NegInfinity)
  date.decode(encoded) |> should.equal(Ok(value.NegInfinity))
}

pub fn time_range_test() {
  let assert Ok(_) = time.encode(value.Time(0))
  let assert Ok(_) = time.encode(value.Time(86_400_000_000))
  let assert Error(_) = time.encode(value.Time(-1))
  let assert Error(_) = time.encode(value.Time(86_400_000_001))
  let assert Ok(_) = timetz.encode(value.TimeTz(0, 3600))
  let assert Error(_) = timetz.encode(value.TimeTz(-1, 0))
  let assert Error(_) = timetz.encode(value.TimeTz(0, 4_294_967_296))
}

pub fn timestamp_range_test() {
  let assert Ok(_) = timestamp.encode(value.Timestamp(int64_max - 1))
  // The two ends are infinity on the wire, not timestamps.
  let assert Error(_) = timestamp.encode(value.Timestamp(int64_max))
  let assert Error(_) = timestamp.encode(value.Timestamp(int64_min))
  let assert Error(_) = timestamp.encode(value.Timestamp(int64_max + 1))
  let assert Ok(_) = timestamptz.encode(value.Timestamptz(int64_min + 1))
  let assert Error(_) = timestamptz.encode(value.Timestamptz(int64_max))
  let assert Error(_) = timestamptz.encode(value.Timestamptz(int64_min - 1))
}

pub fn interval_range_test() {
  let assert Ok(_) = interval.encode(value.Interval(int64_max, 1, 1))
  let assert Error(_) = interval.encode(value.Interval(int64_max + 1, 0, 0))
  let assert Error(_) = interval.encode(value.Interval(0, 2_147_483_648, 0))
  let assert Error(_) = interval.encode(value.Interval(0, 0, -2_147_483_649))
}
