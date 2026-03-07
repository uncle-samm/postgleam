import gleeunit/should
import postgleam/codec/date
import postgleam/codec/interval
import postgleam/codec/time
import postgleam/codec/timestamp
import postgleam/codec/timestamptz
import postgleam/codec/timetz
import postgleam/value

// --- date ---

pub fn date_encode_epoch_test() {
  // 2000-01-01 = 0 days
  date.encode(value.Date(0))
  |> should.equal(Ok(<<0, 0, 0, 0>>))
}

pub fn date_encode_positive_test() {
  // 2000-01-02 = 1 day
  date.encode(value.Date(1))
  |> should.equal(Ok(<<0, 0, 0, 1>>))
}

pub fn date_decode_epoch_test() {
  date.decode(<<0, 0, 0, 0>>)
  |> should.equal(Ok(value.Date(0)))
}

pub fn date_decode_negative_test() {
  // Before 2000-01-01
  let assert Ok(value.Date(days)) = date.decode(<<255, 255, 255, 255>>)
  should.equal(days, -1)
}

pub fn date_roundtrip_test() {
  let vals = [0, 1, -1, 365, -365, 7305]
  date_roundtrip_list(vals)
}

fn date_roundtrip_list(vals: List(Int)) -> Nil {
  case vals {
    [] -> Nil
    [v, ..rest] -> {
      let assert Ok(encoded) = date.encode(value.Date(v))
      let assert Ok(decoded) = date.decode(encoded)
      should.equal(decoded, value.Date(v))
      date_roundtrip_list(rest)
    }
  }
}

pub fn date_wrong_type_test() {
  date.encode(value.Integer(1))
  |> should.be_error()
}

// --- time ---

pub fn time_encode_midnight_test() {
  time.encode(value.Time(0))
  |> should.equal(Ok(<<0, 0, 0, 0, 0, 0, 0, 0>>))
}

pub fn time_decode_midnight_test() {
  time.decode(<<0, 0, 0, 0, 0, 0, 0, 0>>)
  |> should.equal(Ok(value.Time(0)))
}

pub fn time_roundtrip_test() {
  // 12:30:45.123456 = (12*3600 + 30*60 + 45) * 1_000_000 + 123456
  let usec = { 12 * 3600 + 30 * 60 + 45 } * 1_000_000 + 123_456
  let assert Ok(encoded) = time.encode(value.Time(usec))
  time.decode(encoded)
  |> should.equal(Ok(value.Time(usec)))
}

// --- timetz ---

pub fn timetz_encode_test() {
  timetz.encode(value.TimeTz(0, 0))
  |> should.equal(Ok(<<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>))
}

pub fn timetz_decode_test() {
  let assert Ok(value.TimeTz(usec, tz)) =
    timetz.decode(<<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>)
  should.equal(usec, 0)
  should.equal(tz, 0)
}

pub fn timetz_roundtrip_test() {
  let assert Ok(encoded) = timetz.encode(value.TimeTz(45_000_000_000, -18_000))
  let assert Ok(value.TimeTz(usec, tz)) = timetz.decode(encoded)
  should.equal(usec, 45_000_000_000)
  should.equal(tz, -18_000)
}

// --- timestamp ---

pub fn timestamp_encode_epoch_test() {
  timestamp.encode(value.Timestamp(0))
  |> should.equal(Ok(<<0, 0, 0, 0, 0, 0, 0, 0>>))
}

pub fn timestamp_decode_epoch_test() {
  timestamp.decode(<<0, 0, 0, 0, 0, 0, 0, 0>>)
  |> should.equal(Ok(value.Timestamp(0)))
}

pub fn timestamp_infinity_test() {
  let assert Ok(encoded) = timestamp.encode(value.PosInfinity)
  timestamp.decode(encoded)
  |> should.equal(Ok(value.PosInfinity))
}

pub fn timestamp_neg_infinity_test() {
  let assert Ok(encoded) = timestamp.encode(value.NegInfinity)
  timestamp.decode(encoded)
  |> should.equal(Ok(value.NegInfinity))
}

pub fn timestamp_roundtrip_test() {
  // Some timestamp in microseconds
  let usec = 631_152_000_000_000
  let assert Ok(encoded) = timestamp.encode(value.Timestamp(usec))
  timestamp.decode(encoded)
  |> should.equal(Ok(value.Timestamp(usec)))
}

// --- timestamptz ---

pub fn timestamptz_roundtrip_test() {
  let usec = 631_152_000_000_000
  let assert Ok(encoded) = timestamptz.encode(value.Timestamptz(usec))
  timestamptz.decode(encoded)
  |> should.equal(Ok(value.Timestamptz(usec)))
}

pub fn timestamptz_infinity_test() {
  let assert Ok(encoded) = timestamptz.encode(value.PosInfinity)
  timestamptz.decode(encoded)
  |> should.equal(Ok(value.PosInfinity))
}

// --- interval ---

pub fn interval_encode_test() {
  interval.encode(value.Interval(1_000_000, 1, 1))
  |> should.be_ok()
}

pub fn interval_decode_test() {
  let assert Ok(encoded) = interval.encode(value.Interval(1_000_000, 30, 12))
  let assert Ok(value.Interval(usec, days, months)) = interval.decode(encoded)
  should.equal(usec, 1_000_000)
  should.equal(days, 30)
  should.equal(months, 12)
}

pub fn interval_zero_test() {
  let assert Ok(encoded) = interval.encode(value.Interval(0, 0, 0))
  interval.decode(encoded)
  |> should.equal(Ok(value.Interval(0, 0, 0)))
}
