import gleam/option.{None, Some}
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/value

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

pub fn date_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::date AS val", [Some(value.Date(0))])
  should.equal(result.rows, [[Some(value.Date(0))]])
  postgleam.disconnect(conn)
}

pub fn date_negative_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  // 1999-12-31 = -1 days from 2000-01-01
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::date AS val", [Some(value.Date(-1))])
  should.equal(result.rows, [[Some(value.Date(-1))]])
  postgleam.disconnect(conn)
}

pub fn time_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  // 12:30:00 = 45000000000 microseconds
  let usec = { 12 * 3600 + 30 * 60 } * 1_000_000
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::time AS val", [Some(value.Time(usec))])
  should.equal(result.rows, [[Some(value.Time(usec))]])
  postgleam.disconnect(conn)
}

pub fn timestamp_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::timestamp AS val", [Some(value.Timestamp(0))])
  should.equal(result.rows, [[Some(value.Timestamp(0))]])
  postgleam.disconnect(conn)
}

pub fn timestamp_infinity_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT 'infinity'::timestamp AS val", [])
  should.equal(result.rows, [[Some(value.PosInfinity)]])
  postgleam.disconnect(conn)
}

pub fn timestamp_neg_infinity_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT '-infinity'::timestamp AS val", [])
  should.equal(result.rows, [[Some(value.NegInfinity)]])
  postgleam.disconnect(conn)
}

pub fn timestamptz_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::timestamptz AS val", [Some(value.Timestamptz(0))])
  should.equal(result.rows, [[Some(value.Timestamptz(0))]])
  postgleam.disconnect(conn)
}

pub fn interval_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  // 1 month, 2 days, 3 seconds
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::interval AS val", [
      Some(value.Interval(3_000_000, 2, 1)),
    ])
  should.equal(result.rows, [[Some(value.Interval(3_000_000, 2, 1))]])
  postgleam.disconnect(conn)
}

pub fn json_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let json_str = "{\"key\":\"value\"}"
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::json AS val", [Some(value.Json(json_str))])
  should.equal(result.rows, [[Some(value.Json(json_str))]])
  postgleam.disconnect(conn)
}

pub fn jsonb_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::jsonb AS val", [
      Some(value.Jsonb("{\"a\":1}")),
    ])
  // JSONB may reformat the JSON, so just check it decodes
  let assert [[Some(value.Jsonb(_s))]] = result.rows
  postgleam.disconnect(conn)
}

pub fn numeric_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::numeric AS val", [
      Some(value.Numeric("123.45")),
    ])
  let assert [[Some(value.Numeric(s))]] = result.rows
  should.equal(s, "123.45")
  postgleam.disconnect(conn)
}

pub fn numeric_zero_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT 0::numeric AS val", [])
  let assert [[Some(value.Numeric(s))]] = result.rows
  should.equal(s, "0")
  postgleam.disconnect(conn)
}

pub fn numeric_nan_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT 'NaN'::numeric AS val", [])
  should.equal(result.rows, [[Some(value.NaN)]])
  postgleam.disconnect(conn)
}

pub fn point_roundtrip_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::point AS val", [
      Some(value.Point(1.5, -2.5)),
    ])
  should.equal(result.rows, [[Some(value.Point(1.5, -2.5))]])
  postgleam.disconnect(conn)
}

pub fn inet_ipv4_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT '192.168.1.1'::inet AS val", [])
  let assert [[Some(value.Inet(2, addr, 32))]] = result.rows
  should.equal(addr, <<192, 168, 1, 1>>)
  postgleam.disconnect(conn)
}

pub fn inet_cidr_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(conn, "SELECT '10.0.0.0/8'::cidr AS val", [])
  let assert [[Some(value.Inet(2, addr, 8))]] = result.rows
  should.equal(addr, <<10, 0, 0, 0>>)
  postgleam.disconnect(conn)
}

pub fn null_types_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT NULL::date AS d, NULL::timestamp AS t, NULL::numeric AS n, NULL::json AS j",
      [],
    )
  should.equal(result.rows, [[None, None, None, None]])
  postgleam.disconnect(conn)
}

pub fn multiple_types_in_one_query_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT 42::int4 AS i, true::bool AS b, 'hello'::text AS t, 3.14::float8 AS f, $1::date AS d",
      [Some(value.Date(100))],
    )
  let assert [[Some(value.Integer(42)), Some(value.Boolean(True)), Some(value.Text("hello")), Some(value.Float(_)), Some(value.Date(100))]] =
    result.rows
  postgleam.disconnect(conn)
}
