//// Regression tests for bugs found in the 0.8 review. Each test names the
//// bug it guards against.

import gleam/erlang/atom
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/string
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/decode
import postgleam/error
import postgleam/listener
import postgleam/value

fn test_config() {
  config.default()
  |> config.password("postgres")
  |> config.database("postgleam_test")
}

fn now_ms() -> Int {
  monotonic_time(atom.create("millisecond"))
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: atom.Atom) -> Int

/// Bug: without TCP_NODELAY every extended query stalled ~40 ms on delayed
/// ACKs (about 22 queries/s). 500 queries must take well under a second
/// on localhost.
pub fn extended_query_latency_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let started = now_ms()
  list.each(list.repeat(Nil, 500), fn(_) {
    let assert Ok(_) =
      postgleam.query(conn, "SELECT $1::int4", [postgleam.int(1)])
  })
  let elapsed = now_ms() - started
  // 40 ms stalls would make this 20 seconds.
  should.be_true(elapsed < 3000)
  postgleam.disconnect(conn)
}

/// Bug: after a timeout the late reply of the timed-out query was read as
/// the reply to the next query.
pub fn timeout_does_not_desync_connection_test() {
  let cfg = test_config() |> config.timeout(200)
  let assert Ok(conn) = postgleam.connect(cfg)
  let assert Error(error.TimeoutError) =
    postgleam.query(conn, "SELECT pg_sleep(1)", [])
  let assert Ok(result) = postgleam.query(conn, "SELECT 2::int4", [])
  should.equal(result.rows, [[Some(value.Integer(2))]])
  // The slow query was cancelled on the server, not left running.
  let assert Ok(admin) = postgleam.connect(test_config())
  let assert Ok(r) =
    postgleam.query(
      admin,
      "SELECT count(*)::int4 FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(1)' AND state = 'active'",
      [],
    )
  should.equal(r.rows, [[Some(value.Integer(0))]])
  postgleam.disconnect(admin)
  postgleam.disconnect(conn)
}

/// Bug: in PgBouncer mode an error left the connection waiting forever for
/// a ReadyForQuery that never came (no Sync was sent).
pub fn pgbouncer_mode_error_then_query_test() {
  let cfg = test_config() |> config.pgbouncer(True) |> config.timeout(2000)
  let assert Ok(conn) = postgleam.connect(cfg)
  let assert Error(error.PgError(..)) =
    postgleam.query(conn, "SELEC broken", [])
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::int4", [postgleam.int(5)])
  should.equal(result.rows, [[Some(value.Integer(5))]])
  postgleam.disconnect(conn)
}

/// Bug: a cached statement became unusable after ALTER TABLE ("cached plan
/// must not change result type") until the connection was dropped.
pub fn statement_cache_survives_alter_table_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "DROP TABLE IF EXISTS _alter_cache; CREATE TABLE _alter_cache (a int4); INSERT INTO _alter_cache VALUES (1)",
    )
  let assert Ok(r) = postgleam.query(conn, "SELECT * FROM _alter_cache", [])
  should.equal(r.rows, [[Some(value.Integer(1))]])
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "ALTER TABLE _alter_cache ADD COLUMN b int4 DEFAULT 2",
    )
  let assert Ok(r) = postgleam.query(conn, "SELECT * FROM _alter_cache", [])
  should.equal(r.rows, [[Some(value.Integer(1)), Some(value.Integer(2))]])
  let assert Ok(_) = postgleam.simple_query(conn, "DROP TABLE _alter_cache")
  postgleam.disconnect(conn)
}

/// Bug: numeric("1e5") was stored as 635, and invalid strings were sent
/// as garbage instead of failing.
pub fn numeric_parameter_parsing_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let check = fn(input, expected) {
    let assert Ok(r) =
      postgleam.query(conn, "SELECT $1::numeric::text", [
        postgleam.numeric(input),
      ])
    should.equal(r.rows, [[Some(value.Text(expected))]])
  }
  check("1e5", "100000")
  check("1.5E-3", "0.0015")
  check("-12345678901234567890.0001", "-12345678901234567890.0001")
  check("0", "0")
  check("NaN", "NaN")
  let assert Error(error.EncodeError(_)) =
    postgleam.query(conn, "SELECT $1::numeric", [postgleam.numeric("12abc")])
  // The connection is still usable after an encode error.
  let assert Ok(_) = postgleam.query(conn, "SELECT 1", [])
  postgleam.disconnect(conn)
}

/// Bug: notifications that arrived while a query was running were dropped.
pub fn listener_receives_notifications_test() {
  let cfg = test_config()
  let inbox = process.new_subject()
  let assert Ok(l) = listener.start(cfg, inbox)
  let assert Ok(Nil) = listener.listen(l, "regression_chan")
  let assert Ok(conn) = postgleam.connect(cfg)
  let assert Ok(Nil) = postgleam.notify(conn, "regression_chan", "one")
  let assert Ok(Nil) = postgleam.notify(conn, "regression_chan", "two")
  let assert Ok(first) = process.receive(inbox, 2000)
  let assert Ok(second) = process.receive(inbox, 2000)
  should.equal(#(first.channel, first.payload), #("regression_chan", "one"))
  should.equal(second.payload, "two")
  let assert Ok(Nil) = listener.unlisten(l, "regression_chan")
  let assert Ok(Nil) = postgleam.notify(conn, "regression_chan", "three")
  let assert Error(Nil) = process.receive(inbox, 300)
  listener.stop(l)
  postgleam.disconnect(conn)
}

/// The listener reconnects and listens again when its connection dies.
pub fn listener_reconnects_test() {
  let cfg = test_config()
  let inbox = process.new_subject()
  let assert Ok(l) = listener.start(cfg, inbox)
  let assert Ok(Nil) = listener.listen(l, "reconnect_chan")
  let assert Ok(conn) = postgleam.connect(cfg)
  let assert Ok(_) =
    postgleam.query(
      conn,
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = 'LISTEN \"reconnect_chan\"'",
      [],
    )
  let deliver = fn() {
    let assert Ok(Nil) = postgleam.notify(conn, "reconnect_chan", "back")
    process.receive(inbox, 200)
  }
  let got = retry(deliver, 20)
  let assert Ok(n) = got
  should.equal(n.payload, "back")
  listener.stop(l)
  postgleam.disconnect(conn)
}

fn retry(f: fn() -> Result(a, Nil), attempts: Int) -> Result(a, Nil) {
  case f(), attempts {
    Ok(v), _ -> Ok(v)
    Error(Nil), 0 -> Error(Nil)
    Error(Nil), _ -> retry(f, attempts - 1)
  }
}

/// Bug: columns of types without a binary codec were requested in binary
/// and then decoded as if they were text.
pub fn unknown_types_decode_as_text_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(r) =
    postgleam.query(
      conn,
      "SELECT '[1,5)'::int4range, 'a=>1'::hstore, 'gleam'::enum1",
      [],
    )
  should.equal(r.rows, [
    [
      Some(value.Text("[1,5)")),
      Some(value.Text("\"a\"=>\"1\"")),
      Some(value.Text("gleam")),
    ],
  ])
  postgleam.disconnect(conn)
}

/// Bug: text parameters for uuid and enum columns only worked through the
/// pool's enum discovery. Text is now sent in text format for any type.
pub fn text_parameters_for_any_type_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(r) =
    postgleam.query(conn, "SELECT $1::enum1::text, $2::uuid::text", [
      postgleam.text("erlang"),
      postgleam.text("550e8400-e29b-41d4-a716-446655440000"),
    ])
  should.equal(r.rows, [
    [
      Some(value.Text("erlang")),
      Some(value.Text("550e8400-e29b-41d4-a716-446655440000")),
    ],
  ])
  postgleam.disconnect(conn)
}

/// Text containing a NUL byte cannot be sent; it must fail cleanly.
pub fn nul_byte_in_text_is_an_encode_error_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Error(error.EncodeError(_)) =
    postgleam.query(conn, "SELECT $1::text", [postgleam.text("a\u{0000}b")])
  let assert Ok(_) = postgleam.query(conn, "SELECT 1", [])
  postgleam.disconnect(conn)
}

/// A decode error must surface (not be swallowed) and leave the
/// connection usable.
pub fn decode_error_is_reported_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }
  let assert Error(error.DecodeError(message)) =
    postgleam.query_with(conn, "SELECT 'x'::text AS name", [], decoder)
  should.be_true(string.contains(message, "name"))
  let assert Ok(_) = postgleam.query(conn, "SELECT 1", [])
  postgleam.disconnect(conn)
}

/// A connection lost inside a transaction must not silently continue the
/// "transaction" in autocommit mode on a new connection.
pub fn lost_transaction_is_reported_test() {
  let cfg = test_config() |> config.timeout(300)
  let assert Ok(conn) = postgleam.connect(cfg)
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "DROP TABLE IF EXISTS _lost_tx; CREATE TABLE _lost_tx (n int4)",
    )
  let result =
    postgleam.transaction(conn, fn(c) {
      let assert Ok(_) =
        postgleam.query(c, "INSERT INTO _lost_tx VALUES (1)", [])
      // Times out: the connection is dropped and the transaction is gone.
      let assert Error(error.TimeoutError) =
        postgleam.query(c, "SELECT pg_sleep(1)", [])
      // This must not run in autocommit mode on a fresh connection.
      let assert Error(error.ConnectionError(_)) =
        postgleam.query(c, "INSERT INTO _lost_tx VALUES (2)", [])
      Ok(Nil)
    })
  let assert Error(postgleam.TransactionFailed(_)) = result
  let assert Ok(r) =
    postgleam.query(conn, "SELECT count(*)::int4 FROM _lost_tx", [])
  should.equal(r.rows, [[Some(value.Integer(0))]])
  let assert Ok(_) = postgleam.simple_query(conn, "DROP TABLE _lost_tx")
  postgleam.disconnect(conn)
}

/// A server message larger than `max_message_size` is refused instead of
/// being buffered without limit.
pub fn max_message_size_test() {
  let cfg = test_config() |> config.max_message_size(10_000)
  let assert Ok(conn) = postgleam.connect(cfg)
  let assert Error(_) = postgleam.query(conn, "SELECT repeat('x', 100000)", [])
  // The connection reconnects for the next request.
  let assert Ok(r) = postgleam.query(conn, "SELECT 1::int4", [])
  should.equal(r.rows, [[Some(value.Integer(1))]])
  postgleam.disconnect(conn)
}

/// Auth policy: a client that only allows MD5 must refuse a SCRAM server.
pub fn allowed_auth_is_enforced_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("postgleam_scram_pw")
    |> config.allowed_auth([config.AuthMd5])
  let assert Error(error.AuthenticationError(_)) = postgleam.connect(cfg)
}

pub fn query_fold_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let decoder = {
    use n <- decode.field("n", decode.int)
    decode.success(n)
  }
  let assert Ok(sum) =
    postgleam.query_fold(
      conn,
      "SELECT generate_series(1, $1::int4) AS n",
      [postgleam.int(1000)],
      100,
      decoder,
      0,
      int.add,
    )
  should.equal(sum, 500_500)
  // A decode error inside a fold is returned, and the connection survives.
  let bad = {
    use s <- decode.field("n", decode.text)
    decode.success(s)
  }
  let assert Error(error.DecodeError(_)) =
    postgleam.query_fold(
      conn,
      "SELECT generate_series(1, 10) AS n",
      [],
      3,
      bad,
      Nil,
      fn(acc, _) { acc },
    )
  let assert Ok(_) = postgleam.query(conn, "SELECT 1", [])
  postgleam.disconnect(conn)
}

pub fn multidimensional_array_test() {
  let assert Ok(conn) = postgleam.connect(test_config())
  let assert Ok(r) =
    postgleam.query(conn, "SELECT ARRAY[[1,2,3],[4,5,6]]::int4[]", [])
  let int = fn(n) { Some(value.Integer(n)) }
  let expected =
    value.Array([
      Some(value.Array([int(1), int(2), int(3)])),
      Some(value.Array([int(4), int(5), int(6)])),
    ])
  should.equal(r.rows, [[Some(expected)]])
  // And back again as a parameter.
  let assert Ok(r) =
    postgleam.query(conn, "SELECT $1::int4[] = ARRAY[[1,2,3],[4,5,6]]", [
      Some(expected),
    ])
  should.equal(r.rows, [[Some(value.Boolean(True))]])
  // Ragged arrays are rejected before they reach the server.
  let assert Error(error.EncodeError(_)) =
    postgleam.query(conn, "SELECT $1::int4[]", [
      postgleam.array([
        Some(value.Array([int(1)])),
        Some(value.Array([int(1), int(2)])),
      ]),
    ])
  let decoder = {
    use rows <- decode.element(0, decode.list(decode.list(decode.int)))
    decode.success(rows)
  }
  let assert Ok(response) =
    postgleam.query_with(conn, "SELECT ARRAY[[1,2],[3,4]]::int4[]", [], decoder)
  should.equal(response.rows, [[[1, 2], [3, 4]]])
  postgleam.disconnect(conn)
}
