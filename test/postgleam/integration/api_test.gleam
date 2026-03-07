import gleam/option.{None, Some}
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/value

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

pub fn connect_and_disconnect_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)
  postgleam.disconnect(conn)
}

pub fn query_select_int_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(conn, "SELECT 42::int4 AS num", [])

  should.equal(result.tag, "SELECT 1")
  should.equal(result.rows, [[Some(value.Integer(42))]])
  postgleam.disconnect(conn)
}

pub fn query_with_param_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT $1::int4 + $2::int4 AS sum",
      [Some(value.Integer(10)), Some(value.Integer(32))],
    )

  should.equal(result.rows, [[Some(value.Integer(42))]])
  postgleam.disconnect(conn)
}

pub fn query_with_null_param_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::int4 AS val", [None])

  should.equal(result.rows, [[None]])
  postgleam.disconnect(conn)
}

pub fn query_multiple_types_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT 1::int4 AS i, 'hello'::text AS t, true::bool AS b, 3.14::float8 AS f",
      [],
    )

  let assert [[Some(value.Integer(1)), Some(value.Text("hello")), Some(value.Boolean(True)), Some(value.Float(f))]] =
    result.rows
  should.be_true(f >. 3.13 && f <. 3.15)
  postgleam.disconnect(conn)
}

pub fn query_error_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Error(_) =
    postgleam.query(conn, "SELECT * FROM nonexistent_xyz", [])

  // Connection should still be usable after error
  let assert Ok(result) =
    postgleam.query(conn, "SELECT 1::int4 AS num", [])

  should.equal(result.rows, [[Some(value.Integer(1))]])
  postgleam.disconnect(conn)
}

pub fn simple_query_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(results) =
    postgleam.simple_query(conn, "SELECT 1 AS num")

  let assert [result] = results
  should.equal(result.tag, "SELECT 1")
  should.equal(result.columns, ["num"])
  should.equal(result.rows, [[Some("1")]])
  postgleam.disconnect(conn)
}

pub fn prepare_execute_close_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(prepared) =
    postgleam.prepare(conn, "my_stmt", "SELECT $1::int4 AS val")

  should.equal(prepared.param_oids, [23])

  let assert Ok(result) =
    postgleam.execute(conn, prepared, [Some(value.Integer(42))])

  should.equal(result.rows, [[Some(value.Integer(42))]])

  // Execute again
  let assert Ok(result2) =
    postgleam.execute(conn, prepared, [Some(value.Integer(99))])

  should.equal(result2.rows, [[Some(value.Integer(99))]])

  // Close
  let assert Ok(Nil) = postgleam.close(conn, "my_stmt")

  postgleam.disconnect(conn)
}

pub fn sequential_queries_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) = postgleam.query(conn, "SELECT 1::int4", [])
  let assert Ok(_) = postgleam.query(conn, "SELECT 2::int4", [])
  let assert Ok(result) = postgleam.query(conn, "SELECT 3::int4 AS val", [])

  should.equal(result.rows, [[Some(value.Integer(3))]])
  postgleam.disconnect(conn)
}

pub fn transaction_commit_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  // Create temp table
  let assert Ok(_) =
    postgleam.simple_query(conn, "CREATE TEMP TABLE _tx_test (id int4)")

  // Transaction that succeeds
  let assert Ok(42) =
    postgleam.transaction(conn, fn() {
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO _tx_test VALUES ($1::int4)",
          [Some(value.Integer(1))],
        )
      Ok(42)
    })

  // Verify the insert persisted
  let assert Ok(result) =
    postgleam.query(conn, "SELECT id FROM _tx_test", [])

  should.equal(result.rows, [[Some(value.Integer(1))]])
  postgleam.disconnect(conn)
}

pub fn transaction_rollback_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  // Create temp table
  let assert Ok(_) =
    postgleam.simple_query(conn, "CREATE TEMP TABLE _tx_rb_test (id int4)")

  // Transaction that fails
  let assert Error(_) =
    postgleam.transaction(conn, fn() {
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO _tx_rb_test VALUES ($1::int4)",
          [Some(value.Integer(99))],
        )
      Error(postgleam.query_error("deliberate rollback"))
    })

  // Verify the insert was rolled back
  let assert Ok(result) =
    postgleam.query(conn, "SELECT id FROM _tx_rb_test", [])

  should.equal(result.rows, [])
  postgleam.disconnect(conn)
}

pub fn connect_wrong_password_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("wrong_password")

  let assert Error(_) = postgleam.connect(cfg)
}

pub fn bytea_roundtrip_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let data = <<0, 1, 2, 255, 128, 64>>
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::bytea AS data", [Some(value.Bytea(data))])

  should.equal(result.rows, [[Some(value.Bytea(data))]])
  postgleam.disconnect(conn)
}

pub fn float_special_values_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT 'NaN'::float8 AS nan, 'Infinity'::float8 AS inf, '-Infinity'::float8 AS ninf",
      [],
    )

  should.equal(result.rows, [
    [Some(value.NaN), Some(value.PosInfinity), Some(value.NegInfinity)],
  ])
  postgleam.disconnect(conn)
}
