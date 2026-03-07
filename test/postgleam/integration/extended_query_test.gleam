import gleam/option.{None, Some}
import gleeunit/should
import postgleam/codec/defaults
import postgleam/codec/registry
import postgleam/config
import postgleam/connection
import postgleam/value

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

fn test_registry() {
  registry.build(defaults.matchers())
}

pub fn extended_query_select_int_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT 42::int4 AS num",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.tag, "SELECT 1")
  let assert [field] = result.columns
  should.equal(field.name, "num")
  should.equal(result.rows, [[Some(value.Integer(42))]])
  connection.disconnect(state)
}

pub fn extended_query_select_bool_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT true::bool AS flag",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Boolean(True))]])
  connection.disconnect(state)
}

pub fn extended_query_select_text_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT 'hello'::text AS msg",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Text("hello"))]])
  connection.disconnect(state)
}

pub fn extended_query_select_float8_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT 3.14::float8 AS val",
      [],
      reg,
      cfg.timeout,
    )

  let assert [[Some(value.Float(f))]] = result.rows
  // float8 should preserve double precision
  should.be_true(f >. 3.13 && f <. 3.15)
  connection.disconnect(state)
}

pub fn extended_query_select_null_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT NULL::int4 AS val",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[None]])
  connection.disconnect(state)
}

pub fn extended_query_with_param_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::int4 AS num",
      [Some(value.Integer(99))],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Integer(99))]])
  connection.disconnect(state)
}

pub fn extended_query_with_null_param_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::int4 AS num",
      [None],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[None]])
  connection.disconnect(state)
}

pub fn extended_query_multiple_columns_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT 1::int4 AS a, 'hello'::text AS b, true::bool AS c",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [
    [Some(value.Integer(1)), Some(value.Text("hello")), Some(value.Boolean(True))],
  ])
  connection.disconnect(state)
}

pub fn extended_query_multiple_rows_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT generate_series(1,3)::int4 AS n",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [
    [Some(value.Integer(1))],
    [Some(value.Integer(2))],
    [Some(value.Integer(3))],
  ])
  connection.disconnect(state)
}

pub fn extended_query_error_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Error(_) =
    connection.extended_query(
      state,
      "SELECT * FROM nonexistent_table_xyz",
      [],
      reg,
      cfg.timeout,
    )
}

pub fn extended_query_multiple_params_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::int4 + $2::int4 AS sum",
      [Some(value.Integer(10)), Some(value.Integer(32))],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Integer(42))]])
  connection.disconnect(state)
}

pub fn extended_query_bytea_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::bytea AS data",
      [Some(value.Bytea(<<1, 2, 3, 255>>))],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Bytea(<<1, 2, 3, 255>>))]])
  connection.disconnect(state)
}

pub fn extended_query_int2_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::int2 AS val",
      [Some(value.Integer(1234))],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Integer(1234))]])
  connection.disconnect(state)
}

pub fn extended_query_int8_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::int8 AS val",
      [Some(value.Integer(9_999_999_999))],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Integer(9_999_999_999))]])
  connection.disconnect(state)
}

pub fn extended_query_float4_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT 'NaN'::float4 AS nan_val, 'Infinity'::float4 AS inf_val, '-Infinity'::float4 AS neg_inf_val",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [
    [Some(value.NaN), Some(value.PosInfinity), Some(value.NegInfinity)],
  ])
  connection.disconnect(state)
}

pub fn extended_query_float8_special_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT 'NaN'::float8 AS nan_val, 'Infinity'::float8 AS inf_val, '-Infinity'::float8 AS neg_inf_val",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [
    [Some(value.NaN), Some(value.PosInfinity), Some(value.NegInfinity)],
  ])
  connection.disconnect(state)
}

pub fn prepare_and_execute_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  // Prepare a named statement
  let assert Ok(#(prepared, state)) =
    connection.prepare(state, "test_stmt", "SELECT $1::int4 AS val", [], cfg.timeout)

  // Should have 1 param OID (int4 = 23)
  should.equal(prepared.param_oids, [23])
  // Should have 1 result field
  let assert [field] = prepared.result_fields
  should.equal(field.name, "val")

  // Execute it
  let assert Ok(#(result, state)) =
    connection.execute_prepared(state, prepared, [Some(value.Integer(42))], reg, cfg.timeout)

  should.equal(result.rows, [[Some(value.Integer(42))]])

  // Execute again with different param
  let assert Ok(#(result2, state)) =
    connection.execute_prepared(state, prepared, [Some(value.Integer(99))], reg, cfg.timeout)

  should.equal(result2.rows, [[Some(value.Integer(99))]])

  // Close the statement
  let assert Ok(state) =
    connection.close_statement(state, "test_stmt", cfg.timeout)

  connection.disconnect(state)
}

pub fn extended_query_no_rows_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  // CREATE TEMP TABLE returns no rows
  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "CREATE TEMP TABLE _test_eq (id int4)",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [])
  should.equal(result.columns, [])

  // INSERT
  let assert Ok(#(result2, state)) =
    connection.extended_query(
      state,
      "INSERT INTO _test_eq VALUES ($1::int4)",
      [Some(value.Integer(1))],
      reg,
      cfg.timeout,
    )

  // INSERT command tag
  let assert "INSERT " <> _ = result2.tag
  should.equal(result2.rows, [])

  // SELECT it back
  let assert Ok(#(result3, state)) =
    connection.extended_query(
      state,
      "SELECT id FROM _test_eq",
      [],
      reg,
      cfg.timeout,
    )

  should.equal(result3.rows, [[Some(value.Integer(1))]])

  connection.disconnect(state)
}

pub fn extended_query_sequential_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(_, state)) =
    connection.extended_query(state, "SELECT 1::int4", [], reg, cfg.timeout)
  let assert Ok(#(_, state)) =
    connection.extended_query(state, "SELECT 2::int4", [], reg, cfg.timeout)
  let assert Ok(#(result, state)) =
    connection.extended_query(state, "SELECT 3::int4 AS val", [], reg, cfg.timeout)

  should.equal(result.rows, [[Some(value.Integer(3))]])
  connection.disconnect(state)
}

pub fn extended_query_oid_type_test() {
  let cfg = test_config()
  let reg = test_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::oid AS val",
      [Some(value.Oid(12345))],
      reg,
      cfg.timeout,
    )

  should.equal(result.rows, [[Some(value.Oid(12345))]])
  connection.disconnect(state)
}
