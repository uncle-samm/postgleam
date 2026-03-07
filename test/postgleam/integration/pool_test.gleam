import gleam/option.{Some}
import gleeunit/should
import postgleam/config
import postgleam/pool
import postgleam/value

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

pub fn pool_start_and_shutdown_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  pool.shutdown(started.data, cfg.timeout)
}

pub fn pool_query_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  let p = started.data

  let assert Ok(result) =
    pool.query(p, "SELECT 42::int4 AS num", [], cfg.timeout)
  should.equal(result.rows, [[Some(value.Integer(42))]])

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_simple_query_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  let p = started.data

  let assert Ok(results) =
    pool.simple_query(p, "SELECT 1 AS num", cfg.timeout)
  let assert [result] = results
  should.equal(result.tag, "SELECT 1")

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_sequential_queries_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  let p = started.data

  let assert Ok(_) = pool.query(p, "SELECT 1::int4", [], cfg.timeout)
  let assert Ok(_) = pool.query(p, "SELECT 2::int4", [], cfg.timeout)
  let assert Ok(result) = pool.query(p, "SELECT 3::int4 AS val", [], cfg.timeout)
  should.equal(result.rows, [[Some(value.Integer(3))]])

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_with_params_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 3)
  let p = started.data

  let assert Ok(result) =
    pool.query(
      p,
      "SELECT $1::int4 + $2::int4 AS sum",
      [Some(value.Integer(10)), Some(value.Integer(32))],
      cfg.timeout,
    )
  should.equal(result.rows, [[Some(value.Integer(42))]])

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_error_recovery_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 1)
  let p = started.data

  // This should error
  let assert Error(_) =
    pool.query(p, "SELECT * FROM nonexistent_xyz", [], cfg.timeout)

  // The pool should still work after the error
  let assert Ok(result) =
    pool.query(p, "SELECT 1::int4 AS val", [], cfg.timeout)
  should.equal(result.rows, [[Some(value.Integer(1))]])

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_wrong_password_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("wrong_password")

  let assert Error(_) = pool.start(cfg, 1)
}
