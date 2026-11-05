import gleam/dict
import gleam/option.{Some}
import gleeunit/should
import postgleam/config
import postgleam/connection

pub fn connect_default_user_test() {
  let cfg =
    config.default()
    |> config.password("postgres")
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  // Should have a connection_id (backend PID)
  should.be_true(is_some(state.connection_id))
  // Should have server parameters
  should.be_true(dict.size(state.parameters) > 0)
  // Should have server_version parameter
  let assert Ok(_) = dict.get(state.parameters, "server_version")
  connection.disconnect(state)
}

pub fn connect_and_simple_query_test() {
  let cfg =
    config.default()
    |> config.password("postgres")
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT 1 AS num", cfg.timeout)
  let assert [result] = results
  should.equal(result.tag, "SELECT 1")
  should.equal(result.columns, ["num"])
  should.equal(result.rows, [[Some("1")]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_null_test() {
  let cfg =
    config.default()
    |> config.password("postgres")
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT NULL AS val", cfg.timeout)
  let assert [result] = results
  should.equal(result.rows, [[option.None]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_multiple_columns_test() {
  let cfg =
    config.default()
    |> config.password("postgres")
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(
      state,
      "SELECT 1 AS a, 'hello' AS b, true AS c",
      cfg.timeout,
    )
  let assert [result] = results
  should.equal(result.columns, ["a", "b", "c"])
  should.equal(result.rows, [[Some("1"), Some("hello"), Some("t")]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_multiple_rows_test() {
  let cfg =
    config.default()
    |> config.password("postgres")
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(
      state,
      "SELECT generate_series(1,3) AS n",
      cfg.timeout,
    )
  let assert [result] = results
  should.equal(result.rows, [[Some("1")], [Some("2")], [Some("3")]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_error_test() {
  let cfg =
    config.default()
    |> config.password("postgres")
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Error(_) =
    connection.simple_query(
      state,
      "SELECT * FROM nonexistent_table_xyz",
      cfg.timeout,
    )
}

pub fn connect_wrong_password_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
    |> config.password("wrong_password")
    |> config.username("postgleam_scram_pw")
  let assert Error(_) = connection.connect(cfg)
}

pub fn connect_scram_auth_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
    |> config.username("postgleam_scram_pw")
    |> config.password("postgleam_scram_pw")
    |> config.allowed_auth([config.AuthScram])
  let assert Ok(state) = connection.connect(cfg)
  should.be_true(is_some(state.connection_id))
  connection.disconnect(state)
}

pub fn connect_multiple_queries_sequential_test() {
  let cfg =
    config.default()
    |> config.password("postgres")
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(_, state)) =
    connection.simple_query(state, "SELECT 1", cfg.timeout)
  let assert Ok(#(_, state)) =
    connection.simple_query(state, "SELECT 2", cfg.timeout)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT 3 AS val", cfg.timeout)

  let assert [result] = results
  should.equal(result.rows, [[Some("3")]])
  connection.disconnect(state)
}

/// Requesting TLS from a server without it is an error, never a silent
/// fallback to plain text. (When the TLS test certificates are set up the
/// server has TLS; tls_test covers that case.)
pub fn connect_ssl_not_available_test() {
  case is_file("build/tls/ca.pem") {
    True -> Nil
    False -> {
      let cfg =
        config.default()
        |> config.password("postgres")
        |> config.database("postgleam_test")
        |> config.ssl(config.SslUnverified)
      let assert Error(_) = connection.connect(cfg)
      Nil
    }
  }
}

@external(erlang, "filelib", "is_file")
fn is_file(path: String) -> Bool

fn is_some(opt: option.Option(a)) -> Bool {
  case opt {
    Some(_) -> True
    option.None -> False
  }
}
