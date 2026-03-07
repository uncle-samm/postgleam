import gleam/option.{Some}
import gleeunit/should
import postgleam/config
import postgleam/connection
import postgleam/copy

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

pub fn copy_in_test() {
  let cfg = test_config()
  let assert Ok(state) = connection.connect(cfg)

  // Create temp table
  let assert Ok(#(_, state)) =
    connection.simple_query(state, "CREATE TEMP TABLE _copy_test (id int, name text)", cfg.timeout)

  // COPY IN data
  let data = [
    <<"1\tAlice\n":utf8>>,
    <<"2\tBob\n":utf8>>,
    <<"3\tCharlie\n":utf8>>,
  ]
  let assert Ok(#(tag, state)) =
    copy.copy_in(state, "COPY _copy_test FROM STDIN", data, cfg.timeout)

  should.equal(tag, "COPY 3")

  // Verify
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT count(*) FROM _copy_test", cfg.timeout)
  let assert [result] = results
  should.equal(result.rows, [[Some("3")]])

  connection.disconnect(state)
}

pub fn copy_out_test() {
  let cfg = test_config()
  let assert Ok(state) = connection.connect(cfg)

  // Create and populate temp table
  let assert Ok(#(_, state)) =
    connection.simple_query(state, "CREATE TEMP TABLE _copy_out_test (id int, name text)", cfg.timeout)
  let assert Ok(#(_, state)) =
    connection.simple_query(state, "INSERT INTO _copy_out_test VALUES (1, 'Alice'), (2, 'Bob')", cfg.timeout)

  // COPY OUT
  let assert Ok(#(rows, state)) =
    copy.copy_out(state, "COPY _copy_out_test TO STDOUT", cfg.timeout)

  // Should have 2 data rows
  should.equal(list_length(rows), 2)

  connection.disconnect(state)
}

pub fn copy_in_empty_test() {
  let cfg = test_config()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(_, state)) =
    connection.simple_query(state, "CREATE TEMP TABLE _copy_empty (id int)", cfg.timeout)

  let assert Ok(#(tag, state)) =
    copy.copy_in(state, "COPY _copy_empty FROM STDIN", [], cfg.timeout)

  should.equal(tag, "COPY 0")
  connection.disconnect(state)
}

fn list_length(l: List(a)) -> Int {
  case l {
    [] -> 0
    [_, ..rest] -> 1 + list_length(rest)
  }
}
