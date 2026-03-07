import gleeunit/should
import postgleam/config
import postgleam/connection
import postgleam/replication

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

// --- LSN tests (pure, no DB needed) ---

pub fn encode_lsn_test() {
  let assert Ok(s) = replication.encode_lsn(0)
  should.equal(s, "0/0")

  let assert Ok(s2) = replication.encode_lsn(18_446_744_073_709_551_615)
  should.equal(s2, "FFFFFFFF/FFFFFFFF")
}

pub fn encode_lsn_known_value_test() {
  // FEDCBA98/76543210 = sum of i * 16^i for i in 1..15
  let lsn = compute_lsn_value()
  let assert Ok(s) = replication.encode_lsn(lsn)
  should.equal(s, "FEDCBA98/76543210")
}

pub fn decode_lsn_test() {
  let assert Ok(v) = replication.decode_lsn("0/0")
  should.equal(v, 0)

  let assert Ok(v2) = replication.decode_lsn("FFFFFFFF/FFFFFFFF")
  should.equal(v2, 18_446_744_073_709_551_615)
}

pub fn decode_lsn_known_value_test() {
  let expected = compute_lsn_value()
  let assert Ok(v) = replication.decode_lsn("FEDCBA98/76543210")
  should.equal(v, expected)
}

pub fn lsn_roundtrip_test() {
  let lsn = compute_lsn_value()
  let assert Ok(s) = replication.encode_lsn(lsn)
  let assert Ok(v) = replication.decode_lsn(s)
  should.equal(v, lsn)
}

pub fn lsn_roundtrip_string_test() {
  let s = "FEDCBA98/76543210"
  let assert Ok(v) = replication.decode_lsn(s)
  let assert Ok(s2) = replication.encode_lsn(v)
  should.equal(s2, s)
}

pub fn decode_lsn_errors_test() {
  should.be_error(replication.decode_lsn("0123ABC"))
  should.be_error(replication.decode_lsn("/0123ABC"))
  should.be_error(replication.decode_lsn("0123ABC/"))
  should.be_error(replication.decode_lsn("123G/0123ABC"))
  should.be_error(replication.decode_lsn("0/012345678"))
  should.be_error(replication.decode_lsn("012345678/0"))
}

pub fn encode_lsn_errors_test() {
  should.be_error(replication.encode_lsn(-1))
  should.be_error(replication.encode_lsn(18_446_744_073_709_551_616))
}

// --- Integration tests ---

pub fn replication_connect_test() {
  let cfg = test_config()
  let assert Ok(state) = replication.connect(config: cfg)
  replication.disconnect(state)
}

pub fn replication_query_test() {
  let cfg = test_config()
  let assert Ok(state) = replication.connect(config: cfg)

  // Simple query should work on replication connection
  let assert Ok(#(results, state)) =
    replication.query(state, "SELECT 1 AS num", cfg.timeout)
  let assert [result] = results
  should.equal(result.tag, "SELECT 1")

  replication.disconnect(state)
}

pub fn replication_create_slot_test() {
  let cfg = test_config()
  let assert Ok(state) = replication.connect(config: cfg)

  // Create a temporary replication slot
  let assert Ok(#(_results, state)) =
    replication.query(
      state,
      "CREATE_REPLICATION_SLOT postgleam_test_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
      cfg.timeout,
    )

  replication.disconnect(state)
}

pub fn replication_start_streaming_test() {
  let cfg = test_config()
  let assert Ok(state) = replication.connect(config: cfg)

  // Create slot
  let assert Ok(#(_, state)) =
    replication.query(
      state,
      "CREATE_REPLICATION_SLOT postgleam_stream_test TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
      cfg.timeout,
    )

  // Start streaming
  let assert Ok(state) =
    replication.start_streaming(
      state,
      "START_REPLICATION SLOT postgleam_stream_test LOGICAL 0/0 (proto_version '1', publication_names 'postgleam_example')",
      cfg.timeout,
    )

  // We should receive a keepalive message
  let assert Ok(#(result, state)) =
    replication.receive_wal_message(state, cfg.timeout)

  case result {
    Ok(replication.PrimaryKeepalive(_, _, _)) -> Nil
    Ok(replication.XLogData(_, _, _, _)) -> Nil
    _ -> should.fail()
  }

  replication.disconnect(state)
}

pub fn replication_keepalive_reply_test() {
  let cfg = test_config()
  let assert Ok(state) = replication.connect(config: cfg)

  let assert Ok(#(_, state)) =
    replication.query(
      state,
      "CREATE_REPLICATION_SLOT postgleam_ka_test TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
      cfg.timeout,
    )

  let assert Ok(state) =
    replication.start_streaming(
      state,
      "START_REPLICATION SLOT postgleam_ka_test LOGICAL 0/0 (proto_version '1', publication_names 'postgleam_example')",
      cfg.timeout,
    )

  // Receive keepalive
  let assert Ok(#(Ok(replication.PrimaryKeepalive(wal_end, _, _)), state)) =
    replication.receive_wal_message(state, cfg.timeout)

  // Send standby status update
  let assert Ok(state) =
    replication.send_standby_status(
      state,
      wal_end + 1,
      wal_end + 1,
      wal_end + 1,
      0,
      False,
    )

  replication.disconnect(state)
}

pub fn replication_receive_wal_data_test() {
  let cfg = test_config()

  // Replication connection
  let assert Ok(repl_state) = replication.connect(config: cfg)

  let assert Ok(#(_, repl_state)) =
    replication.query(
      repl_state,
      "CREATE_REPLICATION_SLOT postgleam_wal_test TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
      cfg.timeout,
    )

  let assert Ok(repl_state) =
    replication.start_streaming(
      repl_state,
      "START_REPLICATION SLOT postgleam_wal_test LOGICAL 0/0 (proto_version '1', publication_names 'postgleam_example')",
      cfg.timeout,
    )

  // Consume initial keepalive
  let assert Ok(#(_, repl_state)) =
    replication.receive_wal_message(repl_state, cfg.timeout)

  // Use a separate connection to insert data
  let assert Ok(writer) = connection.connect(cfg)
  let assert Ok(#(_, writer)) =
    connection.simple_query(
      writer,
      "CREATE TABLE IF NOT EXISTS repl_test (id int, text text)",
      cfg.timeout,
    )
  let assert Ok(#(_, writer)) =
    connection.simple_query(
      writer,
      "INSERT INTO repl_test VALUES (42, 'fortytwo')",
      cfg.timeout,
    )

  // Wait for WAL data on replication connection
  let assert Ok(#(result, repl_state)) =
    replication.receive_wal_message(repl_state, cfg.timeout)

  // Should get XLogData
  case result {
    Ok(replication.XLogData(_, _, _, _)) -> Nil
    Ok(replication.PrimaryKeepalive(_, _, _)) -> Nil
    _ -> should.fail()
  }

  // Clean up
  let assert Ok(#(_, writer)) =
    connection.simple_query(writer, "DROP TABLE IF EXISTS repl_test", cfg.timeout)
  connection.disconnect(writer)
  replication.disconnect(repl_state)
}

// Helper: compute FEDCBA98/76543210 as integer
// This is sum of i * 16^i for i in 1..15
fn compute_lsn_value() -> Int {
  compute_lsn_loop(1, 0)
}

fn compute_lsn_loop(i: Int, acc: Int) -> Int {
  case i > 15 {
    True -> acc
    False -> compute_lsn_loop(i + 1, acc + i * pow16(i))
  }
}

fn pow16(n: Int) -> Int {
  pow16_loop(n, 1)
}

fn pow16_loop(n: Int, acc: Int) -> Int {
  case n {
    0 -> acc
    _ -> pow16_loop(n - 1, acc * 16)
  }
}
