/// PostgreSQL logical replication connection support.
/// Provides LSN encoding/decoding and a replication connection that can
/// create replication slots, start streaming, and receive WAL data.

import gleam/option.{None}
import gleam/result
import gleam/string
import postgleam/config.{type Config, Config}
import postgleam/connection.{type ConnectionState, ConnectionState}
import postgleam/error.{type Error}
import postgleam/message

/// Log Sequence Number - PostgreSQL's position in the WAL
pub type Lsn {
  Lsn(value: Int)
}

/// A WAL data message from PostgreSQL
pub type WalMessage {
  /// XLogData: WAL start, WAL end, server timestamp, and data payload
  XLogData(
    wal_start: Int,
    wal_end: Int,
    server_time: Int,
    data: BitArray,
  )
  /// Primary keepalive: WAL end, server timestamp, reply requested flag
  PrimaryKeepalive(wal_end: Int, server_time: Int, reply_requested: Bool)
}

/// Encode an LSN integer to its string representation (e.g., "1/F73E0220")
pub fn encode_lsn(lsn: Int) -> Result(String, Nil) {
  let max_uint64 = 18_446_744_073_709_551_615
  case lsn >= 0 && lsn <= max_uint64 {
    True -> {
      let file_id = int_shr(lsn, 32)
      let offset = int_band(lsn, 0xFFFFFFFF)
      Ok(int_to_hex(file_id) <> "/" <> int_to_hex(offset))
    }
    False -> Error(Nil)
  }
}

/// Decode an LSN string to its integer representation
pub fn decode_lsn(lsn: String) -> Result(Int, Nil) {
  case string.split(lsn, "/") {
    [file_id_str, offset_str] -> {
      case
        string.length(file_id_str) > 0
        && string.length(file_id_str) <= 8
        && string.length(offset_str) > 0
        && string.length(offset_str) <= 8
      {
        True -> {
          use file_id <- result.try(hex_to_int(file_id_str))
          use offset <- result.try(hex_to_int(offset_str))
          Ok(int_bor(int_shl(file_id, 32), offset))
        }
        False -> Error(Nil)
      }
    }
    _ -> Error(Nil)
  }
}

/// Start a replication connection by connecting with the replication parameter
pub fn connect(
  config cfg: Config,
) -> Result(ConnectionState, Error) {
  let repl_config =
    Config(
      ..cfg,
      extra_parameters: [
        #("replication", "database"),
        ..cfg.extra_parameters
      ],
    )
  connection.connect(repl_config)
}

/// Execute a simple query on a replication connection.
/// Used for CREATE_REPLICATION_SLOT etc.
pub fn query(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(connection.SimpleQueryResult), ConnectionState), Error) {
  connection.simple_query(state, sql, timeout)
}

/// Start streaming replication.
/// Sends the START_REPLICATION command and waits for CopyBothResponse.
pub fn start_streaming(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use state <- result.try(
    connection.send_message(state, message.SimpleQuery(sql)),
  )
  recv_copy_both_response(state, timeout)
}

/// Receive the next WAL message from a streaming replication connection.
/// Returns either a parsed WAL message or an indication that streaming has ended.
pub fn receive_wal_message(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(Result(WalMessage, Nil), ConnectionState), Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.CopyData(data) -> {
      case parse_wal_message(data) {
        Ok(wal_msg) -> Ok(#(Ok(wal_msg), state))
        Error(Nil) -> Ok(#(Error(Nil), state))
      }
    }
    message.CopyDone -> {
      // Streaming ended
      Ok(#(Error(Nil), state))
    }
    message.ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: None,
      ))
    }
    _ -> Ok(#(Error(Nil), state))
  }
}

/// Send a standby status update to the server.
/// All LSN values are 64-bit unsigned integers (microseconds since 2000-01-01).
pub fn send_standby_status(
  state: ConnectionState,
  written: Int,
  flushed: Int,
  applied: Int,
  timestamp: Int,
  reply_requested: Bool,
) -> Result(ConnectionState, Error) {
  let reply_byte = case reply_requested {
    True -> 1
    False -> 0
  }
  let data =
    <<
      "r":utf8,
      written:size(64),
      flushed:size(64),
      applied:size(64),
      timestamp:size(64),
      reply_byte:size(8),
    >>
  connection.send_message(state, message.CopyDataMsg(data))
}

/// Stop streaming by sending CopyDone, then receive CommandComplete + ReadyForQuery
pub fn stop_streaming(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use state <- result.try(
    connection.send_message(state, message.CopyDoneMsg),
  )
  recv_stream_end(state, timeout)
}

/// Disconnect a replication connection
pub fn disconnect(state: ConnectionState) -> Nil {
  connection.disconnect(state)
}

// --- Internal helpers ---

fn recv_copy_both_response(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.CopyBothResponse(_, _) -> Ok(state)
    message.ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: None,
      ))
    }
    message.NoticeResponse(_) -> recv_copy_both_response(state, timeout)
    _ -> Error(error.ProtocolError("Expected CopyBothResponse"))
  }
}

fn recv_stream_end(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.CommandComplete(_) -> recv_ready_after_stream(state, timeout)
    message.CopyDone -> recv_stream_end(state, timeout)
    message.CopyData(_) -> recv_stream_end(state, timeout)
    message.ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: None,
      ))
    }
    _ -> recv_stream_end(state, timeout)
  }
}

fn recv_ready_after_stream(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(connection.receive_message(state, timeout))
  case msg {
    message.ReadyForQuery(status) ->
      Ok(ConnectionState(..state, transaction_status: status))
    _ -> recv_ready_after_stream(state, timeout)
  }
}

fn parse_wal_message(data: BitArray) -> Result(WalMessage, Nil) {
  case data {
    // 'w' - XLogData
    <<0x77, wal_start:size(64), wal_end:size(64), server_time:size(64),
      rest:bytes>> ->
      Ok(XLogData(
        wal_start: wal_start,
        wal_end: wal_end,
        server_time: server_time,
        data: rest,
      ))
    // 'k' - Primary keepalive
    <<0x6B, wal_end:size(64), server_time:size(64), reply:size(8)>> ->
      Ok(PrimaryKeepalive(
        wal_end: wal_end,
        server_time: server_time,
        reply_requested: reply == 1,
      ))
    _ -> Error(Nil)
  }
}

// --- Bitwise operations via Erlang FFI ---

@external(erlang, "postgleam_ffi", "int_shr")
fn int_shr(a: Int, b: Int) -> Int

@external(erlang, "postgleam_ffi", "int_shl")
fn int_shl(a: Int, b: Int) -> Int

@external(erlang, "postgleam_ffi", "int_band")
fn int_band(a: Int, b: Int) -> Int

@external(erlang, "postgleam_ffi", "int_bor")
fn int_bor(a: Int, b: Int) -> Int

@external(erlang, "postgleam_ffi", "int_to_hex")
fn int_to_hex(n: Int) -> String

@external(erlang, "postgleam_ffi", "hex_to_int")
fn hex_to_int(s: String) -> Result(Int, Nil)
