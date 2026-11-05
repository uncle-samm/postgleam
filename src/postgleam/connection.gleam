//// Low-level PostgreSQL connection: TCP/TLS connect, authentication and the
//// wire protocol state machine.
////
//// Most applications should use `postgleam` (a connection actor) or
//// `postgleam/pool`. This module is for building on the protocol directly
//// (COPY, streaming, replication, LISTEN/NOTIFY).
////
//// Every function takes a `ConnectionState` and returns the updated state.
//// After an `Error`, keep using the state you had *before* the call only if
//// `is_recoverable` says so; otherwise close the connection.

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import postgleam/auth/md5
import postgleam/auth/scram
import postgleam/codec/registry.{type Registry}
import postgleam/config.{
  type AuthMethod, type Config, AuthCleartext, AuthMd5, AuthNone, AuthScram,
  SslDisabled, SslUnverified, SslVerified,
}
import postgleam/error.{type Error}
import postgleam/internal/transport.{type Transport}
import postgleam/message.{
  type BackendMessage, type RowField, type TransactionStatus,
  AuthCleartext as AuthCleartextMsg, AuthMd5 as AuthMd5Msg, AuthOk, AuthSasl,
  AuthSaslContinue, AuthSaslFinal, AuthenticationMsg, BackendKeyData,
  BindComplete, CloseComplete, CommandComplete, DataRow, DecodeFailed, Decoded,
  ErrorResponse, Incomplete, NoData, NoticeResponse, NotificationResponse,
  ParameterDescription, ParameterStatus, ParseComplete, PortalSuspended,
  ReadyForQuery, RowDescription,
}
import postgleam/value.{type Value}

/// Connection state.
pub type ConnectionState {
  ConnectionState(
    transport: Transport,
    connection_id: Option(Int),
    connection_key: Option(Int),
    parameters: Dict(String, String),
    transaction_status: TransactionStatus,
    buffer: BitArray,
    /// LISTEN/NOTIFY notifications received but not yet taken, oldest first.
    notifications: List(Notification),
    max_message_size: Int,
  )
}

/// An asynchronous notification from LISTEN/NOTIFY.
pub type Notification {
  Notification(channel: String, payload: String, pg_pid: Int)
}

/// A failed operation, plus the connection state if the connection is still
/// usable (the server reached ReadyForQuery and nothing is left unread).
///
/// `state: None` means the connection is in an unknown state and must be
/// closed: a timeout, a socket error or a protocol violation.
pub type Failure {
  Failure(error: Error, state: Option(ConnectionState))
}

/// Whether an error leaves the connection usable. Socket errors, timeouts
/// and protocol errors do not; server errors, encode and decode errors do.
pub fn is_recoverable(err: Error) -> Bool {
  case err {
    error.SocketError(_) | error.TimeoutError | error.ProtocolError(_) -> False
    error.ConnectionError(_)
    | error.AuthenticationError(_)
    | error.PoolTimeout -> False
    error.PgError(..) | error.EncodeError(_) | error.DecodeError(_) -> True
  }
}

// =============================================================================
// Connect and authenticate
// =============================================================================

/// Establish a connection, complete authentication, and return ready state.
pub fn connect(config: Config) -> Result(ConnectionState, Error) {
  use config <- result.try(
    config.validate(config) |> result.map_error(error.ConnectionError),
  )
  use tcp_socket <- result.try(transport.connect(
    config.host,
    config.port,
    config.connect_timeout,
  ))
  let tls = fn(verify) {
    transport.TlsOptions(
      verify: verify,
      ca_file: config.ssl_ca_file,
      client_cert: option.map(config.ssl_client_cert, fn(c) {
        #(c.certfile, c.keyfile)
      }),
    )
  }
  let upgraded = case config.ssl {
    SslDisabled -> Ok(transport.Tcp(tcp_socket))
    SslVerified ->
      transport.upgrade_to_ssl(
        tcp_socket,
        config.host,
        config.connect_timeout,
        tls(True),
      )
    SslUnverified ->
      transport.upgrade_to_ssl(
        tcp_socket,
        config.host,
        config.connect_timeout,
        tls(False),
      )
  }
  use conn_transport <- result.try(case upgraded {
    Ok(t) -> Ok(t)
    Error(e) -> {
      transport.close(transport.Tcp(tcp_socket))
      Error(e)
    }
  })

  let state =
    ConnectionState(
      transport: conn_transport,
      connection_id: None,
      connection_key: None,
      parameters: dict.new(),
      transaction_status: message.Idle,
      buffer: <<>>,
      notifications: [],
      max_message_size: config.max_message_size,
    )

  let startup =
    message.StartupMessage(params: [
      #("user", config.username),
      #("database", config.database),
      ..config.extra_parameters
    ])
  let result = {
    use state <- result.try(send_message(state, startup))
    authenticate_loop(state, config, False)
  }
  case result {
    Ok(state) -> Ok(state)
    Error(e) -> {
      transport.close(conn_transport)
      Error(e)
    }
  }
}

fn auth_allowed(config: Config, method: AuthMethod) -> Result(Nil, Error) {
  case list.contains(config.allowed_auth, method) {
    True -> Ok(Nil)
    False ->
      Error(error.AuthenticationError(
        "Server requested "
        <> auth_method_name(method)
        <> " authentication, which this config does not allow",
      ))
  }
}

fn auth_method_name(method: AuthMethod) -> String {
  case method {
    AuthNone -> "no (trust)"
    AuthScram -> "SCRAM-SHA-256"
    AuthMd5 -> "MD5"
    AuthCleartext -> "cleartext password"
  }
}

fn authenticate_loop(
  state: ConnectionState,
  config: Config,
  authenticated: Bool,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  let password = config.reveal(config.password)
  case msg {
    AuthenticationMsg(AuthOk) -> {
      // AuthOk without any challenge means the server asked for no password.
      use _ <- result.try(case authenticated {
        True -> Ok(Nil)
        False -> auth_allowed(config, AuthNone)
      })
      startup_loop(state, config)
    }
    AuthenticationMsg(AuthCleartextMsg) -> {
      use _ <- result.try(auth_allowed(config, AuthCleartext))
      use _ <- result.try(case state.transport {
        transport.Ssl(_) -> Ok(Nil)
        transport.Tcp(_) ->
          Error(error.AuthenticationError(
            "Server requested a cleartext password over an unencrypted "
            <> "connection; refusing. Enable TLS with config.ssl.",
          ))
      })
      use state <- result.try(send_message(
        state,
        message.PasswordMessage(password),
      ))
      authenticate_loop(state, config, True)
    }
    AuthenticationMsg(AuthMd5Msg(salt: salt)) -> {
      use _ <- result.try(auth_allowed(config, AuthMd5))
      let hash = md5.hash_password(password, config.username, salt)
      use state <- result.try(send_message(state, message.PasswordMessage(hash)))
      authenticate_loop(state, config, True)
    }
    AuthenticationMsg(AuthSasl(mechanisms: mechanisms)) -> {
      use _ <- result.try(auth_allowed(config, AuthScram))
      use _ <- result.try(case list.contains(mechanisms, "SCRAM-SHA-256") {
        True -> Ok(Nil)
        False ->
          Error(error.AuthenticationError(
            "Server offers no supported SASL mechanism: "
            <> string.join(mechanisms, ", "),
          ))
      })
      let #(mechanism, client_first_data) = scram.client_first()
      // client_first always produces "n,,<bare>", so this cannot fail.
      let assert Ok(client_first_bare) =
        scram.extract_client_first_bare(client_first_data)
      use state <- result.try(send_message(
        state,
        message.SASLInitialResponse(mechanism, client_first_data),
      ))
      scram_continue(state, config, client_first_bare, password)
    }
    ErrorResponse(fields) ->
      Error(error.AuthenticationError(
        "Authentication failed: " <> error.parse_error_fields(fields).message,
      ))
    other ->
      Error(error.ProtocolError(
        "Unexpected " <> message_name(other) <> " during authentication",
      ))
  }
}

fn scram_continue(
  state: ConnectionState,
  config: Config,
  client_first_bare: String,
  password: String,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  case msg {
    AuthenticationMsg(AuthSaslContinue(data: server_first)) ->
      case scram.client_final(server_first, client_first_bare, password) {
        Ok(#(client_final, scram_state)) -> {
          use state <- result.try(send_message(
            state,
            message.SASLResponse(client_final),
          ))
          scram_final(state, config, scram_state)
        }
        Error(e) -> Error(error.AuthenticationError("SCRAM error: " <> e))
      }
    ErrorResponse(fields) ->
      Error(error.AuthenticationError(error.parse_error_fields(fields).message))
    other ->
      Error(error.ProtocolError(
        "Expected SASL continue, got " <> message_name(other),
      ))
  }
}

fn scram_final(
  state: ConnectionState,
  config: Config,
  scram_state: scram.ScramState,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  case msg {
    AuthenticationMsg(AuthSaslFinal(data: server_final)) ->
      case scram.verify_server(server_final, scram_state) {
        Ok(_) -> authenticate_loop(state, config, True)
        Error(e) -> Error(error.AuthenticationError(e))
      }
    ErrorResponse(fields) ->
      Error(error.AuthenticationError(error.parse_error_fields(fields).message))
    other ->
      Error(error.ProtocolError(
        "Expected SASL final, got " <> message_name(other),
      ))
  }
}

/// Process startup messages after authentication until ReadyForQuery.
fn startup_loop(
  state: ConnectionState,
  config: Config,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  case msg {
    ReadyForQuery(status) ->
      Ok(ConnectionState(..state, transaction_status: status))
    BackendKeyData(pid: pid, key: key) ->
      startup_loop(
        ConnectionState(
          ..state,
          connection_id: Some(pid),
          connection_key: Some(key),
        ),
        config,
      )
    ErrorResponse(fields) ->
      Error(error.PgError(
        fields: error.parse_error_fields(fields),
        connection_id: state.connection_id,
        query: None,
      ))
    other ->
      Error(error.ProtocolError(
        "Unexpected " <> message_name(other) <> " during startup",
      ))
  }
}

/// Send Terminate and close the socket.
pub fn disconnect(state: ConnectionState) -> Nil {
  let _ = send_message(state, message.Terminate)
  transport.close(state.transport)
}

/// Ask the server to cancel whatever this connection is running. Opens a
/// separate short-lived connection, as the protocol requires. Best effort:
/// the server may have finished already.
pub fn cancel(state: ConnectionState, config: Config) -> Result(Nil, Error) {
  case state.connection_id, state.connection_key {
    Some(pid), Some(key) -> {
      use socket <- result.try(transport.connect(
        config.host,
        config.port,
        config.connect_timeout,
      ))
      let t = transport.Tcp(socket)
      let sent =
        transport.send(
          t,
          message.encode_frontend(message.CancelRequest(pid, key)),
        )
      transport.close(t)
      sent
    }
    _, _ -> Error(error.ConnectionError("No backend key data to cancel with"))
  }
}

// =============================================================================
// Sending and receiving
// =============================================================================

/// Send one frontend message.
pub fn send_message(
  state: ConnectionState,
  msg: message.FrontendMessage,
) -> Result(ConnectionState, Error) {
  send_bytes(state, message.encode_frontend(msg))
}

/// Send several frontend messages in one write.
pub fn send_messages(
  state: ConnectionState,
  msgs: List(message.FrontendMessage),
) -> Result(ConnectionState, Error) {
  msgs
  |> list.map(message.encode_frontend)
  |> bit_array.concat
  |> send_bytes(state, _)
}

/// Send raw bytes.
pub fn send_bytes(
  state: ConnectionState,
  bytes: BitArray,
) -> Result(ConnectionState, Error) {
  case transport.send(state.transport, bytes) {
    Ok(_) -> Ok(state)
    Error(e) -> Error(e)
  }
}

/// Receive the next backend message.
///
/// Asynchronous messages are handled here and never returned:
/// NotificationResponse is queued in `state.notifications`, ParameterStatus
/// updates `state.parameters`, and NoticeResponse is dropped.
pub fn receive_message(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(BackendMessage, ConnectionState), Error) {
  use #(msg, state) <- result.try(receive_raw(state, timeout))
  case msg {
    NotificationResponse(pg_pid, channel, payload) ->
      receive_message(
        ConnectionState(..state, notifications: [
          Notification(channel:, payload:, pg_pid:),
          ..state.notifications
        ]),
        timeout,
      )
    ParameterStatus(name:, value:) ->
      receive_message(
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, value),
        ),
        timeout,
      )
    NoticeResponse(_) -> receive_message(state, timeout)
    _ -> Ok(#(msg, state))
  }
}

fn receive_raw(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(BackendMessage, ConnectionState), Error) {
  case message.decode_backend(state.buffer) {
    Decoded(msg, rest) -> Ok(#(msg, ConnectionState(..state, buffer: rest)))
    DecodeFailed(reason) ->
      Error(error.ProtocolError("Decode failed: " <> reason))
    Incomplete -> {
      use _ <- result.try(case message.peek_message_size(state.buffer) {
        Ok(size) if size > state.max_message_size ->
          Error(error.ProtocolError(
            "Server message of "
            <> int.to_string(size)
            <> " bytes exceeds max_message_size",
          ))
        _ -> Ok(Nil)
      })
      use data <- result.try(transport.receive(state.transport, timeout))
      receive_raw(
        ConnectionState(..state, buffer: <<state.buffer:bits, data:bits>>),
        timeout,
      )
    }
  }
}

/// Take the notifications received so far (oldest first) and, if there are
/// none, wait up to `timeout` milliseconds for one to arrive. The connection
/// must be idle (not in the middle of a query).
pub fn wait_for_notifications(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(List(Notification), ConnectionState), Error) {
  case state.notifications, state.buffer {
    [], <<>> ->
      case transport.receive(state.transport, timeout) {
        Ok(data) ->
          drain_buffered_notifications(ConnectionState(..state, buffer: data))
        Error(error.TimeoutError) -> Ok(#([], state))
        Error(e) -> Error(e)
      }
    _, _ -> drain_buffered_notifications(state)
  }
}

/// Decode every complete message already buffered (all must be
/// asynchronous) and return the queued notifications.
fn drain_buffered_notifications(
  state: ConnectionState,
) -> Result(#(List(Notification), ConnectionState), Error) {
  case message.decode_backend(state.buffer) {
    Incomplete -> Ok(take_notifications(state))
    DecodeFailed(reason) ->
      Error(error.ProtocolError("Decode failed: " <> reason))
    Decoded(msg, rest) -> {
      let state = ConnectionState(..state, buffer: rest)
      case msg {
        NotificationResponse(pg_pid, channel, payload) ->
          drain_buffered_notifications(
            ConnectionState(..state, notifications: [
              Notification(channel:, payload:, pg_pid:),
              ..state.notifications
            ]),
          )
        ParameterStatus(name:, value:) ->
          drain_buffered_notifications(
            ConnectionState(
              ..state,
              parameters: dict.insert(state.parameters, name, value),
            ),
          )
        NoticeResponse(_) -> drain_buffered_notifications(state)
        ErrorResponse(fields) ->
          // e.g. the server terminating an idle session
          Error(error.PgError(
            fields: error.parse_error_fields(fields),
            connection_id: state.connection_id,
            query: None,
          ))
        other ->
          Error(error.ProtocolError(
            "Unexpected " <> message_name(other) <> " on an idle connection",
          ))
      }
    }
  }
}

/// Take all queued notifications, oldest first.
pub fn take_notifications(
  state: ConnectionState,
) -> #(List(Notification), ConnectionState) {
  #(
    list.reverse(state.notifications),
    ConnectionState(..state, notifications: []),
  )
}

// =============================================================================
// Error helpers
// =============================================================================

fn fatal(e: Error) -> Failure {
  Failure(error: e, state: None)
}

fn lift(r: Result(a, Error)) -> Result(a, Failure) {
  result.map_error(r, fatal)
}

fn next(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(BackendMessage, ConnectionState), Failure) {
  receive_message(state, timeout) |> lift
}

/// The server reported an error. It now skips messages until Sync, so make
/// sure a Sync is on its way (when `send_sync`), drain to ReadyForQuery and
/// return a recoverable failure. If draining fails the connection is lost.
fn server_error(
  state: ConnectionState,
  fields: Dict(String, String),
  sql: String,
  timeout: Int,
  send_sync: Bool,
) -> Failure {
  let pg_error =
    error.PgError(
      fields: error.parse_error_fields(fields),
      connection_id: state.connection_id,
      query: Some(sql),
    )
  let synced = case send_sync {
    True -> send_message(state, message.Sync)
    False -> Ok(state)
  }
  case result.try(synced, drain_to_ready(_, timeout)) {
    Ok(state) -> Failure(error: pg_error, state: Some(state))
    Error(e) -> fatal(e)
  }
}

fn unexpected(expected: String, got: BackendMessage) -> Failure {
  fatal(error.ProtocolError(
    "Expected " <> expected <> ", got " <> message_name(got),
  ))
}

fn drain_to_ready(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    ReadyForQuery(status) ->
      Ok(ConnectionState(..state, transaction_status: status))
    _ -> drain_to_ready(state, timeout)
  }
}

/// Drop the state from a failure, for the plain `Result(_, Error)` API.
fn plain(r: Result(a, Failure)) -> Result(a, Error) {
  result.map_error(r, fn(f) { f.error })
}

fn check_sql(sql: String) -> Result(Nil, Error) {
  case string.contains(sql, "\u{0000}") {
    True -> Error(error.EncodeError("SQL text must not contain NUL bytes"))
    False -> Ok(Nil)
  }
}

// =============================================================================
// Simple query protocol
// =============================================================================

/// Result from a simple query.
pub type SimpleQueryResult {
  SimpleQueryResult(
    tag: String,
    columns: List(String),
    rows: List(List(Option(String))),
  )
}

/// Execute a simple query (text protocol) and return all results.
pub fn simple_query(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(SimpleQueryResult), ConnectionState), Error) {
  simple_query_f(state, sql, timeout) |> plain
}

@internal
pub fn simple_query_f(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(SimpleQueryResult), ConnectionState), Failure) {
  use _ <- result.try(
    check_sql(sql) |> result.map_error(Failure(_, Some(state))),
  )
  use state <- result.try(send_message(state, message.SimpleQuery(sql)) |> lift)
  simple_query_loop(state, sql, timeout, [], [], [], None)
}

fn simple_query_loop(
  state: ConnectionState,
  sql: String,
  timeout: Int,
  results: List(SimpleQueryResult),
  columns: List(String),
  rows: List(List(Option(String))),
  failure: Option(Error),
) -> Result(#(List(SimpleQueryResult), ConnectionState), Failure) {
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    ReadyForQuery(status) -> {
      let state = ConnectionState(..state, transaction_status: status)
      case failure {
        None -> Ok(#(list.reverse(results), state))
        Some(e) -> Error(Failure(error: e, state: Some(state)))
      }
    }
    RowDescription(fields) ->
      simple_query_loop(
        state,
        sql,
        timeout,
        results,
        list.map(fields, fn(f) { f.name }),
        [],
        failure,
      )
    DataRow(values) -> {
      let row = case decode_text_row(values) {
        Ok(row) -> Ok(row)
        Error(e) -> Error(e)
      }
      case row, failure {
        Ok(row), _ ->
          simple_query_loop(
            state,
            sql,
            timeout,
            results,
            columns,
            [row, ..rows],
            failure,
          )
        Error(e), None ->
          simple_query_loop(
            state,
            sql,
            timeout,
            results,
            columns,
            rows,
            Some(e),
          )
        Error(_), Some(_) ->
          simple_query_loop(
            state,
            sql,
            timeout,
            results,
            columns,
            rows,
            failure,
          )
      }
    }
    CommandComplete(tag) ->
      simple_query_loop(
        state,
        sql,
        timeout,
        [SimpleQueryResult(tag:, columns:, rows: list.reverse(rows)), ..results],
        [],
        [],
        failure,
      )
    message.EmptyQueryResponse ->
      simple_query_loop(state, sql, timeout, results, columns, rows, failure)
    ErrorResponse(fields) ->
      // Simple query always ends with ReadyForQuery; keep reading to it.
      simple_query_loop(
        state,
        sql,
        timeout,
        results,
        columns,
        rows,
        Some(error.PgError(
          fields: error.parse_error_fields(fields),
          connection_id: state.connection_id,
          query: Some(sql),
        )),
      )
    // COPY started from a simple query: refuse it and resync.
    message.CopyInResponse(..) ->
      case
        send_message(
          state,
          message.CopyFail("COPY is not supported by simple_query; use copy_in"),
        )
      {
        Ok(state) ->
          simple_query_loop(
            state,
            sql,
            timeout,
            results,
            columns,
            rows,
            failure,
          )
        Error(e) -> Error(fatal(e))
      }
    other -> Error(unexpected("a simple query response", other))
  }
}

fn decode_text_row(values: BitArray) -> Result(List(Option(String)), Error) {
  case message.extract_row_values(values) {
    Error(_) -> Error(error.ProtocolError("Malformed DataRow"))
    Ok(cells) ->
      list.try_map(cells, fn(cell) {
        case cell {
          None -> Ok(None)
          Some(bytes) ->
            case bit_array.to_string(bytes) {
              Ok(s) -> Ok(Some(s))
              Error(_) ->
                Error(error.DecodeError("Text column is not valid UTF-8"))
            }
        }
      })
  }
}

// =============================================================================
// Extended query protocol
// =============================================================================

/// A prepared statement returned by `prepare`.
pub type PreparedStatement {
  PreparedStatement(
    name: String,
    statement: String,
    param_oids: List(Int),
    result_fields: List(RowField),
  )
}

/// Result from an extended query execution.
pub type ExtendedQueryResult {
  ExtendedQueryResult(
    tag: String,
    columns: List(RowField),
    rows: List(List(Option(Value))),
  )
}

/// Prepare a named statement: Parse + Describe + Sync.
pub fn prepare(
  state: ConnectionState,
  name: String,
  sql: String,
  type_oids: List(Int),
  timeout: Int,
) -> Result(#(PreparedStatement, ConnectionState), Error) {
  prepare_f(state, name, sql, type_oids, timeout) |> plain
}

@internal
pub fn prepare_f(
  state: ConnectionState,
  name: String,
  sql: String,
  type_oids: List(Int),
  timeout: Int,
) -> Result(#(PreparedStatement, ConnectionState), Failure) {
  use _ <- result.try(
    check_sql(sql) |> result.map_error(Failure(_, Some(state))),
  )
  use state <- result.try(
    send_messages(state, [
      message.Parse(name, sql, type_oids),
      message.Describe(message.DescribeStatement, name),
      message.Sync,
    ])
    |> lift,
  )
  use #(prepared, state) <- result.try(recv_description(
    state,
    name,
    sql,
    timeout,
    False,
  ))
  use state <- result.try(recv_ready(state, sql, timeout))
  Ok(#(prepared, state))
}

/// Receive ParseComplete + ParameterDescription + RowDescription/NoData.
fn recv_description(
  state: ConnectionState,
  name: String,
  sql: String,
  timeout: Int,
  needs_sync: Bool,
) -> Result(#(PreparedStatement, ConnectionState), Failure) {
  use #(msg, state) <- result.try(next(state, timeout))
  use state <- result.try(case msg {
    ParseComplete -> Ok(state)
    ErrorResponse(f) -> Error(server_error(state, f, sql, timeout, needs_sync))
    other -> Error(unexpected("ParseComplete", other))
  })
  use #(msg, state) <- result.try(next(state, timeout))
  use #(param_oids, state) <- result.try(case msg {
    ParameterDescription(oids) -> Ok(#(oids, state))
    ErrorResponse(f) -> Error(server_error(state, f, sql, timeout, needs_sync))
    other -> Error(unexpected("ParameterDescription", other))
  })
  use #(msg, state) <- result.try(next(state, timeout))
  use #(fields, state) <- result.try(case msg {
    RowDescription(fields) -> Ok(#(fields, state))
    NoData -> Ok(#([], state))
    ErrorResponse(f) -> Error(server_error(state, f, sql, timeout, needs_sync))
    other -> Error(unexpected("RowDescription or NoData", other))
  })
  Ok(#(
    PreparedStatement(
      name: name,
      statement: sql,
      param_oids: param_oids,
      result_fields: fields,
    ),
    state,
  ))
}

fn recv_ready(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(ConnectionState, Failure) {
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    ReadyForQuery(status) ->
      Ok(ConnectionState(..state, transaction_status: status))
    ErrorResponse(f) -> Error(server_error(state, f, sql, timeout, False))
    other -> Error(unexpected("ReadyForQuery", other))
  }
}

/// Execute a prepared statement: Bind + Execute + Sync, sent as one write.
pub fn execute_prepared(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  execute_prepared_f(state, prepared, params, registry, timeout) |> plain
}

@internal
pub fn execute_prepared_f(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Failure) {
  execute_pipeline_f(state, prepared, [params], registry, timeout)
  |> result.map(fn(pair) {
    let #(results, state) = pair
    case results {
      [r] -> #(r, state)
      // One param set always yields exactly one result.
      _ -> panic as "execute_pipeline_f returned the wrong number of results"
    }
  })
}

/// Execute a prepared statement with multiple param sets in one pipeline:
/// N × (Bind + Execute) + one Sync, in a single write.
pub fn execute_pipeline(
  state: ConnectionState,
  prepared: PreparedStatement,
  param_sets: List(List(Option(Value))),
  registry: Registry,
  timeout: Int,
) -> Result(#(List(ExtendedQueryResult), ConnectionState), Error) {
  execute_pipeline_f(state, prepared, param_sets, registry, timeout) |> plain
}

@internal
pub fn execute_pipeline_f(
  state: ConnectionState,
  prepared: PreparedStatement,
  param_sets: List(List(Option(Value))),
  registry: Registry,
  timeout: Int,
) -> Result(#(List(ExtendedQueryResult), ConnectionState), Failure) {
  // Encode everything before sending anything, so an encode error leaves
  // the connection untouched.
  use binds <- result.try(
    build_binds(prepared, param_sets, registry)
    |> result.map_error(Failure(_, Some(state))),
  )
  use state <- result.try(
    send_messages(state, list.append(binds, [message.Sync])) |> lift,
  )
  let decoders = resolve_row_decoders(prepared.result_fields, registry)
  use #(results, state) <- result.try(recv_results(
    state,
    prepared,
    decoders,
    timeout,
    list.length(param_sets),
    [],
    None,
  ))
  use state <- result.try(recv_ready(state, prepared.statement, timeout))
  Ok(#(results, state))
}

fn build_binds(
  prepared: PreparedStatement,
  param_sets: List(List(Option(Value))),
  registry: Registry,
) -> Result(List(message.FrontendMessage), Error) {
  let result_formats = result_formats(prepared.result_fields, registry)
  use per_set <- result.try(
    list.try_map(param_sets, fn(params) {
      use #(formats, encoded) <- result.try(encode_params(
        params,
        prepared.param_oids,
        registry,
      ))
      Ok([
        message.Bind("", prepared.name, formats, encoded, result_formats),
        message.Execute("", 0),
      ])
    }),
  )
  Ok(list.flatten(per_set))
}

/// Read `remaining` × (BindComplete, DataRow*, CommandComplete).
/// A decode error is remembered and reported after the response is fully
/// read, so the connection stays in sync.
fn recv_results(
  state: ConnectionState,
  prepared: PreparedStatement,
  decoders: List(RowDecoderFn),
  timeout: Int,
  remaining: Int,
  acc: List(ExtendedQueryResult),
  decode_failure: Option(Error),
) -> Result(#(List(ExtendedQueryResult), ConnectionState), Failure) {
  case remaining {
    0 ->
      case decode_failure {
        None -> Ok(#(list.reverse(acc), state))
        Some(e) -> {
          use state <- result.try(recv_ready(state, prepared.statement, timeout))
          Error(Failure(error: e, state: Some(state)))
        }
      }
    _ -> {
      use #(msg, state) <- result.try(next(state, timeout))
      use state <- result.try(case msg {
        BindComplete -> Ok(state)
        ErrorResponse(f) ->
          Error(server_error(state, f, prepared.statement, timeout, False))
        other -> Error(unexpected("BindComplete", other))
      })
      use #(tag, rows, state, decode_failure) <- result.try(recv_rows(
        state,
        prepared.statement,
        decoders,
        timeout,
        [],
        decode_failure,
      ))
      let r =
        ExtendedQueryResult(
          tag: tag,
          columns: prepared.result_fields,
          rows: rows,
        )
      recv_results(
        state,
        prepared,
        decoders,
        timeout,
        remaining - 1,
        [r, ..acc],
        decode_failure,
      )
    }
  }
}

fn recv_rows(
  state: ConnectionState,
  sql: String,
  decoders: List(RowDecoderFn),
  timeout: Int,
  rows: List(List(Option(Value))),
  decode_failure: Option(Error),
) -> Result(
  #(String, List(List(Option(Value))), ConnectionState, Option(Error)),
  Failure,
) {
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    DataRow(values) ->
      case decode_failure {
        Some(_) ->
          recv_rows(state, sql, decoders, timeout, rows, decode_failure)
        None ->
          case decode_row(values, decoders) {
            Ok(row) ->
              recv_rows(state, sql, decoders, timeout, [row, ..rows], None)
            Error(e) -> recv_rows(state, sql, decoders, timeout, rows, Some(e))
          }
      }
    CommandComplete(tag) ->
      Ok(#(tag, list.reverse(rows), state, decode_failure))
    message.EmptyQueryResponse ->
      Ok(#("", list.reverse(rows), state, decode_failure))
    ErrorResponse(f) -> Error(server_error(state, f, sql, timeout, False))
    other -> Error(unexpected("DataRow or CommandComplete", other))
  }
}

/// Close a prepared statement: Close + Sync.
pub fn close_statement(
  state: ConnectionState,
  name: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  close_statement_f(state, name, timeout) |> plain
}

@internal
pub fn close_statement_f(
  state: ConnectionState,
  name: String,
  timeout: Int,
) -> Result(ConnectionState, Failure) {
  use state <- result.try(
    send_messages(state, [
      message.Close(message.DescribeStatement, name),
      message.Sync,
    ])
    |> lift,
  )
  use #(msg, state) <- result.try(next(state, timeout))
  use state <- result.try(case msg {
    CloseComplete -> Ok(state)
    ErrorResponse(f) -> Error(server_error(state, f, "", timeout, False))
    other -> Error(unexpected("CloseComplete", other))
  })
  recv_ready(state, "", timeout)
}

/// One-off extended query with an unnamed statement:
/// Parse + Describe + Sync, then Bind + Execute + Sync (two round trips).
pub fn extended_query(
  state: ConnectionState,
  sql: String,
  params: List(Option(Value)),
  registry: Registry,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  use #(prepared, state) <- result.try(prepare(state, "", sql, [], timeout))
  execute_prepared(state, prepared, params, registry, timeout)
}

/// PgBouncer-safe extended query using the unnamed statement.
/// Parse + Describe + Flush (no ReadyForQuery, so PgBouncer keeps the
/// backend), then Bind + Execute + Sync.
pub fn extended_query_unnamed(
  state: ConnectionState,
  sql: String,
  params: List(Option(Value)),
  registry: Registry,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  extended_query_unnamed_f(state, sql, params, registry, timeout) |> plain
}

@internal
pub fn extended_query_unnamed_f(
  state: ConnectionState,
  sql: String,
  params: List(Option(Value)),
  registry: Registry,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Failure) {
  extended_query_pipeline_unnamed_f(state, sql, [params], registry, timeout)
  |> result.map(fn(pair) {
    let #(results, state) = pair
    case results {
      [r] -> #(r, state)
      _ -> panic as "pipeline returned the wrong number of results"
    }
  })
}

/// PgBouncer-safe batch query: Parse + Describe + Flush, then
/// N × (Bind + Execute) + Sync.
pub fn extended_query_pipeline_unnamed(
  state: ConnectionState,
  sql: String,
  param_sets: List(List(Option(Value))),
  registry: Registry,
  timeout: Int,
) -> Result(#(List(ExtendedQueryResult), ConnectionState), Error) {
  extended_query_pipeline_unnamed_f(state, sql, param_sets, registry, timeout)
  |> plain
}

@internal
pub fn extended_query_pipeline_unnamed_f(
  state: ConnectionState,
  sql: String,
  param_sets: List(List(Option(Value))),
  registry: Registry,
  timeout: Int,
) -> Result(#(List(ExtendedQueryResult), ConnectionState), Failure) {
  use _ <- result.try(
    check_sql(sql) |> result.map_error(Failure(_, Some(state))),
  )
  use state <- result.try(
    send_messages(state, [
      message.Parse("", sql, []),
      message.Describe(message.DescribeStatement, ""),
      message.Flush,
    ])
    |> lift,
  )
  // Errors here need an explicit Sync: Flush does not end the cycle.
  use #(prepared, state) <- result.try(recv_description(
    state,
    "",
    sql,
    timeout,
    True,
  ))
  case build_binds(prepared, param_sets, registry) {
    Error(e) -> {
      // Close the open cycle so the connection stays usable.
      case
        send_message(state, message.Sync)
        |> result.try(drain_to_ready(_, timeout))
      {
        Ok(state) -> Error(Failure(error: e, state: Some(state)))
        Error(e2) -> Error(fatal(e2))
      }
    }
    Ok(binds) -> {
      use state <- result.try(
        send_messages(state, list.append(binds, [message.Sync])) |> lift,
      )
      let decoders = resolve_row_decoders(prepared.result_fields, registry)
      use #(results, state) <- result.try(recv_results(
        state,
        prepared,
        decoders,
        timeout,
        list.length(param_sets),
        [],
        None,
      ))
      use state <- result.try(recv_ready(state, sql, timeout))
      Ok(#(results, state))
    }
  }
}

// =============================================================================
// Portal streaming
// =============================================================================

/// Result of one streamed Execute.
pub type StreamChunk {
  /// More rows available; call `execute_portal` again.
  StreamMore(rows: List(List(Option(Value))))
  /// All rows received, with the command tag. Call `sync_portal` next.
  StreamDone(tag: String, rows: List(List(Option(Value))))
}

/// Bind parameters and fetch the first chunk: Bind + Execute(max_rows) + Flush.
/// Flush (not Sync) keeps the unnamed portal open. On error the connection
/// is resynchronised before returning.
pub fn bind_and_execute_portal(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Error) {
  bind_and_execute_portal_f(
    state,
    prepared,
    params,
    registry,
    max_rows,
    timeout,
  )
  |> plain
}

@internal
pub fn bind_and_execute_portal_f(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Failure) {
  use #(formats, encoded) <- result.try(
    encode_params(params, prepared.param_oids, registry)
    |> result.map_error(Failure(_, Some(state))),
  )
  use state <- result.try(
    send_messages(state, [
      message.Bind(
        "",
        prepared.name,
        formats,
        encoded,
        result_formats(prepared.result_fields, registry),
      ),
      message.Execute("", max_rows),
      message.Flush,
    ])
    |> lift,
  )
  use #(msg, state) <- result.try(next(state, timeout))
  use state <- result.try(case msg {
    BindComplete -> Ok(state)
    ErrorResponse(f) ->
      Error(server_error(state, f, prepared.statement, timeout, True))
    other -> Error(unexpected("BindComplete", other))
  })
  recv_chunk(state, prepared, registry, timeout)
}

/// Fetch the next chunk from the open portal: Execute(max_rows) + Flush.
pub fn execute_portal(
  state: ConnectionState,
  prepared: PreparedStatement,
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Error) {
  execute_portal_f(state, prepared, registry, max_rows, timeout) |> plain
}

@internal
pub fn execute_portal_f(
  state: ConnectionState,
  prepared: PreparedStatement,
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Failure) {
  use state <- result.try(
    send_messages(state, [message.Execute("", max_rows), message.Flush])
    |> lift,
  )
  recv_chunk(state, prepared, registry, timeout)
}

/// End portal streaming: Sync, then ReadyForQuery.
pub fn sync_portal(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  sync_portal_f(state, timeout) |> plain
}

@internal
pub fn sync_portal_f(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Failure) {
  use state <- result.try(send_message(state, message.Sync) |> lift)
  recv_ready(state, "", timeout)
}

fn recv_chunk(
  state: ConnectionState,
  prepared: PreparedStatement,
  registry: Registry,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Failure) {
  let decoders = resolve_row_decoders(prepared.result_fields, registry)
  recv_chunk_loop(state, prepared.statement, decoders, timeout, [])
}

fn recv_chunk_loop(
  state: ConnectionState,
  sql: String,
  decoders: List(RowDecoderFn),
  timeout: Int,
  rows: List(List(Option(Value))),
) -> Result(#(StreamChunk, ConnectionState), Failure) {
  use #(msg, state) <- result.try(next(state, timeout))
  case msg {
    DataRow(values) ->
      case decode_row(values, decoders) {
        Ok(row) -> recv_chunk_loop(state, sql, decoders, timeout, [row, ..rows])
        Error(e) -> {
          // Abandon the portal and resynchronise before reporting.
          case
            send_message(state, message.Sync)
            |> result.try(drain_to_ready(_, timeout))
          {
            Ok(state) -> Error(Failure(error: e, state: Some(state)))
            Error(e2) -> Error(fatal(e2))
          }
        }
      }
    CommandComplete(tag) -> Ok(#(StreamDone(tag, list.reverse(rows)), state))
    message.EmptyQueryResponse ->
      Ok(#(StreamDone("", list.reverse(rows)), state))
    PortalSuspended -> Ok(#(StreamMore(list.reverse(rows)), state))
    ErrorResponse(f) -> Error(server_error(state, f, sql, timeout, True))
    other ->
      Error(unexpected("DataRow, CommandComplete or PortalSuspended", other))
  }
}

// =============================================================================
// Parameter encoding and row decoding
// =============================================================================

type RowDecoderFn =
  fn(BitArray) -> Result(Value, String)

/// Encode parameters. `Text` values are sent in PostgreSQL's text format for
/// any column type, so the server parses them (enums, ranges, UUID strings,
/// custom types); everything else is encoded in binary by the type's codec.
fn encode_params(
  params: List(Option(Value)),
  oids: List(Int),
  reg: Registry,
) -> Result(#(List(message.Format), List(Option(BitArray))), Error) {
  case list.length(params) == list.length(oids) {
    False ->
      Error(error.EncodeError(
        "Expected "
        <> int.to_string(list.length(oids))
        <> " parameters, got "
        <> int.to_string(list.length(params)),
      ))
    True -> {
      use pairs <- result.try(
        list.zip(params, oids)
        |> list.index_map(fn(pair, i) { #(i, pair) })
        |> list.try_map(fn(entry) {
          let #(i, #(param, oid)) = entry
          encode_param(param, oid, reg)
          |> result.map_error(fn(e) {
            error.EncodeError(
              "Parameter $" <> int.to_string(i + 1) <> ": " <> e,
            )
          })
        }),
      )
      Ok(list.unzip(pairs))
    }
  }
}

fn encode_param(
  param: Option(Value),
  oid: Int,
  reg: Registry,
) -> Result(#(message.Format, Option(BitArray)), String) {
  case param {
    None -> Ok(#(message.BinaryFormat, None))
    Some(value.Text(s)) ->
      case string.contains(s, "\u{0000}") {
        True -> Error("text must not contain NUL bytes")
        False -> Ok(#(message.TextFormat, Some(bit_array.from_string(s))))
      }
    Some(val) -> {
      use codec <- result.try(registry.lookup(reg, oid))
      use bytes <- result.try(codec.encode(val))
      Ok(#(message.BinaryFormat, Some(bytes)))
    }
  }
}

/// Columns with a binary codec are requested in binary; anything else
/// (custom types, ranges, extensions) in text, so it decodes as `Text`
/// instead of opaque binary.
fn result_formats(
  fields: List(RowField),
  reg: Registry,
) -> List(message.Format) {
  list.map(fields, fn(field) {
    case registry.lookup(reg, field.type_oid) {
      Ok(_) -> message.BinaryFormat
      Error(_) -> message.TextFormat
    }
  })
}

fn resolve_row_decoders(
  fields: List(RowField),
  reg: Registry,
) -> List(RowDecoderFn) {
  list.map(fields, fn(field) {
    case registry.lookup(reg, field.type_oid) {
      Ok(codec) -> codec.decode
      Error(_) -> fn(bytes) {
        case bit_array.to_string(bytes) {
          Ok(s) -> Ok(value.Text(s))
          Error(_) -> Error("text-format column is not valid UTF-8")
        }
      }
    }
  })
}

fn decode_row(
  values: BitArray,
  decoders: List(RowDecoderFn),
) -> Result(List(Option(Value)), Error) {
  case message.extract_row_values(values) {
    Error(_) -> Error(error.ProtocolError("Malformed DataRow"))
    Ok(cells) -> decode_cells(cells, decoders, 0, [])
  }
}

fn decode_cells(
  cells: List(Option(BitArray)),
  decoders: List(RowDecoderFn),
  index: Int,
  acc: List(Option(Value)),
) -> Result(List(Option(Value)), Error) {
  case cells, decoders {
    [], [] -> Ok(list.reverse(acc))
    [None, ..cells], [_, ..decoders] ->
      decode_cells(cells, decoders, index + 1, [None, ..acc])
    [Some(bytes), ..cells], [decoder, ..decoders] ->
      case decoder(bytes) {
        Ok(v) -> decode_cells(cells, decoders, index + 1, [Some(v), ..acc])
        Error(e) ->
          Error(error.DecodeError(
            "Column " <> int.to_string(index) <> ": " <> e,
          ))
      }
    _, _ ->
      Error(error.DecodeError(
        "DataRow has a different number of columns than its RowDescription",
      ))
  }
}

// =============================================================================
// Helpers
// =============================================================================

/// A short name for a backend message, for error messages.
pub fn message_name(msg: BackendMessage) -> String {
  case msg {
    AuthenticationMsg(_) -> "Authentication"
    BackendKeyData(..) -> "BackendKeyData"
    ReadyForQuery(_) -> "ReadyForQuery"
    ParameterStatus(..) -> "ParameterStatus"
    RowDescription(_) -> "RowDescription"
    DataRow(_) -> "DataRow"
    CommandComplete(_) -> "CommandComplete"
    ParseComplete -> "ParseComplete"
    BindComplete -> "BindComplete"
    CloseComplete -> "CloseComplete"
    NoData -> "NoData"
    PortalSuspended -> "PortalSuspended"
    message.EmptyQueryResponse -> "EmptyQueryResponse"
    ErrorResponse(_) -> "ErrorResponse"
    NoticeResponse(_) -> "NoticeResponse"
    NotificationResponse(..) -> "NotificationResponse"
    ParameterDescription(_) -> "ParameterDescription"
    message.CopyInResponse(..) -> "CopyInResponse"
    message.CopyOutResponse(..) -> "CopyOutResponse"
    message.CopyBothResponse(..) -> "CopyBothResponse"
    message.CopyData(_) -> "CopyData"
    message.CopyDone -> "CopyDone"
  }
}

@internal
pub fn fail_to_ready(
  state: ConnectionState,
  fields: Dict(String, String),
  sql: String,
  timeout: Int,
) -> Failure {
  server_error(state, fields, sql, timeout, False)
}

@internal
pub fn protocol_failure(expected: String, got: BackendMessage) -> Failure {
  unexpected(expected, got)
}
