//// A PostgreSQL driver for Gleam, implementing the wire protocol in Gleam.
////
//// ```gleam
//// import postgleam
//// import postgleam/config
//// import postgleam/decode
////
//// pub fn main() {
////   let assert Ok(cfg) = config.from_url("postgres://app:secret@localhost/app")
////   let assert Ok(conn) = postgleam.connect(cfg)
////   let assert Ok(response) =
////     postgleam.query_with(
////       conn,
////       "SELECT id, name FROM users WHERE active = $1",
////       [postgleam.bool(True)],
////       {
////         use id <- decode.element(0, decode.int)
////         use name <- decode.element(1, decode.text)
////         decode.success(#(id, name))
////       },
////     )
////   postgleam.disconnect(conn)
//// }
//// ```
////
//// A `Connection` is one PostgreSQL session owned by an actor. It runs one
//// request at a time; use `postgleam/pool` to run queries concurrently.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import postgleam/config.{type Config}
import postgleam/connection.{
  type ExtendedQueryResult, type PreparedStatement, type SimpleQueryResult,
}
import postgleam/decode.{type RowDecoder}
import postgleam/error.{type Error}
import postgleam/internal/call
import postgleam/internal/connection_actor.{type Message}
import postgleam/value.{type Value}

/// A query parameter value. `None` is SQL NULL.
pub type Param =
  Option(Value)

/// Result from a decoded query.
pub type Response(a) {
  Response(rows: List(a), count: Int, tag: String)
}

/// A connection handle backed by an OTP actor.
pub opaque type Connection {
  Connection(subject: Subject(Message), pid: Pid, config: Config)
}

/// Connect to PostgreSQL. The connection is owned by a new actor process
/// linked to the caller.
pub fn connect(config: Config) -> Result(Connection, Error) {
  use started <- result.try(connection_actor.start(config))
  Ok(Connection(subject: started.data, pid: started.pid, config: config))
}

@internal
pub fn from_actor(
  subject: Subject(Message),
  pid: Pid,
  config: Config,
) -> Connection {
  Connection(subject:, pid:, config:)
}

@internal
pub fn subject(conn: Connection) -> Subject(Message) {
  conn.subject
}

/// Send a request to the connection actor and wait for the reply.
///
/// Never panics: if the actor dies the result is `ConnectionError`, and if no
/// reply arrives in time it is `TimeoutError`. The actor bounds every
/// network wait by `config.timeout` (cancelling the query on the server),
/// so the caller allows for that plus a reconnect.
fn call(
  conn: Connection,
  make: fn(Subject(Result(a, Error))) -> Message,
) -> Result(a, Error) {
  let wait = conn.config.timeout * 2 + conn.config.connect_timeout + 1000
  call.call(
    conn.subject,
    make,
    call.Within(wait),
    Error(not_running()),
    Error(error.TimeoutError),
  )
}

fn not_running() -> Error {
  error.ConnectionError("Connection process is not running")
}

/// Execute a parameterized query (extended protocol, binary format). The
/// statement is prepared once per connection and cached.
pub fn query(
  conn: Connection,
  sql: String,
  params: List(Param),
) -> Result(ExtendedQueryResult, Error) {
  call(conn, connection_actor.Query(sql, params, _))
}

/// Execute a parameterized query and decode each row with `decoder`.
pub fn query_with(
  conn: Connection,
  sql: String,
  params: List(Param),
  decoder: RowDecoder(a),
) -> Result(Response(a), Error) {
  use result <- result.try(query(conn, sql, params))
  decode_result(result, decoder)
}

@internal
pub fn decode_result(
  result: ExtendedQueryResult,
  decoder: RowDecoder(a),
) -> Result(Response(a), Error) {
  let columns = list.map(result.columns, fn(c) { c.name })
  use rows <- result.try(
    list.try_map(result.rows, decode.run_with_columns(decoder, columns, _)),
  )
  Ok(Response(rows: rows, count: list.length(rows), tag: result.tag))
}

/// Execute the same statement with several parameter sets in one pipeline
/// (one network round trip). Returns one result per parameter set.
pub fn query_batch(
  conn: Connection,
  sql: String,
  param_sets: List(List(Param)),
) -> Result(List(ExtendedQueryResult), Error) {
  call(conn, connection_actor.BatchQuery(sql, param_sets, _))
}

/// Execute a query and decode the first row. Errors if there are no rows.
pub fn query_one(
  conn: Connection,
  sql: String,
  params: List(Param),
  decoder: RowDecoder(a),
) -> Result(a, Error) {
  use response <- result.try(query_with(conn, sql, params, decoder))
  case response.rows {
    [first, ..] -> Ok(first)
    [] -> Error(error.DecodeError("Expected at least one row, got none"))
  }
}

/// Fold over a query's rows without loading them all into memory. Rows are
/// fetched `chunk_size` at a time; `f` runs inside the connection's actor,
/// so it should be quick and must not use this connection.
pub fn query_fold(
  conn: Connection,
  sql: String,
  params: List(Param),
  chunk_size: Int,
  decoder: RowDecoder(a),
  initial: acc,
  f: fn(acc, a) -> acc,
) -> Result(acc, Error) {
  let out = process.new_subject()
  let step = fn(acc: Result(acc, Error), columns, row) {
    use acc <- result.try(acc)
    use decoded <- result.try(decode.run_with_columns(decoder, columns, row))
    Ok(f(acc, decoded))
  }
  let runner =
    connection_actor.fold_runner(
      params,
      chunk_size,
      conn.config.timeout,
      Ok(initial),
      step,
      out,
    )
  // A fold can take many round trips, so wait for the actor without a
  // deadline: it bounds each network wait itself, and the monitor catches
  // a crash.
  use _ <- result.try(call.call(
    conn.subject,
    connection_actor.Fold(sql, runner, _),
    call.UntilReplyOrExit,
    Error(not_running()),
    Error(error.TimeoutError),
  ))
  case process.receive(out, 0) {
    Ok(acc) -> acc
    Error(Nil) ->
      Error(error.ProtocolError("query_fold finished without a result"))
  }
}

/// Execute a simple (text protocol) query. Use for DDL and multi-statement
/// scripts; values come back as text.
pub fn simple_query(
  conn: Connection,
  sql: String,
) -> Result(List(SimpleQueryResult), Error) {
  call(conn, connection_actor.SimpleQuery(sql, _))
}

/// Prepare a named statement for later `execute`.
pub fn prepare(
  conn: Connection,
  name: String,
  sql: String,
) -> Result(PreparedStatement, Error) {
  call(conn, connection_actor.Prepare(name, sql, _))
}

/// Execute a previously prepared statement.
pub fn execute(
  conn: Connection,
  prepared: PreparedStatement,
  params: List(Param),
) -> Result(ExtendedQueryResult, Error) {
  call(conn, connection_actor.ExecutePrepared(prepared, params, _))
}

/// Close a prepared statement.
pub fn close(conn: Connection, name: String) -> Result(Nil, Error) {
  call(conn, connection_actor.CloseStatement(name, _))
}

/// Bulk insert with COPY. `sql` is a `COPY ... FROM STDIN` statement and
/// `data` the rows in COPY text format (each ending with `\n`).
/// Returns the command tag (e.g. `"COPY 1000"`).
pub fn copy_in(
  conn: Connection,
  sql: String,
  data: List(BitArray),
) -> Result(String, Error) {
  call(conn, connection_actor.CopyIn(sql, data, _))
}

/// Bulk export with COPY. `sql` is a `COPY ... TO STDOUT` statement.
pub fn copy_out(
  conn: Connection,
  sql: String,
) -> Result(List(BitArray), Error) {
  call(conn, connection_actor.CopyOut(sql, _))
}

/// Send a LISTEN/NOTIFY notification. Receive them with `postgleam/listener`.
pub fn notify(
  conn: Connection,
  channel: String,
  payload: String,
) -> Result(Nil, Error) {
  query(conn, "SELECT pg_notify($1, $2)", [text(channel), text(payload)])
  |> result.replace(Nil)
}

/// Disconnect and stop the actor.
pub fn disconnect(conn: Connection) -> Nil {
  call.call(
    conn.subject,
    connection_actor.Disconnect,
    call.Within(conn.config.timeout + 1000),
    Nil,
    Nil,
  )
}

/// Why a transaction did not commit.
pub type TransactionError(e) {
  /// The function returned `Error(e)`; the transaction was rolled back.
  RolledBack(e)
  /// BEGIN, COMMIT or ROLLBACK itself failed.
  TransactionFailed(Error)
}

/// Run `f` in a transaction: BEGIN, then COMMIT if `f` returns `Ok` or
/// ROLLBACK if it returns `Error`. Nested calls are not supported (use
/// SAVEPOINT yourself); with a pool, use `pool.transaction`.
pub fn transaction(
  conn: Connection,
  f: fn(Connection) -> Result(a, e),
) -> Result(a, TransactionError(e)) {
  use _ <- result.try(
    simple_query(conn, "BEGIN") |> result.map_error(TransactionFailed),
  )
  case f(conn) {
    Ok(value) ->
      case simple_query(conn, "COMMIT") {
        Ok([connection.SimpleQueryResult(tag: "ROLLBACK", ..)]) ->
          // COMMIT of a failed transaction reports ROLLBACK.
          Error(
            TransactionFailed(error.ConnectionError(
              "Transaction was aborted by an earlier error and rolled back",
            )),
          )
        Ok(_) -> Ok(value)
        Error(e) -> Error(TransactionFailed(e))
      }
    Error(e) ->
      case simple_query(conn, "ROLLBACK") {
        Ok(_) -> Error(RolledBack(e))
        Error(rollback_error) -> Error(TransactionFailed(rollback_error))
      }
  }
}

// =============================================================================
// Parameter constructors
// =============================================================================

/// Create an integer parameter (int2, int4, int8).
pub fn int(val: Int) -> Param {
  Some(value.Integer(val))
}

/// Create a float parameter (float4, float8).
pub fn float(val: Float) -> Param {
  Some(value.Float(val))
}

/// Create a text/string parameter.
pub fn text(val: String) -> Param {
  Some(value.Text(val))
}

/// Create a boolean parameter.
pub fn bool(val: Bool) -> Param {
  Some(value.Boolean(val))
}

/// Create a NULL parameter.
pub fn null() -> Param {
  None
}

/// Create a bytea (binary data) parameter.
pub fn bytea(val: BitArray) -> Param {
  Some(value.Bytea(val))
}

/// Create a UUID parameter (16-byte binary).
pub fn uuid(val: BitArray) -> Param {
  Some(value.Uuid(val))
}

/// Create a UUID parameter from a string like
/// "550e8400-e29b-41d4-a716-446655440000". Errors if the string is not a
/// valid UUID (it never silently becomes NULL).
pub fn uuid_string(val: String) -> Result(Param, Nil) {
  value.uuid_from_string(val) |> result.map(Some)
}

/// Create a JSON parameter.
pub fn json(val: String) -> Param {
  Some(value.Json(val))
}

/// Create a JSONB parameter.
pub fn jsonb(val: String) -> Param {
  Some(value.Jsonb(val))
}

/// Create a numeric/decimal parameter (string representation).
pub fn numeric(val: String) -> Param {
  Some(value.Numeric(val))
}

/// Create a date parameter (days since 2000-01-01).
pub fn date(val: Int) -> Param {
  Some(value.Date(val))
}

/// Create a timestamp parameter (microseconds since 2000-01-01 00:00:00).
pub fn timestamp(val: Int) -> Param {
  Some(value.Timestamp(val))
}

/// Create a timestamptz parameter (microseconds since 2000-01-01 00:00:00 UTC).
pub fn timestamptz(val: Int) -> Param {
  Some(value.Timestamptz(val))
}

/// Create a time parameter (microseconds since midnight).
pub fn time(val: Int) -> Param {
  Some(value.Time(val))
}

/// Create a timetz parameter (microseconds since midnight, tz offset in seconds).
pub fn timetz(microseconds: Int, tz_offset: Int) -> Param {
  Some(value.TimeTz(microseconds, tz_offset))
}

/// Create an interval parameter (microseconds, days, months).
pub fn interval(microseconds: Int, days: Int, months: Int) -> Param {
  Some(value.Interval(microseconds, days, months))
}

/// Create an XML parameter.
pub fn xml(val: String) -> Param {
  Some(value.Xml(val))
}

/// Create a JSONPath parameter.
pub fn jsonpath(val: String) -> Param {
  Some(value.Jsonpath(val))
}

/// Create a money parameter (int64 cents).
pub fn money(val: Int) -> Param {
  Some(value.Money(val))
}

/// Create a point parameter (x, y).
pub fn point(x: Float, y: Float) -> Param {
  Some(value.Point(x, y))
}

/// Create a circle parameter (center x, center y, radius).
pub fn circle(x: Float, y: Float, radius: Float) -> Param {
  Some(value.Circle(x, y, radius))
}

/// Create a macaddr parameter (6-byte binary).
pub fn macaddr(val: BitArray) -> Param {
  Some(value.Macaddr(val))
}

/// Create a macaddr8 parameter (8-byte binary).
pub fn macaddr8(val: BitArray) -> Param {
  Some(value.Macaddr8(val))
}

/// Create an array parameter from a list of optional values.
///
/// ```gleam
/// postgleam.array([Some(value.Integer(1)), Some(value.Integer(2)), None])
/// ```
pub fn array(val: List(Option(Value))) -> Param {
  Some(value.Array(val))
}

/// Create a line parameter (Ax + By + C = 0).
pub fn line(a: Float, b: Float, c: Float) -> Param {
  Some(value.Line(a, b, c))
}

/// Create a line segment parameter (two endpoints).
pub fn lseg(x1: Float, y1: Float, x2: Float, y2: Float) -> Param {
  Some(value.Lseg(x1, y1, x2, y2))
}

/// Create a box parameter (upper-right and lower-left corners).
pub fn box(x1: Float, y1: Float, x2: Float, y2: Float) -> Param {
  Some(value.Box(x1, y1, x2, y2))
}

/// Create a path parameter (closed/open flag + list of points).
pub fn path(closed: Bool, points: List(#(Float, Float))) -> Param {
  Some(value.Path(closed, points))
}

/// Create a polygon parameter (list of vertices).
pub fn polygon(vertices: List(#(Float, Float))) -> Param {
  Some(value.Polygon(vertices))
}

/// Create an inet/cidr parameter (family, address bytes, netmask).
/// Family: 2 for IPv4, 3 for IPv6.
pub fn inet(family: Int, address: BitArray, netmask: Int) -> Param {
  Some(value.Inet(family, address, netmask))
}

/// Create a bit string parameter (bit count + data bytes).
pub fn bit_string(bit_count: Int, data: BitArray) -> Param {
  Some(value.BitString(bit_count, data))
}

/// Create a nullable parameter from an Option value.
///
/// ```gleam
/// postgleam.nullable(Some("hello"), postgleam.text)
/// // => Some(value.Text("hello"))
///
/// postgleam.nullable(None, postgleam.text)
/// // => None
/// ```
pub fn nullable(val: Option(a), to_param: fn(a) -> Param) -> Param {
  case val {
    Some(v) -> to_param(v)
    None -> None
  }
}
