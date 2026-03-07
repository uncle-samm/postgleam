/// Postgleam - A native Gleam PostgreSQL driver implementing the wire protocol.
///
/// ## Quick Start
///
/// ```gleam
/// let assert Ok(conn) = postgleam.connect(config.default() |> config.database("mydb"))
/// let assert Ok(result) = postgleam.query(conn, "SELECT $1::int4", [Some(value.Integer(42))])
/// postgleam.disconnect(conn)
/// ```

import gleam/erlang/process.{type Subject}
import gleam/option.{type Option}
import gleam/otp/actor
import postgleam/config.{type Config}
import postgleam/connection.{
  type ExtendedQueryResult, type PreparedStatement, type SimpleQueryResult,
}
import postgleam/error.{type Error}
import postgleam/internal/connection_actor.{type Message}
import postgleam/value.{type Value}

/// An opaque connection handle backed by an OTP actor
pub opaque type Connection {
  Connection(subject: Subject(Message), config: Config)
}

/// Connect to PostgreSQL and return a connection handle.
/// The connection is managed by an OTP actor process.
pub fn connect(config: Config) -> Result(Connection, String) {
  case connection_actor.start(config) {
    Ok(started) -> Ok(Connection(subject: started.data, config: config))
    Error(err) -> Error(start_error_to_string(err))
  }
}

/// Execute a parameterized query using the extended protocol (binary format).
/// Returns typed values decoded through the codec registry.
///
/// Panics if the actor does not respond within the configured timeout.
pub fn query(
  conn: Connection,
  sql: String,
  params: List(Option(Value)),
) -> Result(ExtendedQueryResult, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.Query(sql, params, reply)
  })
}

/// Execute a simple text query (no parameters).
/// Returns text-formatted results.
pub fn simple_query(
  conn: Connection,
  sql: String,
) -> Result(List(SimpleQueryResult), Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.SimpleQuery(sql, reply)
  })
}

/// Prepare a named statement for later execution.
pub fn prepare(
  conn: Connection,
  name: String,
  sql: String,
) -> Result(PreparedStatement, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.Prepare(name, sql, reply)
  })
}

/// Execute a previously prepared statement.
pub fn execute(
  conn: Connection,
  prepared: PreparedStatement,
  params: List(Option(Value)),
) -> Result(ExtendedQueryResult, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.ExecutePrepared(prepared, params, reply)
  })
}

/// Close a prepared statement.
pub fn close(conn: Connection, name: String) -> Result(Nil, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.CloseStatement(name, reply)
  })
}

/// Disconnect from PostgreSQL and stop the actor.
pub fn disconnect(conn: Connection) -> Nil {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.Disconnect(reply)
  })
}

/// Execute a function within a transaction.
/// Automatically BEGINs, and COMMITs on Ok or ROLLBACKs on Error.
pub fn transaction(
  conn: Connection,
  f: fn() -> Result(a, Error),
) -> Result(a, Error) {
  case simple_query(conn, "BEGIN") {
    Error(e) -> Error(e)
    Ok(_) -> {
      case f() {
        Ok(val) -> {
          case simple_query(conn, "COMMIT") {
            Ok(_) -> Ok(val)
            Error(e) -> Error(e)
          }
        }
        Error(e) -> {
          let _ = simple_query(conn, "ROLLBACK")
          Error(e)
        }
      }
    }
  }
}

/// Create an error value for use in transactions or other contexts.
pub fn query_error(message: String) -> Error {
  error.ConnectionError(message)
}

// --- Helpers ---

fn start_error_to_string(err: actor.StartError) -> String {
  case err {
    actor.InitTimeout -> "Connection initialization timed out"
    actor.InitFailed(reason) -> reason
    actor.InitExited(_) -> "Connection process exited during init"
  }
}
