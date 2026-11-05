//// Connection configuration for PostgreSQL.
////
//// Build a config with `default()` and the builder functions, or parse a
//// connection URL with `from_url`:
////
//// ```gleam
//// let assert Ok(cfg) =
////   config.from_url("postgres://app:secret@db.internal:5432/app?sslmode=verify-full")
//// ```

import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/uri

/// Connection configuration.
///
/// The password is held in an opaque `Secret` so that printing a config
/// (with `echo` or `string.inspect`) does not reveal it.
pub type Config {
  Config(
    host: String,
    port: Int,
    database: String,
    username: String,
    password: Secret,
    /// Milliseconds to wait for each response from the server. A query that
    /// exceeds it is cancelled on the server and returns `TimeoutError`.
    timeout: Int,
    /// Milliseconds to wait for the TCP connection and TLS handshake.
    connect_timeout: Int,
    extra_parameters: List(#(String, String)),
    ssl: SslMode,
    /// PEM file with the CA certificates used to verify the server
    /// (`SslVerified` only). `None` uses the operating system's trust store.
    ssl_ca_file: Option(String),
    /// PEM certificate and key files for TLS client certificate auth.
    ssl_client_cert: Option(ClientCert),
    /// Authentication methods the client will agree to use.
    allowed_auth: List(AuthMethod),
    pgbouncer: Bool,
    /// Pool only: idle connections are pinged after this many milliseconds.
    idle_interval: Int,
    /// Pool only: how long a caller waits for a free connection.
    queue_timeout: Int,
    /// Largest single protocol message accepted from the server, in bytes.
    max_message_size: Int,
  )
}

/// A secret string (a password). `string.inspect` shows only `//fn() { ... }`.
pub opaque type Secret {
  Secret(reveal: fn() -> String)
}

/// Wrap a string as a secret.
pub fn secret(value: String) -> Secret {
  Secret(fn() { value })
}

/// Read the value of a secret.
pub fn reveal(secret: Secret) -> String {
  secret.reveal()
}

/// A TLS client certificate and its private key, both PEM file paths.
pub type ClientCert {
  ClientCert(certfile: String, keyfile: String)
}

/// SSL connection mode.
pub type SslMode {
  /// No TLS (plain TCP). Only for local development.
  SslDisabled
  /// TLS required; the server certificate and host name are verified.
  SslVerified
  /// TLS required, but the certificate is NOT verified. Protects against
  /// passive sniffing only; an active attacker can impersonate the server.
  SslUnverified
}

/// Authentication methods a server may ask for.
pub type AuthMethod {
  /// The server accepts the connection without a password (trust auth).
  AuthNone
  /// SCRAM-SHA-256 (recommended; mutual authentication).
  AuthScram
  /// MD5 password hash (legacy; vulnerable to replay with a stolen hash).
  AuthMd5
  /// Cleartext password. Refused on non-TLS connections even when allowed.
  AuthCleartext
}

/// Default config for a local server: `postgres@localhost:5432/postgres`,
/// no password, no TLS. Set a password with `password`.
pub fn default() -> Config {
  Config(
    host: "localhost",
    port: 5432,
    database: "postgres",
    username: "postgres",
    password: secret(""),
    timeout: 15_000,
    connect_timeout: 5000,
    extra_parameters: [],
    ssl: SslDisabled,
    ssl_ca_file: None,
    ssl_client_cert: None,
    allowed_auth: [AuthNone, AuthScram, AuthMd5, AuthCleartext],
    pgbouncer: False,
    idle_interval: 1000,
    queue_timeout: 5000,
    max_message_size: 1_073_741_824,
  )
}

/// Errors from `from_url`.
pub type UrlError {
  InvalidUrl
  UnsupportedScheme(String)
  InvalidSslMode(String)
  InvalidConnectTimeout(String)
}

/// Parse a connection URL such as
/// `postgres://user:password@host:5432/database?sslmode=verify-full`.
///
/// Supported query parameters: `sslmode` (`disable`, `require`, `verify-ca`,
/// `verify-full`), `sslrootcert`, `connect_timeout` (seconds) and
/// `application_name`. Unknown parameters are sent to the server as startup
/// parameters.
pub fn from_url(url: String) -> Result(Config, UrlError) {
  use parsed <- result.try(uri.parse(url) |> result.replace_error(InvalidUrl))
  use _ <- result.try(case parsed.scheme {
    Some("postgres") | Some("postgresql") -> Ok(Nil)
    Some(other) -> Error(UnsupportedScheme(other))
    None -> Error(InvalidUrl)
  })
  let base = default()
  let base = case parsed.host {
    Some(h) if h != "" -> host(base, h)
    _ -> base
  }
  let base = case parsed.port {
    Some(p) -> port(base, p)
    None -> base
  }
  let base = case parsed.userinfo {
    Some(info) ->
      case string.split_once(info, ":") {
        Ok(#(user, pass)) ->
          base
          |> username(percent_decode(user))
          |> password(percent_decode(pass))
        Error(_) -> username(base, percent_decode(info))
      }
    None -> base
  }
  let base = case string.remove_prefix(parsed.path, "/") {
    "" -> base
    db -> database(base, percent_decode(db))
  }
  let query_params = case parsed.query {
    Some(q) -> uri.parse_query(q) |> result.unwrap([])
    None -> []
  }
  list.try_fold(query_params, base, apply_url_param)
}

fn apply_url_param(
  cfg: Config,
  param: #(String, String),
) -> Result(Config, UrlError) {
  case param {
    #("sslmode", "disable") -> Ok(ssl(cfg, SslDisabled))
    #("sslmode", "require") -> Ok(ssl(cfg, SslUnverified))
    #("sslmode", "verify-ca") | #("sslmode", "verify-full") ->
      Ok(ssl(cfg, SslVerified))
    #("sslmode", other) -> Error(InvalidSslMode(other))
    #("sslrootcert", path) -> Ok(Config(..cfg, ssl_ca_file: Some(path)))
    #("connect_timeout", secs) ->
      case int.parse(secs) {
        Ok(s) if s > 0 -> Ok(connect_timeout(cfg, s * 1000))
        _ -> Error(InvalidConnectTimeout(secs))
      }
    #(key, value) ->
      Ok(extra_parameters(
        cfg,
        list.append(cfg.extra_parameters, [#(key, value)]),
      ))
  }
}

fn percent_decode(s: String) -> String {
  uri.percent_decode(s) |> result.unwrap(s)
}

/// Set the server host name or IP address.
pub fn host(config: Config, host: String) -> Config {
  Config(..config, host: host)
}

/// Set the server port.
pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port: port)
}

/// Set the database name.
pub fn database(config: Config, database: String) -> Config {
  Config(..config, database: database)
}

/// Set the user name.
pub fn username(config: Config, username: String) -> Config {
  Config(..config, username: username)
}

/// Set the password.
pub fn password(config: Config, password: String) -> Config {
  Config(..config, password: secret(password))
}

/// Set the per-response timeout in milliseconds.
pub fn timeout(config: Config, timeout: Int) -> Config {
  Config(..config, timeout: timeout)
}

/// Set the TLS mode.
pub fn ssl(config: Config, mode: SslMode) -> Config {
  Config(..config, ssl: mode)
}

/// Verify the server against the CA certificates in this PEM file instead of
/// the system trust store (for RDS, Cloud SQL or a private CA).
pub fn ssl_ca_file(config: Config, path: String) -> Config {
  Config(..config, ssl_ca_file: Some(path))
}

/// Authenticate with a TLS client certificate (PEM certificate and key files).
pub fn ssl_client_cert(
  config: Config,
  certfile certfile: String,
  keyfile keyfile: String,
) -> Config {
  Config(..config, ssl_client_cert: Some(ClientCert(certfile:, keyfile:)))
}

/// Restrict which authentication methods the client will use, like libpq's
/// `require_auth`. For example `[AuthScram]` refuses MD5, cleartext and
/// passwordless servers.
pub fn allowed_auth(config: Config, methods: List(AuthMethod)) -> Config {
  Config(..config, allowed_auth: methods)
}

/// Set connect timeout in milliseconds.
pub fn connect_timeout(config: Config, ms: Int) -> Config {
  Config(..config, connect_timeout: ms)
}

/// Set extra startup parameters sent to PostgreSQL (e.g. `application_name`).
pub fn extra_parameters(
  config: Config,
  params: List(#(String, String)),
) -> Config {
  Config(..config, extra_parameters: params)
}

/// Enable PgBouncer compatibility mode.
/// Uses unnamed prepared statements with Flush to avoid mid-query
/// backend reassignment in PgBouncer transaction pooling mode.
pub fn pgbouncer(config: Config, enabled: Bool) -> Config {
  Config(..config, pgbouncer: enabled)
}

/// Set the idle ping interval in milliseconds (pool only).
/// Connections idle for longer than this are pinged. Default: 1000ms.
pub fn idle_interval(config: Config, ms: Int) -> Config {
  Config(..config, idle_interval: ms)
}

/// Set the maximum time in milliseconds to wait for a pool connection.
/// If no connection becomes available within this time, returns TimeoutError.
/// Default: 5000ms.
pub fn queue_timeout(config: Config, ms: Int) -> Config {
  Config(..config, queue_timeout: ms)
}

/// Set the largest protocol message accepted from the server. Default 1 GiB.
pub fn max_message_size(config: Config, bytes: Int) -> Config {
  Config(..config, max_message_size: bytes)
}

/// Check a config for values that can never work. Called by `connect`.
pub fn validate(config: Config) -> Result(Config, String) {
  case config {
    _ if config.port < 1 || config.port > 65_535 ->
      Error("port must be between 1 and 65535")
    _ if config.timeout < 1 -> Error("timeout must be positive")
    _ if config.connect_timeout < 1 -> Error("connect_timeout must be positive")
    _ if config.idle_interval < 1 -> Error("idle_interval must be positive")
    _ if config.queue_timeout < 0 -> Error("queue_timeout must not be negative")
    _ if config.max_message_size < 5 ->
      Error("max_message_size must be at least 5 bytes")
    _ ->
      case
        string.contains(config.username, "\u{0000}")
        || string.contains(config.database, "\u{0000}")
        || string.contains(config.host, "\u{0000}")
      {
        True -> Error("host, username and database must not contain NUL bytes")
        False -> Ok(config)
      }
  }
}
