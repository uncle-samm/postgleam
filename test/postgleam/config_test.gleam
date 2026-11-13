import gleam/option.{Some}
import gleam/string
import gleeunit/should
import postgleam/config

pub fn from_url_full_test() {
  let assert Ok(cfg) =
    config.from_url(
      "postgres://app%40corp:s%3Acret@db.example.com:6543/my_db?sslmode=verify-full&sslrootcert=/etc/ca.pem&connect_timeout=3&application_name=api",
    )
  should.equal(cfg.host, "db.example.com")
  should.equal(cfg.port, 6543)
  should.equal(cfg.database, "my_db")
  should.equal(cfg.username, "app@corp")
  should.equal(config.reveal(cfg.password), "s:cret")
  should.equal(cfg.ssl, config.SslVerified)
  should.equal(cfg.ssl_ca_file, Some("/etc/ca.pem"))
  should.equal(cfg.connect_timeout, 3000)
  should.equal(cfg.extra_parameters, [#("application_name", "api")])
}

pub fn from_url_defaults_test() {
  let assert Ok(cfg) = config.from_url("postgresql://localhost")
  should.equal(cfg.host, "localhost")
  should.equal(cfg.port, 5432)
  should.equal(cfg.ssl, config.SslDisabled)
}

pub fn from_url_errors_test() {
  should.equal(
    config.from_url("mysql://localhost/db"),
    Error(config.UnsupportedScheme("mysql")),
  )
  should.equal(
    config.from_url("postgres://localhost/db?sslmode=sometimes"),
    Error(config.InvalidSslMode("sometimes")),
  )
  should.equal(
    config.from_url("postgres://localhost/db?connect_timeout=soon"),
    Error(config.InvalidConnectTimeout("soon")),
  )
  let assert Error(_) = config.from_url("not a url")
}

pub fn sslmode_require_does_not_verify_test() {
  let assert Ok(cfg) = config.from_url("postgres://h/db?sslmode=require")
  should.equal(cfg.ssl, config.SslUnverified)
}

/// The password must not leak through `string.inspect` (logs, crash
/// reports).
pub fn password_is_not_printed_test() {
  let cfg = config.default() |> config.password("hunter2")
  should.be_false(string.contains(string.inspect(cfg), "hunter2"))
}

pub fn validate_test() {
  let ok = config.default()
  should.be_ok(config.validate(ok))
  should.be_error(config.validate(config.port(ok, 0)))
  should.be_error(config.validate(config.port(ok, 70_000)))
  should.be_error(config.validate(config.timeout(ok, 0)))
  should.be_error(config.validate(config.username(ok, "a\u{0000}b")))
}

/// Like libpq: no database in the URL means the database named after the
/// user.
pub fn from_url_database_defaults_to_user_test() {
  let assert Ok(cfg) = config.from_url("postgres://alice:pw@db.example.com")
  should.equal(cfg.database, "alice")
  let assert Ok(cfg) = config.from_url("postgres://alice@db.example.com/")
  should.equal(cfg.database, "alice")
  let assert Ok(cfg) = config.from_url("postgres://alice@db.example.com/app")
  should.equal(cfg.database, "app")
}

/// Client-side settings from other tools' URLs must not be sent to the
/// server, which refuses startup parameters it doesn't know.
pub fn from_url_client_side_parameters_test() {
  let assert Ok(cfg) =
    config.from_url(
      "postgres://u:p@h/db?sslmode=require&channel_binding=prefer&pgbouncer=true&connection_limit=5&pool_timeout=10&schema=app",
    )
  should.equal(cfg.pgbouncer, True)
  should.equal(cfg.extra_parameters, [#("search_path", "app")])
}

/// Neon's default URL asks for channel binding, which postgleam can't do:
/// say so instead of ignoring a security setting or failing at the server.
pub fn from_url_channel_binding_require_test() {
  should.equal(
    config.from_url(
      "postgres://u:p@h/db?sslmode=require&channel_binding=require",
    ),
    Error(config.UnsupportedParameter("channel_binding", "require")),
  )
  should.equal(
    config.from_url("postgres://u:p@h/db?sslmode=prefer"),
    Error(config.InvalidSslMode("prefer")),
  )
}
