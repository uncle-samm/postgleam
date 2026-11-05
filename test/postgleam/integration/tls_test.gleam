//// TLS tests. They run only when `build/tls/ca.pem` exists and the server
//// uses the matching certificate (see `scripts/make_test_certs.sh`; CI sets
//// this up). Otherwise each test returns early.

import gleam/option.{Some}
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/error
import postgleam/value

const ca_file = "build/tls/ca.pem"

const other_ca_file = "build/tls/other_ca.pem"

fn tls_available() -> Bool {
  is_file(ca_file)
}

@external(erlang, "filelib", "is_file")
fn is_file(path: String) -> Bool

fn base_config() {
  config.default()
  |> config.password("postgres")
  |> config.database("postgleam_test")
}

fn assert_encrypted(conn: postgleam.Connection) -> Nil {
  let assert Ok(r) =
    postgleam.query(
      conn,
      "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
      [],
    )
  should.equal(r.rows, [[Some(value.Boolean(True))]])
}

pub fn tls_verified_with_ca_file_test() {
  use <- run_if_available
  let cfg =
    base_config()
    |> config.host("localhost")
    |> config.ssl(config.SslVerified)
    |> config.ssl_ca_file(ca_file)
  let assert Ok(conn) = postgleam.connect(cfg)
  assert_encrypted(conn)
  postgleam.disconnect(conn)
}

/// IP hosts are matched against the certificate's IP addresses.
pub fn tls_verified_ip_host_test() {
  use <- run_if_available
  let cfg =
    base_config()
    |> config.host("127.0.0.1")
    |> config.ssl(config.SslVerified)
    |> config.ssl_ca_file(ca_file)
  let assert Ok(conn) = postgleam.connect(cfg)
  assert_encrypted(conn)
  postgleam.disconnect(conn)
}

pub fn tls_verification_fails_with_wrong_ca_test() {
  use <- run_if_available
  let cfg =
    base_config()
    |> config.host("localhost")
    |> config.ssl(config.SslVerified)
    |> config.ssl_ca_file(other_ca_file)
  let assert Error(error.SocketError(_)) = postgleam.connect(cfg)
  Nil
}

/// Without a CA file the system store is used, which does not trust the
/// test CA.
pub fn tls_verification_fails_with_system_store_test() {
  use <- run_if_available
  let cfg =
    base_config()
    |> config.host("localhost")
    |> config.ssl(config.SslVerified)
  let assert Error(error.SocketError(_)) = postgleam.connect(cfg)
  Nil
}

pub fn tls_unverified_test() {
  use <- run_if_available
  let cfg =
    base_config()
    |> config.host("localhost")
    |> config.ssl(config.SslUnverified)
  let assert Ok(conn) = postgleam.connect(cfg)
  assert_encrypted(conn)
  postgleam.disconnect(conn)
}

pub fn tls_from_url_test() {
  use <- run_if_available
  let assert Ok(cfg) =
    config.from_url(
      "postgres://postgres:postgres@localhost/postgleam_test?sslmode=verify-full&sslrootcert="
      <> ca_file,
    )
  let assert Ok(conn) = postgleam.connect(cfg)
  assert_encrypted(conn)
  postgleam.disconnect(conn)
}

fn run_if_available(test_body: fn() -> Nil) -> Nil {
  case tls_available() {
    True -> test_body()
    False -> Nil
  }
}
