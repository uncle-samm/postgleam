//// Transport abstraction over TCP (mug) and TLS sockets, so the protocol
//// code works the same with either.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/option.{type Option, None, Some}
import mug
import postgleam/error.{type Error}

/// Either a plain TCP socket or a TLS socket.
pub type Transport {
  Tcp(socket: mug.Socket)
  Ssl(socket: SslSocket)
}

/// Opaque TLS socket from Erlang's `ssl` module.
pub type SslSocket

/// TLS settings for `upgrade_to_ssl`.
pub type TlsOptions {
  TlsOptions(
    verify: Bool,
    ca_file: Option(String),
    client_cert: Option(#(String, String)),
  )
}

/// Open a TCP connection with Nagle's algorithm disabled.
///
/// Without TCP_NODELAY, the small Bind/Execute/Sync writes of the extended
/// protocol interact with delayed ACKs and each query stalls ~40ms on Linux.
pub fn connect(
  host: String,
  port: Int,
  timeout: Int,
) -> Result(mug.Socket, Error) {
  case
    mug.new(host, port: port)
    |> mug.timeout(milliseconds: timeout)
    |> mug.connect()
  {
    Ok(socket) -> {
      // Best effort: a socket that rejects the option still works, only slower.
      let _ = inet_setopts(socket, [#(atom.create("nodelay"), True)])
      Ok(socket)
    }
    Error(e) ->
      Error(error.SocketError("TCP connect failed: " <> connect_error(e)))
  }
}

/// Send bytes over the transport.
pub fn send(transport: Transport, data: BitArray) -> Result(Nil, Error) {
  case transport {
    Tcp(socket) ->
      case mug.send(socket, data) {
        Ok(_) -> Ok(Nil)
        Error(e) ->
          Error(error.SocketError("TCP send failed: " <> mug.describe_error(e)))
      }
    Ssl(socket) ->
      case ssl_send(socket, data) {
        Ok(_) -> Ok(Nil)
        Error(reason) -> Error(error.SocketError("SSL send failed: " <> reason))
      }
  }
}

/// Receive bytes from the transport. A timeout returns `TimeoutError`.
pub fn receive(transport: Transport, timeout: Int) -> Result(BitArray, Error) {
  case transport {
    Tcp(socket) ->
      case mug.receive(socket, timeout_milliseconds: timeout) {
        Ok(data) -> Ok(data)
        Error(mug.Timeout) -> Error(error.TimeoutError)
        Error(e) ->
          Error(error.SocketError(
            "TCP receive failed: " <> mug.describe_error(e),
          ))
      }
    Ssl(socket) ->
      case ssl_recv(socket, timeout) {
        Ok(data) -> Ok(data)
        Error("timeout") -> Error(error.TimeoutError)
        Error(reason) ->
          Error(error.SocketError("SSL receive failed: " <> reason))
      }
  }
}

/// Close the transport.
pub fn close(transport: Transport) -> Nil {
  case transport {
    Tcp(socket) -> {
      let _ = mug.shutdown(socket)
      Nil
    }
    Ssl(socket) -> ssl_close(socket)
  }
}

/// Upgrade a TCP socket to TLS.
/// Sends SSLRequest to PostgreSQL, reads the 1-byte answer and, on 'S',
/// performs the TLS handshake.
pub fn upgrade_to_ssl(
  tcp_socket: mug.Socket,
  host: String,
  timeout: Int,
  options: TlsOptions,
) -> Result(Transport, Error) {
  // SSLRequest: length 8, code 80877103
  let ssl_request = <<0, 0, 0, 8, 4, 210, 22, 47>>
  case mug.send(tcp_socket, ssl_request) {
    Error(e) ->
      Error(error.SocketError(
        "Failed to send SSLRequest: " <> mug.describe_error(e),
      ))
    Ok(_) ->
      case mug.receive_exact(tcp_socket, 1, timeout_milliseconds: timeout) {
        Error(e) ->
          Error(error.SocketError(
            "Failed to receive SSL response: " <> mug.describe_error(e),
          ))
        Ok(<<0x53>>) -> {
          let ca_file = case options.ca_file {
            Some(path) -> path
            None -> ""
          }
          let #(certfile, keyfile) = case options.client_cert {
            Some(pair) -> pair
            None -> #("", "")
          }
          case
            ssl_upgrade(
              tcp_socket,
              host,
              timeout,
              options.verify,
              ca_file,
              certfile,
              keyfile,
            )
          {
            Ok(ssl_socket) -> Ok(Ssl(ssl_socket))
            Error(reason) ->
              Error(error.SocketError("SSL upgrade failed: " <> reason))
          }
        }
        Ok(<<0x4E>>) -> Error(error.SocketError("Server does not support SSL"))
        Ok(_) -> Error(error.SocketError("Unexpected SSL negotiation response"))
      }
  }
}

fn connect_error(err: mug.ConnectError) -> String {
  case err {
    mug.ConnectFailedIpv4(e) -> "IPv4: " <> mug.describe_error(e)
    mug.ConnectFailedIpv6(e) -> "IPv6: " <> mug.describe_error(e)
    mug.ConnectFailedBoth(a, b) ->
      "IPv4: " <> mug.describe_error(a) <> "; IPv6: " <> mug.describe_error(b)
  }
}

// --- Erlang FFI: TLS and socket options have no Gleam-native API ---

@external(erlang, "inet", "setopts")
fn inet_setopts(socket: mug.Socket, options: List(#(Atom, Bool))) -> Dynamic

@external(erlang, "postgleam_ffi", "ssl_upgrade")
fn ssl_upgrade(
  tcp_socket: mug.Socket,
  host: String,
  timeout: Int,
  verify: Bool,
  ca_file: String,
  certfile: String,
  keyfile: String,
) -> Result(SslSocket, String)

@external(erlang, "postgleam_ffi", "ssl_send")
fn ssl_send(socket: SslSocket, data: BitArray) -> Result(Nil, String)

@external(erlang, "postgleam_ffi", "ssl_recv")
fn ssl_recv(socket: SslSocket, timeout: Int) -> Result(BitArray, String)

@external(erlang, "postgleam_ffi", "ssl_close")
fn ssl_close(socket: SslSocket) -> Nil
