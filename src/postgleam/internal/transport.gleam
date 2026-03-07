/// Transport abstraction over TCP (mug) and SSL sockets.
/// Allows the connection module to work transparently with either.

import gleam/result
import mug
import postgleam/error.{type Error}

/// Opaque transport — either a plain TCP socket or an SSL socket
pub type Transport {
  Tcp(socket: mug.Socket)
  Ssl(socket: SslSocket)
}

/// Opaque SSL socket from Erlang :ssl module
pub type SslSocket

/// Send bytes over the transport
pub fn send(transport: Transport, data: BitArray) -> Result(Nil, Error) {
  case transport {
    Tcp(socket) ->
      mug.send(socket, data)
      |> result.map_error(fn(e) {
        error.SocketError("TCP send failed: " <> mug_error_to_string(e))
      })
    Ssl(socket) ->
      ssl_send(socket, data)
      |> result.map_error(fn(reason) {
        error.SocketError("SSL send failed: " <> reason)
      })
  }
}

/// Receive bytes from the transport
pub fn receive(
  transport: Transport,
  timeout: Int,
) -> Result(BitArray, Error) {
  case transport {
    Tcp(socket) ->
      mug.receive(socket, timeout_milliseconds: timeout)
      |> result.map_error(fn(e) {
        error.SocketError("TCP receive failed: " <> mug_error_to_string(e))
      })
    Ssl(socket) ->
      ssl_recv(socket, timeout)
      |> result.map_error(fn(reason) {
        error.SocketError("SSL receive failed: " <> reason)
      })
  }
}

/// Close/shutdown the transport
pub fn close(transport: Transport) -> Nil {
  case transport {
    Tcp(socket) -> {
      let _ = mug.shutdown(socket)
      Nil
    }
    Ssl(socket) -> {
      ssl_close(socket)
      Nil
    }
  }
}

/// Upgrade a TCP socket to SSL.
/// Sends SSLRequest to PostgreSQL, reads the 1-byte response,
/// and if 'S', upgrades via :ssl.connect/3.
pub fn upgrade_to_ssl(
  tcp_socket: mug.Socket,
  host: String,
  timeout: Int,
  verify: Bool,
) -> Result(Transport, Error) {
  // Send SSLRequest message (8 bytes: length=8, code=80877103)
  let ssl_request = <<0, 0, 0, 8, 4, 210, 22, 47>>
  case mug.send(tcp_socket, ssl_request) {
    Error(e) ->
      Error(error.SocketError(
        "Failed to send SSLRequest: " <> mug_error_to_string(e),
      ))
    Ok(_) -> {
      // Read 1-byte response: 'S' = upgrade, 'N' = no SSL
      case mug.receive(tcp_socket, timeout_milliseconds: timeout) {
        Error(e) ->
          Error(error.SocketError(
            "Failed to receive SSL response: " <> mug_error_to_string(e),
          ))
        Ok(<<0x53>>) -> {
          // Server said 'S' — upgrade the socket
          case ssl_upgrade(tcp_socket, host, timeout, verify) {
            Ok(ssl_socket) -> Ok(Ssl(ssl_socket))
            Error(reason) ->
              Error(error.SocketError("SSL upgrade failed: " <> reason))
          }
        }
        Ok(<<0x4E>>) ->
          Error(error.SocketError("Server does not support SSL"))
        Ok(_) ->
          Error(error.SocketError("Unexpected SSL negotiation response"))
      }
    }
  }
}

fn mug_error_to_string(err: mug.Error) -> String {
  mug.describe_error(err)
}

// --- Erlang FFI ---

@external(erlang, "postgleam_ffi", "ssl_upgrade")
fn ssl_upgrade(
  tcp_socket: mug.Socket,
  host: String,
  timeout: Int,
  verify: Bool,
) -> Result(SslSocket, String)

@external(erlang, "postgleam_ffi", "ssl_send")
fn ssl_send(socket: SslSocket, data: BitArray) -> Result(Nil, String)

@external(erlang, "postgleam_ffi", "ssl_recv")
fn ssl_recv(socket: SslSocket, timeout: Int) -> Result(BitArray, String)

@external(erlang, "postgleam_ffi", "ssl_close")
fn ssl_close(socket: SslSocket) -> Nil
