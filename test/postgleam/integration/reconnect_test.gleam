//// Pool recovery from sockets that died silently, as happens when the host
//// is suspended and resumed: the server has closed its end long ago, but
//// the client's sockets still look open and every request just times out.
////
//// The tests put a TCP proxy between the pool and the server. `freeze`
//// makes every connection open at that moment swallow all traffic in both
//// directions without closing, while new connections work normally.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/pool
import postgleam/value

// =============================================================================
// Freezable proxy
// =============================================================================

type Socket

type Proxy {
  Proxy(port: Int, epoch_key: Atom)
}

@external(erlang, "gen_tcp", "listen")
fn tcp_listen(
  port: Int,
  options: List(#(Atom, Atom)),
) -> Result(Socket, Dynamic)

@external(erlang, "gen_tcp", "accept")
fn tcp_accept(socket: Socket) -> Result(Socket, Dynamic)

@external(erlang, "gen_tcp", "connect")
fn tcp_connect(
  address: #(Int, Int, Int, Int),
  port: Int,
  options: List(#(Atom, Atom)),
) -> Result(Socket, Dynamic)

@external(erlang, "gen_tcp", "recv")
fn tcp_recv(socket: Socket, length: Int, timeout: Int) -> Result(BitArray, Atom)

@external(erlang, "gen_tcp", "send")
fn tcp_send(socket: Socket, data: BitArray) -> Dynamic

@external(erlang, "gen_tcp", "close")
fn tcp_close(socket: Socket) -> Dynamic

@external(erlang, "inet", "port")
fn inet_port(socket: Socket) -> Result(Int, Dynamic)

@external(erlang, "persistent_term", "put")
fn term_put(key: Atom, value: Int) -> Dynamic

@external(erlang, "persistent_term", "get")
fn term_get(key: Atom) -> Int

fn socket_options() -> List(#(Atom, Atom)) {
  [
    #(atom.create("mode"), atom.create("binary")),
    #(atom.create("active"), atom.create("false")),
  ]
}

/// Start a proxy to the local PostgreSQL server on 127.0.0.1:5432. It runs
/// in a process linked to the test.
fn start_proxy() -> Proxy {
  let options = [
    #(atom.create("reuseaddr"), atom.create("true")),
    ..socket_options()
  ]
  let assert Ok(listener) = tcp_listen(0, options)
  let assert Ok(port) = inet_port(listener)
  let epoch_key = atom.create("postgleam_test_proxy_" <> int.to_string(port))
  let _ = term_put(epoch_key, 0)
  let proxy = Proxy(port:, epoch_key:)
  process.spawn(fn() { accept_loop(proxy, listener) })
  proxy
}

fn accept_loop(proxy: Proxy, listener: Socket) -> Nil {
  let assert Ok(client) = tcp_accept(listener)
  let assert Ok(server) = tcp_connect(#(127, 0, 0, 1), 5432, socket_options())
  let generation = term_get(proxy.epoch_key)
  process.spawn_unlinked(fn() { pump(proxy, generation, client, server) })
  process.spawn_unlinked(fn() { pump(proxy, generation, server, client) })
  accept_loop(proxy, listener)
}

/// Forward `from` to `to` until either side closes. Data is dropped once
/// the connection's generation has been frozen.
fn pump(proxy: Proxy, generation: Int, from: Socket, to: Socket) -> Nil {
  case tcp_recv(from, 0, 50) {
    Ok(data) -> {
      case term_get(proxy.epoch_key) > generation {
        True -> Nil
        False -> {
          let _ = tcp_send(to, data)
          Nil
        }
      }
      pump(proxy, generation, from, to)
    }
    Error(reason) ->
      case atom.to_string(reason) {
        "timeout" -> pump(proxy, generation, from, to)
        _ -> {
          let _ = tcp_close(to)
          Nil
        }
      }
  }
}

/// Make every connection open now go silent, like a socket whose peer
/// vanished. Connections opened afterwards work.
fn freeze(proxy: Proxy) -> Nil {
  let _ = term_put(proxy.epoch_key, term_get(proxy.epoch_key) + 1)
  Nil
}

// =============================================================================
// Tests
// =============================================================================

fn proxied_config(proxy: Proxy) {
  config.default()
  |> config.host("127.0.0.1")
  |> config.port(proxy.port)
  |> config.ssl(config.SslDisabled)
  |> config.password("postgres")
  |> config.database("postgleam_test")
  |> config.timeout(500)
  |> config.connect_timeout(500)
  // Keep the idle ping out of the way unless a test wants it.
  |> config.idle_interval(60_000)
  |> config.queue_timeout(5000)
}

fn now_ms() -> Int {
  monotonic_time(atom.create("millisecond"))
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: Atom) -> Int

/// Run `n` queries at the same time; return how many succeeded and how
/// long the slowest took.
fn concurrent_queries(p: pool.Pool, n: Int) -> #(Int, Int) {
  let done = process.new_subject()
  let started = now_ms()
  list.each(list.repeat(Nil, n), fn(_) {
    process.spawn(fn() {
      process.send(
        done,
        pool.with_connection(p, fn(conn) {
          let result = postgleam.query(conn, "SELECT 1", [])
          // Hold the connection briefly so each caller gets its own.
          process.sleep(50)
          result
        }),
      )
    })
  })
  let ok =
    list.repeat(Nil, n)
    |> list.count(fn(_) {
      let assert Ok(result) = process.receive(done, 5000)
      case result {
        Ok(_) -> True
        Error(_) -> False
      }
    })
  #(ok, now_ms() - started)
}

fn backend_pid(p: pool.Pool) -> Int {
  let assert Ok(r) = pool.query(p, "SELECT pg_backend_pid()", [])
  let assert [[Some(value.Integer(pid))]] = r.rows
  pid
}

/// One query timing out on a dead socket makes the pool check all idle
/// connections at once, so the rest are replaced before callers hit them.
pub fn dead_socket_triggers_sweep_test() {
  let proxy = start_proxy()
  let assert Ok(p) = pool.start(proxied_config(proxy), size: 3)
  freeze(proxy)
  let assert Error(_) = pool.query(p, "SELECT 1", [])
  // The sweep pings the other two (each fails after the 500 ms ping
  // timeout) and reconnects them.
  process.sleep(900)
  let #(ok, elapsed) = concurrent_queries(p, 3)
  should.equal(ok, 3)
  // A caller that hit a dead socket would wait the full 500 ms timeout.
  should.be_true(elapsed < 450)
  pool.shutdown(p)
}

/// The idle ping uses the short connect timeout and reconnects on failure.
pub fn idle_ping_replaces_dead_sockets_test() {
  let proxy = start_proxy()
  let cfg = proxied_config(proxy) |> config.idle_interval(100)
  let assert Ok(p) = pool.start(cfg, size: 3)
  freeze(proxy)
  process.sleep(1000)
  let #(ok, elapsed) = concurrent_queries(p, 3)
  should.equal(ok, 3)
  should.be_true(elapsed < 450)
  pool.shutdown(p)
}

/// `reconnect_all` replaces dead sockets without waiting for timeouts.
pub fn reconnect_all_replaces_dead_sockets_test() {
  let proxy = start_proxy()
  let assert Ok(p) = pool.start(proxied_config(proxy), size: 3)
  freeze(proxy)
  let assert Ok(Nil) = pool.reconnect_all(p)
  let #(ok, elapsed) = concurrent_queries(p, 3)
  should.equal(ok, 3)
  should.be_true(elapsed < 450)
  pool.shutdown(p)
}

/// A connection in use during `reconnect_all` is reconnected when it is
/// checked back in, not taken from its holder.
pub fn reconnect_all_waits_for_busy_connections_test() {
  let proxy = start_proxy()
  let assert Ok(p) = pool.start(proxied_config(proxy), size: 1)
  let before = backend_pid(p)
  let assert Ok(during) =
    pool.with_connection(p, fn(conn) {
      let assert Ok(Nil) = pool.reconnect_all(p)
      // Still the same session while held.
      let assert Ok(r) = postgleam.query(conn, "SELECT pg_backend_pid()", [])
      let assert [[Some(value.Integer(pid))]] = r.rows
      Ok(pid)
    })
  should.equal(during, before)
  should.not_equal(backend_pid(p), before)
  pool.shutdown(p)
}
