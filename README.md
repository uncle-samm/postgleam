# postgleam

[![Package Version](https://img.shields.io/hexpm/v/postgleam)](https://hex.pm/packages/postgleam)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/postgleam/)

A PostgreSQL driver for Gleam on the BEAM. The wire protocol, type codecs,
authentication and connection pool are written in Gleam. The only Erlang code
is a small module that sets up TLS through OTP's `ssl` application (see
[What is not Gleam](#what-is-not-gleam)). No NIFs, no C, and no wrapping of
another driver.

```sh
gleam add postgleam
```

> **A personal project.** I built postgleam mainly for my own use. It's
> tested and I use it, but there is no support or roadmap promise. Issues and
> pull requests are welcome, and so are forks if you want to take it in your
> own direction.

## Quick start

```gleam
import gleam/option.{type Option}
import postgleam
import postgleam/config
import postgleam/decode

pub type User {
  User(id: Int, name: String, email: Option(String))
}

pub fn main() {
  let assert Ok(cfg) =
    config.from_url("postgres://app:secret@localhost:5432/app_db")
  let assert Ok(conn) = postgleam.connect(cfg)

  let user = {
    use id <- decode.field("id", decode.int)
    use name <- decode.field("name", decode.text)
    use email <- decode.field("email", decode.optional(decode.text))
    decode.success(User(id:, name:, email:))
  }

  // Parameters are sent separately from the SQL, so this is injection safe.
  let assert Ok(response) =
    postgleam.query_with(
      conn,
      "SELECT id, name, email FROM users WHERE active = $1",
      [postgleam.bool(True)],
      user,
    )
  // response.rows: List(User)

  postgleam.disconnect(conn)
}
```

## Features

- PostgreSQL wire protocol v3, binary format, with cached prepared statements.
- 35+ type codecs, including arrays (multi-dimensional too), json/jsonb,
  numeric, uuid, dates and times, intervals, geometric and network types.
  Types without a codec (ranges, hstore, enums, ...) come back as text.
- TLS with certificate and host name verification, a custom CA file, and
  client certificates.
- SCRAM-SHA-256, MD5 and cleartext authentication, with a policy for which
  ones the client accepts.
- A connection pool that runs queries concurrently, with queue timeouts,
  health checks and supervision.
- Transactions, LISTEN/NOTIFY, COPY in/out, streaming large results, and
  logical replication.
- Recovery that keeps a connection correct after timeouts and errors.

## Connecting

Use a URL or the builder functions.

```gleam
import postgleam
import postgleam/config

// From a URL. Supported query parameters: sslmode (disable, require,
// verify-ca, verify-full), sslrootcert, connect_timeout (seconds),
// pgbouncer=true and schema=name. channel_binding=require is refused,
// because postgleam has no channel binding. Anything else is sent to the
// server as a startup parameter.
let assert Ok(cfg) =
  config.from_url("postgres://app:secret@db.internal/app?sslmode=verify-full")

// Or with builders. default() is postgres@localhost:5432/postgres, no TLS.
let cfg =
  config.default()
  |> config.host("db.internal")
  |> config.database("app")
  |> config.username("app")
  |> config.password("secret")
  |> config.ssl(config.SslVerified)

let assert Ok(conn) = postgleam.connect(cfg)
postgleam.disconnect(conn)
```

A `Connection` is one PostgreSQL session, owned by an actor process that runs
one request at a time. To run queries concurrently, use a [pool](#pool).

Every call waits at most about `config.timeout` (15 s by default) for each
reply. A query that runs longer is cancelled on the server and returns
`TimeoutError`, and the connection reconnects before the next request. Calls
never panic: if the connection's process has died you get `ConnectionError`.

## Queries

```gleam
// Raw rows: List(List(Option(Value)))
let assert Ok(result) =
  postgleam.query(conn, "SELECT $1::int4 + 1", [postgleam.int(41)])

// Decoded rows
let assert Ok(response) = postgleam.query_with(conn, sql, params, decoder)

// Exactly the first row (an error if there are none)
let assert Ok(user) =
  postgleam.query_one(conn, "SELECT id, name, email FROM users WHERE id = $1",
    [postgleam.int(1)], user_decoder)

// One statement, many parameter sets, one round trip
let assert Ok(results) =
  postgleam.query_batch(conn, "INSERT INTO tags (name) VALUES ($1)", [
    [postgleam.text("gleam")],
    [postgleam.text("erlang")],
  ])

// Large results without loading them all: rows are fetched 500 at a time
let amount = {
  use n <- decode.element(0, decode.int)
  decode.success(n)
}
let assert Ok(total) =
  postgleam.query_fold(conn, "SELECT amount FROM payments", [], 500, amount,
    0, fn(sum, n) { sum + n })

// Text protocol: DDL, several statements at once, values come back as text
let assert Ok(_) =
  postgleam.simple_query(conn, "CREATE TABLE t (id serial); INSERT INTO t DEFAULT VALUES")
```

### Decoders

Decode columns by name with `decode.field` or by position with
`decode.element`. Errors name the column that failed.

```gleam
let decoder = {
  use id <- decode.field("id", decode.int)
  use tags <- decode.field("tags", decode.list(decode.text))
  use deleted_at <- decode.field("deleted_at", decode.optional(decode.timestamp))
  decode.success(#(id, tags, deleted_at))
}
```

### Parameters

```gleam
postgleam.query(conn, "INSERT INTO users (name, age, email) VALUES ($1, $2, $3)", [
  postgleam.text("alice"),
  postgleam.int(30),
  postgleam.nullable(maybe_email, postgleam.text),  // None becomes NULL
])
```

Constructors include `int`, `float`, `text`, `bool`, `null`, `nullable`,
`bytea`, `uuid`, `uuid_string`, `json`, `jsonb`, `numeric`, `oid`, `date`,
`timestamp`, `timestamptz`, `time`, `interval`, `array` and the geometric and
network types. `text` works for any column type PostgreSQL can parse from
text (enums, uuid, ranges, ...). `uuid_string` returns a `Result`, so an
invalid UUID is an error rather than a NULL.

A parameter must match the type PostgreSQL expects, with three exceptions
that lose nothing: `int` and `float` also go into `numeric`, and `json` and
`jsonb` go into each other. A number that doesn't fit the column's type,
such as an `int` above the `int8` range, is an `EncodeError`; it is never
wrapped around.

### Rows changed and error codes

```gleam
let assert Ok(result) = postgleam.query(conn, "DELETE FROM jobs WHERE done", [])
let assert Ok(deleted) = postgleam.rows_affected(result.tag)

case postgleam.query(conn, insert_sql, params) {
  Error(e) ->
    case error.is_unique_violation(e) {
      True -> // the row already exists
      False -> // error.sqlstate(e) gives the code of any server error
    }
  Ok(_) -> // inserted
}
```

## Transactions

```gleam
let result =
  postgleam.transaction(conn, fn(conn) {
    use _ <- result.try(
      postgleam.query(conn, "UPDATE accounts SET balance = balance - 10 WHERE id = 1", [])
      |> result.map_error(DbError),
    )
    use _ <- result.try(check_limits(conn))  // returns Result(_, MyError)
    Ok(Nil)
  })

case result {
  Ok(Nil) -> // committed
  Error(postgleam.RolledBack(my_error)) -> // your function returned Error; rolled back
  Error(postgleam.TransactionFailed(db_error)) -> // BEGIN, COMMIT or ROLLBACK failed
}
```

If the connection is lost in the middle of a transaction, the rest of the
transaction fails with `ConnectionError` instead of running outside it, and
`transaction` returns `TransactionFailed`. If your function panics, the
transaction is rolled back before the panic carries on.

A `Connection` is not tied to one process. If several processes share one,
their statements join whatever transaction is open on it, so give each
process its own connection or use a pool.

## Pool

```gleam
import postgleam/pool

let assert Ok(db) = pool.start(cfg, size: 10)

let assert Ok(response) = pool.query_with(db, sql, params, decoder)

// Several statements on one connection
let assert Ok(n) =
  pool.with_connection(db, fn(conn) {
    use _ <- result.try(postgleam.query(conn, "SET LOCAL work_mem = '64MB'", []))
    postgleam.query_one(conn, report_sql, [], decoder)
  })

let assert Ok(_) = pool.transaction(db, fn(conn) { ... })

pool.shutdown(db)
```

Each pooled connection is its own actor, so queries from different processes
run in parallel. When no connection is free a caller waits up to
`config.queue_timeout` (5 s) and then gets `PoolTimeout`. When a connection
comes back, anything left open (such as a transaction) is rolled back first;
this also happens if the process holding it exits or your function panics. Idle connections are
pinged every `config.idle_interval`, and a crashed connection is replaced.

When the pool finds a broken connection (a ping or query timed out or the
socket closed), it checks every idle connection at once and reconnects the
ones that don't answer. Sockets that died together, for example while the
machine was suspended, are replaced in one sweep instead of one failed query
at a time. Health-check pings wait at most `config.connect_timeout`. If your
application knows its sockets are dead, such as right after a resume, call
`pool.reconnect_all(db)`. It reconnects idle connections right away, and ones
in use as soon as they are checked back in.

Under a supervisor:

```gleam
let name = process.new_name("db")
let assert Ok(_) =
  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(pool.supervised(cfg, 10, name))
  |> static_supervisor.start
let db = pool.named(name)
```

`pool.stats(db)` reports idle, busy and waiting counts.

## TLS and authentication

```gleam
config.default()
|> config.ssl(config.SslVerified)          // verify certificate and host name
|> config.ssl_ca_file("/etc/ssl/db-ca.pem") // default: the system trust store
|> config.ssl_client_cert(certfile: "client.pem", keyfile: "client-key.pem")
|> config.allowed_auth([config.AuthScram])  // refuse anything weaker
```

- `SslVerified` checks the certificate chain and the host name (or, for an IP
  address, the certificate's IP entries). Use it for anything that crosses a
  network.
- `SslUnverified` encrypts but accepts any certificate, so it only protects
  against passive eavesdropping.
- `SslDisabled` (the default) is plain TCP, for local development.
- Cleartext passwords are never sent over a connection without TLS, even if
  `AuthCleartext` is allowed.
- The password is stored as an opaque `Secret`, so it does not show up when a
  config is printed or included in a crash report.

## LISTEN/NOTIFY

A listener owns a connection and sends every notification to a `Subject`.
If its connection drops it reconnects and listens again (notifications sent
while it was disconnected are lost, as PostgreSQL does not keep them).

```gleam
import postgleam/listener

let inbox = process.new_subject()
let assert Ok(l) = listener.start(cfg, inbox)
let assert Ok(Nil) = listener.listen(l, "jobs")

let assert Ok(Nil) = postgleam.notify(conn, "jobs", "42")

let assert Ok(notification) = process.receive(inbox, 1000)
// notification.channel == "jobs", notification.payload == "42"
listener.stop(l)
```

## COPY

```gleam
let assert Ok("COPY 2") =
  postgleam.copy_in(conn, "COPY users (id, name) FROM STDIN", [
    <<"1\tAlice\n":utf8>>,
    <<"2\tBob\n":utf8>>,
  ])

let assert Ok(rows) = postgleam.copy_out(conn, "COPY users TO STDOUT")
```

## PgBouncer

With `config.pgbouncer(True)` the driver uses only unnamed statements, which
work with PgBouncer in transaction pooling mode. It costs one extra round
trip's worth of work per query (see the benchmarks).

## Low-level API

`postgleam/connection` exposes the protocol directly on a `ConnectionState`
value with no actor, for when you want full control. `postgleam/stream`,
`postgleam/copy`, `postgleam/notifications` and `postgleam/replication`
(logical replication) work on it too.

## Benchmarks

Numbers from `make bench-throughput` on a 4-core Linux machine with
PostgreSQL 16 on localhost, OTP 27. Every query is `SELECT $1::int4`, so they
measure the driver and the round trip, not PostgreSQL's work.

| Scenario | Queries/s |
|---|---:|
| `postgleam.query`, one connection (cached statement) | 16,800 |
| `postgleam.simple_query`, one connection | 13,600 |
| `postgleam.query`, PgBouncer mode | 9,100 |
| `pool.query`, 1 caller | 10,600 |
| `pool.query`, 10 concurrent callers, pool of 10 | 42,300 |
| 10 concurrent callers, one connection each | 65,300 |
| For reference: `pgbench -M prepared`, 1 client | 20,100 |

On one connection postgleam reaches about 85% of libpq's speed (pgbench).
Through the pool, throughput grows with the number of callers until the pool
size or the server's CPUs run out. On a real network the round trip
dominates: expect each sequential query to take roughly one network round
trip, and use the pool, `query_batch` or `copy_in` to do more per trip.

Version 0.9 fixed two large slowdowns found in review. Without TCP_NODELAY,
Linux delayed ACKs held every extended query for about 40 ms (22 queries/s),
and the old pool ran every query inside the pool process, one at a time, so
ten callers got no more throughput than one.

The other benchmark suites:

```sh
make bench-pure          # codecs, message parsing, decoders (no database)
make bench-integration   # common operations against a database
make bench-realworld     # insert/query workloads modelled on go-sqlite-bench
make bench-throughput    # the table above
```

## What is not Gleam

`src/postgleam_ffi.erl` upgrades the socket to TLS with OTP's `ssl` module.
Gleam has no TLS library for the BEAM, and building the `ssl` options
(including the host name check for IP addresses) is clearer in Erlang. A few
other calls go straight to the Erlang standard library with `@external`:
`crypto:pbkdf2_hmac` and `unicode:characters_to_nfkc_binary` for SCRAM, and
`inet:setopts` to turn on TCP_NODELAY.

## Development

```sh
docker compose up -d            # PostgreSQL 16
./scripts/setup_test_db.sh      # create the test databases and roles
gleam test                      # 550+ tests (README examples are compiled too)
make check                      # format check, build, tests, docs
```

The TLS tests run when `build/tls/ca.pem` exists and the server uses the
matching certificate (`scripts/make_test_certs.sh` creates them). The
PgBouncer tests run when PgBouncer in transaction mode answers on
`localhost:6432`. CI runs both.

## Target

Erlang only. The JavaScript target is not supported, since the driver uses
TCP sockets and OTP processes.
