# Changelog

## 0.9.0

This release fixes the problems found in a full review of the driver. It has
breaking changes; see "Upgrading" below.

### Upgrading from 0.8

- `config.default()` no longer has a password. Add `|> config.password(...)`
  or use `config.from_url`.
- `config.Config` has new fields and `password` is now an opaque `Secret`.
  Build configs with `default()` and the builder functions instead of the
  record constructor.
- `postgleam.transaction` takes `fn(Connection) -> Result(a, e)` with your own
  error type and returns `Result(a, TransactionError(e))`
  (`RolledBack(e)` or `TransactionFailed(Error)`). `query_error` is gone.
- `postgleam.uuid_string` returns `Result(Param, Nil)` instead of silently
  turning an invalid UUID into NULL.
- `pool.start(config, size)` returns `Result(Pool, Error)`. `pool.query`,
  `pool.query_with` and `pool.simple_query` no longer take a timeout (the
  config's `timeout` and `queue_timeout` apply) and `pool.query_with` returns
  `postgleam.Response`. `pool.shutdown(pool)` takes no timeout.
- `error.Error` has a new `PoolTimeout` variant.
- `message.extract_row_values` returns a `Result`.
- `value.Null` was removed; NULL is `None` everywhere.
- The unused `postgleam/query`, `postgleam/result` and `postgleam/type_info`
  modules were removed. `postgleam/message`, `postgleam/auth/*` and
  `postgleam/internal/*` are now internal.
- Minimum Gleam version is 1.19.

### Fixed

- Extended queries stalled about 40 ms each on Linux (Nagle's algorithm and
  delayed ACKs), limiting a connection to about 22 queries per second. The
  driver now sets TCP_NODELAY and sends each request in one write.
- The pool ran every query inside the pool process, one at a time. It is now
  a checkout pool and queries run in parallel.
- After a timeout, the reply to the timed-out query was read as the reply to
  the next query. The query is now cancelled and the connection replaced.
- In PgBouncer mode a server error left the connection hanging.
- Cached statements broke after `ALTER TABLE` until the connection was
  dropped; they are now re-prepared automatically.
- `numeric("1e5")` was sent as 635, and invalid numeric strings were sent as
  garbage. Numeric parameters are now parsed and validated.
- Decode errors were swallowed, and types without a binary codec were decoded
  incorrectly. They are now fetched as text.
- Notifications arriving during a query were lost.
- TLS failed with `ssl_not_started` unless the application had started the
  `ssl` application itself.
- A connection lost inside a transaction could run the rest of the
  transaction's statements in autocommit mode on a new connection.
- Calls to a dead or slow connection actor panicked the caller.

### Security

- SCRAM-SHA-256 checks that the server nonce extends the client nonce and
  bounds the iteration count.
- New `config.allowed_auth` policy. Cleartext passwords are never sent over a
  connection without TLS.
- `config.ssl_ca_file` for a custom CA and `config.ssl_client_cert` for client
  certificates. IP address hosts are checked against the certificate's IP
  entries.
- Passwords are stored as an opaque `Secret` and do not appear in logs or
  crash reports.
- `config.max_message_size` (default 1 GiB) limits server messages. NUL bytes
  in SQL and text parameters are rejected instead of truncating the value.
- LISTEN/NOTIFY quoting hardened; `notify` uses a parameterised `pg_notify`.
- SCRAM server-first parsing hardened against invalid base64.

### Added

- `config.from_url` for `postgres://` URLs, including `sslmode`,
  `sslrootcert` and `connect_timeout`.
- `postgleam/listener`: a process that delivers notifications to a `Subject`
  and reconnects on its own.
- `postgleam.query_fold` for large results, `postgleam.notify`,
  `error.to_string`.
- `decode.field` (decode by column name), `decode.list` (arrays),
  `decode.map`, `decode.run_with_columns`. Decode errors name the column.
- Multi-dimensional arrays.
- Text parameters work for any type PostgreSQL can parse (enums, uuid,
  ranges, ...).
- Pool: `with_connection`, `transaction`, `query_one`, `query_batch`, `stats`,
  `supervised` and `named`, queue timeouts, idle pings, replacement of crashed
  connections, and rollback of leftovers on check-in or when the holder
  exits.
- TLS tests in CI against a TLS-enabled server, regression tests for every
  fix above, and `make bench-throughput`.

### Changed

- All dependencies updated to their latest versions.
- Hashing uses `gleam_crypto`. The only Erlang source left is the TLS setup;
  PBKDF2, NFKC normalisation and socket options are direct calls into the
  Erlang standard library.
- Per-commit benchmark snapshot files are no longer written or kept.
- CI checks formatting, build, tests and docs; a manual workflow runs the
  benchmarks.
