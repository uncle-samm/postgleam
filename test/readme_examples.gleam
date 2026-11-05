//// The README's examples, so that `gleam build` fails if they stop
//// compiling. These functions are not tests and are never run.

import gleam/erlang/process
import gleam/option.{type Option}
import gleam/otp/static_supervisor
import gleam/result
import postgleam
import postgleam/config
import postgleam/decode
import postgleam/error
import postgleam/listener
import postgleam/pool

pub type User {
  User(id: Int, name: String, email: Option(String))
}

pub fn quick_start() {
  let assert Ok(cfg) =
    config.from_url("postgres://app:secret@localhost:5432/app_db")
  let assert Ok(conn) = postgleam.connect(cfg)
  let user = {
    use id <- decode.field("id", decode.int)
    use name <- decode.field("name", decode.text)
    use email <- decode.field("email", decode.optional(decode.text))
    decode.success(User(id:, name:, email:))
  }
  let assert Ok(response) =
    postgleam.query_with(
      conn,
      "SELECT id, name, email FROM users WHERE active = $1",
      [postgleam.bool(True)],
      user,
    )
  let _rows: List(User) = response.rows
  postgleam.disconnect(conn)
}

pub fn connecting() {
  let cfg =
    config.default()
    |> config.host("db.internal")
    |> config.database("app")
    |> config.username("app")
    |> config.password("secret")
    |> config.ssl(config.SslVerified)
    |> config.ssl_ca_file("/etc/ssl/db-ca.pem")
    |> config.ssl_client_cert(certfile: "client.pem", keyfile: "client-key.pem")
    |> config.allowed_auth([config.AuthScram])
  postgleam.connect(cfg)
}

pub fn queries(conn: postgleam.Connection, maybe_email: Option(String)) {
  let assert Ok(_) =
    postgleam.query(conn, "SELECT $1::int4 + 1", [postgleam.int(41)])
  let assert Ok(_) =
    postgleam.query_batch(conn, "INSERT INTO tags (name) VALUES ($1)", [
      [postgleam.text("gleam")],
      [postgleam.text("erlang")],
    ])
  let assert Ok(_total) =
    postgleam.query_fold(
      conn,
      "SELECT amount FROM payments",
      [],
      500,
      {
        use n <- decode.element(0, decode.int)
        decode.success(n)
      },
      0,
      fn(sum, n) { sum + n },
    )
  let assert Ok(_) =
    postgleam.query(
      conn,
      "INSERT INTO users (name, age, email) VALUES ($1, $2, $3)",
      [
        postgleam.text("alice"),
        postgleam.int(30),
        postgleam.nullable(maybe_email, postgleam.text),
      ],
    )
  let _decoder = {
    use id <- decode.field("id", decode.int)
    use tags <- decode.field("tags", decode.list(decode.text))
    use deleted_at <- decode.field(
      "deleted_at",
      decode.optional(decode.timestamp),
    )
    decode.success(#(id, tags, deleted_at))
  }
  let assert Ok("COPY 2") =
    postgleam.copy_in(conn, "COPY users (id, name) FROM STDIN", [
      <<"1\tAlice\n":utf8>>,
      <<"2\tBob\n":utf8>>,
    ])
  let assert Ok(_rows) = postgleam.copy_out(conn, "COPY users TO STDOUT")
  Nil
}

pub type MyError {
  DbError(error.Error)
  OverLimit
}

pub fn transactions(conn: postgleam.Connection) {
  let check_limits = fn(_conn) { Error(OverLimit) }
  let result =
    postgleam.transaction(conn, fn(conn) {
      use _ <- result.try(
        postgleam.query(
          conn,
          "UPDATE accounts SET balance = balance - 10 WHERE id = 1",
          [],
        )
        |> result.map_error(DbError),
      )
      use _ <- result.try(check_limits(conn))
      Ok(Nil)
    })
  case result {
    Ok(Nil) -> "committed"
    Error(postgleam.RolledBack(_my_error)) -> "rolled back"
    Error(postgleam.TransactionFailed(_db_error)) -> "failed"
  }
}

pub fn pooling(cfg: config.Config) {
  let assert Ok(db) = pool.start(cfg, size: 10)
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }
  let assert Ok(_response) = pool.query_with(db, "SELECT 1", [], decoder)
  let assert Ok(_n) =
    pool.with_connection(db, fn(conn) {
      use _ <- result.try(
        postgleam.query(conn, "SET LOCAL work_mem = '64MB'", []),
      )
      postgleam.query_one(conn, "SELECT 1", [], decoder)
    })
  let assert Ok(_) = pool.transaction(db, fn(_conn) { Ok(Nil) })
  let assert Ok(_stats) = pool.stats(db)
  pool.shutdown(db)

  let name = process.new_name("db")
  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool.supervised(cfg, 10, name))
    |> static_supervisor.start
  let _db = pool.named(name)
  Nil
}

pub fn listening(cfg: config.Config, conn: postgleam.Connection) {
  let inbox = process.new_subject()
  let assert Ok(l) = listener.start(cfg, inbox)
  let assert Ok(Nil) = listener.listen(l, "jobs")
  let assert Ok(Nil) = postgleam.notify(conn, "jobs", "42")
  let assert Ok(notification) = process.receive(inbox, 1000)
  let _ = #(notification.channel, notification.payload)
  listener.stop(l)
}
