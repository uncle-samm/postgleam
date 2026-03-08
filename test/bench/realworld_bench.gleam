/// Real-world benchmarks modeled after go-sqlite-bench.
/// Tests realistic database patterns: relational inserts, JOINs, read-heavy loops.
///
/// Benchmarks:
///   1. simple   — Bulk insert N users in a transaction, then query all
///   2. real     — 100 users × 20 articles × 20 comments, per-user transactions + JOIN queries
///   3. many     — Insert N users, query all 1000 times (read-heavy)
///   4. large    — Insert 1000 users with large text columns, query all

import gleam/int
import gleam/io
import gleam/list
import gleam/string
import postgleam
import postgleam/config.{type Config}
import postgleam/decode
import postgleam/error.{type Error}

/// Result: benchmark name, insert ms, query ms
pub type RealworldResult {
  RealworldResult(name: String, insert_ms: Int, query_ms: Int)
}

pub fn run(cfg: Config) -> List(RealworldResult) {
  io.println("Running real-world benchmarks...")
  io.println("")
  let results = [
    bench_simple(cfg, 10_000),
    bench_simple(cfg, 100_000),
    bench_real(cfg),
    bench_many(cfg, 10),
    bench_many(cfg, 100),
    bench_many(cfg, 1000),
    bench_large(cfg, 1000),
    bench_large(cfg, 10_000),
  ]
  print_realworld_table(results)
  results
}

// =============================================================================
// 1. Simple — Bulk insert + full scan
// =============================================================================

fn bench_simple(cfg: Config, nusers: Int) -> RealworldResult {
  let label = "simple/" <> format_k(nusers)
  io.println("  " <> label <> "...")
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE IF EXISTS bench_users")
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TABLE bench_users (
        id INTEGER PRIMARY KEY,
        created BIGINT NOT NULL,
        email TEXT NOT NULL,
        active BOOLEAN NOT NULL
      )",
    )
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE INDEX bench_users_created ON bench_users(created)",
    )

  // Insert N users in a single transaction
  let t0 = now_ms()
  let assert Ok(_) =
    postgleam.transaction(conn, fn(c) {
      insert_users_loop(c, 1, nusers)
    })
  let insert_ms = now_ms() - t0

  // Query all users
  let user_decoder = {
    use id <- decode.element(0, decode.int)
    use _created <- decode.element(1, decode.int)
    use _email <- decode.element(2, decode.text)
    use _active <- decode.element(3, decode.bool)
    decode.success(id)
  }
  let t0 = now_ms()
  let assert Ok(result) =
    postgleam.query_with(
      conn,
      "SELECT id, created, email, active FROM bench_users ORDER BY id",
      [],
      user_decoder,
    )
  let query_ms = now_ms() - t0
  let assert True = result.count == nusers

  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE bench_users")
  postgleam.disconnect(conn)

  RealworldResult(name: label, insert_ms: insert_ms, query_ms: query_ms)
}

fn insert_users_loop(
  conn: postgleam.Connection,
  current: Int,
  max: Int,
) -> Result(Nil, Error) {
  case current > max {
    True -> Ok(Nil)
    False -> {
      let email =
        "user" <> pad_int(current, 8) <> "@example.com"
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO bench_users (id, created, email, active) VALUES ($1, $2, $3, $4)",
          [
            postgleam.int(current),
            postgleam.int(current * 60_000),
            postgleam.text(email),
            postgleam.bool(True),
          ],
        )
      insert_users_loop(conn, current + 1, max)
    }
  }
}

// =============================================================================
// 2. Real — Multi-table relational pattern with JOINs
// =============================================================================

fn bench_real(cfg: Config) -> RealworldResult {
  let label = "real"
  io.println("  " <> label <> "...")
  let assert Ok(conn) = postgleam.connect(cfg)

  // Setup schema
  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE IF EXISTS bench_comments")
  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE IF EXISTS bench_articles")
  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE IF EXISTS bench_real_users")
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TABLE bench_real_users (
        id INTEGER PRIMARY KEY,
        created BIGINT NOT NULL,
        email TEXT NOT NULL,
        active BOOLEAN NOT NULL
      )",
    )
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TABLE bench_articles (
        id INTEGER PRIMARY KEY,
        created BIGINT NOT NULL,
        user_id INTEGER NOT NULL REFERENCES bench_real_users(id),
        body TEXT NOT NULL
      )",
    )
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TABLE bench_comments (
        id INTEGER PRIMARY KEY,
        created BIGINT NOT NULL,
        article_id INTEGER NOT NULL REFERENCES bench_articles(id),
        body TEXT NOT NULL
      )",
    )
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE INDEX bench_articles_user_id ON bench_articles(user_id)",
    )
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE INDEX bench_comments_article_id ON bench_comments(article_id)",
    )

  let nusers = 100
  let articles_per_user = 20
  let comments_per_article = 20
  let article_text =
    "text text text text text text text text text text text text"

  // Insert: each user in a separate transaction (like go-sqlite-bench)
  let t0 = now_ms()
  insert_real_loop(
    conn,
    1,
    nusers,
    articles_per_user,
    comments_per_article,
    article_text,
  )
  let insert_ms = now_ms() - t0

  // Query: for each user, LEFT JOIN articles and comments
  let join_sql =
    "SELECT u.id, u.email, a.id, a.body, c.id, c.body
     FROM bench_real_users u
     LEFT JOIN bench_articles a ON a.user_id = u.id
     LEFT JOIN bench_comments c ON c.article_id = a.id
     WHERE u.email = $1
     ORDER BY u.id, a.id, c.id"

  let join_decoder = {
    use uid <- decode.element(0, decode.int)
    use _email <- decode.element(1, decode.text)
    use _aid <- decode.element(2, decode.optional(decode.int))
    use _abody <- decode.element(3, decode.optional(decode.text))
    use _cid <- decode.element(4, decode.optional(decode.int))
    use _cbody <- decode.element(5, decode.optional(decode.text))
    decode.success(uid)
  }

  let t0 = now_ms()
  query_real_loop(conn, 1, nusers, join_sql, join_decoder)
  let query_ms = now_ms() - t0

  // Cleanup
  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE bench_comments")
  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE bench_articles")
  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE bench_real_users")
  postgleam.disconnect(conn)

  RealworldResult(name: label, insert_ms: insert_ms, query_ms: query_ms)
}

fn insert_real_loop(
  conn: postgleam.Connection,
  user_id: Int,
  max_users: Int,
  articles_per_user: Int,
  comments_per_article: Int,
  article_text: String,
) -> Nil {
  case user_id > max_users {
    True -> Nil
    False -> {
      let email = "user" <> pad_int(user_id, 8) <> "@example.com"
      let assert Ok(_) =
        postgleam.transaction(conn, fn(c) {
          // Insert user
          let assert Ok(_) =
            postgleam.query(
              c,
              "INSERT INTO bench_real_users (id, created, email, active) VALUES ($1, $2, $3, $4)",
              [
                postgleam.int(user_id),
                postgleam.int(user_id * 1000),
                postgleam.text(email),
                postgleam.bool(True),
              ],
            )
          // Insert articles for this user
          let base_article_id = { user_id - 1 } * articles_per_user
          insert_articles_loop(
            c,
            1,
            articles_per_user,
            user_id,
            base_article_id,
            article_text,
            comments_per_article,
          )
        })
      insert_real_loop(
        conn,
        user_id + 1,
        max_users,
        articles_per_user,
        comments_per_article,
        article_text,
      )
    }
  }
}

fn insert_articles_loop(
  conn: postgleam.Connection,
  i: Int,
  max: Int,
  user_id: Int,
  base_article_id: Int,
  text: String,
  comments_per_article: Int,
) -> Result(Nil, Error) {
  case i > max {
    True -> Ok(Nil)
    False -> {
      let article_id = base_article_id + i
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO bench_articles (id, created, user_id, body) VALUES ($1, $2, $3, $4)",
          [
            postgleam.int(article_id),
            postgleam.int(article_id * 1000),
            postgleam.int(user_id),
            postgleam.text(text),
          ],
        )
      // Insert comments for this article
      let base_comment_id = { article_id - 1 } * comments_per_article
      let assert Ok(_) =
        insert_comments_loop(
          conn,
          1,
          comments_per_article,
          article_id,
          base_comment_id,
          text,
        )
      insert_articles_loop(
        conn,
        i + 1,
        max,
        user_id,
        base_article_id,
        text,
        comments_per_article,
      )
    }
  }
}

fn insert_comments_loop(
  conn: postgleam.Connection,
  i: Int,
  max: Int,
  article_id: Int,
  base_comment_id: Int,
  text: String,
) -> Result(Nil, Error) {
  case i > max {
    True -> Ok(Nil)
    False -> {
      let comment_id = base_comment_id + i
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO bench_comments (id, created, article_id, body) VALUES ($1, $2, $3, $4)",
          [
            postgleam.int(comment_id),
            postgleam.int(comment_id * 1000),
            postgleam.int(article_id),
            postgleam.text(text),
          ],
        )
      insert_comments_loop(conn, i + 1, max, article_id, base_comment_id, text)
    }
  }
}

fn query_real_loop(
  conn: postgleam.Connection,
  user_id: Int,
  max_users: Int,
  sql: String,
  decoder: decode.RowDecoder(Int),
) -> Nil {
  case user_id > max_users {
    True -> Nil
    False -> {
      let email = "user" <> pad_int(user_id, 8) <> "@example.com"
      let assert Ok(_) =
        postgleam.query_with(conn, sql, [postgleam.text(email)], decoder)
      query_real_loop(conn, user_id + 1, max_users, sql, decoder)
    }
  }
}

// =============================================================================
// 3. Many — Read-heavy (insert once, query 1000 times)
// =============================================================================

fn bench_many(cfg: Config, nusers: Int) -> RealworldResult {
  let label = "many/" <> int.to_string(nusers)
  io.println("  " <> label <> "...")
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE IF EXISTS bench_many_users")
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TABLE bench_many_users (
        id INTEGER PRIMARY KEY,
        created BIGINT NOT NULL,
        email TEXT NOT NULL,
        active BOOLEAN NOT NULL
      )",
    )

  // Insert
  let t0 = now_ms()
  let assert Ok(_) =
    postgleam.transaction(conn, fn(c) {
      insert_users_many_loop(c, 1, nusers)
    })
  let insert_ms = now_ms() - t0

  // Query 1000 times
  let user_decoder = {
    use id <- decode.element(0, decode.int)
    use _created <- decode.element(1, decode.int)
    use _email <- decode.element(2, decode.text)
    use _active <- decode.element(3, decode.bool)
    decode.success(id)
  }
  let t0 = now_ms()
  query_many_loop(conn, 0, 1000, nusers, user_decoder)
  let query_ms = now_ms() - t0

  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE bench_many_users")
  postgleam.disconnect(conn)

  RealworldResult(name: label, insert_ms: insert_ms, query_ms: query_ms)
}

fn insert_users_many_loop(
  conn: postgleam.Connection,
  current: Int,
  max: Int,
) -> Result(Nil, Error) {
  case current > max {
    True -> Ok(Nil)
    False -> {
      let email = "user" <> pad_int(current, 8) <> "@example.com"
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO bench_many_users (id, created, email, active) VALUES ($1, $2, $3, $4)",
          [
            postgleam.int(current),
            postgleam.int(current * 60_000),
            postgleam.text(email),
            postgleam.bool(True),
          ],
        )
      insert_users_many_loop(conn, current + 1, max)
    }
  }
}

fn query_many_loop(
  conn: postgleam.Connection,
  iteration: Int,
  max_iterations: Int,
  expected_count: Int,
  decoder: decode.RowDecoder(Int),
) -> Nil {
  case iteration >= max_iterations {
    True -> Nil
    False -> {
      let assert Ok(result) =
        postgleam.query_with(
          conn,
          "SELECT id, created, email, active FROM bench_many_users ORDER BY id",
          [],
          decoder,
        )
      let assert True = result.count == expected_count
      query_many_loop(
        conn,
        iteration + 1,
        max_iterations,
        expected_count,
        decoder,
      )
    }
  }
}

// =============================================================================
// 4. Large — Big row payloads
// =============================================================================

fn bench_large(cfg: Config, nusers: Int) -> RealworldResult {
  let label = "large/" <> format_k(nusers)
  io.println("  " <> label <> "...")
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE IF EXISTS bench_large_users")
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TABLE bench_large_users (
        id INTEGER PRIMARY KEY,
        created BIGINT NOT NULL,
        email TEXT NOT NULL,
        active BOOLEAN NOT NULL
      )",
    )

  // Insert users with ~1KB email field
  let big_email = string.repeat("a", 1000)
  let t0 = now_ms()
  let assert Ok(_) =
    postgleam.transaction(conn, fn(c) {
      insert_large_loop(c, 1, nusers, big_email)
    })
  let insert_ms = now_ms() - t0

  // Query all
  let user_decoder = {
    use id <- decode.element(0, decode.int)
    use _created <- decode.element(1, decode.int)
    use _email <- decode.element(2, decode.text)
    use _active <- decode.element(3, decode.bool)
    decode.success(id)
  }
  let t0 = now_ms()
  let assert Ok(result) =
    postgleam.query_with(
      conn,
      "SELECT id, created, email, active FROM bench_large_users ORDER BY id",
      [],
      user_decoder,
    )
  let query_ms = now_ms() - t0
  let assert True = result.count == nusers

  let assert Ok(_) =
    postgleam.simple_query(conn, "DROP TABLE bench_large_users")
  postgleam.disconnect(conn)

  RealworldResult(name: label, insert_ms: insert_ms, query_ms: query_ms)
}

fn insert_large_loop(
  conn: postgleam.Connection,
  current: Int,
  max: Int,
  big_email: String,
) -> Result(Nil, Error) {
  case current > max {
    True -> Ok(Nil)
    False -> {
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO bench_large_users (id, created, email, active) VALUES ($1, $2, $3, $4)",
          [
            postgleam.int(current),
            postgleam.int(current * 1000),
            postgleam.text(big_email),
            postgleam.bool(True),
          ],
        )
      insert_large_loop(conn, current + 1, max, big_email)
    }
  }
}

// =============================================================================
// Helpers
// =============================================================================

fn pad_int(n: Int, width: Int) -> String {
  let s = int.to_string(n)
  let len = string.length(s)
  case len >= width {
    True -> s
    False -> string.repeat("0", width - len) <> s
  }
}

fn format_k(n: Int) -> String {
  case n >= 1000 {
    True -> int.to_string(n / 1000) <> "K"
    False -> int.to_string(n)
  }
}

fn print_realworld_table(results: List(RealworldResult)) -> Nil {
  io.println("")
  io.println(
    "Real-World Benchmark Results (modeled after go-sqlite-bench)",
  )
  io.println(string.repeat("=", 65))
  io.println(
    pad_right("Benchmark", 25)
    <> pad_right("Insert (ms)", 15)
    <> pad_right("Query (ms)", 15)
    <> "Total (ms)",
  )
  io.println(string.repeat("-", 65))

  list.each(results, fn(r) {
    io.println(
      pad_right(r.name, 25)
      <> pad_right(int.to_string(r.insert_ms), 15)
      <> pad_right(int.to_string(r.query_ms), 15)
      <> int.to_string(r.insert_ms + r.query_ms),
    )
  })
  io.println("")
}

fn pad_right(s: String, width: Int) -> String {
  let len = string.length(s)
  case len >= width {
    True -> s
    False -> s <> string.repeat(" ", width - len)
  }
}

@external(erlang, "bench_ffi", "now_ms")
fn now_ms() -> Int
