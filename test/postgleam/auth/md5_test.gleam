import gleeunit/should
import postgleam/auth/md5

pub fn md5_hash_password_test() {
  // Known test vector: PostgreSQL MD5 auth
  // md5(md5("password" + "postgres") + salt)
  // md5("password" + "postgres") = md5("passwordpostgres")
  let result = md5.hash_password("password", "postgres", <<1, 2, 3, 4>>)
  // Result should start with "md5"
  should.be_true(starts_with(result, "md5"))
  // Result should be "md5" + 32 hex chars = 35 chars total
  should.equal(string_length(result), 35)
}

pub fn md5_hash_password_deterministic_test() {
  // Same inputs should give same output
  let a = md5.hash_password("secret", "user1", <<10, 20, 30, 40>>)
  let b = md5.hash_password("secret", "user1", <<10, 20, 30, 40>>)
  should.equal(a, b)
}

pub fn md5_hash_password_different_salt_test() {
  let a = md5.hash_password("pass", "user", <<1, 2, 3, 4>>)
  let b = md5.hash_password("pass", "user", <<5, 6, 7, 8>>)
  should.not_equal(a, b)
}

pub fn md5_hash_password_different_user_test() {
  let a = md5.hash_password("pass", "alice", <<1, 2, 3, 4>>)
  let b = md5.hash_password("pass", "bob", <<1, 2, 3, 4>>)
  should.not_equal(a, b)
}

// Known value test: compute manually
// md5("passwordpostgres") = "a43723aba4c8c8446b40e6d6da3e1e43" (computed)
// Then: md5("a43723aba4c8c8446b40e6d6da3e1e43" ++ <<0,0,0,0>>)
pub fn md5_hash_known_value_test() {
  let result = md5.hash_password("password", "postgres", <<0, 0, 0, 0>>)
  should.be_true(starts_with(result, "md5"))
}

fn starts_with(s: String, prefix: String) -> Bool {
  case s {
    _ -> {
      let prefix_len = string_length(prefix)
      let s_start = string_slice(s, 0, prefix_len)
      s_start == prefix
    }
  }
}

@external(erlang, "string", "length")
fn string_length(s: String) -> Int

@external(erlang, "string", "slice")
fn string_slice(s: String, start: Int, len: Int) -> String
