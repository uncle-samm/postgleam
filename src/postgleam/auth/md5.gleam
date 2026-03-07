/// MD5 password authentication
/// PostgreSQL MD5 auth: "md5" ++ md5(md5(password ++ username) ++ salt)

import gleam/bit_array


/// Generate the MD5 password hash for PostgreSQL authentication
pub fn hash_password(password: String, username: String, salt: BitArray) -> String {
  let pass_user = bit_array.from_string(password <> username)
  let inner_hash = md5_hex(pass_user)
  let outer_input = <<bit_array.from_string(inner_hash):bits, salt:bits>>
  let outer_hash = md5_hex(outer_input)
  "md5" <> outer_hash
}

fn md5_hex(data: BitArray) -> String {
  let hash = md5_hash(data)
  hex_encode(hash)
}

@external(erlang, "postgleam_ffi", "md5_hash")
fn md5_hash(data: BitArray) -> BitArray

fn hex_encode(data: BitArray) -> String {
  hex_encode_loop(data, "")
}

fn hex_encode_loop(data: BitArray, acc: String) -> String {
  case data {
    <<>> -> acc
    <<byte:8, rest:bits>> -> {
      let high = byte / 16
      let low = byte % 16
      hex_encode_loop(rest, acc <> hex_digit(high) <> hex_digit(low))
    }
    _ -> acc
  }
}

fn hex_digit(n: Int) -> String {
  case n {
    0 -> "0"
    1 -> "1"
    2 -> "2"
    3 -> "3"
    4 -> "4"
    5 -> "5"
    6 -> "6"
    7 -> "7"
    8 -> "8"
    9 -> "9"
    10 -> "a"
    11 -> "b"
    12 -> "c"
    13 -> "d"
    14 -> "e"
    15 -> "f"
    _ -> "?"
  }
}
