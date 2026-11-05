//// MD5 password authentication:
//// `"md5" <> md5_hex(md5_hex(password <> username) <> salt)`.

import gleam/bit_array
import gleam/crypto
import gleam/string

/// Generate the MD5 password hash for PostgreSQL authentication.
pub fn hash_password(
  password: String,
  username: String,
  salt: BitArray,
) -> String {
  let inner = md5_hex(bit_array.from_string(password <> username))
  "md5" <> md5_hex(<<bit_array.from_string(inner):bits, salt:bits>>)
}

fn md5_hex(data: BitArray) -> String {
  crypto.hash(crypto.Md5, data)
  |> bit_array.base16_encode
  |> string.lowercase
}
