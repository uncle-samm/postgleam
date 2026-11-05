//// SCRAM-SHA-256 authentication (RFC 5802 / RFC 7677).

import gleam/bit_array
import gleam/crypto
import gleam/erlang/atom.{type Atom}
import gleam/int
import gleam/list
import gleam/result
import gleam/string

/// 18 random bytes encode to a 24-character base64 nonce.
const nonce_rand_bytes = 18

/// Iteration counts above this are refused: a malicious server could
/// otherwise make the client burn CPU for minutes. PostgreSQL's default is
/// 4096 and its `scram_iterations` setting is rarely raised past 100k.
pub const max_iterations = 10_000_000

/// State kept between the SCRAM messages.
pub type ScramState {
  ScramState(
    client_nonce: String,
    salt: BitArray,
    iterations: Int,
    auth_message: String,
    client_key: BitArray,
    server_key: BitArray,
  )
}

/// Build the client-first message. Returns the mechanism name and the
/// message bytes (`n,,n=,r=<nonce>`).
pub fn client_first() -> #(String, BitArray) {
  let nonce =
    bit_array.base64_encode(crypto.strong_random_bytes(nonce_rand_bytes), True)
  let msg = "n,,n=,r=" <> nonce
  #("SCRAM-SHA-256", bit_array.from_string(msg))
}

/// Extract the client-first-bare part (`n=,r=<nonce>`) of a client-first message.
pub fn extract_client_first_bare(
  client_first_data: BitArray,
) -> Result(String, Nil) {
  use s <- result.try(bit_array.to_string(client_first_data))
  case string.split_once(s, ",,") {
    Ok(#(_, bare)) -> Ok(bare)
    Error(_) -> Error(Nil)
  }
}

/// Process the server-first message and build the client-final message.
pub fn client_final(
  server_first: BitArray,
  client_first_bare: String,
  password: String,
) -> Result(#(BitArray, ScramState), String) {
  use server_first_str <- result.try(
    bit_array.to_string(server_first)
    |> result.replace_error("Invalid server-first encoding"),
  )
  let fields = parse_server_data(server_first_str)
  use client_nonce <- result.try(
    get_field(parse_server_data(client_first_bare), "r")
    |> result.replace_error("Missing client nonce in client-first message"),
  )
  use server_nonce <- result.try(
    get_field(fields, "r") |> result.replace_error("Missing server nonce (r)"),
  )
  use _ <- result.try(
    case
      string.starts_with(server_nonce, client_nonce)
      && string.length(server_nonce) > string.length(client_nonce)
    {
      True -> Ok(Nil)
      False -> Error("Server nonce does not extend the client nonce")
    },
  )
  use salt_b64 <- result.try(
    get_field(fields, "s") |> result.replace_error("Missing salt (s)"),
  )
  use iterations_str <- result.try(
    get_field(fields, "i") |> result.replace_error("Missing iterations (i)"),
  )
  use salt <- result.try(
    bit_array.base64_decode(salt_b64)
    |> result.replace_error("Invalid salt base64"),
  )
  use iterations <- result.try(
    int.parse(iterations_str) |> result.replace_error("Invalid iteration count"),
  )
  use _ <- result.try(case iterations >= 1 && iterations <= max_iterations {
    True -> Ok(Nil)
    False ->
      Error(
        "SCRAM iteration count out of range (1.."
        <> int.to_string(max_iterations)
        <> "): "
        <> iterations_str,
      )
  })

  let salted_password = hi(saslprep(password), salt, iterations)
  let client_key = hmac(salted_password, <<"Client Key":utf8>>)
  let server_key = hmac(salted_password, <<"Server Key":utf8>>)
  let stored_key = crypto.hash(crypto.Sha256, client_key)

  let client_final_without_proof = "c=biws,r=" <> server_nonce
  let auth_message =
    client_first_bare
    <> ","
    <> server_first_str
    <> ","
    <> client_final_without_proof

  let client_sig = hmac(stored_key, bit_array.from_string(auth_message))
  let proof = xor_bytes(client_key, client_sig)
  let client_final =
    client_final_without_proof <> ",p=" <> bit_array.base64_encode(proof, True)

  Ok(#(
    bit_array.from_string(client_final),
    ScramState(
      client_nonce: client_nonce,
      salt: salt,
      iterations: iterations,
      auth_message: auth_message,
      client_key: client_key,
      server_key: server_key,
    ),
  ))
}

/// Verify the server-final message (mutual authentication).
pub fn verify_server(
  server_final: BitArray,
  state: ScramState,
) -> Result(Nil, String) {
  use server_final_str <- result.try(
    bit_array.to_string(server_final)
    |> result.replace_error("Invalid server-final encoding"),
  )
  let fields = parse_server_data(server_final_str)
  case get_field(fields, "e") {
    Ok(err) -> Error("SCRAM server error: " <> err)
    Error(_) -> {
      use verifier_b64 <- result.try(
        get_field(fields, "v")
        |> result.replace_error("Missing server verifier (v)"),
      )
      use server_sig <- result.try(
        bit_array.base64_decode(verifier_b64)
        |> result.replace_error("Invalid verifier base64"),
      )
      let expected =
        hmac(state.server_key, bit_array.from_string(state.auth_message))
      case crypto.secure_compare(server_sig, expected) {
        True -> Ok(Nil)
        False -> Error("Cannot verify SCRAM server signature")
      }
    }
  }
}

/// Simplified SASLprep (RFC 4013): NFKC-normalise the password, as
/// PostgreSQL does. ASCII passwords are unchanged.
fn saslprep(password: String) -> String {
  nfkc(password)
}

/// Hi(password, salt, i) = PBKDF2-HMAC-SHA256.
fn hi(password: String, salt: BitArray, iterations: Int) -> BitArray {
  pbkdf2_hmac(
    atom.create("sha256"),
    bit_array.from_string(password),
    salt,
    iterations,
    32,
  )
}

fn hmac(key: BitArray, data: BitArray) -> BitArray {
  crypto.hmac(data, crypto.Sha256, key)
}

/// XOR two 32-byte values.
fn xor_bytes(a: BitArray, b: BitArray) -> BitArray {
  let assert <<x:256>> = a
  let assert <<y:256>> = b
  <<int.bitwise_exclusive_or(x, y):256>>
}

fn get_field(
  fields: List(#(String, String)),
  key: String,
) -> Result(String, Nil) {
  list.key_find(fields, key)
}

fn parse_server_data(data: String) -> List(#(String, String)) {
  data
  |> string.split(",")
  |> list.filter_map(string.split_once(_, "="))
}

// Erlang standard library calls (no Gleam API exists for these).

@external(erlang, "crypto", "pbkdf2_hmac")
fn pbkdf2_hmac(
  digest: Atom,
  password: BitArray,
  salt: BitArray,
  iterations: Int,
  key_length: Int,
) -> BitArray

@external(erlang, "unicode", "characters_to_nfkc_binary")
fn nfkc(s: String) -> String
