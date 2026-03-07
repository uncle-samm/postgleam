/// SCRAM-SHA-256 authentication implementation
/// Based on RFC 5802 / 7677

import gleam/bit_array
import gleam/int
import gleam/list
import gleam/result
import gleam/string

const nonce_rand_bytes = 18

// 18 bytes -> 24 chars base64

/// State maintained across SCRAM message exchanges
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

/// Generate the client-first message for SCRAM-SHA-256 auth.
/// Returns (mechanism, initial_response_data)
pub fn client_first() -> #(String, BitArray) {
  let nonce = generate_nonce()
  let bare = "n=,r=" <> nonce
  let msg = "n,," <> bare
  #("SCRAM-SHA-256", bit_array.from_string(msg))
}

/// Process the server-first message and produce the client-final message.
/// Returns (client_final_bytes, scram_state) or an error.
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

  use server_nonce <- result.try(
    get_field(fields, "r") |> result.replace_error("Missing server nonce (r)"),
  )
  use salt_b64 <- result.try(
    get_field(fields, "s") |> result.replace_error("Missing salt (s)"),
  )
  use iterations_str <- result.try(
    get_field(fields, "i") |> result.replace_error("Missing iterations (i)"),
  )
  use salt <- result.try(
    base64_decode(salt_b64) |> result.replace_error("Invalid salt base64"),
  )
  use iterations <- result.try(
    int.parse(iterations_str) |> result.replace_error("Invalid iteration count"),
  )

  // Derive keys
  let salted_password = hi(password, salt, iterations)
  let client_key = hmac_sha256(salted_password, <<"Client Key":utf8>>)
  let server_key = hmac_sha256(salted_password, <<"Server Key":utf8>>)
  let stored_key = sha256(client_key)

  // Build auth message
  let client_final_without_proof = "c=biws,r=" <> server_nonce
  let auth_message =
    client_first_bare
    <> ","
    <> server_first_str
    <> ","
    <> client_final_without_proof

  // Compute proof
  let client_sig = hmac_sha256(stored_key, bit_array.from_string(auth_message))
  let proof = xor_bytes(client_key, client_sig)
  let proof_b64 = base64_encode(proof)

  let client_final = client_final_without_proof <> ",p=" <> proof_b64

  let state =
    ScramState(
      client_nonce: "",
      salt: salt,
      iterations: iterations,
      auth_message: auth_message,
      client_key: client_key,
      server_key: server_key,
    )

  Ok(#(bit_array.from_string(client_final), state))
}

/// Verify the server-final message
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
        base64_decode(verifier_b64)
        |> result.replace_error("Invalid verifier base64"),
      )

      let expected =
        hmac_sha256(state.server_key, bit_array.from_string(state.auth_message))

      case server_sig == expected {
        True -> Ok(Nil)
        False -> Error("Cannot verify SCRAM server signature")
      }
    }
  }
}

/// Extract the client-first-bare from a client-first message.
/// The client-first message is "n,,n=,r=<nonce>" — the bare part is "n=,r=<nonce>"
pub fn extract_client_first_bare(client_first_data: BitArray) -> Result(String, Nil) {
  use s <- result.try(bit_array.to_string(client_first_data))
  // Skip the "n,," prefix (GS2 header)
  case string.split_once(s, ",,") {
    Ok(#(_, bare)) -> Ok(bare)
    Error(_) -> Error(Nil)
  }
}

// --- Hi (PBKDF2-SHA-256) ---

fn hi(password: String, salt: BitArray, iterations: Int) -> BitArray {
  let password_bytes = bit_array.from_string(password)
  // U1 = HMAC(password, salt || INT(1))
  let salt_with_one = <<salt:bits, 0, 0, 0, 1>>
  let u1 = hmac_sha256(password_bytes, salt_with_one)
  iterate(password_bytes, iterations - 1, u1, u1)
}

fn iterate(
  key: BitArray,
  remaining: Int,
  prev: BitArray,
  acc: BitArray,
) -> BitArray {
  case remaining {
    0 -> acc
    _ -> {
      let next = hmac_sha256(key, prev)
      let new_acc = xor_bytes(next, acc)
      iterate(key, remaining - 1, next, new_acc)
    }
  }
}

// --- FFI wrappers ---

@external(erlang, "postgleam_ffi", "crypto_exor")
fn xor_bytes(a: BitArray, b: BitArray) -> BitArray

@external(erlang, "postgleam_ffi", "crypto_strong_rand_bytes")
fn strong_rand_bytes(n: Int) -> BitArray

@external(erlang, "postgleam_ffi", "crypto_mac_hmac")
fn hmac_sha256(key: BitArray, data: BitArray) -> BitArray

@external(erlang, "postgleam_ffi", "sha256_hash")
fn sha256(data: BitArray) -> BitArray

fn generate_nonce() -> String {
  let bytes = strong_rand_bytes(nonce_rand_bytes)
  base64_encode(bytes)
}

@external(erlang, "base64", "encode")
fn base64_encode_raw(data: BitArray) -> BitArray

fn base64_encode(data: BitArray) -> String {
  let encoded = base64_encode_raw(data)
  let assert Ok(s) = bit_array.to_string(encoded)
  s
}

@external(erlang, "base64", "decode")
fn base64_decode_raw(data: BitArray) -> BitArray

fn base64_decode(s: String) -> Result(BitArray, Nil) {
  let data = bit_array.from_string(s)
  // base64:decode can crash on invalid input, but we'll trust it for now
  Ok(base64_decode_raw(data))
}

// --- Parse "key=value,key=value" format ---

fn parse_server_data(data: String) -> List(#(String, String)) {
  data
  |> string.split(",")
  |> list.filter_map(fn(kv) {
    case string.split_once(kv, "=") {
      Ok(#(k, v)) -> Ok(#(k, v))
      Error(_) -> Error(Nil)
    }
  })
}

fn get_field(fields: List(#(String, String)), key: String) -> Result(String, Nil) {
  case fields {
    [] -> Error(Nil)
    [#(k, v), ..] if k == key -> Ok(v)
    [_, ..rest] -> get_field(rest, key)
  }
}
