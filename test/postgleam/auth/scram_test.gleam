import gleam/bit_array
import gleeunit/should
import postgleam/auth/scram

pub fn client_first_produces_valid_message_test() {
  let #(mechanism, data) = scram.client_first()
  should.equal(mechanism, "SCRAM-SHA-256")

  // Data should be "n,,n=,r=<nonce>" format
  let assert Ok(s) = bit_array.to_string(data)
  should.be_true(string_starts_with(s, "n,,n=,r="))
}

pub fn client_first_nonce_is_random_test() {
  // Two calls should produce different nonces
  let #(_, data1) = scram.client_first()
  let #(_, data2) = scram.client_first()
  should.not_equal(data1, data2)
}

pub fn extract_client_first_bare_test() {
  let data = bit_array.from_string("n,,n=,r=abc123")
  let assert Ok(bare) = scram.extract_client_first_bare(data)
  should.equal(bare, "n=,r=abc123")
}

pub fn scram_full_exchange_test() {
  // Simulate a SCRAM exchange with known values
  // Client first
  let client_first_bare = "n=,r=rOprNGfwEbeRWgbNEkqO"

  // Server first (simulated)
  let server_first =
    "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"

  // Client final
  let result =
    scram.client_final(
      bit_array.from_string(server_first),
      client_first_bare,
      "pencil",
    )

  // Should succeed
  let assert Ok(#(client_final_bytes, state)) = result

  // Client final should be a valid string
  let assert Ok(client_final_str) = bit_array.to_string(client_final_bytes)
  should.be_true(string_starts_with(client_final_str, "c=biws,r="))
  should.be_true(string_contains(client_final_str, ",p="))

  // State should have the correct salt and iterations
  should.equal(state.iterations, 4096)
}

pub fn scram_verify_server_error_test() {
  // Server sends an error
  let state =
    scram.ScramState(
      client_nonce: "",
      salt: <<>>,
      iterations: 4096,
      auth_message: "",
      client_key: <<>>,
      server_key: <<>>,
    )
  let result = scram.verify_server(<<"e=invalid-proof":utf8>>, state)
  let assert Error(msg) = result
  should.be_true(string_contains(msg, "SCRAM server error"))
}

@external(erlang, "postgleam_test_ffi", "string_starts_with")
fn string_starts_with(s: String, prefix: String) -> Bool

@external(erlang, "postgleam_test_ffi", "string_contains")
fn string_contains(s: String, substr: String) -> Bool
