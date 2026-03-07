import gleeunit/should
import postgleam/codec/bytea
import postgleam/codec/name
import postgleam/codec/text
import postgleam/value

// --- text ---

pub fn text_encode_test() {
  let assert Ok(encoded) = text.encode(value.Text("hello"))
  should.equal(encoded, <<"hello":utf8>>)
}

pub fn text_encode_empty_test() {
  text.encode(value.Text(""))
  |> should.equal(Ok(<<>>))
}

pub fn text_encode_unicode_test() {
  let assert Ok(encoded) = text.encode(value.Text("héllo 世界"))
  should.equal(encoded, <<"héllo 世界":utf8>>)
}

pub fn text_decode_test() {
  text.decode(<<"hello":utf8>>)
  |> should.equal(Ok(value.Text("hello")))
}

pub fn text_decode_empty_test() {
  text.decode(<<>>)
  |> should.equal(Ok(value.Text("")))
}

pub fn text_decode_unicode_test() {
  text.decode(<<"héllo 世界":utf8>>)
  |> should.equal(Ok(value.Text("héllo 世界")))
}

pub fn text_roundtrip_test() {
  let vals = ["", "hello", "héllo 世界", "line1\nline2", "tab\there"]
  text_roundtrip_list(vals)
}

pub fn text_wrong_type_test() {
  text.encode(value.Integer(1))
  |> should.be_error()
}

fn text_roundtrip_list(vals: List(String)) -> Nil {
  case vals {
    [] -> Nil
    [v, ..rest] -> {
      let assert Ok(encoded) = text.encode(value.Text(v))
      let assert Ok(decoded) = text.decode(encoded)
      should.equal(decoded, value.Text(v))
      text_roundtrip_list(rest)
    }
  }
}

// --- name ---

pub fn name_encode_test() {
  let assert Ok(encoded) = name.encode(value.Text("my_table"))
  should.equal(encoded, <<"my_table":utf8>>)
}

pub fn name_encode_too_long_test() {
  // 64 chars exactly should fail
  let long =
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl"
  name.encode(value.Text(long))
  |> should.be_error()
}

pub fn name_encode_63_chars_test() {
  // 63 chars should succeed
  let ok63 =
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk"
  name.encode(value.Text(ok63))
  |> should.be_ok()
}

pub fn name_decode_test() {
  name.decode(<<"my_table":utf8>>)
  |> should.equal(Ok(value.Text("my_table")))
}

pub fn name_roundtrip_test() {
  let assert Ok(encoded) = name.encode(value.Text("pg_catalog"))
  name.decode(encoded)
  |> should.equal(Ok(value.Text("pg_catalog")))
}

// --- bytea ---

pub fn bytea_encode_test() {
  bytea.encode(value.Bytea(<<1, 2, 3>>))
  |> should.equal(Ok(<<1, 2, 3>>))
}

pub fn bytea_encode_empty_test() {
  bytea.encode(value.Bytea(<<>>))
  |> should.equal(Ok(<<>>))
}

pub fn bytea_decode_test() {
  bytea.decode(<<1, 2, 3>>)
  |> should.equal(Ok(value.Bytea(<<1, 2, 3>>)))
}

pub fn bytea_decode_empty_test() {
  bytea.decode(<<>>)
  |> should.equal(Ok(value.Bytea(<<>>)))
}

pub fn bytea_roundtrip_test() {
  let data = <<0, 1, 2, 255, 128, 64>>
  let assert Ok(encoded) = bytea.encode(value.Bytea(data))
  bytea.decode(encoded)
  |> should.equal(Ok(value.Bytea(data)))
}

pub fn bytea_wrong_type_test() {
  bytea.encode(value.Integer(1))
  |> should.be_error()
}
