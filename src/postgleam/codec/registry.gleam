/// Codec registry - maps PostgreSQL OIDs to codecs for encode/decode dispatch.
/// Built at connection bootstrap time from the default matcher list.

import gleam/dict.{type Dict}
import gleam/option.{Some}
import postgleam/codec.{type Codec, type CodecMatcher}

/// A registry mapping OID -> Codec
pub type Registry =
  Dict(Int, Codec)

/// Build a registry from a list of matchers and known OIDs
pub fn build(matchers: List(CodecMatcher)) -> Registry {
  build_loop(matchers, dict.new())
}

fn build_loop(
  matchers: List(CodecMatcher),
  acc: Dict(Int, Codec),
) -> Dict(Int, Codec) {
  case matchers {
    [] -> acc
    [matcher, ..rest] -> {
      let acc = register_oids(matcher.oids, matcher.build, acc)
      build_loop(rest, acc)
    }
  }
}

fn register_oids(
  oids: List(Int),
  build: fn(Int) -> Codec,
  acc: Dict(Int, Codec),
) -> Dict(Int, Codec) {
  case oids {
    [] -> acc
    [oid, ..rest] -> {
      let codec = build(oid)
      register_oids(rest, build, dict.insert(acc, oid, codec))
    }
  }
}

/// Look up a codec by OID
pub fn lookup(registry: Registry, oid: Int) -> Result(Codec, String) {
  case dict.get(registry, oid) {
    Ok(codec) -> Ok(codec)
    Error(_) -> Error("No codec registered for OID " <> int_to_string(oid))
  }
}

/// Find a codec matcher by send function name
pub fn find_by_send(
  matchers: List(CodecMatcher),
  send_fn: String,
) -> Result(CodecMatcher, Nil) {
  case matchers {
    [] -> Error(Nil)
    [matcher, ..rest] ->
      case matcher.send {
        Some(s) if s == send_fn -> Ok(matcher)
        _ -> find_by_send(rest, send_fn)
      }
  }
}

/// Register a single OID with a specific matcher's build function
pub fn register(
  registry: Registry,
  oid: Int,
  matcher: CodecMatcher,
) -> Registry {
  let codec = { matcher.build }(oid)
  dict.insert(registry, oid, codec)
}

@external(erlang, "erlang", "integer_to_binary")
fn int_to_string(n: Int) -> String
