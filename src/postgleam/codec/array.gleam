/// PostgreSQL array codec - binary format
/// Wire: ndim(int32) + has_null(int32) + elem_oid(int32)
///       + per-dim: length(int32) + lower_bound(int32)
///       + per-element: -1(int32) for NULL | byte_len(int32) + data
///
/// Multi-dimensional arrays are nested `Array` values (an array of rows).
/// Lower bounds other than 1 are accepted when decoding and dropped.
import gleam/bit_array
import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import postgleam/codec.{type Codec, Binary, Codec}
import postgleam/codec/registry.{type Registry}
import postgleam/value.{type Value, Array}

/// Build an array codec that wraps an element codec.
/// The element codec is captured in closures for encode/decode.
pub fn build_array_codec(element_codec: Codec, array_oid: Int) -> Codec {
  let elem_oid = element_codec.oid
  let elem_encode = element_codec.encode
  let elem_decode = element_codec.decode
  Codec(
    type_name: "array",
    oid: array_oid,
    format: Binary,
    encode: fn(val) { encode_array(val, elem_oid, elem_encode) },
    decode: fn(data) { decode_array(data, elem_decode) },
  )
}

/// Hardcoded array OID -> element OID mapping for common types
pub fn array_oid_map() -> List(#(Int, Int)) {
  [
    #(1000, 16),
    // bool[]
    #(1005, 21),
    // int2[]
    #(1007, 23),
    // int4[]
    #(1016, 20),
    // int8[]
    #(1021, 700),
    // float4[]
    #(1022, 701),
    // float8[]
    #(1009, 25),
    // text[]
    #(1015, 1043),
    // varchar[]
    #(1001, 17),
    // bytea[]
    #(2951, 2950),
    // uuid[]
    #(199, 114),
    // json[]
    #(3807, 3802),
    // jsonb[]
    #(1231, 1700),
    // numeric[]
    #(1182, 1082),
    // date[]
    #(1183, 1083),
    // time[]
    #(1270, 1266),
    // timetz[]
    #(1115, 1114),
    // timestamp[]
    #(1185, 1184),
    // timestamptz[]
    #(1187, 1186),
    // interval[]
    #(1028, 26),
    // oid[]
    #(1003, 19),
    // name[]
    #(1041, 869),
    // inet[]
    #(1040, 829),
    // macaddr[]
    #(1017, 600),
    // point[]
  ]
}

/// Register array codecs into an existing registry.
/// Looks up each element codec from the base registry, builds an array codec.
pub fn register_arrays(base: Registry) -> Registry {
  register_arrays_loop(array_oid_map(), base)
}

fn register_arrays_loop(
  mappings: List(#(Int, Int)),
  reg: Registry,
) -> Registry {
  case mappings {
    [] -> reg
    [#(array_oid, elem_oid), ..rest] -> {
      let reg = case registry.lookup(reg, elem_oid) {
        Ok(elem_codec) -> {
          let array_codec = build_array_codec(elem_codec, array_oid)
          dict.insert(reg, array_oid, array_codec)
        }
        Error(_) -> reg
      }
      register_arrays_loop(rest, reg)
    }
  }
}

// --- Encode ---

fn encode_array(
  val: Value,
  elem_oid: Int,
  elem_encode: fn(Value) -> Result(BitArray, String),
) -> Result(BitArray, String) {
  case val {
    Array([]) -> Ok(<<0:32-big, 0:32-big, elem_oid:32-big>>)
    Array(elements) -> {
      use dims <- result.try(dimensions(val))
      use leaves <- result.try(flatten(elements, list.length(dims)))
      let has_null = case list.contains(leaves, None) {
        True -> 1
        False -> 0
      }
      let dim_headers =
        list.fold(dims, <<>>, fn(acc, len) {
          <<acc:bits, len:32-big, 1:32-big>>
        })
      use body <- result.try(encode_elements(leaves, elem_encode, <<>>))
      Ok(<<
        { list.length(dims) }:32-big,
        has_null:32-big,
        elem_oid:32-big,
        dim_headers:bits,
        body:bits,
      >>)
    }
    _ -> Error("array codec: expected Array value")
  }
}

/// The dimension lengths of a (possibly nested) array, outermost first.
/// The first element decides how deep the nesting goes.
fn dimensions(val: Value) -> Result(List(Int), String) {
  case val {
    Array([Some(Array(_) as inner), ..] as elements) -> {
      use rest <- result.try(dimensions(inner))
      Ok([list.length(elements), ..rest])
    }
    Array(elements) -> Ok([list.length(elements)])
    _ -> Error("array codec: expected Array value")
  }
}

/// Flatten `depth` levels of nesting into the element list, checking that
/// every sub-array has the same shape.
fn flatten(
  elements: List(Option(Value)),
  depth: Int,
) -> Result(List(Option(Value)), String) {
  case depth {
    1 ->
      case list.any(elements, is_array) {
        True -> Error("array codec: sub-arrays must all have the same depth")
        False -> Ok(elements)
      }
    _ -> {
      use rows <- result.try(
        list.try_map(elements, fn(e) {
          case e {
            Some(Array(inner)) -> Ok(inner)
            _ -> Error("array codec: sub-arrays must all have the same depth")
          }
        }),
      )
      let lengths = list.map(rows, list.length) |> list.unique
      case lengths {
        [_] -> {
          use flat <- result.try(list.try_map(rows, flatten(_, depth - 1)))
          Ok(list.flatten(flat))
        }
        _ -> Error("array codec: sub-arrays must all have the same length")
      }
    }
  }
}

fn is_array(e: Option(Value)) -> Bool {
  case e {
    Some(Array(_)) -> True
    _ -> False
  }
}

fn encode_elements(
  elements: List(Option(Value)),
  elem_encode: fn(Value) -> Result(BitArray, String),
  acc: BitArray,
) -> Result(BitArray, String) {
  case elements {
    [] -> Ok(acc)
    [None, ..rest] ->
      encode_elements(rest, elem_encode, <<acc:bits, 255, 255, 255, 255>>)
    [Some(val), ..rest] ->
      case elem_encode(val) {
        Ok(encoded) -> {
          let len = bit_array.byte_size(encoded)
          encode_elements(rest, elem_encode, <<
            acc:bits,
            len:32-big,
            encoded:bits,
          >>)
        }
        Error(e) -> Error("array codec: element encode failed: " <> e)
      }
  }
}

// --- Decode ---

/// PostgreSQL allows at most 6 dimensions.
const max_dimensions = 6

fn decode_array(
  data: BitArray,
  elem_decode: fn(BitArray) -> Result(Value, String),
) -> Result(Value, String) {
  case data {
    <<0:32-big, _has_null:32-big, _elem_oid:32-big>> -> Ok(Array([]))
    <<ndim:32-big, _has_null:32-big, _elem_oid:32-big, rest:bits>>
      if ndim >= 1 && ndim <= max_dimensions
    -> {
      use #(dims, rest) <- result.try(decode_dims(rest, ndim, []))
      let total = list.fold(dims, 1, int.multiply)
      use #(elements, rest) <- result.try(
        decode_elements(rest, total, elem_decode, []),
      )
      case rest {
        <<>> -> Ok(nest(list.reverse(elements), dims))
        _ -> Error("array codec: trailing bytes after array data")
      }
    }
    _ -> Error("array codec: unsupported array format")
  }
}

fn decode_dims(
  data: BitArray,
  remaining: Int,
  acc: List(Int),
) -> Result(#(List(Int), BitArray), String) {
  case remaining, data {
    0, _ -> Ok(#(list.reverse(acc), data))
    _, <<len:32-big, _lower_bound:32-big-signed, rest:bits>> ->
      decode_dims(rest, remaining - 1, [len, ..acc])
    _, _ -> Error("array codec: truncated dimension header")
  }
}

/// Rebuild nested arrays from the flat element list.
fn nest(elements: List(Option(Value)), dims: List(Int)) -> Value {
  case dims {
    [] | [_] -> Array(elements)
    [_, ..inner] -> {
      let size = list.fold(inner, 1, int.multiply)
      case size {
        0 -> Array([])
        _ ->
          Array(
            list.sized_chunk(elements, size)
            |> list.map(fn(chunk) { Some(nest(chunk, inner)) }),
          )
      }
    }
  }
}

fn decode_elements(
  data: BitArray,
  remaining: Int,
  elem_decode: fn(BitArray) -> Result(Value, String),
  acc: List(Option(Value)),
) -> Result(#(List(Option(Value)), BitArray), String) {
  case remaining {
    0 -> Ok(#(acc, data))
    _ ->
      case data {
        <<-1:32-big-signed, rest:bits>> ->
          decode_elements(rest, remaining - 1, elem_decode, [None, ..acc])
        <<len:32-big, elem_data:bytes-size(len), rest:bits>> ->
          case elem_decode(elem_data) {
            Ok(val) ->
              decode_elements(rest, remaining - 1, elem_decode, [
                Some(val),
                ..acc
              ])
            Error(e) -> Error("array codec: element decode failed: " <> e)
          }
        _ -> Error("array codec: unexpected end of array data")
      }
  }
}
