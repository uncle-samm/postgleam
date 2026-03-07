/// Codec framework for encoding/decoding PostgreSQL binary wire format values.
/// Each codec handles one or more PostgreSQL types identified by their OID.

import gleam/option.{type Option}
import postgleam/value.{type Value}

/// Wire format: binary or text
pub type Format {
  Binary
  Text
}

/// A resolved codec that can encode and decode values for a specific OID
pub type Codec {
  Codec(
    /// PostgreSQL type name (e.g. "bool", "int4")
    type_name: String,
    /// OID of this type
    oid: Int,
    /// Wire format this codec uses
    format: Format,
    /// Encode a Value to binary payload (without length header)
    encode: fn(Value) -> Result(BitArray, String),
    /// Decode binary payload (without length header) to a Value
    decode: fn(BitArray) -> Result(Value, String),
  )
}

/// A codec matcher - used to build the registry by matching OIDs to codecs
pub type CodecMatcher {
  CodecMatcher(
    /// PostgreSQL type name
    type_name: String,
    /// Known OIDs this matcher handles (empty = match by send function)
    oids: List(Int),
    /// PostgreSQL send function name (for matching by catalog)
    send: Option(String),
    /// Wire format
    format: Format,
    /// Create a codec for a given OID
    build: fn(Int) -> Codec,
  )
}
