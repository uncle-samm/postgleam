/// Information about a PostgreSQL type, queried from pg_type catalog
pub type TypeInfo {
  TypeInfo(
    oid: Int,
    type_name: String,
    send: String,
    receive: String,
    output: String,
    input: String,
    /// If this is an array type, the element type's OID (0 if not array)
    array_elem: Int,
    /// If this is a range type, the base type's OID (0 if not range)
    base_type: Int,
    /// If this is a composite type, the element OIDs
    comp_elems: List(Int),
  )
}
