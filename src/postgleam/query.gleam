import gleam/option.{type Option}

/// A prepared query
pub type Query {
  Query(
    name: String,
    statement: String,
    param_oids: Option(List(Int)),
    result_oids: Option(List(Int)),
    columns: Option(List(String)),
  )
}
