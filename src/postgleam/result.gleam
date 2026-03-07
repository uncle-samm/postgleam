import gleam/option.{type Option}

/// The command that was executed
pub type Command {
  Select
  Insert
  Update
  Delete
  Create
  Drop
  Alter
  Begin
  Commit
  Rollback
  Savepoint
  Release
  Copy
  Move
  Fetch
  Set
  Discard
  Reset
  Listen
  Unlisten
  Notify
  Other(String)
}

/// Result returned from a successful query
pub type QueryResult {
  QueryResult(
    command: Command,
    columns: List(String),
    rows: Option(List(List(String))),
    num_rows: Int,
    connection_id: Option(Int),
  )
}
