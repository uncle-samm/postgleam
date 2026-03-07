/// Connection configuration for PostgreSQL
pub type Config {
  Config(
    host: String,
    port: Int,
    database: String,
    username: String,
    password: String,
    timeout: Int,
    connect_timeout: Int,
    extra_parameters: List(#(String, String)),
  )
}

/// Create a default config for localhost
pub fn default() -> Config {
  Config(
    host: "localhost",
    port: 5432,
    database: "postgres",
    username: "postgres",
    password: "postgres",
    timeout: 15_000,
    connect_timeout: 5000,
    extra_parameters: [],
  )
}

pub fn host(config: Config, host: String) -> Config {
  Config(..config, host: host)
}

pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port: port)
}

pub fn database(config: Config, database: String) -> Config {
  Config(..config, database: database)
}

pub fn username(config: Config, username: String) -> Config {
  Config(..config, username: username)
}

pub fn password(config: Config, password: String) -> Config {
  Config(..config, password: password)
}

pub fn timeout(config: Config, timeout: Int) -> Config {
  Config(..config, timeout: timeout)
}
