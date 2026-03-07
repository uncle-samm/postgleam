-- Postgleam test database setup
-- Ported from reference/test/test_helper.exs

-- Create test database
DROP DATABASE IF EXISTS postgleam_test;
CREATE DATABASE postgleam_test
  TEMPLATE=template0
  ENCODING='UTF8'
  LC_COLLATE='en_US.UTF-8'
  LC_CTYPE='en_US.UTF-8';

-- Create test database for schema tests
DROP DATABASE IF EXISTS postgleam_test_with_schemas;
CREATE DATABASE postgleam_test_with_schemas
  TEMPLATE=template0
  ENCODING='UTF8'
  LC_COLLATE='en_US.UTF-8'
  LC_CTYPE='en_US.UTF-8';

-- Create test users
DROP ROLE IF EXISTS postgleam_cleartext_pw;
CREATE USER postgleam_cleartext_pw WITH PASSWORD 'postgleam_cleartext_pw';

DROP ROLE IF EXISTS postgleam_md5_pw;
CREATE USER postgleam_md5_pw WITH PASSWORD 'postgleam_md5_pw';

DROP ROLE IF EXISTS postgleam_scram_pw;
SET password_encryption = 'scram-sha-256';
CREATE USER postgleam_scram_pw WITH PASSWORD 'postgleam_scram_pw';

-- Grant connect permissions
GRANT ALL PRIVILEGES ON DATABASE postgleam_test TO postgleam_cleartext_pw;
GRANT ALL PRIVILEGES ON DATABASE postgleam_test TO postgleam_md5_pw;
GRANT ALL PRIVILEGES ON DATABASE postgleam_test TO postgleam_scram_pw;

-- Setup test tables and types in postgleam_test
\c postgleam_test

-- Extensions
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS ltree;

-- Composite types (via tables)
DROP TABLE IF EXISTS composite1;
CREATE TABLE composite1 (a int, b text);

DROP TABLE IF EXISTS composite2;
CREATE TABLE composite2 (a int, b int, c int);

-- Enum type
DROP TYPE IF EXISTS enum1;
CREATE TYPE enum1 AS ENUM ('gleam', 'erlang');

-- Test tables
CREATE TABLE uniques (a int UNIQUE);

CREATE TABLE timestamps (
  micro timestamp,
  milli timestamp(3),
  sec timestamp(0),
  sec_arr timestamp(0)[]
);

CREATE TABLE timestamps_stream (
  micro timestamp,
  milli timestamp(3),
  sec timestamp(0),
  sec_arr timestamp(0)[]
);

DROP TABLE IF EXISTS missing_oid;
DROP TYPE IF EXISTS missing_enum;
DROP TYPE IF EXISTS missing_comp;

CREATE TABLE altering (a int2);

CREATE TABLE calendar (
  a timestamp without time zone,
  b timestamp with time zone
);

-- Domain types
DROP DOMAIN IF EXISTS points_domain;
CREATE DOMAIN points_domain AS point[]
  CONSTRAINT is_populated CHECK (COALESCE(array_length(VALUE, 1), 0) >= 1);

DROP DOMAIN IF EXISTS floats_domain;
CREATE DOMAIN floats_domain AS float[]
  CONSTRAINT is_populated CHECK (COALESCE(array_length(VALUE, 1), 0) >= 1);

-- Replication publication
CREATE PUBLICATION postgleam_example FOR ALL TABLES;

-- Grant permissions to test users
GRANT ALL ON ALL TABLES IN SCHEMA public TO postgleam_cleartext_pw;
GRANT ALL ON ALL TABLES IN SCHEMA public TO postgleam_md5_pw;
GRANT ALL ON ALL TABLES IN SCHEMA public TO postgleam_scram_pw;

-- Setup schema test database
\c postgleam_test_with_schemas

DROP SCHEMA IF EXISTS test CASCADE;
CREATE SCHEMA test;
CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA test;
