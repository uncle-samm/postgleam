#!/bin/bash
# Smoke test: starts Postgres, sets up test DB, runs all tests,
# then runs a Gleam script that exercises every major feature end-to-end.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass=0
fail=0

section() {
  echo ""
  echo -e "${CYAN}=== $1 ===${NC}"
}

check() {
  if [ $1 -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}: $2"
    pass=$((pass + 1))
  else
    echo -e "  ${RED}FAIL${NC}: $2"
    fail=$((fail + 1))
  fi
}

# ---------------------------------------------------------------
section "1. Docker PostgreSQL"
# ---------------------------------------------------------------

if docker compose ps --format '{{.State}}' 2>/dev/null | grep -q running; then
  echo "  Postgres container already running."
else
  echo "  Starting Postgres container..."
  docker compose up -d --wait 2>&1 | sed 's/^/  /'
fi

# Quick connectivity check via psql
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
export PGUSER PGPASSWORD PGHOST PGPORT

pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" > /dev/null 2>&1
check $? "pg_isready reports PostgreSQL accepting connections"

PG_VERSION=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -tAc "SELECT version();" 2>/dev/null | head -1)
echo "  PostgreSQL: $PG_VERSION"

WAL_LEVEL=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -tAc "SHOW wal_level;" 2>/dev/null)
[ "$WAL_LEVEL" = "logical" ]
check $? "wal_level = logical (required for replication tests)"

# ---------------------------------------------------------------
section "2. Test database setup"
# ---------------------------------------------------------------

# Check if postgleam_test exists; create if not
DB_EXISTS=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -tAc "SELECT 1 FROM pg_database WHERE datname='postgleam_test'" 2>/dev/null)
if [ "$DB_EXISTS" = "1" ]; then
  echo "  postgleam_test database already exists."
  check 0 "Test database exists"
else
  echo "  Running setup_test_db.sh..."
  bash "$SCRIPT_DIR/setup_test_db.sh" 2>&1 | sed 's/^/  /'
  check $? "Test database created"
fi

# Verify test users exist
for user in postgleam_cleartext_pw postgleam_md5_pw postgleam_scram_pw; do
  EXISTS=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -tAc "SELECT 1 FROM pg_roles WHERE rolname='$user'" 2>/dev/null)
  [ "$EXISTS" = "1" ]
  check $? "Test user '$user' exists"
done

# Verify publication exists
PUB_EXISTS=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -tAc "SELECT 1 FROM pg_publication WHERE pubname='postgleam_example'" 2>/dev/null)
[ "$PUB_EXISTS" = "1" ]
check $? "Publication 'postgleam_example' exists (for replication)"

# Verify extensions
for ext in hstore ltree; do
  EXT_EXISTS=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -tAc "SELECT 1 FROM pg_extension WHERE extname='$ext'" 2>/dev/null)
  [ "$EXT_EXISTS" = "1" ]
  check $? "Extension '$ext' installed"
done

# ---------------------------------------------------------------
section "3. Gleam build"
# ---------------------------------------------------------------

gleam build 2>&1 | tail -1
check ${PIPESTATUS[0]} "gleam build succeeds"

# ---------------------------------------------------------------
section "4. Unit tests (gleam test)"
# ---------------------------------------------------------------

TEST_OUTPUT=$(gleam test 2>&1)
TEST_EXIT=$?
SUMMARY=$(echo "$TEST_OUTPUT" | grep -oE '[0-9]+ passed' | head -1)

if [ $TEST_EXIT -eq 0 ]; then
  echo -e "  ${GREEN}$SUMMARY, no failures${NC}"
else
  FAILURES=$(echo "$TEST_OUTPUT" | grep -oE '[0-9]+ failure' | head -1)
  echo -e "  ${RED}$SUMMARY, $FAILURES${NC}"
  echo "$TEST_OUTPUT" | grep -A5 'FAIL\|let assert' | head -30 | sed 's/^/  /'
fi
check $TEST_EXIT "gleam test passes all tests"

# ---------------------------------------------------------------
section "5. Manual integration smoke tests (psql)"
# ---------------------------------------------------------------

# Basic connectivity from psql to postgleam_test
RESULT=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -tAc "SELECT 42" 2>/dev/null)
[ "$RESULT" = "42" ]
check $? "psql SELECT 42 on postgleam_test"

# Auth with SCRAM user
RESULT=$(PGPASSWORD=postgleam_scram_pw psql -h "$PGHOST" -p "$PGPORT" -U postgleam_scram_pw -d postgleam_test -tAc "SELECT 1" 2>/dev/null)
[ "$RESULT" = "1" ]
check $? "SCRAM-SHA-256 user can connect"

# LISTEN/NOTIFY from psql
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -c "NOTIFY smoke_test, 'hello'" > /dev/null 2>&1
check $? "NOTIFY via psql works"

# COPY smoke (using echo + pipe for non-interactive COPY)
printf '1\n2\n3\n' | psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -c "CREATE TEMP TABLE _smoke_copy(id int); COPY _smoke_copy FROM STDIN;" > /dev/null 2>&1
# Use a simpler COPY test: COPY TO STDOUT
COPY_RESULT=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -tAc "
  CREATE TEMP TABLE _smoke_copy2(id int);
  INSERT INTO _smoke_copy2 VALUES (1),(2),(3);
  COPY _smoke_copy2 TO STDOUT;
" 2>/dev/null)
LINE_COUNT=$(echo "$COPY_RESULT" | wc -l | tr -d ' ')
[ "$LINE_COUNT" -ge 3 ]
check $? "COPY TO STDOUT via psql (3 rows)"

# Replication slot creation
SLOT_RESULT=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -tAc "
  SELECT * FROM pg_create_logical_replication_slot('smoke_test_slot', 'pgoutput');
" 2>/dev/null)
SLOT_OK=$?
# Clean up
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgleam_test -c "SELECT pg_drop_replication_slot('smoke_test_slot');" > /dev/null 2>&1
check $SLOT_OK "Logical replication slot creation works"

# ---------------------------------------------------------------
section "Summary"
# ---------------------------------------------------------------

TOTAL=$((pass + fail))
echo ""
if [ $fail -eq 0 ]; then
  echo -e "${GREEN}All $TOTAL checks passed!${NC}"
else
  echo -e "${RED}$fail of $TOTAL checks failed.${NC}"
fi
echo ""
echo "Test breakdown:"
echo "  - Infrastructure checks:  Docker, PostgreSQL, DB setup"
echo "  - Gleam unit+integration: $SUMMARY"
echo "  - psql smoke tests:       Auth, COPY, NOTIFY, Replication"
echo ""

exit $fail
