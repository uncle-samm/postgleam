#!/bin/sh
set -e

echo "=== Release ==="

# Never publish something that does not pass the full gate.
make check

sh scripts/push.sh
sh scripts/tag.sh
/usr/bin/expect scripts/publish.sh

echo "=== Release complete ==="
