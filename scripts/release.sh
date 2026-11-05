#!/bin/sh
set -e

echo "=== Release ==="

# Never release something that does not pass the full gate.
make check
sh scripts/push.sh

version=$(sed -n 's/^version = "\(.*\)"/\1/p' gleam.toml)
# Publishing the GitHub release starts the Release workflow, which runs CI
# at the tag and publishes to Hex.
if command -v gh >/dev/null 2>&1; then
  gh release create "v$version" --target main --generate-notes
else
  echo "Publish a GitHub release with tag v$version to release it to Hex."
fi

echo "=== Done ==="
