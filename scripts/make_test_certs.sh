#!/bin/sh
# Generate a throwaway CA and server certificate for the TLS tests.
#
#   scripts/make_test_certs.sh [out_dir]   (default: build/tls)
#
# Writes ca.pem, server.crt and server.key (valid for localhost and
# 127.0.0.1) plus other_ca.pem, an unrelated CA the tests use to check that
# verification fails. Give server.crt/server.key to PostgreSQL with
# `ssl = on`; the TLS tests run only when build/tls/ca.pem exists.
set -eu

OUT="${1:-build/tls}"
mkdir -p "$OUT"
cd "$OUT"

openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=postgleam test CA" \
  -keyout ca.key -out ca.pem 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=unrelated CA" \
  -keyout other_ca.key -out other_ca.pem 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
  -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -days 30 -extfile san.ext -out server.crt 2>/dev/null
chmod 600 server.key
rm -f server.csr san.ext ca.srl
echo "Test certificates written to $OUT"
