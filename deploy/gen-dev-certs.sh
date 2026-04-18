#!/usr/bin/env bash
# gen-dev-certs.sh — Generate a self-signed CA + server cert + client cert
# for LOCAL DEVELOPMENT and TESTING of Prism's mTLS Flight transport.
#
# ===========================================================================
#  *** NOT FOR PRODUCTION ***
#
#  These certs have hardcoded CNs, a long validity, and no revocation
#  infrastructure. Production deployments MUST use cert-manager (or an
#  equivalent issuer) to mint short-lived certs and mount them via
#  Kubernetes Secrets. See `docs/production-plan.md` Phase 7.
# ===========================================================================
#
# Output: $OUT_DIR/{ca.crt,ca.key,server.crt,server.key,client.crt,client.key}
#   - ca.crt / ca.key:         throwaway dev CA, signs server + client
#   - server.crt / server.key: served by the Rust worker
#   - client.crt / client.key: presented by the Java client (and tests)
#
# Usage:
#   ./deploy/gen-dev-certs.sh [OUT_DIR]
#
# End-to-end smoke test after running this script:
#
#   # 1. Generate the certs
#   ./deploy/gen-dev-certs.sh /tmp/prism-dev-tls
#
#   # 2. Start the Rust worker with TLS enabled
#   cat >/tmp/worker.toml <<EOF
#   [server]
#   bind = "127.0.0.1:50051"
#
#   [tls]
#   enabled = true
#   cert_path = "/tmp/prism-dev-tls/server.crt"
#   key_path  = "/tmp/prism-dev-tls/server.key"
#   client_ca_path = "/tmp/prism-dev-tls/ca.crt"
#   client_cn_pattern = ""
#   EOF
#   cargo run --release --bin prism-worker -- --config /tmp/worker.toml
#
#   # 3. Point the Trino connector at the dev certs via catalog props:
#   #    prism.workers                  = localhost:50051
#   #    prism.tls.enabled              = true
#   #    prism.tls.server-ca-path       = /tmp/prism-dev-tls/ca.crt
#   #    prism.tls.client-cert-path     = /tmp/prism-dev-tls/client.crt
#   #    prism.tls.client-key-path      = /tmp/prism-dev-tls/client.key
#   #    prism.tls.server-name-override = prism-worker   # matches server cert CN
#
#   # 4. Sanity-check from the shell:
#   #    openssl s_client -connect 127.0.0.1:50051 \
#   #        -CAfile /tmp/prism-dev-tls/ca.crt \
#   #        -cert /tmp/prism-dev-tls/client.crt \
#   #        -key  /tmp/prism-dev-tls/client.key \
#   #        -alpn h2 -servername prism-worker </dev/null

set -euo pipefail

OUT_DIR="${1:-./dev-tls}"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

# --- Guard against silently regenerating a working config -----------------
if [[ -f server.crt && -f server.key && -f ca.crt && -f client.crt && -f client.key ]]; then
  echo "Certs already exist in $OUT_DIR — delete them first to regenerate." >&2
  echo "Files: $(ls)" >&2
  exit 0
fi

umask 077   # keys should not be world-readable

# --- CA --------------------------------------------------------------------
# 10-year validity is fine for a dev CA. Production: cert-manager, days=90.
openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -key ca.key -sha256 -days 3650 \
    -subj "/CN=prism-dev-ca/O=prism-dev" \
    -out ca.crt

# --- Server ----------------------------------------------------------------
# SAN includes the localhost addresses so both `localhost` and `127.0.0.1`
# work without triggering a name-mismatch in strict verifiers.
cat >server.ext <<EOF
subjectAltName = DNS:prism-worker, DNS:localhost, IP:127.0.0.1, IP:::1
extendedKeyUsage = serverAuth
EOF

openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key \
    -subj "/CN=prism-worker/O=prism-dev" \
    -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 3650 -sha256 -extfile server.ext \
    -out server.crt
rm -f server.csr server.ext

# --- Client ----------------------------------------------------------------
cat >client.ext <<EOF
extendedKeyUsage = clientAuth
EOF

openssl genrsa -out client.key 2048 2>/dev/null
openssl req -new -key client.key \
    -subj "/CN=prism-client/O=prism-dev" \
    -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 3650 -sha256 -extfile client.ext \
    -out client.crt
rm -f client.csr client.ext ca.srl

chmod 0644 ca.crt server.crt client.crt
chmod 0600 ca.key server.key client.key

echo "Dev TLS material written to $(pwd):"
ls -l ca.crt server.crt server.key client.crt client.key
echo
echo "Server CN:  prism-worker  (SAN: prism-worker, localhost, 127.0.0.1, ::1)"
echo "Client CN:  prism-client"
echo
echo "!!! These certs are dev-only. Do NOT ship them to production. !!!"
