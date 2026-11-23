#!/usr/bin/env bash
# TradeCore Provisioning Control Plane V1 — non-mutating Linux preflight.
#
# Run this only on the isolated control-plane host after a signed Git checkout.
# It creates nothing, starts nothing, reads no secret value and never accepts a
# data-plane or broker endpoint.  A failure is a deployment gate, not a reason
# to weaken systemd, TLS, PostgreSQL or secret-directory permissions.
set -euo pipefail

control_root="${TRADECORE_CONTROL_ROOT:-/etc/tradecore-control}"
service_user="${TRADECORE_CONTROL_SERVICE_USER:-tradecore-control}"
repo_root="${TRADECORE_REPOSITORY_ROOT:-/opt/projects/tradecore-python/current}"

fail() {
  printf 'CONTROL_PLANE_PREFLIGHT_FAILED: %s\n' "$1" >&2
  exit 1
}

[[ "$(id -u)" -eq 0 ]] || fail "run as root or a deployment operator able to inspect ownership"
id "$service_user" >/dev/null 2>&1 || fail "dedicated service user is absent"
[[ -d "$control_root" ]] || fail "protected control root is absent"
[[ -d "$repo_root" ]] || fail "signed Git checkout is absent"
[[ -x "$repo_root/.venv/bin/python" ]] || fail "project CPython virtual environment is absent"

root_mode="$(stat -c '%a' "$control_root")"
[[ $((8#$root_mode & 077)) -eq 0 ]] || fail "control root must not be group/other accessible"

for required in tls/server.crt tls/server.key tls/client-ca.crt; do
  [[ -f "$control_root/$required" ]] || fail "missing protected TLS input: $required"
done

key_mode="$(stat -c '%a' "$control_root/tls/server.key")"
[[ $((8#$key_mode & 077)) -eq 0 ]] || fail "server private key must not be group/other accessible"

"$repo_root/.venv/bin/python" -c 'import h2, psycopg; print("CONTROL_PLANE_PYTHON_DEPS_OK")'
systemd-analyze verify "$repo_root/deploy/systemd/xuanling-tradecore-provisioning.service"

printf '%s\n' 'CONTROL_PLANE_PREFLIGHT_OK_DISARMED'
