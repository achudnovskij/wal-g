#!/bin/bash
# wal-receive container entrypoint.
#
# Single-tenant: streams from one primary, pushes to one standby.
# Inputs are env vars (K8s-pod-friendly) + four mounted file paths.
# This script:
#   1) validates required env
#   2) maps the K8s-friendly env names onto the libpq + wal-g vars
#      the binary actually reads
#   3) writes the per-tenant push registry (single-element JSON)
#   4) execs `wal-g wal-receive` as PID 1
#
# Container contract (paths and env names below are stable):
#
#   ENV (required):
#     WALG_PRIMARY_HOST            primary postgres hostname / IP
#     WALG_PRIMARY_USER            replication user (e.g. ubi_replication)
#     WALG_SLOT_NAME               replication slot name on the primary
#     WALG_TENANT_NAME             tenant id (used for log lines + push)
#
#   ENV (optional, with defaults):
#     WALG_PRIMARY_PORT            5432
#     WALG_PRIMARY_DB              postgres
#     WALG_APPLICATION_NAME        walg_sync          (shows in pg_stat_replication)
#     WALG_WAL_RECEIVE_PARTIAL_DIR /var/lib/walg/partials
#                                  (legacy alias WALG_PARTIAL_DIR still honored)
#     WALG_WAL_RECEIVE_DRAIN_BATCHING       true
#     WALG_WAL_RECEIVE_SKIP_UPLOAD          true       (sync-standby mode)
#     WALG_WAL_RECEIVE_JANITOR_INTERVAL_SECONDS  30
#     WALG_LOG_LEVEL               NORMAL
#
#   ENV (autonomous-push fallback only — unset under Option B control
#   orchestration, where the control plane supplies the standby host/port at
#   push-time via POST /v1/dr-catchup):
#     WALG_STANDBY_HOST            standby hostname/IP for primary-loss push
#     WALG_STANDBY_HTTP_PORT       standby wal-receive-serve mTLS port (8443)
#     WALG_STANDBY_TARGET_DIR      where on the standby to drop partials
#
#   Mounted file paths (read-only):
#     /etc/walg/tls/client.crt              mTLS cert presented to primary +
#                                           to the standby's wal-receive-serve
#     /etc/walg/tls/client.key              private key for the above
#     /etc/walg/tls/server-ca.crt           CA bundle for verifying primary
#
#   Volume (read-write):
#     /var/lib/walg/partials                local NVMe-backed partial WAL dir
#                                           (instance-store local PV in K8s,
#                                           or emptyDir on i8g/i7i nodes)

set -euo pipefail

die() { echo "wal-receive-entrypoint: ERROR: $*" >&2; exit 1; }
log() { echo "wal-receive-entrypoint: $*"; }

# 1) Required env --------------------------------------------------------
for var in WALG_PRIMARY_HOST WALG_PRIMARY_USER WALG_SLOT_NAME \
           WALG_TENANT_NAME; do
  [ -n "${!var:-}" ] || die "$var is required"
done

# 2) Required mounted files (mTLS client identity + server CA) ----------
# Same client cert the receiver uses to stream from the primary doubles as
# its identity for the receiver->standby push to wal-receive-serve.
for f in /etc/walg/tls/client.crt /etc/walg/tls/client.key \
         /etc/walg/tls/server-ca.crt; do
  [ -r "$f" ] || die "expected mounted secret at $f (mount it from a K8s Secret)"
done

# 3) Map K8s-friendly inputs onto the libpq + wal-g vars -----------------
export PGHOST="$WALG_PRIMARY_HOST"
export PGPORT="${WALG_PRIMARY_PORT:-5432}"
export PGUSER="$WALG_PRIMARY_USER"
export PGDATABASE="${WALG_PRIMARY_DB:-postgres}"
export PGSSLMODE=verify-ca
export PGSSLCERT=/etc/walg/tls/client.crt
export PGSSLKEY=/etc/walg/tls/client.key
export PGSSLROOTCERT=/etc/walg/tls/server-ca.crt
export PGAPPNAME="${WALG_APPLICATION_NAME:-walg_sync}"

# wal-g specific
# Partial dir: the binary reads WALG_WAL_RECEIVE_PARTIAL_DIR (PartialDirEnv).
# The operator configmap now emits that canonical name directly; we keep a
# backward-compat fallback to the legacy WALG_PARTIAL_DIR so an older configmap
# or an external override under the old name still works. Precedence:
# canonical > legacy > built-in default.
PARTIAL_DIR="${WALG_WAL_RECEIVE_PARTIAL_DIR:-${WALG_PARTIAL_DIR:-/var/lib/walg/partials}}"
export WALG_FILE_PREFIX="$PARTIAL_DIR"
export WALG_SLOTNAME="$WALG_SLOT_NAME"
export WALG_WAL_RECEIVE_PARTIAL_DIR="$PARTIAL_DIR"
export WALG_WAL_RECEIVE_SKIP_UPLOAD="${WALG_WAL_RECEIVE_SKIP_UPLOAD:-true}"
export WALG_WAL_RECEIVE_JANITOR_INTERVAL_SECONDS="${WALG_WAL_RECEIVE_JANITOR_INTERVAL_SECONDS:-30}"
export WALG_WAL_RECEIVE_DRAIN_BATCHING="${WALG_WAL_RECEIVE_DRAIN_BATCHING:-true}"
export WALG_LOG_LEVEL="${WALG_LOG_LEVEL:-NORMAL}"

mkdir -p "$WALG_WAL_RECEIVE_PARTIAL_DIR"

# 4) Render the single-tenant push registry (autonomous-fallback only) ---
# Under Option B control orchestration the control plane supplies the standby
# host/port at push-time (POST /v1/dr-catchup) and the receiver's push mTLS
# material comes from the WALG_WAL_RECEIVE_PUSH_* env, so no registry is
# needed. Only emit the registry when a static WALG_STANDBY_HOST is provided
# (the autonomous push-on-primary-loss fallback). The schema lives in
# internal/databases/postgres/wal_receive_push.go; keep this generator and
# that struct in sync.
if [ -n "${WALG_STANDBY_HOST:-}" ]; then
  REGISTRY=/tmp/tenants.json
  # Reject inputs containing a literal `"` — the simple heredoc below can't
  # escape them, and none of these fields should ever contain one (hostnames,
  # paths). Fail loudly so we never write malformed JSON.
  for v in "$WALG_TENANT_NAME" "$WALG_STANDBY_HOST" "${WALG_STANDBY_HTTP_PORT:-8443}" \
           "${WALG_STANDBY_TARGET_DIR:-/var/lib/postgresql/walg-tail}"; do
    case "$v" in *\"*|*\\*) die "tenant-registry input contains unsupported character: $v" ;; esac
  done

  cat > "$REGISTRY" <<EOF
[
  {
    "name":        "${WALG_TENANT_NAME}",
    "base_url":    "https://${WALG_STANDBY_HOST}:${WALG_STANDBY_HTTP_PORT:-8443}",
    "client_cert": "/etc/walg/tls/client.crt",
    "client_key":  "/etc/walg/tls/client.key",
    "server_ca":   "/etc/walg/tls/server-ca.crt",
    "target_dir":  "${WALG_STANDBY_TARGET_DIR:-/var/lib/postgresql/walg-tail}"
  }
]
EOF
  export WALG_TENANT_REGISTRY="$REGISTRY"
fi

# 5) Hand off to wal-g as PID 1 ------------------------------------------
log "tenant=$WALG_TENANT_NAME primary=$PGHOST:$PGPORT slot=$WALG_SLOTNAME"
log "partial_dir=$WALG_WAL_RECEIVE_PARTIAL_DIR drain_batching=$WALG_WAL_RECEIVE_DRAIN_BATCHING"
if [ -n "${WALG_STANDBY_HOST:-}" ]; then
  log "standby_push=https://$WALG_STANDBY_HOST:${WALG_STANDBY_HTTP_PORT:-8443} (mTLS, autonomous fallback)"
else
  log "standby_push=control-orchestrated (Option B; target supplied at push-time)"
fi

exec /usr/bin/wal-g wal-receive
