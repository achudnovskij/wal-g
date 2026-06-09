#!/bin/bash
# Reference renderer for the wal-receive K8s templates. Reads template
# parameters from env, expands ${...} placeholders via envsubst, and
# emits the rendered manifests to stdout.
#
# The real control plane will substitute these inline rather than
# shelling out — but this script is a useful smoke test and a clear
# definition of "the complete parameter contract."
#
# Usage:
#   export UBID=pvda4whfnm2y2gp0tkt8e2w5rs
#   export TENANT_NAME=async-ha-demo
#   export PRIMARY_HOST=44.225.105.230
#   export PRIMARY_PORT=5432
#   export PRIMARY_USER=ubi_replication
#   export SLOT_NAME=walg_sync
#   export NODE_INSTANCE_TYPE=i8g.large
#   export IMAGE=ghcr.io/myorg/wal-g-receive:0.1.0
#   export IMAGE_PULL_POLICY=IfNotPresent
#   export CPU_REQUEST=500m  CPU_LIMIT=2
#   export MEMORY_REQUEST=256Mi  MEMORY_LIMIT=512Mi
#   export PARTIAL_DIR_SIZE_LIMIT=20Gi
#   export CLIENT_CRT_B64=$(base64 -w0 /path/to/client.crt)
#   export CLIENT_KEY_B64=$(base64 -w0 /path/to/client.key)
#   export SERVER_CA_CRT_B64=$(base64 -w0 /path/to/server-ca.crt)
#
#   ./render.sh                # all manifests in dependency order
#   ./render.sh configmap      # just the ConfigMap
#   ./render.sh secret         # just the Secret
#   ./render.sh statefulset    # just the StatefulSet
#
# Pipe to `kubectl apply -f -` to install.

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

# --- required params (deterministic naming + connection) ---
required=(
  UBID TENANT_NAME
  PRIMARY_HOST PRIMARY_PORT PRIMARY_USER SLOT_NAME
  NODE_INSTANCE_TYPE IMAGE IMAGE_PULL_POLICY
  CPU_REQUEST CPU_LIMIT MEMORY_REQUEST MEMORY_LIMIT
  PARTIAL_DIR_SIZE_LIMIT
  CLIENT_CRT_B64 CLIENT_KEY_B64 SERVER_CA_CRT_B64
)
for v in "${required[@]}"; do
  [ -n "${!v:-}" ] || { echo "render.sh: $v is required" >&2; exit 1; }
done

target="${1:-all}"

render_one() {
  # Whitelist exactly the placeholders we expect so we don't accidentally
  # expand random ${...} the YAML might contain in a comment or value.
  envsubst '
    ${UBID} ${TENANT_NAME}
    ${PRIMARY_HOST} ${PRIMARY_PORT} ${PRIMARY_USER}
    ${SLOT_NAME}
    ${NODE_INSTANCE_TYPE} ${IMAGE} ${IMAGE_PULL_POLICY}
    ${CPU_REQUEST} ${CPU_LIMIT} ${MEMORY_REQUEST} ${MEMORY_LIMIT}
    ${PARTIAL_DIR_SIZE_LIMIT}
    ${CLIENT_CRT_B64} ${CLIENT_KEY_B64} ${SERVER_CA_CRT_B64}
  ' < "$1"
}

case "$target" in
  configmap)   render_one "$HERE/configmap.template.yaml" ;;
  secret)      render_one "$HERE/secret.template.yaml" ;;
  statefulset) render_one "$HERE/statefulset.template.yaml" ;;
  all)
    render_one "$HERE/configmap.template.yaml"
    echo "---"
    render_one "$HERE/secret.template.yaml"
    echo "---"
    render_one "$HERE/statefulset.template.yaml"
    ;;
  *) echo "usage: $0 [configmap|secret|statefulset|all]" >&2; exit 1 ;;
esac
