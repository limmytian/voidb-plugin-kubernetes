#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUN_ID=""
ROOT="${REPO_ROOT}/target/fixtures"
REPORT_PATH="${REPO_ROOT}/target/tmp/kubernetes-fixture-smoke-evidence.md"
IMAGE="kindest/node@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"
TIMEOUT=180
TAIL_LINES=120
PULL=0

usage() {
  cat <<'EOF'
Usage: scripts/kubernetes-fixture-smoke.sh [options]

Starts a disposable local kind fixture, exercises Kubernetes plugin
capabilities, writes redacted evidence, and tears the fixture down.

Options:
  --run-id <id>         Stable run id. Generated when omitted.
  --root <dir>          Fixture state root. Default: target/fixtures.
  --report <path>       Evidence report path. Default: target/tmp/kubernetes-fixture-smoke-evidence.md.
  --image <image>       kind node image for the fixture. Default: pinned kindest/node digest.
  --timeout <seconds>   Health wait timeout. Default: 180.
  --tail <lines>        Log lines to keep. Default: 120.
  --pull                Pull the fixture image when it is missing.
  -h, --help            Show this help.

The script prints resource names and variable names only. It does not print
kubeconfig paths, API server URLs, manifest bodies, or raw target diagnostics.
EOF
}

die() {
  echo "error: $*" >&2
  exit 2
}

generate_run_id() {
  printf 'k8s-cap-%s-%s\n' "$(date -u +%Y%m%dT%H%M%SZ)" "$$"
}

sanitize_id() {
  local value="$1"
  [[ "${value}" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid id: ${value}"
}

fixture_dir() {
  printf '%s/%s\n' "${ROOT}" "${RUN_ID}"
}

env_path() {
  printf '%s/kubernetes.env\n' "$(fixture_dir)"
}

log_path() {
  printf '%s/kubernetes.log\n' "$(fixture_dir)"
}

kubernetes_safe_id() {
  local safe
  safe="$(printf '%s' "${RUN_ID}" \
    | tr '[:upper:]_' '[:lower:]-' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-28 \
    | sed -E 's/-+$//')"
  if [[ -z "${safe}" ]]; then
    safe="run"
  fi
  printf '%s\n' "${safe}"
}

cluster_name() {
  printf 'voidb-k8s-%s\n' "$(kubernetes_safe_id)"
}

container_name() {
  printf '%s-control-plane\n' "$(cluster_name)"
}

cleanup_fixture() {
  "${SCRIPT_DIR}/local-fixture-smoke.sh" teardown \
    --fixture kubernetes \
    --run-id "${RUN_ID}" \
    --root "${ROOT}" >/dev/null 2>&1 || true
}

write_evidence() {
  local status="$1"
  local cleanup_status="$2"
  local smoke_log="$3"
  local commit
  commit="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo "unknown")"
  local docker_version
  docker_version="$(docker version --format 'client={{.Client.Version}} server={{.Server.Version}}' 2>/dev/null || echo "unknown")"
  local kind_version
  kind_version="$(kind version 2>/dev/null || echo "unknown")"
  local kubectl_version
  kubectl_version="$(kubectl version --client=true 2>/dev/null | head -n 1 || echo "unknown")"

  mkdir -p "$(dirname "${REPORT_PATH}")"
  cat > "${REPORT_PATH}" <<EOF
# Kubernetes Fixture Capability Smoke Evidence

- Requirement: 67 - Promote Kubernetes fixture-backed readiness
- Fixture: kubernetes
- Run id: ${RUN_ID}
- Commit: ${commit}
- Status: ${status}
- Platform: $(uname -s)-$(uname -m)
- Docker: ${docker_version}
- kind: ${kind_version}
- kubectl: ${kubectl_version}
- Image: ${IMAGE}
- Cluster: $(cluster_name)
- Container: $(container_name)
- Network: kind
- Health: kind cluster node is Ready and scratch namespace/configmap exist
- Capabilities exercised: kubernetes.diagnostics, kubernetes.contexts, kubernetes.namespaces, kubernetes.list, kubernetes.get_yaml, kubernetes.apply, kubernetes.delete, kubernetes.scale dry-run, kubernetes.restart dry-run, kubernetes.logs error path
- Destructive policy: apply/delete/scale/restart dry-run succeeded without a live target; acknowledged apply/delete touched only generated configmaps inside the scratch namespace
- Resource coverage: paged namespace list, bounded node list, pod list scoped to the scratch namespace, configmap list, bounded configmap YAML, secret YAML policy block, missing-pod target error, and unavailable-target redaction
- Redaction: diagnostics and target errors checked for withheld kubeconfig path/API server; fixture logs captured through redaction filter
- Variables present by name: VOIDB_FIXTURE_RUN_ID, VOIDB_FIXTURE_NAME, VOIDB_FIXTURE_CONTAINER, VOIDB_FIXTURE_NETWORK, VOIDB_FIXTURE_IMAGE, VOIDB_FIXTURE_HOST, VOIDB_FIXTURE_PORT, VOIDB_K8S_SMOKE_PROFILE, VOIDB_K8S_SMOKE_CONNECTION, VOIDB_K8S_SMOKE_CLUSTER, VOIDB_K8S_SMOKE_CONTEXT, VOIDB_K8S_SMOKE_NAMESPACE, VOIDB_K8S_SMOKE_CONFIGMAP, VOIDB_K8S_SMOKE_KUBECONFIG, VOIDB_K8S_SMOKE_API_SERVER
- Log capture: $(log_path)
- Capability log: ${smoke_log}
- Cleanup: ${cleanup_status}
- Release decision: capability smoke passed; readiness docs pending
EOF

  echo "report: ${REPORT_PATH}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      [[ $# -ge 2 ]] || die "missing value for --run-id"
      RUN_ID="$2"
      shift 2
      ;;
    --root)
      [[ $# -ge 2 ]] || die "missing value for --root"
      ROOT="$2"
      shift 2
      ;;
    --report)
      [[ $# -ge 2 ]] || die "missing value for --report"
      REPORT_PATH="$2"
      shift 2
      ;;
    --image)
      [[ $# -ge 2 ]] || die "missing value for --image"
      IMAGE="$2"
      shift 2
      ;;
    --timeout)
      [[ $# -ge 2 ]] || die "missing value for --timeout"
      TIMEOUT="$2"
      [[ "${TIMEOUT}" =~ ^[0-9]+$ ]] || die "timeout must be numeric"
      shift 2
      ;;
    --tail)
      [[ $# -ge 2 ]] || die "missing value for --tail"
      TAIL_LINES="$2"
      [[ "${TAIL_LINES}" =~ ^[0-9]+$ ]] || die "tail must be numeric"
      shift 2
      ;;
    --pull)
      PULL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="$(generate_run_id)"
fi
sanitize_id "${RUN_ID}"

START_ARGS=(
  --fixture kubernetes
  --run-id "${RUN_ID}"
  --root "${ROOT}"
  --image "${IMAGE}"
  --timeout "${TIMEOUT}"
  --tail "${TAIL_LINES}"
)
if [[ "${PULL}" -eq 1 ]]; then
  START_ARGS+=(--pull)
fi

mkdir -p "$(fixture_dir)"
cleanup_status="not_run"
smoke_log="$(fixture_dir)/kubernetes-capability-smoke.log"
trap 'cleanup_fixture' EXIT INT TERM ERR

"${SCRIPT_DIR}/local-fixture-smoke.sh" start "${START_ARGS[@]}"
"${SCRIPT_DIR}/local-fixture-smoke.sh" wait "${START_ARGS[@]}"

set -a
# shellcheck disable=SC1090
source "$(env_path)"
set +a

(
  cd "${REPO_ROOT}"
  cargo run -p voidb-plugin-kubernetes --example kubernetes_fixture_smoke --quiet
) > "${smoke_log}" 2>&1

"${SCRIPT_DIR}/local-fixture-smoke.sh" logs "${START_ARGS[@]}"
cleanup_fixture
cleanup_status="removed kind cluster, generated kubeconfig, seed manifest, and env file"
trap - EXIT INT TERM ERR

write_evidence "passed" "${cleanup_status}" "${smoke_log}"
