#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf -- "${tmp_dir}"' EXIT
touch "${tmp_dir}/kubeconfig"

# Stop at the first Kubernetes call. Its argv must use the selected context,
# and invalid cluster names must be rejected before any Kubernetes operation.
kubectl() { printf '%s\n' "$@" >"${CALLS}"; return 1; }
export -f kubectl
export CALLS="${tmp_dir}/calls"
if STATE_DIR="${tmp_dir}" CLUSTER_NAME=alfred-e2e-review bash "${dir}/kwok.sh" verify >"${tmp_dir}/output" 2>&1; then
  echo 'KWOK unexpectedly bypassed the fake Kubernetes failure' >&2; exit 1
fi
grep -Fxq 'kind-alfred-e2e-review' "${CALLS}" || {
  echo 'KWOK ignored the selected cluster context' >&2; exit 1;
}
mv "${CALLS}" "${CALLS}.valid"
if STATE_DIR="${tmp_dir}" CLUSTER_NAME=production bash "${dir}/kwok.sh" verify >"${tmp_dir}/output" 2>&1; then
  echo 'KWOK accepted an unscoped cluster name' >&2; exit 1
fi
[[ ! -e "${CALLS}" ]] || { echo 'invalid cluster name reached kubectl' >&2; exit 1; }

# Guard the ordering of the real runner's completion check. This is a script
# contract test, not a substitute for the live defragmentation scenario.
awk '
  /if jq.*phase=="Completed"/ { completed=1; next }
  completed && /get pods .*current-pods.json/ { refreshed=1 }
  completed && /all\(.items\[\];.metadata.uid!=\$uid\)/ {
    if (!refreshed) exit 1
    checked=1
  }
  END { if (!checked) exit 1 }
' "${dir}/useful-defrag.sh" || {
  echo 'completion uses a pre-completion Pod snapshot' >&2; exit 1;
}
watch_timeout="$(sed -n 's/.*--watch --request-timeout=\([0-9]*\)s.*/\1/p' "${dir}/useful-defrag.sh")"
((watch_timeout > 120 + 180)) || { echo 'watch cannot cover Helm plus observation budget' >&2; exit 1; }
grep -Fq 'kill -0 "${watch_pid}"' "${dir}/useful-defrag.sh" || {
  echo 'runner does not detect premature watch exit' >&2; exit 1;
}
echo 'review regression checks passed'
