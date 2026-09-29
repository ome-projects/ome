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

# Exercise the raw completion contract: an IR Completed record cannot make an
# old held Pod snapshot pass. Three fresh cycle witnesses are additionally
# required by the full verifier; a source-text ordering regex cannot prove that.
baseline="$(jq -n -f "${dir}/useful-defrag-fixture.jq")"
jq -e -L "${dir}" -f "${dir}/verify-useful-defrag.jq" <<<"${baseline}" >/dev/null
if jq '.completionBaseline.pods=.held.pods | .completionBaseline.allPods=.held.allPods' <<<"${baseline}" |
  jq -e -L "${dir}" -f "${dir}/verify-useful-defrag.jq" >/dev/null; then
  echo 'completion accepted a pre-completion Pod snapshot' >&2; exit 1
fi
source "${dir}/namespace-churn-lib.sh"
watch_timeout="$(churn_watch_timeout_seconds 420)"
((watch_timeout > 120 + 240)) || { echo 'watch cannot cover Helm plus observation budget' >&2; exit 1; }
grep -Fq 'kill -0 "${watch_pid}"' "${dir}/useful-defrag.sh" || {
  echo 'runner does not detect premature watch exit' >&2; exit 1;
}
echo 'review regression checks passed'
