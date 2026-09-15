#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -f "${tmp_dir}/kubeconfig"; rmdir "${tmp_dir}"' EXIT
reject() {
  local want="$1" output
  shift
  if output="$("$@" 2>&1)"; then
    echo "unexpected gang runner success" >&2; exit 1
  fi
  if [[ "${output}" != *"${want}"* ]]; then
    echo "missing expected rejection ${want}: ${output}" >&2; exit 1
  fi
}
reject 'STATE_DIR must be an absolute directory' env -u STATE_DIR bash "${dir}/gang.sh"
reject 'STATE_DIR must be an absolute directory' env STATE_DIR=relative bash "${dir}/gang.sh"
reject 'kubeconfig not found' env STATE_DIR="${tmp_dir}" bash "${dir}/gang.sh"
touch "${tmp_dir}/kubeconfig"
reject 'timeouts must be integer seconds' env STATE_DIR="${tmp_dir}" ALFRED_E2E_DEADLINE_SECONDS=never bash "${dir}/gang.sh"
reject 'surge hold must be >=2s' env STATE_DIR="${tmp_dir}" ALFRED_E2E_SURGE_HOLD_SECONDS=0 bash "${dir}/gang.sh"
echo 'gang preflight tests passed'

# Hand-constructed Kubernetes API objects: check extraction, not fabricated
# controller status. Live acceptance still requires the real cluster runner.
pods='{"items":[{"metadata":{"name":"leader","uid":"uid-leader","labels":{"ome.io/instance-index":"0","ome.io/runner":"leader","scheduling.x-k8s.io/pod-group":"gang-engine-0"}},"spec":{"nodeName":"node-a","schedulerName":"ome-scheduler","containers":[{"resources":{"requests":{"nvidia.com/gpu":"8"}}}]},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"ome.io/serving","status":"True"}]}},{"metadata":{"name":"worker","uid":"uid-worker","labels":{"ome.io/instance-index":"0","ome.io/runner":"worker","scheduling.x-k8s.io/pod-group":"gang-engine-0"}},"spec":{"nodeName":"node-b","schedulerName":"ome-scheduler","containers":[{"resources":{"requests":{"nvidia.com/gpu":"8"}}}]},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"ome.io/serving","status":"True"}]}}]}'
nodes='{"items":[{"metadata":{"name":"node-a","labels":{"topology.kubernetes.io/zone":"zone-a"}}},{"metadata":{"name":"node-b","labels":{"topology.kubernetes.io/zone":"zone-a"}}}]}'
groups='{"items":[{"metadata":{"name":"gang-engine-0","annotations":{"ome.io/topology-key":"topology.kubernetes.io/zone"}},"spec":{"minMember":2}}]}'
endpoints='{"items":[{"endpoints":[{"targetRef":{"uid":"uid-leader"},"conditions":{"ready":true}}]}]}'
snapshot() {
  jq -cn --argjson pods "${pods}" --argjson nodes "${nodes}" --argjson groups "${groups}" --argjson endpoints "${endpoints}" --argjson instance "$1" -f "${dir}/gang-snapshot.jq"
}
snapshot 0 | jq -e '.instance == 0 and .zone == "zone-a" and .podGroup.minMember == 2 and .podGroup.topologyKey == "topology.kubernetes.io/zone" and .leaderEndpointReady and (.pods|map(.uid)) == ["uid-leader","uid-worker"] and all(.pods[]; .gpuRequest == 8 and .ready and .serving and .scheduler == "ome-scheduler")' >/dev/null
snapshot 1 | jq -e '.instance == 1 and .pods == [] and .podGroup == {} and (.leaderEndpointReady|not)' >/dev/null
pods="$(jq '.items[0].metadata.deletionTimestamp="2026-09-15T00:00:00Z"' <<<"${pods}")"
snapshot 0 | jq -e '.pods[0].terminating == true' >/dev/null
pods="$(jq 'del(.items[0].metadata.deletionTimestamp)' <<<"${pods}")"
snapshot 0 | jq -e 'all(.pods[]; .terminating == false)' >/dev/null
endpoints="$(jq '.items[0].endpoints[0].conditions.terminating=true' <<<"${endpoints}")"
snapshot 0 | jq -e '(.leaderEndpointReady|not)' >/dev/null
endpoints='{"items":[{"endpoints":[{"targetRef":{"uid":"uid-worker"},"conditions":{"ready":true}}]}]}'
snapshot 0 | jq -e '(.leaderEndpointReady|not)' >/dev/null
pods="$(jq '.items[1].status.conditions = [] | .items[1].spec.nodeName = "missing-node"' <<<"${pods}")"
snapshot 0 | jq -e '.pods[1].ready == false and .pods[1].serving == false and .pods[1].zone == ""' >/dev/null
echo 'gang API snapshot extraction tests passed'
