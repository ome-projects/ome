#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scenario="${script_dir}/tpu-defrag.sh"
fail() {
  echo "tpu-defrag test: $*" >&2
  exit 1
}

test -x "${scenario}" || fail "tpu-defrag.sh must be executable"
bash -n "${scenario}"
bash -n "${script_dir}/tpu-defrag-lib.sh"
# shellcheck source=tpu-defrag-lib.sh
source "${script_dir}/tpu-defrag-lib.sh"
nodes_json="$(printf '%s\n' "${TPU_NODES[@]}" | jq -R . | jq -sc .)"

# The TPU stages are the harness's own pod lifecycle, scoped to the TPU hosts
# only, so no GPU fixture changes behavior and the GPU health stages stay out.
stages="$(tpu_stages "${script_dir}/manifests/kwok-stages.yaml" | yq -o=json -I=0 '.')"
source_pods="$(yq -o=json -I=0 'select(.spec.resourceRef.kind == "Pod") | .metadata.name' \
  "${script_dir}/manifests/kwok-stages.yaml" | jq -sc 'map(. + "-tpu") | sort')"
jq -se --argjson nodes "${nodes_json}" --argjson pods "${source_pods}" --arg virtual "${TPU_NODE_LABEL}" '
  ([.[] | select(.spec.resourceRef.kind == "Pod") | .metadata.name] | sort) == $pods and
  ([.[] | select(.spec.resourceRef.kind == "Node") | .metadata.name] | sort) ==
    ["alfred-e2e-node-heartbeat-tpu", "alfred-e2e-node-initialize-tpu"] and
  all(.[]; .metadata.labels[$virtual] == "true") and
  all(.[]; [.spec.selector.matchExpressions[] | select(.key == ".metadata.name" or .key == ".spec.nodeName")] |
    length == 1 and .[0].values == $nodes)' <<<"${stages}" >/dev/null ||
  fail "TPU stages are not the pod lifecycle re-scoped to the TPU hosts"
grep -q 'alfred-kwok-gpu\|nvidia.com/gpu' <<<"${stages}" && fail "TPU stages still name GPU hosts or chips"
[[ "$(grep -o 'google.com/tpu: \\"4\\"' <<<"${stages}" | wc -l | tr -d ' ')" == 2 ]] ||
  fail "TPU hosts must report four chips as both capacity and allocatable"

# One TPU pod of eight healthy, provision-only hosts: two complete 2x2x4
# partitions of four hosts, four 2x2x2 of two, eight 2x2x1 of one. The hosts
# do not carry the GPU fixtures' virtual-node label.
tpu_nodes 172.18.0.2 | jq -e --arg virtual "${TPU_NODE_LABEL}" --arg pod "${TPU_POD}" '
  def sizes($shape): [.items[].metadata.labels["cloud.google.com/gke-tpu-partition-\($shape)-id"]] |
    group_by(.) | map(length);
  (.items | length) == 8 and
  sizes("2x2x4") == [4, 4] and sizes("2x2x2") == [2, 2, 2, 2] and sizes("2x2x1") == [1, 1, 1, 1, 1, 1, 1, 1] and
  all(.items[].metadata.labels; .[$virtual] == "true" and .["alfred-e2e/virtual"] == null and
    .["cloud.google.com/gce-topology-block"] == $pod and
    .["cloud.google.com/gke-accelerator-topology-mode"] == "PROVISION_ONLY" and
    ([to_entries[] | select(.key | endswith("-state")) | .value] | length == 3 and all(. == "HEALTHY"))) and
  ([.items[].metadata.annotations["alfred-e2e.ome.io/internal-ip"]] | unique | length) == 8' >/dev/null ||
  fail "TPU hosts do not form the expected partition tree"

# Cleanup must restore Alfred's configuration, which ends any open drain,
# before it deletes fixtures, and keep the provider running until OME has
# released every slice.
line_of() { grep -nF -- "$1" "${scenario}" | head -1 | cut -d: -f1; }
restore="$(line_of '--reset-values')"
delete_ns="$(line_of 'delete namespace "${ns}"')"
released="$(line_of 'no_slice_labels')"
stop_provider="$(line_of 'kill "${provider_pid}"')"
[[ -n "${restore}" && -n "${delete_ns}" && -n "${released}" && -n "${stop_provider}" ]] || fail "cleanup steps missing"
((restore < delete_ns && delete_ns < released && released < stop_provider)) || fail "cleanup steps out of order"
# drain_counts fails, rather than printing nothing, when the metrics cannot be
# read or carry no drain count, so two failed reads never compare equal.
metrics() { printf '%s\n' 'alfred_tpu_drains_total{outcome="completed"} 1' 'alfred_tpu_fenced_hosts 0' 'alfred_tpu_drains_total{outcome="aborted"} 0'; }
[[ "$(drain_counts)" == $'alfred_tpu_drains_total{outcome="aborted"} 0\nalfred_tpu_drains_total{outcome="completed"} 1' ]] ||
  fail "drain_counts must print every drain series, sorted"
metrics() { echo 'alfred_tpu_fenced_hosts 0'; }
! drain_counts >/dev/null || fail "drain_counts must fail when no drain series is present"
metrics() { return 1; }
! drain_counts >/dev/null || fail "drain_counts must fail when the metrics cannot be read"
echo "tpu-defrag test: ok"
