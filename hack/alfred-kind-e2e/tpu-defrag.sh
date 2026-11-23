#!/usr/bin/env bash
# TPU partition defragmentation (OEP-0008.1). Two single-host Instances leave
# one busy host in each of a virtual TPU pod's two 2x2x4 partitions. With the
# policy enabled, Alfred must fence one partition, migrate its Instance through
# the real OME controllers onto a new slice placed outside the fence, release
# the emptied partition, and then stop: one whole free 2x2x4 is all the pod can
# hold.
set -euo pipefail
: "${STATE_DIR:?STATE_DIR is required}"
[[ "${STATE_DIR}" == /* && -f "${STATE_DIR}/kubeconfig" ]] || exit 2
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${dir}/../.." && pwd)"
source "${dir}/tpu-defrag-lib.sh"
k=(kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e --request-timeout=15s)
h=(helm --kubeconfig "${STATE_DIR}/kubeconfig" --kube-context kind-alfred-e2e)
ns=alfred-e2e-tpu
runtime=alfred-e2e-tpu-runtime
fence_key=alfred.ome.io/cordoned-for-defragmentation
art="${STATE_DIR}/artifacts/tpu-defrag-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "${art}"
passed=false
nodes_created=false
cordoned=()
configured=false
provider_pid=''
watch_pid=''

cleanup() {
  local rc=$?
  if [[ -n "${watch_pid}" ]]; then kill "${watch_pid}" 2>/dev/null || true; wait "${watch_pid}" 2>/dev/null || true; fi
  "${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o json >"${art}/final-nodes.json" 2>/dev/null || true
  "${k[@]}" get slices.accelerator.gke.io -o json >"${art}/final-slices.json" 2>/dev/null || true
  "${k[@]}" -n "${ns}" get pods,inferenceservices.ome.io,inferencereplicas.ome.io -o json >"${art}/final-objects.json" 2>/dev/null || true
  "${k[@]}" -n ome get cm alfred-recommendations alfred-dispatch-state -o json >"${art}/alfred-final.json" 2>/dev/null || true
  "${k[@]}" -n ome logs deployment/ome-alfred --tail=-1 >"${art}/alfred.log" 2>/dev/null || true
  # Disabling the policy first makes the fencer end any open drain, so no
  # fence outlives the run.
  if [[ "${configured}" == true ]]; then
    "${h[@]}" upgrade ome-alfred "${repo}/charts/ome-alfred" -n ome --reset-values \
      -f "${art}/original-values.json" --wait --timeout=120s >"${art}/restore-config.log" 2>&1 ||
      { echo 'restoring the Alfred configuration failed; retaining fixtures' >&2; exit 1; }
  fi
  if ((${#cordoned[@]} > 0)); then "${k[@]}" uncordon "${cordoned[@]}" >/dev/null || rc=1; fi
  if [[ "${passed}" == true && "${rc}" == 0 ]]; then
    {
      tpu_wait 120 'every fence to end' no_fences &&
        "${k[@]}" delete namespace "${ns}" --wait=true --timeout=120s &&
        tpu_wait 120 'OME to release every slice' no_slices &&
        tpu_wait 60 'the provider to clear every slice label' no_slice_labels &&
        "${k[@]}" delete clusterservingruntime "${runtime}" --wait=true --timeout=60s &&
        "${k[@]}" delete node "${TPU_NODES[@]}" --wait=true --timeout=60s &&
        "${k[@]}" delete stages.kwok.x-k8s.io -l "${TPU_NODE_LABEL}=true" --wait=true --timeout=60s
    } >>"${art}/cleanup.log" 2>&1 || rc=1
  else
    ((rc != 0)) || rc=1
    echo 'Retaining the failed TPU fixture and raw evidence' >&2
  fi
  if [[ -n "${provider_pid}" ]]; then kill "${provider_pid}" 2>/dev/null || true; wait "${provider_pid}" 2>/dev/null || true; fi
  echo "tpu-defrag exit=${rc}; evidence: ${art}"
  exit "${rc}"
}
trap cleanup EXIT

[[ "$("${k[@]}" get node alfred-e2e-control-plane -o jsonpath='{.metadata.labels.alfred-e2e/infrastructure}')" == true ]]
if "${k[@]}" get namespace "${ns}" >/dev/null 2>&1 || "${k[@]}" get clusterservingruntime "${runtime}" >/dev/null 2>&1 ||
  [[ -n "$("${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o name)" ]]; then
  echo 'TPU fixture already exists' >&2; exit 1
fi
"${k[@]}" wait --for=condition=Established crd/slices.accelerator.gke.io --timeout=10s >/dev/null
"${k[@]}" get slices.accelerator.gke.io -o json | jq -e '.items|length == 0' >/dev/null
"${h[@]}" get values ome-alfred -n ome -o json >"${art}/original-values.json"
jq -e '.alfredConfig.policies.tpuDefragmentation.enabled == false and .alfredConfig.tpuSliceMigrationEnabled == true and
  .alfredConfig.mode == "execute" and .defragmentationFence.enabled == true' "${art}/original-values.json" >/dev/null ||
  { echo 'Alfred is not deployed with the harness TPU settings; re-run deploy.sh' >&2; exit 1; }

echo 'Starting the stand-in slice provider'
go build -C "${repo}" -o "${STATE_DIR}/slice-provider" ./hack/alfred-kind-e2e/slice-provider
"${STATE_DIR}/slice-provider" --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e \
  --node-selector "${TPU_NODE_LABEL}=true" >"${art}/slice-provider.log" 2>&1 &
provider_pid=$!

echo "Creating ${#TPU_NODES[@]} virtual TPU7x hosts in TPU pod ${TPU_POD}"
tpu_stages "${dir}/manifests/kwok-stages.yaml" >"${art}/tpu-stages.yaml"
"${k[@]}" apply -f "${art}/tpu-stages.yaml" >/dev/null
infra_ip="$("${k[@]}" get node alfred-e2e-control-plane -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
nodes_created=true
tpu_nodes "${infra_ip}" | "${k[@]}" apply -f - >/dev/null
"${k[@]}" wait --for=condition=Ready "${TPU_NODES[@]/#/node/}" --timeout=120s >/dev/null
"${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o json >"${art}/nodes-created.json"
jq -e 'all(.items[]; .status.allocatable["google.com/tpu"] == "4")' "${art}/nodes-created.json" >/dev/null

echo 'Creating tpu-a, then tpu-b with the rest of its partition cordoned'
"${k[@]}" create namespace "${ns}" >/dev/null
"${k[@]}" create --dry-run=client -f "${dir}/manifests/workload-tpu.yaml" -o json | jq -s '{apiVersion:"v1",kind:"List",items:.}' >"${art}/fixture.json"
"${k[@]}" apply -f "${art}/fixture.json" >/dev/null
tpu_wait 120 'tpu-a to become ready' ready_pod tpu-a
read -r a_pod a_node a_uid <<<"$(ready_pod tpu-a)"
[[ "${a_node}" == "${TPU_NODES[0]}" ]] || { echo "tpu-a landed on ${a_node}, want ${TPU_NODES[0]}" >&2; exit 1; }
# Without this harness cordon, best fit packs tpu-b next to tpu-a. The cordon
# stands in for the churn that fragments a real pool.
cordoned=("${TPU_NODES[@]:1:3}")
"${k[@]}" cordon "${cordoned[@]}" >/dev/null
jq '.items[] | select(.kind == "InferenceService") | .metadata.name = "tpu-b"' "${art}/fixture.json" | "${k[@]}" apply -f - >/dev/null
tpu_wait 120 'tpu-b to become ready' ready_pod tpu-b
read -r b_pod b_node b_uid <<<"$(ready_pod tpu-b)"
[[ "${b_node}" == "${TPU_NODES[4]}" ]] || { echo "tpu-b landed on ${b_node}, want ${TPU_NODES[4]}" >&2; exit 1; }
"${k[@]}" uncordon "${cordoned[@]}" >/dev/null
cordoned=()
capture before
jq -e --arg a "${a_node}" --arg b "${b_node}" '[.nodes.items[] | select(.metadata.labels["cloud.google.com/gke-tpu-slice"] != null) | .metadata.name] | sort == ([$a, $b] | sort)' \
  "${art}/before.json" >/dev/null || { echo 'unexpected slice layout before the drain' >&2; exit 1; }

"${k[@]}" -n "${ns}" get inferenceservices --watch --request-timeout=900s -o json \
  >"${art}/isvc-watch.jsonl" 2>"${art}/isvc-watch.stderr" &
watch_pid=$!

echo 'Enabling TPU defragmentation (target 2x2x4) in an always-open window'
configured=true
"${h[@]}" upgrade ome-alfred "${repo}/charts/ome-alfred" -n ome --reuse-values \
  --set alfredConfig.policies.tpuDefragmentation.enabled=true \
  --set-json 'alfredConfig.maintenanceWindows=[{"days":["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],"start":"00:00","end":"23:59"}]' \
  --wait --timeout=120s >"${art}/enable.log" 2>&1
"${k[@]}" -n ome rollout status deployment/ome-alfred --timeout=180s >>"${art}/enable.log" 2>&1

echo 'Waiting for Alfred to fence the partition holding tpu-a'
want_fence="$(jq -cn --argjson n "$(printf '%s\n' "${TPU_NODES[@]:0:4}" | jq -R . | jq -sc .)" '$n')"
tpu_wait 180 'the fence on hosts 0-3' fence_is "${want_fence}"
capture fenced
jq -e --arg key "${fence_key}" '[.nodes.items[] | select(.metadata.annotations[$key] != null) |
  .metadata.annotations[$key] | fromjson | .drain] | unique | length == 1' "${art}/fenced.json" >/dev/null

echo 'Waiting for the migration request and its completion'
tpu_wait 180 'one migration request on tpu-a' request_uuid tpu-a
uuid="$(request_uuid tpu-a)"
settle="$(jq -er '.alfredConfig.policies.tpuDefragmentation.fenceSettleSeconds' "${art}/original-values.json")"
request_after_settle tpu-a "${uuid}" "${a_node}" "${settle}" ||
  { echo 'the move was requested before the fence settled, or names the wrong source or reason' >&2; exit 1; }
tpu_wait 300 'OME to complete the migration' migration_completed tpu-a "${uuid}"
tpu_wait 60 'the replacement to be the only ready tpu-a pod' replaced tpu-a "${a_uid}"
read -r r_pod r_node r_uid <<<"$(ready_pod tpu-a)"
case " ${TPU_NODES[*]:0:4} " in
  *" ${r_node} "*) echo "replacement ${r_pod} landed inside the fence on ${r_node}" >&2; exit 1 ;;
esac

echo 'Waiting for Alfred to release the emptied partition'
tpu_wait 120 'every fence to end' no_fences
capture released
tpu_wait 60 'one completed drain in the metrics' metric_is 'alfred_tpu_drains_total{outcome="completed"}' 1
tpu_wait 60 'one whole free 2x2x4 in the metrics' metric_is "alfred_tpu_free_partitions{shape=\"2x2x4\",tpu_pod=\"${TPU_POD}\"}" 1
metrics >"${art}/metrics-released.txt"

echo 'Checking that three more decision cycles fence and move nothing'
drains="$(drain_counts)" || { echo 'reading the drain counts failed' >&2; exit 1; }
cycles_after "$(last_cycle)" 3
capture settled
[[ "$(fence_list)" == '[]' ]] || { echo 'a second drain was fenced' >&2; exit 1; }
# A drain that opened and ended between two checks leaves no fence, only a count.
after="$(drain_counts)" || { echo 'reading the drain counts failed' >&2; exit 1; }
[[ "${after}" == "${drains}" ]] || { echo 'a drain opened and ended during the quiet cycles' >&2; exit 1; }
read -r b_pod2 b_node2 b_uid2 <<<"$(ready_pod tpu-b)"
[[ "${b_uid2}" == "${b_uid}" && "${b_node2}" == "${b_node}" ]] || { echo 'tpu-b was disturbed' >&2; exit 1; }
# A watch that ended early would undercount the requests.
kill -0 "${watch_pid}" 2>/dev/null || { echo 'the migration request watch ended early' >&2; exit 1; }
[[ "$(request_count)" == 1 ]] || { echo "observed $(request_count) migration requests, want 1" >&2; exit 1; }
metric_is 'alfred_tpu_drains_total{outcome="completed"}' 1

jq -n --arg uuid "${uuid}" --arg source "${a_pod}@${a_node}" --arg replacement "${r_pod}@${r_node}" \
  --arg untouched "${b_pod}@${b_node}" --argjson fence "${want_fence}" \
  --slurpfile fenced "${art}/fenced.json" --arg key "${fence_key}" \
  '{requestUUID:$uuid, source:$source, replacement:$replacement, untouched:$untouched, fencedHosts:$fence,
    drain:([$fenced[0].nodes.items[] | select(.metadata.annotations[$key] != null) | .metadata.annotations[$key] | fromjson][0])}' \
  >"${art}/evidence.json"
cat "${art}/evidence.json"
passed=true
