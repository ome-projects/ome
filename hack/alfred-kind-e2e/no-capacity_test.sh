#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
valid='{"requestCount":0,"migrations":[],"watchComplete":true,"sourceUID":"source","samples":[{"timestamp":"2026-09-14T00:00:00Z","sourceReady":true,"sourceServing":true,"sourceRouted":true,"podUIDs":["source"],"drained":false,"maintenanceRequested":true,"occupantsPresent":true,"requestCount":0,"migrations":[],"scheduling":{"status":"Infeasible","reason":"NoFeasiblePlacement","provenance":"deployed-worker","validated":true,"requestID":"live-1","snapshotID":"snap-1"}},{"timestamp":"2026-09-14T00:00:16Z","sourceReady":true,"sourceServing":true,"sourceRouted":true,"podUIDs":["source"],"drained":false,"maintenanceRequested":true,"occupantsPresent":true,"requestCount":0,"migrations":[],"scheduling":{"status":"Infeasible","reason":"NoFeasiblePlacement","provenance":"deployed-worker","validated":true,"requestID":"live-2","snapshotID":"snap-2"}}]}'
valid="$(jq '.samples[].scheduling |= (.status="Unsupported" | .reason="Unsupported" | .placements=[])' <<<"${valid}")"
worker="$(jq -n '
  def node($name): {kind:"Node",metadata:{name:$name,labels:{"alfred-e2e/virtual":"true"}},status:{allocatable:{"nvidia.com/gpu":"8"}}};
  def pod($name;$node;$gpu): {kind:"Pod",metadata:{name:$name,uid:$name,namespace:"test",labels:{"alfred-e2e/capacity-blocker":"true"}},spec:{nodeName:$node,schedulerName:"alfred-default-scheduler",priority:0,nodeSelector:{"kubernetes.io/hostname":$node,"alfred-e2e/virtual":"true"},containers:[{name:"runner",resources:{requests:{"nvidia.com/gpu":$gpu}}}]},status:{phase:"Running"}};
  {validated:true,provenance:"deployed-worker",
   result:{decision:"Unsupported",reason:"Unsupported",requestID:"live-1",snapshotID:"snap-1"},
   request:{requestID:"live-1",snapshotID:"snap-1",migrationFromNode:"node-source",excludedNodes:["node-source"],
     sourcePods:[pod("source";"node-source";"1")],
     replacementPods:[pod("replacement";"";"1") | .spec.nodeSelector={"alfred-e2e/virtual":"true"}],
     clusterObjects:[node("node-source"),node("node-a"),node("node-b"),node("node-c"),
       pod("source";"node-source";"1"),pod("block-node-a";"node-a";"8"),pod("block-node-b";"node-b";"8"),pod("block-node-c";"node-c";"8")]}}')"
capacity_proof() { jq -e --arg uid source --arg source node-source -f "${dir}/no-capacity-proof.jq"; }
proof="$(capacity_proof <<<"${worker}")"
jq -e '.sourceUID=="source" and .replacementGPU==1 and
  .eligibleNodes==["node-a","node-b","node-c","node-source"] and
  (.destinations|length)==3 and all(.destinations[];.allocatableGPU==8 and .blockerGPU==8 and .unallocatedGPU==0)' <<<"${proof}" >/dev/null
for mutation in \
  '.request=null' \
  '.result.placements=[{}]' \
  '.result.reason="WorkerFailed"' \
  '.result.requestID="different"' \
  '.request.replacementPods[0].spec.containers[0].resources.requests["nvidia.com/gpu"]="2"' \
  '.request.excludedNodes=[]' \
  '.request.sourcePods[0].metadata.uid="wrong"' \
  '.request.clusterObjects[4].metadata.uid="wrong"' \
  '.request.clusterObjects[1].metadata.labels["alfred-e2e/virtual"]="false"' \
  '.request.clusterObjects[5].spec.containers[0].resources.requests["nvidia.com/gpu"]="7"' \
  '.request.clusterObjects[5].spec.containers[0].resources.requests["nvidia.com/gpu"]="bogus"' \
  '.request.clusterObjects[5].spec.priority=-1' \
  '.request.clusterObjects[5].spec.nodeName=""' \
  '.request.clusterObjects[5].status.phase="Pending"' \
  '.request.clusterObjects[5].metadata.deletionTimestamp="2026-09-14T00:00:00Z"'; do
  if jq "${mutation}" <<<"${worker}" | capacity_proof >/dev/null 2>&1; then
    echo "incorrectly accepted capacity proof ${mutation}" >&2; exit 1
  fi
done
jq '.result.decision="Infeasible" | .result.reason="NoFeasiblePlacement"' <<<"${worker}" | capacity_proof >/dev/null
valid="$(jq --argjson proof "${proof}" '.samples |= map(.capacityProof=($proof + {requestID:.scheduling.requestID,snapshotID:.scheduling.snapshotID}))' <<<"${valid}")"
jq -e -f "${dir}/verify-no-capacity.jq" <<<"${valid}" >/dev/null
for mutation in \
  '.requestCount=1' \
  '.migrations=[{}]' \
  '.watchComplete=false' \
  '.samples[0].requestCount=1' \
  '.samples[0].migrations=[{}]' \
  '.samples[0].maintenanceRequested=false' \
  '.samples[0].occupantsPresent=false' \
  '.samples[0].scheduling.provenance="dispatch-reason"' \
  '.samples[0].scheduling.validated=false' \
  '.samples[0].scheduling.requestID=""' \
  '.samples[0].sourceReady=false' \
  '.samples[1].sourceServing=false' \
  '.samples[1].sourceRouted=false' \
  '.samples[1].podUIDs=["replacement"]' \
  '.samples[1].drained=true' \
  '.samples[1].scheduling.status="Feasible"' \
  '.samples[1].scheduling.placements=[{}]' \
  '.samples[1].capacityProof=null' \
  '.samples[1].capacityProof.sourceUID="wrong"' \
  '.samples[1].capacityProof.snapshotID="wrong"' \
  '.samples[1].capacityProof.destinations[0].blockerGPU=7' \
  '.samples[1].capacityProof.destinations[0].blockerPriority=-1' \
  '.samples[1].scheduling.reason="WorkerFailed"' \
  '.samples[1].timestamp="2026-09-14T00:00:01Z"'; do
  if jq "${mutation}" <<<"${valid}" | jq -e -f "${dir}/verify-no-capacity.jq" >/dev/null; then
    echo "incorrectly accepted ${mutation}" >&2; exit 1
  fi
done
echo "no-capacity fail-closed proof and verifier tests passed"

tmp_dir="$(mktemp -d)"
trap 'rm -f "${tmp_dir}/kubeconfig"; rmdir "${tmp_dir}"' EXIT
reject() {
  local want="$1" output
  shift
  if output="$("$@" 2>&1)"; then echo 'unexpected runner success' >&2; exit 1; fi
  [[ "${output}" == *"${want}"* ]] || { echo "missing rejection ${want}: ${output}" >&2; exit 1; }
}
reject 'STATE_DIR must be an absolute directory' env -u STATE_DIR bash "${dir}/no-capacity.sh"
reject 'kubeconfig not found' env STATE_DIR="${tmp_dir}" bash "${dir}/no-capacity.sh"
touch "${tmp_dir}/kubeconfig"
reject 'timeouts must be integer seconds' env STATE_DIR="${tmp_dir}" ALFRED_E2E_DEADLINE_SECONDS=never bash "${dir}/no-capacity.sh"
reject 'baseline must be >=65s' env STATE_DIR="${tmp_dir}" ALFRED_E2E_MIN_SOURCE_AGE_SECONDS=0 bash "${dir}/no-capacity.sh"
reject 'observation must be >=15s' env STATE_DIR="${tmp_dir}" ALFRED_E2E_NO_CAPACITY_HOLD_SECONDS=1 bash "${dir}/no-capacity.sh"
echo 'no-capacity preflight tests passed'

check_watch() { jq -es --arg kind "$1" --arg uid source --arg key node.source -f "${dir}/no-capacity-watch.jq"; }
pod_event='{"type":"ADDED","object":{"metadata":{"uid":"source"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"ome.io/serving","status":"True"}]}}}'
check_watch pods <<<"${pod_event}" >/dev/null
if jq '.type="DELETED"' <<<"${pod_event}" | check_watch pods >/dev/null; then
  echo 'accepted a transient source deletion event' >&2; exit 1
fi
if check_watch pods </dev/null >/dev/null; then echo 'accepted empty watch' >&2; exit 1; fi
if printf '%s\n{"type":' "${pod_event}" | check_watch pods >/dev/null 2>&1; then
  echo 'accepted incomplete watch framing' >&2; exit 1
fi
echo 'no-capacity watch tests passed'
