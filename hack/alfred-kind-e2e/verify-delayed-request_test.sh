#!/usr/bin/env bash
set -euo pipefail
dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
filter="$dir/verify-delayed-request.jq"
# These literals model public API evidence, not controller-generated fixtures.
valid=$(jq -cn '
  def node($name;$cordon): {metadata:{name:$name,uid:($name+"-uid"),labels:{"alfred-e2e/virtual":"true","nvidia.com/gpu.product":"H100"}},spec:{unschedulable:$cordon},status:{allocatable:{"nvidia.com/gpu":"8"},conditions:[{type:"Ready",status:"True"}]}};
  {scenario:"hint-exhaustion-single",sourceNode:"alfred-kwok-gpu-a",fallback:"alfred-kwok-gpu-d",
   sourceUID:"source-uid",elapsedSinceRequestSeconds:10,
   request:{uuid:"123e4567-e89b-42d3-a456-426614174000",annotationKey:"ome.io/migration-request-v1-123e4567-e89b-42d3-a456-426614174000",payload:{schemaVersion:"v1",component:"engine",instance:0,from_node:"alfred-kwok-gpu-a",hint_target_nodes:["alfred-kwok-gpu-b","alfred-kwok-gpu-c"],requested_by:"alfred",reason:"NodeMaintenance",requested_at:"2026-09-15T01:00:00Z"}},
   pausedISVC:{metadata:{name:"single",namespace:"alfred-e2e",uid:"owner-uid",annotations:{}}},
   pausedIR:{metadata:{uid:"ir-uid",ownerReferences:[{kind:"InferenceService",uid:"owner-uid",controller:true}]},status:{migrations:[]}},
   mutationIR:{metadata:{uid:"ir-uid"},status:{migrations:[]}},
   journal:{uuid:"123e4567-e89b-42d3-a456-426614174000",workloadUID:"owner-uid",irUID:"ir-uid",phase:"submitted",payload:""},
   fallbackBefore:node("alfred-kwok-gpu-d";true),fallbackAfter:node("alfred-kwok-gpu-d";false),
   sourcePod:{metadata:{uid:"source-uid"},spec:{nodeName:"alfred-kwok-gpu-a"},status:{conditions:[{type:"Ready",status:"True"},{type:"ome.io/serving",status:"True"}]}},
   managerPause:{originalArgs:["--leader-elect","--webhook"],pausedArgs:["--leader-elect","--webhook","--enable-inferencereplica-controller=false"],oldPodUIDs:["old-manager"],pods:{items:[{metadata:{name:"new-manager",uid:"new-manager-uid"},spec:{containers:[{name:"manager",args:["--leader-elect","--webhook","--enable-inferencereplica-controller=false"]}]}}]},lease:{spec:{holderIdentity:"new-manager_abc"}},renewedLease:{spec:{holderIdentity:"new-manager_abc",renewTime:"2026-09-15T01:00:05Z"}},webhookReady:true},
   targets:(["alfred-kwok-gpu-b","alfred-kwok-gpu-c"] | . as $names |
   {nodes:[$names[] | node(.;false)],pods:{items:[$names[] | . as $n | {metadata:{name:("block-"+$n),uid:("block-"+$n+"-uid"),labels:{"alfred-e2e.ome.io/delayed-request":"run-1"}},spec:{nodeName:$n,schedulerName:"alfred-default-scheduler",containers:[{resources:{requests:{"nvidia.com/gpu":"8"},limits:{"nvidia.com/gpu":"8"}}}]},status:{phase:"Running",conditions:[{type:"PodScheduled",status:"True"}]}}]},blockerUIDs:[$names[] | "block-"+.+"-uid"]}),runID:"run-1"}
  | .pausedISVC.metadata.annotations[.request.annotationKey] = (.request.payload|tojson)
  | .journal.payload=(.request.payload|tojson)
  | .originalNodes={items:(.targets.nodes + [.fallbackBefore])}
  | .managerPause.pods.items[0].status={conditions:[{type:"Ready",status:"True"}]}
  | .managerPause.lease.spec.renewTime="2026-09-15T01:00:01Z"')
accept() { jq -e -f "$filter" <<<"$1" >/dev/null; }
reject() {
  local label=$1 mutation=$2
  if accept "$(jq "$mutation" <<<"$valid")"; then
    echo "FAIL: accepted $label" >&2; exit 1
  fi
}
accept "$valid" || { echo 'FAIL: valid full hinted-node capacity proof rejected' >&2; exit 1; }
reject 'prepared but not submitted journal' '.journal.phase="prepared"'
reject 'acknowledged before consumption barrier' '.journal.phase="acknowledged"'
reject 'submitted journal already carries acknowledgement' '.journal.acknowledgedAt="2026-09-15T01:00:03Z"'
reject 'missing real annotation' '.pausedISVC.metadata.annotations={}'
reject 'different annotation payload' '.pausedISVC.metadata.annotations[.request.annotationKey]="{}"'
reject 'second real UUID' '.pausedISVC.metadata.annotations["ome.io/migration-request-v1-another"]="{}"'
reject 'prior controller acceptance' '.pausedIR.status.migrations=[{requestUUID:.request.uuid}]'
reject 'acceptance before mutation barrier' '.mutationIR.status.migrations=[{requestUUID:.request.uuid}]'
reject 'hinted fallback' '.fallback=.request.payload.hint_target_nodes[0]'
reject 'still-cordoned capacity fallback' '.fallbackAfter.spec.unschedulable=true'
reject 'only one full hinted node' '.targets.pods.items[1].spec.containers[0].resources.requests["nvidia.com/gpu"]="7"'
reject 'GPU init allocation cannot be silently ignored' '.targets.pods.items[0].spec.initContainers=[{resources:{requests:{"nvidia.com/gpu":"8"}}}]'
reject 'hinted node identity changed' '.targets.nodes[0].metadata.uid="recreated-node"'
reject 'blocker not scheduled by real standard scheduler' '.targets.pods.items[0].status.conditions=[]'
reject 'unowned blocker evidence' '.targets.pods.items[0].metadata.labels={}'
reject 'no old manager deletion barrier' '.managerPause.pods.items[0].metadata.uid="old-manager"'
reject 'manager remained enabled' '.managerPause.pods.items[0].spec.containers[0].args=["--webhook"]'
reject 'manager not Ready at barrier' '.managerPause.pods.items[0].status.conditions=[]'
reject 'webhook not available' '.managerPause.webhookReady=false'
reject 'leadership never renewed' '.managerPause.renewedLease.spec.renewTime=.managerPause.lease.spec.renewTime'
reject 'request exceeded intentional pause budget' '.elapsedSinceRequestSeconds=180'
health=$(jq '.scenario="target-health-race-single" | .fallbackAfter.spec.unschedulable=true |
  .targets.blockerUIDs=[] | .targets.pods.items=[] |
  .targets.nodes |= map(.metadata.annotations["alfred-e2e.ome.io/gpu-health"]="unhealthy" |
    .status.conditions += [{type:"GpuUnhealthy",status:"True",reason:"AlfredE2EGpuUnhealthy",lastTransitionTime:"2026-09-15T01:00:02Z"}])' <<<"$valid")
accept "$health" || { echo 'FAIL: valid unhealthy hinted-node characterization rejected' >&2; exit 1; }
health_reject() {
  if accept "$(jq "$2" <<<"$health")"; then echo "FAIL: accepted $1" >&2; exit 1; fi
}
health_reject 'health transition predates real request' '.targets.nodes[0].status.conditions[-1].lastTransitionTime="2026-09-15T00:59:59Z"'
health_reject 'invalid health transition timestamp' '.targets.nodes[0].status.conditions[-1].lastTransitionTime="invalid"'
health_reject 'preexisting unhealthy condition' '.originalNodes.items[0].status.conditions += [{type:"GpuUnhealthy",status:"True",lastTransitionTime:"2026-09-15T00:55:00Z"}]'
health_reject 'same unhealthy transition replayed' '.originalNodes.items[0].status.conditions += [.targets.nodes[0].status.conditions[-1]]'
health_reject 'missing KWOK probe input' '.targets.nodes[0].metadata.annotations={}'
health_reject 'wrong condition producer reason' '.targets.nodes[0].status.conditions[-1].reason="UnrelatedGpuFailure"'
if accept "$(jq '.targets.nodes[1].status.conditions |= map(select(.type != "GpuUnhealthy"))' <<<"$health")"; then
  echo 'FAIL: accepted one unobserved hinted-node health condition' >&2; exit 1
fi
if accept "$(jq '.fallbackAfter.spec.unschedulable=false' <<<"$health")"; then
  echo 'FAIL: accepted eligible fallback in sole-unhealthy-target characterization' >&2; exit 1
fi
echo 'delayed-request verifier: positive proofs and 30 rejection cases passed'
helper="$dir/delayed-request.sh"
bash -c 'set -u; source "$1"; kube=(kubectl --context production --kubeconfig /tmp/no-config);
  namespace=alfred-e2e; if delayed_request_prepare; then exit 1; fi;
  delayed_request_cleanup; delayed_request_cleanup' bash "$helper"
gpu=$(bash -c 'source "$1"; _delayed_request_allocated_gpu "$2" node-h' bash "$helper" \
  '{"items":[{"spec":{"nodeName":"node-h","containers":[{"resources":{"requests":{"nvidia.com/gpu":"8"}}}]},"status":{"phase":"Running"}},{"spec":{"nodeName":"node-h","containers":[{"resources":{"requests":{"nvidia.com/gpu":"8"}}}]},"status":{"phase":"Succeeded"}}]}')
[[ "$gpu" == 8 ]] || { echo 'FAIL: allocation did not ignore terminal Pods' >&2; exit 1; }
echo 'delayed-request helper: scope rejection, idempotent unused cleanup, allocation tests passed'
# At the API boundary, simulate name recreation after a successful GET. The
# actual helper must send DeleteOptions.preconditions.uid and surface conflict;
# a name-only DELETE would erase the replacement and make this test fail.
bash -c '
  set -u; source "$1"; namespace=alfred-e2e; remote_uid=recreated-uid
  declare -F _delayed_request_delete_blocker >/dev/null || { echo "FAIL: missing UID-fenced delete helper" >&2; exit 1; }
  fake_api() {
    local arg raw="" body
    for arg in "$@"; do case "$arg" in --raw=*) raw=${arg#--raw=};; esac; done
    if [[ -n "$raw" ]]; then
      body=$(< /dev/stdin)
      [[ "$raw" == /api/v1/namespaces/alfred-e2e/pods/test-blocker ]] || return 9
      if [[ "$(jq -r ".preconditions.uid // empty" <<<"$body")" != "$remote_uid" ]]; then
        echo "Conflict: UID precondition failed" >&2; return 1
      fi
      remote_uid=""; return 0
    fi
    case " $* " in
      *" get pod test-blocker "*)
        [[ -z "$remote_uid" ]] || echo "{\"metadata\":{\"uid\":\"$remote_uid\"}}"
        return 0;;
    esac
    echo "Unexpected non-raw deletion" >&2; remote_uid=""; return 0
  }
  kube=(fake_api)
  remote_uid=original-uid
  _delayed_request_delete_blocker test-blocker original-uid || {
    echo "FAIL: UID-matched DeleteOptions deletion rejected" >&2; exit 1
  }
  [[ -z "$remote_uid" ]] || { echo "FAIL: original UID was not deleted" >&2; exit 1; }
  remote_uid=recreated-uid
  if _delayed_request_delete_blocker test-blocker original-uid; then
    echo "FAIL: recreation conflict was not propagated" >&2; exit 1
  fi
  [[ "$remote_uid" == recreated-uid ]] || { echo "FAIL: replacement was deleted" >&2; exit 1; }
' bash "$helper"
echo 'delayed-request helper: API UID-precondition recreation conflict preserved replacement'
bash -c 'source "$1"; declare -F _delayed_request_blocker_identity >/dev/null || exit 1;
  if _delayed_request_blocker_identity "{\"metadata\":{\"uid\":\"recreated-uid\"}}" original-uid; then
    echo "FAIL: scheduled recreation replaced CREATE identity" >&2; exit 1
  fi' bash "$helper"
for variant in hint-exhaustion-single target-health-race-single; do
  /bin/bash -c 'set -u; source "$1"; scenario="$2"; dr_initialized=true;
    namespace=alfred-e2e; source_node=alfred-kwok-gpu-a;
    dr_original_nodes="{\"items\":[{\"metadata\":{\"name\":\"alfred-kwok-gpu-b\"}}]}"
    if [[ "$scenario" == hint-exhaustion-single ]]; then
      dr_blocker_names=(test-blocker); dr_blocker_uids=(original-uid)
    else dr_health_nodes=(alfred-kwok-gpu-b); fi
    offline_api() {
      case " $* " in
        *" get pod "*) return 0;;
        *" get node "*) echo "{\"metadata\":{\"labels\":{}},\"status\":{\"conditions\":[{\"type\":\"GpuUnhealthy\",\"status\":\"False\"}]}}";;
        *" annotate node "*) return 0;;
        *) echo "Unexpected offline cleanup command" >&2; return 1;;
      esac
    }
    kube=(offline_api); delayed_request_cleanup' bash "$helper" "$variant"
done
echo 'delayed-request helper: Bash3.2 initialized cleanup handles empty opposite arrays'
# KWOK transitions can take more than 30 seconds alongside heartbeat processing.
# Advance the shell clock in the fake sleep, without slowing offline tests.
/bin/bash -c 'set -eu; source "$1"; SECONDS=0;
  dr_initialized=true; namespace=alfred-e2e; source_node=alfred-kwok-gpu-a;
  dr_health_nodes=(alfred-kwok-gpu-b)
  dr_original_nodes="{\"items\":[{\"metadata\":{\"name\":\"alfred-kwok-gpu-b\"}}]}"
  sleep() { SECONDS=$((SECONDS+10)); }
  offline_api() {
    local status=False
    case " $* " in
      *" get node alfred-kwok-gpu-b "*)
        if ((SECONDS<40)); then status=True; fi;;
      *" get node "*) ;;
      *" annotate node "*) return 0;;
      *) return 1;;
    esac
    printf "{\"metadata\":{\"labels\":{}},\"status\":{\"conditions\":[{\"type\":\"GpuUnhealthy\",\"status\":\"%s\"}]}}\n" "$status"
  }
  kube=(offline_api)
  delayed_request_cleanup
  [[ "$dr_initialized" == false && "$SECONDS" -ge 40 ]]
' bash "$helper"
echo 'delayed-request helper: cleanup waits for a delayed KWOK health transition'
