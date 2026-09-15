#!/usr/bin/env bash
# Real controller/scheduler acceptance; KWOK controls pod readiness only.
set -euo pipefail
variant="${1:-maintenance-gang}"
case "${variant}" in maintenance-gang|partial-restart) ;; *) echo 'unsupported gang scenario' >&2; exit 2 ;; esac
state_dir="${STATE_DIR:-}"
if [[ -z "${state_dir}" || "${state_dir}" != /* || ! -d "${state_dir}" ]]; then
  echo 'STATE_DIR must be an absolute directory' >&2; exit 2
fi
kubeconfig="${state_dir}/kubeconfig"
if [[ ! -f "${kubeconfig}" ]]; then
  echo "kubeconfig not found: ${kubeconfig}" >&2; exit 2
fi
for command in kubectl jq; do
  command -v "${command}" >/dev/null || { echo "required command not found: ${command}" >&2; exit 2; }
done
min_source_age_seconds="${ALFRED_E2E_MIN_SOURCE_AGE_SECONDS:-65}"
surge_hold_seconds="${ALFRED_E2E_SURGE_HOLD_SECONDS:-3}"
deadline_seconds="${ALFRED_E2E_DEADLINE_SECONDS:-360}"
for value in "${min_source_age_seconds}" "${surge_hold_seconds}" "${deadline_seconds}"; do
  [[ "${value}" =~ ^[0-9]+$ ]] || { echo 'timeouts must be integer seconds' >&2; exit 2; }
done
(( surge_hold_seconds >= 2 && deadline_seconds > surge_hold_seconds )) || {
  echo 'surge hold must be >=2s and shorter than deadline' >&2; exit 2;
}
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/state-json.sh"
kube=(kubectl --kubeconfig "${kubeconfig}" --context kind-alfred-e2e --request-timeout=15s)
namespace=alfred-e2e
alfred_namespace="${ALFRED_E2E_ALFRED_NAMESPACE:-ome}"
maintenance_key="${ALFRED_E2E_MAINTENANCE_KEY:-maintenance.example.com/state}"
maintenance_value="${ALFRED_E2E_MAINTENANCE_VALUE:-patching}"
artifact_dir="${state_dir}/artifacts/${variant}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "${artifact_dir}"
triggered_node=''
request_watch_pid=''
passed=false
routing_service=''
diagnostics() {
  "${kube[@]}" get nodes -o yaml >"${artifact_dir}/nodes.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get inferenceservice gang -o yaml >"${artifact_dir}/isvc.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get inferencereplica gang-engine -o yaml >"${artifact_dir}/ir.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get pods,podgroups.scheduling.x-k8s.io,endpointslices -o yaml >"${artifact_dir}/workloads.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get events --sort-by=.lastTimestamp >"${artifact_dir}/events.txt" 2>&1 || true
  "${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations alfred-dispatch-state -o yaml >"${artifact_dir}/alfred.yaml" 2>&1 || true
}
cleanup() {
  local rc=$?
  if [[ -n "${request_watch_pid}" ]]; then
    kill "${request_watch_pid}" >/dev/null 2>&1 || true
    wait "${request_watch_pid}" >/dev/null 2>&1 || true
  fi
  if [[ "${passed}" != true ]]; then
    diagnostics
    echo "maintenance-gang failed; diagnostics: ${artifact_dir}" >&2
  fi
  if [[ -n "${triggered_node}" ]]; then
    "${kube[@]}" label node "${triggered_node}" "${maintenance_key}-" >/dev/null 2>&1 || true
  fi
  exit "${rc}"
}
trap cleanup EXIT
pods_json() { "${kube[@]}" -n "${namespace}" get pods -l 'ome.io/inferenceservice=gang,ome.io/managed-by=OMENative' -o json; }
endpoints_json() { "${kube[@]}" -n "${namespace}" get endpointslices -l "kubernetes.io/service-name=${routing_service}" -o json; }
observe() {
  pods="$(pods_json)"
  if [[ -z "${routing_service}" ]]; then
    revision="$(jq -r '[.items[] | select(.metadata.labels["ome.io/instance-index"] == "0") |
      .metadata.labels["ome.io/revision-hash"] // empty] | unique | if length == 1 then .[0] else empty end' <<<"${pods}")"
    if [[ -n "${revision}" ]]; then routing_service="gang-engine-rev-${revision}"; fi
  fi
  ir="$("${kube[@]}" -n "${namespace}" get inferencereplica gang-engine -o json)"
  isvc="$("${kube[@]}" -n "${namespace}" get inferenceservice gang -o json)"
  endpoints="$(endpoints_json)"
  nodes="$("${kube[@]}" get nodes -l alfred-e2e/virtual=true -o json)"
  groups="$("${kube[@]}" -n "${namespace}" get podgroups.scheduling.x-k8s.io -o json)"
}
instance_summary() {
  jq -cn --argjson pods "${pods}" --argjson nodes "${nodes}" --argjson groups "${groups}" \
    --argjson endpoints "${endpoints}" --argjson instance "$1" -f "${dir}/gang-snapshot.jq"
}
healthy_ir() {
  jq -e '.status.readyReplicas == 1 and .status.servingReplicas == 1 and .status.availableReplicas == 1' <<<"${ir}" >/dev/null
}
whole_gang() {
  jq -e --argjson ready "$2" --argjson serving "$3" '
    .podGroup.minMember == 2 and .podGroup.topologyKey == "topology.kubernetes.io/zone" and
    (.zone | type == "string" and length > 0) and (.pods | length) == 2 and
    (.pods | map(.uid) | unique | length) == 2 and
    (.pods | map(.node) | unique | length) == 2 and
    (.pods | map(.zone) | unique | length) == 1 and
    (.pods | map(.runner) | sort) == ["leader","worker"] and
    all(.pods[]; .node != "" and .gpuRequest == 8 and .scheduler == "ome-scheduler" and
      .podGroup == ("gang-engine-" + ($instance|tostring)) and
      .terminating == false and .ready == $ready and .serving == $serving)' --argjson instance "$(jq '.instance' <<<"$1")" <<<"$1" >/dev/null
}
request_count() { jq '[.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] | length' <<<"${isvc}"; }
node_record() {
  local cm
  cm="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json)"
  jq -r --arg key "node.${source_node}" '.data[$key] // empty' <<<"${cm}"
}
source_safe() {
  local current
  current="$(instance_summary 0)"
  whole_gang "${current}" true true &&
    jq -e --argjson uids "${source_uids}" '.leaderEndpointReady and (.pods | map(.uid) | sort) == ($uids|sort)' <<<"${current}" >/dev/null
}

assert_handoff_continuity() {
  local snapshot sample
  snapshot="$(jq -cn --argjson pods "${pods}" --argjson endpoints "${endpoints}" \
    --arg service "${routing_service}" --argjson source "${source_uids}" \
    --argjson replacement "${replacement_uids}" \
    --arg sourceLeader "$(jq -r '.pods[] | select(.runner == "leader") | .uid' <<<"${source}")" \
    --arg replacementLeader "$(jq -r '.pods[] | select(.runner == "leader") | .uid' <<<"${surge}")" \
    '{pods:$pods,endpoints:$endpoints,routingService:$service,sourceUIDs:$source,
      replacementUIDs:$replacement,sourceRoutingUID:$sourceLeader,replacementRoutingUID:$replacementLeader}')"
  printf '%s\n' "${snapshot}" >>"${artifact_dir}/handoff-api-samples.jsonl"
  sample="$(jq -c -f "${dir}/handoff-snapshot.jq" <<<"${snapshot}")"
  printf '%s\n' "${sample}" >>"${artifact_dir}/handoff-samples.jsonl"
  jq -e '.sourceSafe or .replacementSafe' <<<"${sample}" >/dev/null
}

echo 'Resetting the maintenance-gang fixture'
"${kube[@]}" -n "${namespace}" delete inferenceservice gang --ignore-not-found --wait=true --timeout=90s >/dev/null
"${kube[@]}" -n "${namespace}" wait --for=delete inferencereplica/gang-engine --timeout=90s >/dev/null 2>&1 || {
  if "${kube[@]}" -n "${namespace}" get inferencereplica gang-engine >/dev/null 2>&1; then exit 1; fi
}
"${kube[@]}" -n "${namespace}" wait --for=delete pod -l 'ome.io/inferenceservice=gang,ome.io/managed-by=OMENative' --timeout=90s >/dev/null
# Do not silently evict another acceptance fixture to make gang capacity.
"${kube[@]}" get pods -A -o json | jq -e '
  [.items[] | select(.spec.nodeName // "" | startswith("alfred-kwok-")) |
   select(any(.spec.containers[]?; (.resources.requests["nvidia.com/gpu"] // "0" | tonumber) > 0))] | length == 0' >/dev/null || {
  echo 'gang requires four unoccupied virtual GPU nodes; remove other workload fixtures first' >&2; exit 1;
}
"${kube[@]}" get nodes -l alfred-e2e/virtual=true -o json | jq -e '
  (.items|length) == 4 and all(.items[]; (.status.allocatable["nvidia.com/gpu"]|tonumber) == 8 and
    (.metadata.labels["topology.kubernetes.io/zone"]|type == "string" and length > 0)) and
  ([.items[].metadata.labels["topology.kubernetes.io/zone"]] | group_by(.) | map(length) | sort) == [2,2]' >/dev/null || {
  echo 'gang requires four 8-GPU virtual nodes in two 2-node zones' >&2; exit 1;
}
"${kube[@]}" label nodes -l alfred-e2e/virtual=true "${maintenance_key}-" >/dev/null 2>&1 || true
"${kube[@]}" apply -f "${dir}/manifests/workload-gang.yaml" >"${artifact_dir}/apply.txt"
echo 'Waiting for the entire source gang to be healthy and routed'
source=''
ready_deadline=$((SECONDS + 180))
while (( SECONDS < ready_deadline )); do
  if observe 2>/dev/null; then
    candidate="$(instance_summary 0)"
    if whole_gang "${candidate}" true true && healthy_ir &&
      [[ "$(request_count)" == 0 ]] &&
      jq -e '.leaderEndpointReady' <<<"${candidate}" >/dev/null &&
      jq -e '((.status.migrations // [])|length) == 0' <<<"${ir}" >/dev/null &&
      [[ "$(jq '.items|length' <<<"${pods}")" == 2 ]]; then
      source="$(jq -c --arg service "${routing_service}" '. + {routingService:$service}' <<<"${candidate}")"
      pretrigger="$(jq -cn --argjson ir "${ir}" '{migrationRequestCount:0,irReadyReplicas:$ir.status.readyReplicas,irServingReplicas:$ir.status.servingReplicas,irAvailableReplicas:$ir.status.availableReplicas}')"
      break
    fi
  fi
  sleep 1
done
[[ -n "${source}" ]] || { echo 'source never reached whole-gang Ready+Serving+routing' >&2; exit 1; }
source_uids="$(jq -c '.pods|map(.uid)' <<<"${source}")"
source_node="$(jq -r '.pods[]|select(.runner == "leader")|.node' <<<"${source}")"
source_zone="$(jq -r '.zone' <<<"${source}")"
echo "Holding healthy source baseline for ${min_source_age_seconds}s"
age_deadline=$((SECONDS + min_source_age_seconds))
while (( SECONDS < age_deadline )); do
  observe
  source_safe && healthy_ir && [[ "$(request_count)" == 0 ]] &&
    jq -e '((.status.migrations // [])|length) == 0' <<<"${ir}" >/dev/null || {
    echo 'source changed before maintenance trigger' >&2; exit 1;
  }
  sleep 1
done
request_watch_file="${artifact_dir}/request-annotation-watch.jsonl"
"${kube[@]}" -n "${namespace}" get inferenceservice gang --watch --request-timeout="${deadline_seconds}s" -o json \
  >"${request_watch_file}" 2>"${artifact_dir}/request-annotation-watch.stderr" &
request_watch_pid=$!
sleep 1
kill -0 "${request_watch_pid}" || { echo 'annotation watch failed' >&2; exit 1; }
echo "Triggering maintenance on source leader node ${source_node}"
"${kube[@]}" label node "${source_node}" "${maintenance_key}=${maintenance_value}" --overwrite >/dev/null
triggered_node="${source_node}"
request_deadline=$((SECONDS + deadline_seconds))
request=''
recommendation=''
surge=''
completed=''
released=false
while (( SECONDS < request_deadline )); do
  observe
  if [[ "${released}" == true ]]; then
    assert_handoff_continuity || {
      echo 'source gang lost Ready+Serving+routing before the held replacement gang became healthy and routed' >&2
      exit 1
    }
  fi
  if [[ -z "${request}" && -s "${request_watch_file}" ]]; then
    request="$(jq -cs '[.[]|.metadata.annotations // {}|to_entries[]|select(.key|startswith("ome.io/migration-request-v1-"))|{uuid:(.key|sub("^ome.io/migration-request-v1-";"")),annotationKey:.key,payload:(.value|fromjson)}]|unique_by(.uuid)|if length == 1 then .[0] else empty end' "${request_watch_file}" 2>/dev/null || true)"
  fi
  if [[ -n "${request}" ]]; then
    uuid="$(jq -r '.uuid' <<<"${request}")"
    jq -e --arg node "${source_node}" '.payload.component == "engine" and .payload.instance == 0 and .payload.from_node == $node and .payload.requested_by == "alfred" and .payload.reason == "NodeMaintenance"' <<<"${request}" >/dev/null || { echo 'request does not identify real source' >&2; exit 1; }
    if [[ -z "${recommendation}" ]]; then
      last_cycle="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o jsonpath='{.data.last-cycle\.json}')"
      recommendation="$(jq -c --arg uuid "${uuid}" '[.recommendations[]?|select(.requestUUID == $uuid)]|last // empty' <<<"${last_cycle}" 2>/dev/null || true)"
    fi
    migration="$(jq -c --arg uuid "${uuid}" '[.status.migrations[]?|select(.requestUUID == $uuid)]|if length == 1 then .[0] else empty end' <<<"${ir}")"
    [[ "$(alfred_migration_phase "${migration}")" != Failed ]] || { echo "migration failed: ${migration}" >&2; exit 1; }
  fi
  replacement="$(instance_summary 1)"
  if [[ "${released}" == false ]]; then
    source_safe || { echo 'source gang lost Ready+Serving+routing before replacement release' >&2; exit 1; }
    jq -e 'any(.pods[]; .ready or .serving)' <<<"${replacement}" >/dev/null && { echo 'replacement escaped KWOK hold' >&2; exit 1; }
    if [[ -n "${request}" ]] && whole_gang "${replacement}" false false &&
       [[ "$(jq -r '.zone' <<<"${replacement}")" != "${source_zone}" ]]; then
      record="$(node_record)"
      if [[ -n "${record}" ]] && jq -e '.maintenance.requested and .maintenanceDrainedAt == null and .omeGpuOccupantsPresent' <<<"${record}" >/dev/null; then
        replacement_uids="$(jq -c '.pods|map(.uid)' <<<"${replacement}")"
        hold_start="${SECONDS}"
        echo "Holding entire scheduled replacement gang for ${surge_hold_seconds}s"
        while (( SECONDS - hold_start < surge_hold_seconds )); do
          observe
          source_safe || { echo 'source gang changed during held replacement' >&2; exit 1; }
          replacement="$(instance_summary 1)"
          whole_gang "${replacement}" false false &&
            jq -e --argjson uids "${replacement_uids}" --arg zone "${source_zone}" '(.pods|map(.uid)|sort) == ($uids|sort) and .zone != $zone and (.leaderEndpointReady|not)' <<<"${replacement}" >/dev/null || {
            echo 'replacement was not continuously whole, scheduled and held' >&2; exit 1;
          }
          record="$(node_record)"
          jq -e '.maintenanceDrainedAt == null and .omeGpuOccupantsPresent' <<<"${record}" >/dev/null || { echo 'Alfred prematurely reported drained source' >&2; exit 1; }
          jq -cn --argjson source "$(instance_summary 0)" --argjson replacement "${replacement}" --argjson node "${record}" '{source:$source,replacement:$replacement,node:$node}' >>"${artifact_dir}/held-samples.jsonl"
          sleep 0.2
        done
        surge="$(jq -cn --argjson replacement "${replacement}" --argjson hold "$((SECONDS - hold_start))" --argjson source "$(instance_summary 0)" --argjson record "${record}" '$replacement + {holdSeconds:$hold,sourcePodsPresent:($source.pods|length),sourcePodsReady:([$source.pods[]|select(.ready)]|length),sourcePodsServing:([$source.pods[]|select(.serving)]|length),sourceLeaderEndpointReady:$source.leaderEndpointReady,replacementLeaderEndpointReady:$replacement.leaderEndpointReady,alfredMaintenanceDrainedAt:$record.maintenanceDrainedAt,alfredOMEGPUOccupantsPresent:$record.omeGpuOccupantsPresent}')"
        if [[ "${variant}" == partial-restart ]]; then
          source "${dir}/partial-gang.sh"
          partial_gang_restart
        fi
        # No ISVC/template mutation: release only the two actual surge pods.
        while IFS= read -r name; do
          "${kube[@]}" -n "${namespace}" annotate pod "${name}" alfred-e2e.ome.io/readiness=immediate --overwrite >/dev/null
        done < <(jq -r '.pods[].name' <<<"${replacement}")
        released=true
      fi
    fi
  elif [[ -n "${migration:-}" && "$(jq -r '.phase' <<<"${migration}")" == Completed ]] && whole_gang "${replacement}" true true && healthy_ir &&
    jq -e --argjson uids "${replacement_uids}" --arg zone "${source_zone}" '(.pods|map(.uid)|sort) == ($uids|sort) and .zone != $zone' <<<"${replacement}" >/dev/null; then
    remaining="$(jq -c --argjson uids "${source_uids}" '[.items[]|.metadata.uid as $uid|select($uids|index($uid))|.metadata.uid]' <<<"${pods}")"
    source_endpoint="$(jq --argjson uids "${source_uids}" 'any(.items[].endpoints[]?; .targetRef.uid as $uid|($uids|index($uid)) != null)' <<<"${endpoints}")"
    if [[ "${remaining}" == '[]' && "${source_endpoint}" == false ]] && jq -e '.leaderEndpointReady' <<<"${replacement}" >/dev/null; then
      completed="$(jq -cn --argjson migration "${migration}" --argjson replacement "${replacement}" --argjson ir "${ir}" '{migration:$migration,sourcePodUIDsPresent:[],sourceEndpointPresent:false,replacementPodsReady:([$replacement.pods[]|select(.ready)]|length),replacementPodsServing:([$replacement.pods[]|select(.serving)]|length),replacementLeaderEndpointReady:$replacement.leaderEndpointReady,irReadyReplicas:$ir.status.readyReplicas,irServingReplicas:$ir.status.servingReplicas,irAvailableReplicas:$ir.status.availableReplicas}')"
      break
    fi
  fi
  sleep 0.2
done
[[ -n "${surge}" && -n "${completed}" && -n "${recommendation}" ]] || { echo 'no observed safe whole-gang completed migration' >&2; exit 1; }
alfred=''
echo "Waiting for durable completed dispatch and drained-node observation (${uuid})"
while (( SECONDS < request_deadline )); do
  record="$(node_record)"
  dispatch_raw="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-dispatch-state -o jsonpath='{.data.state\.json}')"
  dispatch="$(jq -c --arg uuid "${uuid}" '[.entries[]?|select(.uuid == $uuid)]|if length == 1 then .[0] else empty end' <<<"${dispatch_raw}")"
  if [[ -n "${record}" && -n "${dispatch}" ]] &&
    jq -e '.maintenance.requested and .maintenanceDrainedAt != null and (.omeGpuOccupantsPresent|not)' <<<"${record}" >/dev/null &&
    jq -e '.phase == "completed" and .completedAt != null' <<<"${dispatch}" >/dev/null; then
    alfred="$(jq -cn --argjson recommendation "${recommendation}" --argjson record "${record}" --argjson dispatch "${dispatch}" '{workload:$recommendation.workload,component:$recommendation.component,instance:$recommendation.instance,policy:$recommendation.policy,reason:$recommendation.reason,fromNode:$recommendation.fromNode,requestUUID:$dispatch.uuid,dispatchStatus:$dispatch.phase,maintenanceTriggers:$record.maintenance.triggers,maintenanceRequestedAt:$record.maintenanceRequestedAt,maintenanceDrainedAt:$record.maintenanceDrainedAt,omeGPUOccupantsPresent:$record.omeGpuOccupantsPresent}')"
    break
  fi
  sleep 1
done
[[ -n "${alfred}" ]] || { echo 'Alfred did not durably observe completion and drained source' >&2; exit 1; }
jq -n --argjson source "${source}" --argjson preTrigger "${pretrigger}" --argjson request "${request}" --argjson surge "${surge}" --argjson completed "${completed}" --argjson alfred "${alfred}" --slurpfile handoff "${artifact_dir}/handoff-samples.jsonl" '{scenario:"maintenance-gang",source:$source,preTrigger:$preTrigger,request:$request,surge:$surge,handoff:$handoff,completed:$completed,alfred:$alfred}' >"${artifact_dir}/evidence.json"
jq -e -f "${dir}/verify-gang-evidence.jq" "${artifact_dir}/evidence.json" >/dev/null || { echo 'captured gang evidence failed verifier' >&2; exit 1; }
diagnostics
passed=true
echo "maintenance-gang passed; evidence: ${artifact_dir}/evidence.json"
