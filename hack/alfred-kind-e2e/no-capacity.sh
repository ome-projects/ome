#!/usr/bin/env bash
# Captured GPU exhaustion and fail-closed worker output, with serving preserved.
set -euo pipefail
state_dir="${STATE_DIR:-}"
[[ -n "${state_dir}" && "${state_dir}" == /* && -d "${state_dir}" ]] || {
  echo 'STATE_DIR must be an absolute directory' >&2; exit 2;
}
kubeconfig="${state_dir}/kubeconfig"
[[ -f "${kubeconfig}" ]] || { echo "kubeconfig not found: ${kubeconfig}" >&2; exit 2; }
baseline_seconds="${ALFRED_E2E_MIN_SOURCE_AGE_SECONDS:-65}"
hold_seconds="${ALFRED_E2E_NO_CAPACITY_HOLD_SECONDS:-15}"
deadline_seconds="${ALFRED_E2E_DEADLINE_SECONDS:-240}"
for value in "${baseline_seconds}" "${hold_seconds}" "${deadline_seconds}"; do
  [[ "${value}" =~ ^[0-9]+$ ]] || { echo 'timeouts must be integer seconds' >&2; exit 2; }
done
(( baseline_seconds >= 65 )) || { echo 'baseline must be >=65s' >&2; exit 2; }
(( hold_seconds >= 15 && deadline_seconds > hold_seconds )) || {
  echo 'observation must be >=15s and shorter than deadline' >&2; exit 2;
}
for command in kubectl jq go; do
  command -v "${command}" >/dev/null || { echo "required command not found: ${command}" >&2; exit 2; }
done
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/no-capacity-ownership.sh"
repo_dir="$(cd "${dir}/../.." && pwd)"
kube=(kubectl --kubeconfig "${kubeconfig}" --context kind-alfred-e2e --request-timeout=15s)
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
namespace="alfred-e2e-no-capacity-${run_id}"
name=no-capacity
runtime="alfred-e2e-no-capacity-runtime-${run_id}"
alfred_namespace="${ALFRED_E2E_ALFRED_NAMESPACE:-ome}"
maintenance_key="${ALFRED_E2E_MAINTENANCE_KEY:-maintenance.example.com/state}"
maintenance_value="${ALFRED_E2E_MAINTENANCE_VALUE:-patching}"
artifact_dir="${state_dir}/artifacts/no-capacity-${run_id}"
mkdir -p "${artifact_dir}"
watch_pids=()
triggered_node=''
triggered_node_uid=''
trigger_active=false
trigger_uncertain=false
passed=false
namespace_uid=''
runtime_uid=''
isvc_uid=''
ir_uid=''
stop_watches() {
  local pid
  (( ${#watch_pids[@]} > 0 )) || return 0
  for pid in "${watch_pids[@]}"; do kill "${pid}" >/dev/null 2>&1 || true; done
  for pid in "${watch_pids[@]}"; do wait "${pid}" >/dev/null 2>&1 || true; done
  watch_pids=()
}
cleanup() {
  local rc=$?
  stop_watches
  if [[ "${passed}" != true ]]; then
    "${kube[@]}" get nodes -o yaml >"${artifact_dir}/nodes.yaml" 2>&1 || true
    "${kube[@]}" -n "${namespace}" get inferenceservices.ome.io,inferencereplicas.ome.io,pods,endpointslices -o yaml >"${artifact_dir}/workloads.yaml" 2>&1 || true
    "${kube[@]}" -n "${namespace}" get events -o yaml >"${artifact_dir}/events.yaml" 2>&1 || true
    "${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations alfred-dispatch-state -o yaml >"${artifact_dir}/alfred.yaml" 2>&1 || true
    echo "no-capacity failed; evidence: ${artifact_dir}" >&2
  fi
  if nc_cleanup "${rc}"; then rc=0; else rc=$?; fi
  exit "${rc}"
}
trap cleanup EXIT
pods_json() { "${kube[@]}" -n "${namespace}" get pods -l "ome.io/inferenceservice=${name},ome.io/managed-by=OMENative" -o json; }
endpoints_json() { "${kube[@]}" -n "${namespace}" get endpointslices -l "kubernetes.io/service-name=${routing_service}" -o json; }
observe() {
  nc_require_namespace || return 1
  pods="$(pods_json)" || return 1
  # The component headless Service publishes not-ready peer addresses;
  # migration safety depends on the actual per-revision routing Service.
  routing_service="${name}-engine-rev-$(jq -r '.items[0].metadata.labels["ome.io/revision-hash"] // ""' <<<"${pods}")" || return 1
  ir="$("${kube[@]}" -n "${namespace}" get inferencereplicas.ome.io "${name}-engine" -o json)" || return 1
  isvc="$("${kube[@]}" -n "${namespace}" get inferenceservices.ome.io "${name}" -o json)" || return 1
  endpoints="$(endpoints_json)" || return 1
}
request_count() { jq '[.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] | length' <<<"${isvc}"; }
source_safe() {
  nc_source_owned "${isvc_uid}" "${ir_uid}" "${source_uid}" "${isvc}" "${ir}" "${pods}" &&
  jq -e --arg uid "${source_uid}" '
    (.items|length) == 1 and all(.items[]; .metadata.uid == $uid and .metadata.deletionTimestamp == null and
      any(.status.conditions[]?; .type == "Ready" and .status == "True") and
      any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True"))' <<<"${pods}" >/dev/null &&
  jq -e --arg uid "${source_uid}" 'any(.items[].endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true)' <<<"${endpoints}" >/dev/null &&
  jq -e '.status.readyReplicas == 1 and .status.servingReplicas == 1 and .status.availableReplicas == 1 and
    ((.status.migrations // [])|length) == 0' <<<"${ir}" >/dev/null && [[ "$(request_count)" == 0 ]]
}
start_watch() {
  local key="$1"; shift
  "${kube[@]}" "$@" --watch --output-watch-events --request-timeout="${deadline_seconds}s" -o json \
    >"${artifact_dir}/${key}-watch.json" 2>"${artifact_dir}/${key}-watch.stderr" &
  watch_pids+=("$!")
}
watch_alive() {
  local pid
  (( ${#watch_pids[@]} > 0 )) || return 1
  for pid in "${watch_pids[@]}"; do kill -0 "${pid}" >/dev/null 2>&1 || return 1; done
}
watch_counts() {
  # kubectl streams concatenated pretty JSON objects; partial final frames must
  # cause a retry, never be interpreted as an empty history.
  jq -s '[.[] | (.object // .) | (.items // [.])[] | .metadata.annotations // {} | keys[] |
    select(startswith("ome.io/migration-request-v1-"))] | length' "${artifact_dir}/isvc-watch.json" &&
  jq -s '[.[] | (.object // .) | (.items // [.])[] | .status.migrations[]?] | length' "${artifact_dir}/ir-watch.json"
}

echo 'Checking exclusive no-capacity fixture ownership'
"${kube[@]}" get node alfred-e2e-control-plane -o json | jq -e '
  .kind == "Node" and .metadata.name == "alfred-e2e-control-plane" and
  .metadata.labels["alfred-e2e/infrastructure"] == "true"' >/dev/null || {
  echo 'dedicated alfred-e2e control-plane fixture marker missing' >&2; exit 1;
}
"${kube[@]}" get inferenceservices.ome.io -A -o json | jq -e '
  [.items[]|select(.spec.deploymentMode == "OMENative")]|length == 0' >/dev/null || {
  echo 'remove other OMENative fixtures before no-capacity test' >&2; exit 1;
}
nodes="$("${kube[@]}" get nodes -l alfred-e2e/virtual=true -o json)"
jq -e --arg key "${maintenance_key}" '(.items|length) == 4 and all(.items[];
  (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
  (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0 and
  (.status.allocatable["nvidia.com/gpu"]|tonumber) == 8 and .metadata.labels[$key] == null)' <<<"${nodes}" >/dev/null || {
  echo 'requires four 8GPU virtual nodes without preexisting maintenance labels' >&2; exit 1;
}
"${kube[@]}" get pods -A -o json | jq -e '
  [.items[]|select(.spec.nodeName // ""|startswith("alfred-kwok-"))|
   select(.metadata.deletionTimestamp == null)|
   select(any(.spec.containers[]?; (.resources.requests["nvidia.com/gpu"] // "0"|tonumber)>0))]|length == 0' >/dev/null || {
  echo 'virtual GPU nodes must be unoccupied before test' >&2; exit 1;
}
echo 'Building read-only public-input worker replay helper'
(cd "${repo_dir}" && GOCACHE="${GOCACHE:-${state_dir}/go-build-cache}" go build -o "${artifact_dir}/worker-result" ./hack/alfred-kind-e2e/worker-result)
# Client-side conversion returns multiple JSON objects for multi-document YAML.
"${kube[@]}" create --dry-run=client -f "${dir}/manifests/workload-single.yaml" -o json |
  jq -s --arg ns "${namespace}" --arg name "${name}" --arg runtime "${runtime}" '
    map(if .kind == "Namespace" then .metadata.name=$ns
      elif .kind == "ClusterServingRuntime" then .metadata.name=$runtime
      elif .kind == "InferenceService" then .metadata.namespace=$ns | .metadata.name=$name | .spec.runtime.name=$runtime
      else error("unexpected fixture kind") end) | {apiVersion:"v1",kind:"List",items:.}' >"${artifact_dir}/fixture.json"
: >"${artifact_dir}/ownership.jsonl"
jq -e '.items[] | select(.kind == "Namespace")' "${artifact_dir}/fixture.json" >"${artifact_dir}/namespace-desired.json"
nc_create_owned "${artifact_dir}/namespace-desired.json" "${artifact_dir}/namespace-created.json" \
  Namespace "${namespace}" ''
namespace_uid="$(jq -er '.metadata.uid' "${artifact_dir}/namespace-created.json")"
jq -e '.items[] | select(.kind == "ClusterServingRuntime")' "${artifact_dir}/fixture.json" >"${artifact_dir}/runtime-desired.json"
nc_create_owned "${artifact_dir}/runtime-desired.json" "${artifact_dir}/runtime-created.json" \
  ClusterServingRuntime "${runtime}" ''
runtime_uid="$(jq -er '.metadata.uid' "${artifact_dir}/runtime-created.json")"
jq -e '.items[] | select(.kind == "InferenceService")' "${artifact_dir}/fixture.json" >"${artifact_dir}/isvc-desired.json"
nc_create_owned "${artifact_dir}/isvc-desired.json" "${artifact_dir}/isvc-created.json" \
  InferenceService "${name}" "${namespace}"
isvc_uid="$(jq -er '.metadata.uid' "${artifact_dir}/isvc-created.json")"
source_uid=''
ready_deadline=$((SECONDS + 180))
while (( SECONDS < ready_deadline )); do
  if observe 2>/dev/null; then
    if [[ -z "${source_uid}" ]]; then
      identity="$(nc_source_identity "${isvc_uid}" "${isvc}" "${ir}" "${pods}" 2>/dev/null)" || { sleep 1; continue; }
      ir_uid="$(jq -r '.irUID' <<<"${identity}")"
      source_uid="$(jq -r '.sourceUID' <<<"${identity}")"
    fi
    if [[ -n "${source_uid}" ]] && source_safe; then break; fi
  fi
  sleep 1
done
[[ -n "${source_uid}" ]] && source_safe || { echo 'source never became healthy and routed' >&2; exit 1; }
source_node="$(jq -r '.items[0].spec.nodeName' <<<"${pods}")"
jq -e --arg node "${source_node}" 'any(.items[];.metadata.name == $node)' <<<"${nodes}" >/dev/null
source_node_uid="$(jq -er --arg node "${source_node}" '.items[] | select(.metadata.name == $node) | .metadata.uid' <<<"${nodes}")"
echo "Holding healthy baseline for ${baseline_seconds}s"
baseline_deadline=$((SECONDS + baseline_seconds))
while (( SECONDS < baseline_deadline )); do observe; source_safe || { echo 'baseline changed' >&2; exit 1; }; sleep 1; done
echo 'Consuming destination GPUs with ordinarily scheduled Pods'
while IFS= read -r node; do
  blocker_name="block-${node}"
  jq -n --arg ns "${namespace}" --arg node "${node}" --arg name "${blocker_name}" \
    '{apiVersion:"v1",kind:"Pod",metadata:{namespace:$ns,name:$name,labels:{"alfred-e2e/capacity-blocker":"true"}},spec:{schedulerName:"alfred-default-scheduler",nodeSelector:{"kubernetes.io/hostname":$node,"alfred-e2e/virtual":"true"},tolerations:[{key:"alfred-e2e/virtual",operator:"Equal",value:"true",effect:"NoSchedule"}],containers:[{name:"blocker",image:"registry.k8s.io/pause:3.10",resources:{requests:{cpu:"10m",memory:"16Mi","nvidia.com/gpu":"8"},limits:{cpu:"10m",memory:"16Mi","nvidia.com/gpu":"8"}}}]}}' \
    >"${artifact_dir}/${blocker_name}-desired.json"
  nc_create_owned "${artifact_dir}/${blocker_name}-desired.json" "${artifact_dir}/${blocker_name}-created.json" \
    Pod "${blocker_name}" "${namespace}"
done < <(jq -r --arg node "${source_node}" '.items[]|select(.metadata.name!=$node)|.metadata.name' <<<"${nodes}")
nc_require_namespace
"${kube[@]}" -n "${namespace}" wait pod -l alfred-e2e/capacity-blocker=true --for=jsonpath='{.status.phase}'=Running --timeout=120s
blockers="$("${kube[@]}" -n "${namespace}" get pods -l alfred-e2e/capacity-blocker=true -o json)"
nc_require_namespace
jq -e --slurpfile owned "${artifact_dir}/ownership.jsonl" '
  ([$owned[] | select(.kind == "Pod") | {key:.name,value:.uid}] | from_entries) as $uids |
  (.items|length)==3 and all(.items[];
    .metadata.uid == $uids[.metadata.name] and .status.phase=="Running" and
    .spec.nodeName==.spec.nodeSelector["kubernetes.io/hostname"] and
    .spec.schedulerName=="alfred-default-scheduler")' <<<"${blockers}" >/dev/null
printf '%s\n' "${blockers}" >"${artifact_dir}/blockers.json"
observe; source_safe
start_watch isvc -n "${namespace}" get inferenceservices.ome.io "${name}"
start_watch ir -n "${namespace}" get inferencereplicas.ome.io "${name}-engine"
start_watch pods -n "${namespace}" get pods -l "ome.io/inferenceservice=${name},ome.io/managed-by=OMENative"
start_watch endpoints -n "${namespace}" get endpointslices -l "kubernetes.io/service-name=${routing_service}"
start_watch recommendations -n "${alfred_namespace}" get configmap alfred-recommendations
sleep 1
watch_alive || { echo 'failed to establish evidence watches' >&2; exit 1; }
nc_set_maintenance "${source_node}" "${source_node_uid}" "${maintenance_key}" "${maintenance_value}"
echo "Observing fail-closed worker output and full destination capacity on ${source_node}"
observation_deadline=$((SECONDS + deadline_seconds - 5))
first_sample_seconds=''
sample_index=0
while (( SECONDS < observation_deadline )); do
  watch_alive || { echo 'evidence watch ended early' >&2; exit 1; }
  observe; source_safe || { echo 'source changed after maintenance' >&2; exit 1; }
  counts="$(watch_counts 2>/dev/null)" || { sleep 0.2; continue; }
  [[ "${counts}" == $'0\n0' ]] || { echo 'migration request/status observed' >&2; exit 1; }
  record="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json |
    jq -r --arg key "node.${source_node}" '.data[$key] // empty')"
  if [[ -z "${record}" ]] || ! jq -e '.maintenance.requested and .omeGpuOccupantsPresent and .maintenanceDrainedAt == null and .drainedAt == null' <<<"${record}" >/dev/null; then
    sleep 1; continue
  fi
  worker_file="${artifact_dir}/worker-${sample_index}.json"
  "${artifact_dir}/worker-result" --kubeconfig "${kubeconfig}" --context kind-alfred-e2e \
    --namespace "${namespace}" --name "${name}" --node "${source_node}" --alfred-namespace "${alfred_namespace}" >"${worker_file}"
  # Unsupported alone is not a capacity diagnosis. Independently prove from
  # this same snapshot that non-preemptible blockers fill every destination.
  capacity_proof="$(jq -e --arg uid "${source_uid}" --arg source "${source_node}" \
    -f "${dir}/no-capacity-proof.jq" "${worker_file}")" || {
    echo 'worker result or independent captured capacity proof failed' >&2; exit 1;
  }
  observe; source_safe
  jq -cn --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg uid "${source_uid}" \
    --argjson capacityProof "${capacity_proof}" --slurpfile worker "${worker_file}" \
    '{timestamp:$timestamp,sourceReady:true,sourceServing:true,sourceRouted:true,podUIDs:[$uid],drained:false,maintenanceRequested:true,occupantsPresent:true,requestCount:0,migrations:[],capacityProof:$capacityProof,scheduling:{status:$worker[0].result.decision,reason:$worker[0].result.reason,placements:($worker[0].result.placements // []),provenance:$worker[0].provenance,validated:$worker[0].validated,requestID:$worker[0].result.requestID,snapshotID:$worker[0].result.snapshotID}}' >>"${artifact_dir}/samples.jsonl"
  sample_index=$((sample_index+1))
  if [[ -z "${first_sample_seconds}" ]]; then first_sample_seconds="${SECONDS}"; fi
  elapsed="$(jq -s '(.[-1].timestamp|fromdateiso8601)-(.[0].timestamp|fromdateiso8601)' "${artifact_dir}/samples.jsonl")"
  if (( elapsed >= hold_seconds )); then break; fi
  sleep 1
done
[[ -n "${first_sample_seconds}" ]] && (( SECONDS - first_sample_seconds >= hold_seconds )) || { echo 'bounded no-capacity observation timed out' >&2; exit 1; }
watch_alive
stop_watches
counts="$(watch_counts)"
[[ "${counts}" == $'0\n0' ]] || { echo 'migration appeared in complete watch history' >&2; exit 1; }
# A same-name replacement in any owned workload watch invalidates the run even
# if its readiness/status fields happen to satisfy the scenario assertions.
for kind in isvc ir pods; do
  nc_watch_owned "${kind}" "${isvc_uid}" "${ir_uid}" "${source_uid}" "${artifact_dir}/${kind}-watch.json"
done
# Validate every complete event, including transient readiness/routing losses.
for kind in isvc ir pods endpoints recommendations; do
  jq -es --arg kind "${kind}" --arg uid "${source_uid}" --arg key "node.${source_node}" \
    -f "${dir}/no-capacity-watch.jq" "${artifact_dir}/${kind}-watch.json" >/dev/null
done
jq -s --arg uid "${source_uid}" '{sourceUID:$uid,requestCount:0,migrations:[],watchComplete:true,samples:.}' "${artifact_dir}/samples.jsonl" >"${artifact_dir}/evidence.json"
jq -e -f "${dir}/verify-no-capacity.jq" "${artifact_dir}/evidence.json" >/dev/null
passed=true
echo "PASS no-capacity fail-closed safety: ${artifact_dir}/evidence.json"
