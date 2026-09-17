#!/usr/bin/env bash

set -euo pipefail

annotation_watch_timeout_seconds() {
  # The shared request/migration/drain deadline starts after trigger setup.
  # Cover health initialization (30s), quarantine (60s), restart sampling
  # (5s), initial watch startup (1s), plus bounded API-call overhead. Cleanup
  # stops the watch as soon as the scenario finishes.
  printf '%s\n' "$(($1 + 30 + 60 + 5 + 1 + 300))"
}

assert_annotation_watch_alive() {
  if [[ -z "$1" ]] || ! kill -0 "$1" >/dev/null 2>&1; then
    echo "InferenceService annotation watch ended before evidence was sealed; request-count evidence is incomplete (watch stderr: $2)" >&2
    return 1
  fi
}

scenario="${1:-}"
state_dir="${STATE_DIR:-}"
if [[ -z "${state_dir}" || "${state_dir}" != /* || ! -d "${state_dir}" ]]; then
  echo "STATE_DIR must be an absolute directory" >&2
  exit 2
fi
kubeconfig="${state_dir}/kubeconfig"
if [[ ! -f "${kubeconfig}" ]]; then
  echo "kubeconfig not found: ${kubeconfig}" >&2
  exit 2
fi
if [[ "${scenario}" != "maintenance-single" && "${scenario}" != "maintenance-columnar" && "${scenario}" != "unhealthy-single" && "${scenario}" != "restart-single" && "${scenario}" != "hint-exhaustion-single" && "${scenario}" != "target-health-race-single" ]]; then
  echo "unsupported scenario: ${scenario}" >&2
  exit 2
fi

for command in kubectl jq go; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 2
  fi
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/state-json.sh"
source "${script_dir}/scenario-status.sh"
# build.sh cross-compiles Linux binaries. This helper is invoked by the host
# shell, so explicitly select Go's host platform even if GOOS was exported.
project_dir="$(cd "${script_dir}/../.." && pwd)"
ir_status="${state_dir}/ir-status"
(cd "${project_dir}" && GOOS="$(go env GOHOSTOS)" GOARCH="$(go env GOHOSTARCH)" \
  go build -o "${ir_status}" ./hack/alfred-kind-e2e/ir-status)
initial_count=1
workload_manifest="${script_dir}/manifests/workload-single.yaml"
columnar_json=null
if [[ "${scenario}" == "maintenance-columnar" ]]; then
  initial_count=4
  workload_manifest="${script_dir}/manifests/workload-columnar.yaml"
fi
delayed=false
if [[ "${scenario}" == hint-exhaustion-single || "${scenario}" == target-health-race-single ]]; then
  source "${script_dir}/delayed-request.sh"
  delayed=true
fi
kube=(kubectl --kubeconfig "${kubeconfig}" --context kind-alfred-e2e --request-timeout=15s)
namespace="alfred-e2e"
isvc_name="single"
ir_name="single-engine"
alfred_namespace="${ALFRED_E2E_ALFRED_NAMESPACE:-ome}"
maintenance_key="${ALFRED_E2E_MAINTENANCE_KEY:-maintenance.example.com/state}"
maintenance_value="${ALFRED_E2E_MAINTENANCE_VALUE:-patching}"
min_source_age_seconds="${ALFRED_E2E_MIN_SOURCE_AGE_SECONDS:-65}"
surge_hold_seconds="${ALFRED_E2E_SURGE_HOLD_SECONDS:-3}"
deadline_seconds="${ALFRED_E2E_DEADLINE_SECONDS:-360}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
artifact_dir="${state_dir}/artifacts/${scenario}-${run_id}"
mkdir -p "${artifact_dir}"

triggered_node=""
request_watch_pid=""
passed=false
health_json=null
restart_json=null
routing_service=""
expected_reason=NodeMaintenance
if [[ "${scenario}" == "unhealthy-single" ]]; then expected_reason=NodeUnhealthy; fi

dump_diagnostics() {
  "${kube[@]}" get nodes -o wide >"${artifact_dir}/nodes.txt" 2>&1 || true
  "${kube[@]}" get nodes -o yaml >"${artifact_dir}/nodes.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" -o yaml >"${artifact_dir}/isvc.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o yaml >"${artifact_dir}/ir.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get pods -o yaml >"${artifact_dir}/pods.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get endpointslices -o yaml >"${artifact_dir}/endpointslices.yaml" 2>&1 || true
  "${kube[@]}" -n "${namespace}" get events --sort-by=.lastTimestamp >"${artifact_dir}/events.txt" 2>&1 || true
  "${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o yaml >"${artifact_dir}/alfred-recommendations.yaml" 2>&1 || true
  "${kube[@]}" -n "${alfred_namespace}" get configmap alfred-dispatch-state -o yaml >"${artifact_dir}/alfred-dispatch-state.yaml" 2>&1 || true
  "${kube[@]}" get pods -A -o wide >"${artifact_dir}/all-pods.txt" 2>&1 || true
}

cleanup() {
  local rc=$?
  if [[ -n "${request_watch_pid}" ]]; then
    kill "${request_watch_pid}" >/dev/null 2>&1 || true
    wait "${request_watch_pid}" >/dev/null 2>&1 || true
  fi
  if [[ "${passed}" != "true" ]]; then
    dump_diagnostics
    echo "${scenario} failed; diagnostics: ${artifact_dir}" >&2
  fi
  if [[ -n "${triggered_node}" ]]; then
    "${kube[@]}" label node "${triggered_node}" "${maintenance_key}-" >/dev/null 2>&1 || true
    if [[ "${scenario}" == "unhealthy-single" ]]; then
      "${kube[@]}" annotate node "${triggered_node}" alfred-e2e.ome.io/gpu-health=healthy --overwrite >/dev/null 2>&1 || true
    fi
  fi
  if [[ "${delayed}" == true ]]; then delayed_request_cleanup || rc=1; fi
  exit "${rc}"
}
trap cleanup EXIT

pods_json() {
  "${kube[@]}" -n "${namespace}" get pods \
    -l "ome.io/inferenceservice=${isvc_name},ome.io/managed-by=OMENative" -o json
}

routing_endpoints_json() {
  "${kube[@]}" -n "${namespace}" get endpointslices \
    -l "kubernetes.io/service-name=${routing_service}" -o json
}

assert_handoff_continuity() {
  local snapshot sample
  snapshot="$(jq -cn --argjson pods "${pod_list}" --argjson endpoints "$(routing_endpoints_json)" \
    --arg service "${routing_service}" --arg source "${source_uid}" \
    --arg replacement "$(jq -r '.replacement.uid' <<<"${surge_json}")" \
    '{pods:$pods,endpoints:$endpoints,routingService:$service,sourceUIDs:[$source],
      replacementUIDs:[$replacement],sourceRoutingUID:$source,replacementRoutingUID:$replacement}')"
  printf '%s\n' "${snapshot}" >>"${artifact_dir}/handoff-api-samples.jsonl"
  sample="$(jq -c -f "${script_dir}/handoff-snapshot.jq" <<<"${snapshot}")"
  printf '%s\n' "${sample}" >>"${artifact_dir}/handoff-samples.jsonl"
  jq -e '.sourceSafe or .replacementSafe' <<<"${sample}" >/dev/null
}

assert_source_continuity() {
  local current_pods current_endpoints
  current_pods="$(pods_json)"
  current_endpoints="$(routing_endpoints_json)"
  jq -e --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid and
    .metadata.deletionTimestamp == null and
    any(.status.conditions[]?; .type == "Ready" and .status == "True") and
    any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True"))' \
    <<<"${current_pods}" >/dev/null &&
  jq -e --arg uid "${source_uid}" 'any(.items[].endpoints[]?;
    .targetRef.uid == $uid and .conditions.ready == true and .conditions.terminating != true)' <<<"${current_endpoints}" >/dev/null
}

dispatch_for_request() {
  "${kube[@]}" -n "${alfred_namespace}" get configmap alfred-dispatch-state \
    -o jsonpath='{.data.state\.json}' |
    jq -c --arg uuid "${request_uuid}" '[.entries[]? | select(.uuid == $uuid)] |
      if length == 1 then .[0] else empty end'
}

pod_summary_filter='def yes($t): any(.status.conditions[]?; .type == $t and .status == "True");
  {
    name: .metadata.name,
    uid: .metadata.uid,
    node: (.spec.nodeName // ""),
    instance: (.metadata.labels["ome.io/instance-index"] | tonumber),
    incarnation: (.metadata.labels["ome.io/instance-incarnation"] | tonumber),
    ready: yes("Ready"),
    serving: yes("ome.io/serving")
  }'

echo "Resetting the ${scenario} fixture"
"${kube[@]}" label nodes -l alfred-e2e/virtual=true "${maintenance_key}-" >/dev/null 2>&1 || true
if "${kube[@]}" get namespace "${namespace}" >/dev/null 2>&1; then
  "${kube[@]}" -n "${namespace}" delete inferenceservice "${isvc_name}" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null
  if "${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" >/dev/null 2>&1; then
    "${kube[@]}" -n "${namespace}" wait --for=delete \
      "inferencereplica/${ir_name}" --timeout=90s >/dev/null
  fi
  old_pods="$("${kube[@]}" -n "${namespace}" get pods \
    -l "ome.io/inferenceservice=${isvc_name},ome.io/managed-by=OMENative" \
    -o name)"
  if [[ -n "${old_pods}" ]]; then
    "${kube[@]}" -n "${namespace}" wait --for=delete pod \
      -l "ome.io/inferenceservice=${isvc_name},ome.io/managed-by=OMENative" \
      --timeout=90s >/dev/null
  fi
fi
"${kube[@]}" apply -f "${workload_manifest}" \
  >"${artifact_dir}/apply.txt"

echo "Waiting for the controller-created source Instance to be healthy"
ready_deadline=$((SECONDS + 180))
source_json=""
pretrigger_json=""
while (( SECONDS < ready_deadline )); do
  pod_list="$(pods_json 2>/dev/null || true)"
  ir="$("${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json 2>/dev/null || true)"
  isvc="$("${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" -o json 2>/dev/null || true)"
  if [[ -z "${pod_list}" || -z "${ir}" || -z "${isvc}" ]]; then
    sleep 1
    continue
  fi
  decoded_ir="$("${ir_status}" <<<"${ir}")"
  if [[ "${scenario}" == "maintenance-columnar" ]]; then
    while IFS= read -r initial_pod; do
      [[ -n "${initial_pod}" ]] || continue
      "${kube[@]}" -n "${namespace}" annotate pod "${initial_pod}" \
        alfred-e2e.ome.io/readiness=immediate --overwrite >/dev/null
    done < <(initial_release_names "${initial_count}" <<<"${pod_list}")
  fi
  candidate="$(jq -c --argjson instance 0 \
    '[.items[] | select((.metadata.labels["ome.io/instance-index"] | tonumber) == $instance)] |
     if length == 1 then .[0] else empty end' <<<"${pod_list}")"
  if [[ -z "${candidate}" ]]; then
    sleep 1
    continue
  fi
  summary="$(jq -c "${pod_summary_filter}" <<<"${candidate}")"
  revision="$(jq -r '.metadata.labels["ome.io/revision-hash"] // empty' <<<"${candidate}")"
  if [[ -z "${revision}" ]]; then sleep 1; continue; fi
  routing_service="${isvc_name}-engine-rev-${revision}"
  endpoints="$(routing_endpoints_json 2>/dev/null || true)"
  if [[ -z "${endpoints}" ]]; then sleep 1; continue; fi
  candidate_uid="$(jq -r '.uid' <<<"${summary}")"
  endpoint_ready="$(jq --arg uid "${candidate_uid}" '
    any(.items[].endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true)' \
    <<<"${endpoints}")"
  request_count="$(jq '[.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] | length' <<<"${isvc}")"
  if jq -e --argjson requestCount "${request_count}" --argjson expected "${initial_count}" \
      --argjson logical "${decoded_ir}" \
      '.status.readyReplicas == $expected and .status.servingReplicas == $expected and
       .status.availableReplicas == $expected and ($logical.rows | length) == $expected and
       all($logical.rows[]; .phase == "Ready") and
       any($logical.rows[]; .index == 0 and .phase == "Ready") and
       ((.status.migrations // []) | length) == 0 and $requestCount == 0' \
      <<<"${ir}" >/dev/null &&
     jq -e '.node != "" and .ready and .serving and .incarnation >= 1' \
      <<<"${summary}" >/dev/null; then
    if [[ "${endpoint_ready}" != "true" ]]; then
      sleep 1
      continue
    fi
    if [[ "${scenario}" == "maintenance-columnar" ]]; then
      if ! assert_initial_spread "${initial_count}" <<<"${pod_list}"; then
        echo "columnar fixture must have four distinct initial Pods on four distinct nodes; refusing maintenance" >&2
        exit 1
      fi
      if ! jq -e '.status.instanceStatusEncoding == "ColumnarV2" and
        (.status.instanceStatusColumns | type == "object") and
        (.status | has("instanceStatuses") | not)' <<<"${ir}" >/dev/null; then
        sleep 1
        continue
      fi
    fi
    baseline_uids="$(jq -c '[.items[].metadata.uid]' <<<"${pod_list}")"
    baseline_nodes="$(jq -c '[.items[].spec.nodeName]' <<<"${pod_list}")"
    source_json="$(jq -c --arg service "${routing_service}" '. + {endpointReady:true,routingService:$service}' <<<"${summary}")"
    pretrigger_json="$(jq -cn --argjson migrationRequestCount "${request_count}" \
      --argjson ready "$(jq '.status.readyReplicas' <<<"${ir}")" \
      --argjson serving "$(jq '.status.servingReplicas' <<<"${ir}")" \
      --argjson available "$(jq '.status.availableReplicas' <<<"${ir}")" \
      '{migrationRequestCount:$migrationRequestCount, irReadyReplicas:$ready,
        irServingReplicas:$serving, irAvailableReplicas:$available}')"
    break
  fi
  sleep 1
done
if [[ -z "${source_json}" ]]; then
  echo "source never reached Ready+Serving+Available with no migration" >&2
  exit 1
fi
source_node="$(jq -r '.node' <<<"${source_json}")"
source_uid="$(jq -r '.uid' <<<"${source_json}")"
if ! "${kube[@]}" get node "${source_node}" -o json |
  jq -e '.metadata.labels["alfred-e2e/virtual"] == "true"' >/dev/null; then
  echo "source scheduled on a non-virtual node: ${source_node}" >&2
  exit 1
fi
if [[ "${scenario}" == "maintenance-columnar" ]]; then
  virtual_nodes="$("${kube[@]}" get nodes -l alfred-e2e/virtual=true -o json)"
  jq -e --argjson nodes "${baseline_nodes}" \
    '[.items[].metadata.name] as $virtual | all($nodes[]; . as $node | ($virtual | index($node)) != null)' \
    <<<"${virtual_nodes}" >/dev/null || { echo "columnar baseline includes a non-virtual node" >&2; exit 1; }
fi

echo "Holding a healthy baseline for ${min_source_age_seconds}s (configured cooldown floor)"
age_deadline=$((SECONDS + min_source_age_seconds))
while (( SECONDS < age_deadline )); do
  assert_source_continuity || { echo "source terminating or unrouted during baseline" >&2; exit 1; }
  isvc="$("${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" -o json)"
  ir="$("${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json)"
  pod_list="$(pods_json)"
  endpoints="$(routing_endpoints_json)"
  if ! jq -e '[.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] | length == 0' <<<"${isvc}" >/dev/null ||
     ! jq -e --argjson expected "${initial_count}" '.status.readyReplicas == $expected and .status.servingReplicas == $expected and .status.availableReplicas == $expected and ((.status.migrations // []) | length) == 0' <<<"${ir}" >/dev/null ||
     ! jq -e --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid and any(.status.conditions[]?; .type == "Ready" and .status == "True") and any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True"))' <<<"${pod_list}" >/dev/null; then
    echo "healthy pre-trigger baseline changed during cooldown wait" >&2
    exit 1
  fi
  if ! jq -e --arg uid "${source_uid}" \
    'any(.items[].endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true)' \
    <<<"${endpoints}" >/dev/null; then
    echo "source left the real routing EndpointSlice before the trigger" >&2
    exit 1
  fi
  sleep 1
done

# Keep API objects byte-for-byte separate from the logical decoder view.
"${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json >"${artifact_dir}/ir-before-trigger.raw.json"
"${ir_status}" <"${artifact_dir}/ir-before-trigger.raw.json" >"${artifact_dir}/ir-before-trigger.decoded.json"
if [[ "${scenario}" == "maintenance-columnar" ]]; then
  pod_list="$(pods_json)"
  assert_initial_spread "${initial_count}" <<<"${pod_list}" || { echo "columnar baseline spread changed before trigger" >&2; exit 1; }
  jq -e --argjson baseline "${baseline_uids}" '[.items[].metadata.uid] | sort == ($baseline | sort)' \
    <<<"${pod_list}" >/dev/null || { echo "columnar baseline UIDs changed before trigger" >&2; exit 1; }
  jq -e '.status.instanceStatusEncoding == "ColumnarV2" and
    (.status.instanceStatusColumns | type == "object") and (.status | has("instanceStatuses") | not) and
    .status.readyReplicas == 4 and .status.servingReplicas == 4 and .status.availableReplicas == 4' \
    "${artifact_dir}/ir-before-trigger.raw.json" >/dev/null || { echo "columnar raw baseline changed before trigger" >&2; exit 1; }
fi

if [[ "${delayed}" == true ]]; then delayed_request_prepare; fi
request_watch_file="${artifact_dir}/request-annotation-watch.jsonl"
request_watch_stderr="${artifact_dir}/request-annotation-watch.stderr"
watch_timeout_seconds="$(annotation_watch_timeout_seconds "${deadline_seconds}")"
"${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" \
  --watch --request-timeout="${watch_timeout_seconds}s" -o json \
  >"${request_watch_file}" 2>"${request_watch_stderr}" &
request_watch_pid=$!
sleep 1
assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"

triggered_node="${source_node}"
if [[ "${scenario}" == "unhealthy-single" ]]; then
  echo "Observing the one-minute False recovery quarantine without evacuation"
  # Remove only the harness condition to let KWOK create a fresh False
  # transition on reruns; no OME request or migration status is synthesized.
  node="$("${kube[@]}" get node "${source_node}" -o json)"
  condition_index="$(jq -r '.status.conditions | to_entries[] |
    select(.value.type == "GpuUnhealthy") | .key' <<<"${node}")"
  "${kube[@]}" annotate node "${source_node}" alfred-e2e.ome.io/gpu-health- >/dev/null 2>&1 || true
  if [[ -n "${condition_index}" ]]; then
    "${kube[@]}" patch node "${source_node}" --subresource=status --type=strategic \
      -p '{"status":{"conditions":[{"type":"GpuUnhealthy","$patch":"delete"}]}}' >/dev/null
  fi
  "${kube[@]}" annotate node "${source_node}" alfred-e2e.ome.io/gpu-health=healthy --overwrite >/dev/null
  recovery_ready_deadline=$((SECONDS + 30))
  recovery_transition=""
  while (( SECONDS < recovery_ready_deadline )); do
    assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
    recovery_transition="$("${kube[@]}" get node "${source_node}" -o json |
      jq -r '.status.conditions[] | select(.type == "GpuUnhealthy" and .status == "False") | .lastTransitionTime')"
    if [[ -n "${recovery_transition}" ]]; then break; fi
    sleep 0.2
  done
  if [[ -z "${recovery_transition}" ]]; then echo "KWOK did not create False recovery signal" >&2; exit 1; fi
  recovery_start=${SECONDS}
  recovery_deadline=$((SECONDS + 60))
  recovery_suspect=false
  while (( SECONDS < recovery_deadline )); do
    assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
    assert_source_continuity || { echo "source lost readiness/routing in recovery window" >&2; exit 1; }
    isvc="$("${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" -o json)"
    ir="$("${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json)"
    if ! jq -e '[.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] | length == 0' <<<"${isvc}" >/dev/null ||
       ! jq -e '((.status.migrations // []) | length) == 0' <<<"${ir}" >/dev/null; then
      echo "unexpected movement during False recovery suspicion window" >&2; exit 1
    fi
    window_request_count="$(jq -cs '[.[] | .metadata.annotations // {} | keys[] |
      select(startswith("ome.io/migration-request-v1-"))] | unique | length' "${request_watch_file}" 2>/dev/null || true)"
    if [[ -n "${window_request_count}" && "${window_request_count}" != "0" ]]; then
      echo "annotation watch observed a transient request during recovery quarantine" >&2; exit 1
    fi
    cm="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json 2>/dev/null || true)"
    if alfred_node_state_is "${cm}" "${source_node}" Suspect; then recovery_suspect=true; fi
    sleep 0.2
  done
  if [[ "${recovery_suspect}" != "true" ]]; then echo "Alfred never observed recovery quarantine" >&2; exit 1; fi
  window_request_count="$(jq -cs '[.[] | .metadata.annotations // {} | keys[] |
    select(startswith("ome.io/migration-request-v1-"))] | unique | length' "${request_watch_file}")"
  if [[ "${window_request_count}" != "0" ]]; then echo "request observed before unhealthy trigger" >&2; exit 1; fi
  health_json="$(jq -cn --argjson observed "$((SECONDS - recovery_start))" --arg recoveryTransition "${recovery_transition}" \
    '{recoveryTransitionTime:$recoveryTransition,recoveryWindowSeconds:60,recoveryObservedSeconds:$observed,recoveryRequestCount:0,
      recoveryMigrationCount:0,recoverySourceReady:true,recoverySourceServing:true,
      recoverySourceEndpointReady:true,recoverySuspectObserved:true}')"
  echo "Triggering GpuUnhealthy=True on ${source_node}"
  "${kube[@]}" annotate node "${source_node}" alfred-e2e.ome.io/gpu-health=unhealthy --overwrite >/dev/null
else
  echo "Triggering planned maintenance on ${source_node}"
  "${kube[@]}" label node "${source_node}" "${maintenance_key}=${maintenance_value}" --overwrite >/dev/null
fi

request_json=""
recommendation_json=""
request_deadline=$((SECONDS + deadline_seconds))
while (( SECONDS < request_deadline )); do
  assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
  if [[ -s "${request_watch_file}" ]]; then
    request_json="$(jq -cs '
      [.[] | .metadata.annotations // {} | to_entries[] |
       select(.key | startswith("ome.io/migration-request-v1-")) |
       {uuid:(.key | sub("^ome.io/migration-request-v1-"; "")), annotationKey:.key, payload:(.value | fromjson)}] |
      unique_by(.uuid) | if length == 1 then .[0] else empty end' \
      "${request_watch_file}" 2>/dev/null || true)"
  fi
  if [[ -n "${request_json}" ]]; then
    request_uuid="$(jq -r '.uuid' <<<"${request_json}")"
    recommendations="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations \
      -o jsonpath='{.data.last-cycle\.json}' 2>/dev/null || true)"
    if [[ -n "${recommendations}" ]]; then
      recommendation_json="$(jq -c --arg uuid "${request_uuid}" \
        '[.recommendations[]? | select(.requestUUID == $uuid)] | last // empty' \
        <<<"${recommendations}" 2>/dev/null || true)"
    fi
    if [[ -n "${recommendation_json}" ]]; then
      break
    fi
  fi
  sleep 1
done
if [[ -z "${request_json}" || -z "${recommendation_json}" ]]; then
  echo "Alfred did not publish one UUID-backed migration request before the deadline" >&2
  exit 1
fi
request_uuid="$(jq -r '.uuid' <<<"${request_json}")"
if ! jq -e --arg node "${source_node}" --arg reason "${expected_reason}" \
  '.payload.schemaVersion == "v1" and .payload.component == "engine" and
   .payload.instance == 0 and .payload.from_node == $node and
   .payload.requested_by == "alfred" and .payload.reason == $reason' \
  <<<"${request_json}" >/dev/null; then
  echo "migration annotation payload does not identify the real source" >&2
  exit 1
fi

if [[ "${delayed}" == true ]]; then delayed_request_resume; fi
echo "Following request ${request_uuid} through surge, drain, and completion"
surge_json=""
completed_json=""
replacement_released=false
while (( SECONDS < request_deadline )); do
  assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
  pod_list="$(pods_json 2>/dev/null || true)"
  ir="$("${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json 2>/dev/null || true)"
  if [[ -z "${pod_list}" || -z "${ir}" ]]; then
    sleep 0.2
    continue
  fi

  if [[ "${replacement_released}" == "true" ]]; then
    assert_handoff_continuity || {
      echo "source lost Ready+Serving+routing before the held replacement became healthy and routed" >&2
      exit 1
    }
  else
    assert_source_continuity || { echo "source terminating or unrouted before replacement release" >&2; exit 1; }
  fi

  source_present="$(jq --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid)' <<<"${pod_list}")"
  source_ready="$(jq --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid and any(.status.conditions[]?; .type == "Ready" and .status == "True"))' <<<"${pod_list}")"
  source_serving="$(jq --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid and any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True"))' <<<"${pod_list}")"
  replacement="$(replacement_pod "${baseline_uids}" <<<"${pod_list}")"
  if [[ "${replacement_released}" == "true" ]]; then
    replacement="$(jq -c --arg uid "$(jq -r '.replacement.uid' <<<"${surge_json}")" \
      '.items[] | select(.metadata.uid == $uid)' <<<"${pod_list}")"
  fi

  if [[ -n "${replacement}" ]]; then
    replacement_json="$(jq -c "${pod_summary_filter}" <<<"${replacement}")"
    replacement_ready="$(jq -r '.ready and .serving' <<<"${replacement_json}")"
    if [[ "${replacement_ready}" != "true" ]] &&
       [[ "${source_present}" != "true" || "${source_ready}" != "true" || "${source_serving}" != "true" ]]; then
      echo "source disappeared or left routing before the replacement was Ready+Serving" >&2
      exit 1
    fi
    if [[ -z "${surge_json}" && "${replacement_ready}" == "true" ]]; then
      echo "replacement became Ready before the controlled KWOK hold" >&2
      exit 1
    fi
    if [[ -z "${surge_json}" && "${source_present}" == "true" && "${source_ready}" == "true" && "${source_serving}" == "true" ]] &&
       jq -e --arg source "${source_node}" '.node != "" and .node != $source' <<<"${replacement_json}" >/dev/null; then
      recommendations_cm="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json 2>/dev/null || true)"
      node_record="$(jq -r --arg key "node.${source_node}" '.data[$key] // empty' <<<"${recommendations_cm:-null}" 2>/dev/null || true)"
      if [[ -n "${node_record}" ]] && jq -e --arg scenario "${scenario}" '
        (if $scenario == "unhealthy-single" then .state == "Unhealthy" and .drainedAt == null
         else .maintenance.requested == true and .maintenanceDrainedAt == null end) and
        .omeGpuOccupantsPresent == true' <<<"${node_record}" >/dev/null; then
        echo "Holding replacement $(jq -r '.name' <<<"${replacement_json}") not-ready for ${surge_hold_seconds}s"
        hold_deadline=$((SECONDS + surge_hold_seconds))
        while (( SECONDS < hold_deadline )); do
          assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
          assert_source_continuity || { echo "source terminating or unrouted during held replacement" >&2; exit 1; }
          held_pods="$(pods_json)"
          held_source_present="$(jq --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid)' <<<"${held_pods}")"
          held_source_ready="$(jq --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid and any(.status.conditions[]?; .type == "Ready" and .status == "True"))' <<<"${held_pods}")"
          held_source_serving="$(jq --arg uid "${source_uid}" 'any(.items[]; .metadata.uid == $uid and any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True"))' <<<"${held_pods}")"
          held_replacement="$(jq -c --arg uid "$(jq -r '.uid' <<<"${replacement_json}")" \
            '[.items[] | select(.metadata.uid == $uid)] | if length == 1 then .[0] else empty end' <<<"${held_pods}")"
          if [[ -z "${held_replacement}" || "${held_source_present}" != "true" ||
                "${held_source_ready}" != "true" || "${held_source_serving}" != "true" ]]; then
            echo "source was not continuously Ready+Serving during the held surge" >&2
            exit 1
          fi
          held_replacement_json="$(jq -c "${pod_summary_filter}" <<<"${held_replacement}")"
          jq -e --arg node "$(jq -r '.node' <<<"${replacement_json}")" \
            '.node == $node and .node != ""' <<<"${held_replacement_json}" >/dev/null || {
            echo "held replacement lost its verified target binding" >&2; exit 1;
          }
          if jq -e '.ready or .serving' <<<"${held_replacement_json}" >/dev/null; then
            echo "KWOK did not keep the replacement held not-ready" >&2
            exit 1
          fi
          held_endpoints="$(routing_endpoints_json)"
          if ! jq -e --arg uid "${source_uid}" \
              'any(.items[].endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true)' \
              <<<"${held_endpoints}" >/dev/null ||
             jq -e --arg uid "$(jq -r '.uid' <<<"${held_replacement_json}")" \
              'any(.items[].endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true)' \
              <<<"${held_endpoints}" >/dev/null; then
            echo "EndpointSlice routing changed before the held replacement was released" >&2
            exit 1
          fi
          recommendations_cm="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json)"
          held_node_record="$(jq -r --arg key "node.${source_node}" '.data[$key] // empty' <<<"${recommendations_cm}")"
          if [[ -z "${held_node_record}" ]] || ! jq -e --arg scenario "${scenario}" '
            .maintenanceDrainedAt == null and .omeGpuOccupantsPresent == true and
            ($scenario != "unhealthy-single" or .drainedAt == null)' \
            <<<"${held_node_record}" >/dev/null; then
            echo "Alfred reported the maintenance node drained while its source was serving" >&2
            exit 1
          fi
          sleep 0.2
        done
        replacement_json="${held_replacement_json}"
        surge_json="$(jq -cn --argjson replacement "${replacement_json}" \
          --argjson holdSeconds "${surge_hold_seconds}" \
          '{observed:true, holdSeconds:$holdSeconds,
            sourcePresent:true, sourceReady:true, sourceServing:true,
            sourceEndpointReady:true,
            replacementReady:false, replacementServing:false,
            replacementEndpointReady:false,
            alfredMaintenanceDrainedAt:null, alfredOMEGPUOccupantsPresent:true,
            replacement:$replacement}')"
        if [[ "${scenario}" == "unhealthy-single" ]]; then
          health_condition="$("${kube[@]}" get node "${source_node}" -o json |
            jq -c '.status.conditions[] | select(.type == "GpuUnhealthy" and .status == "True")')"
          if [[ -z "${health_condition}" ]]; then echo "missing actual True health condition" >&2; exit 1; fi
          health_transition="$(jq -r '.lastTransitionTime' <<<"${health_condition}")"
          health_heartbeat="$(jq -r '.lastHeartbeatTime' <<<"${health_condition}")"
          heartbeat_deadline=$((SECONDS + 25))
          heartbeat_advanced=false
          while (( SECONDS < heartbeat_deadline )); do
            assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
            assert_source_continuity || { echo "source lost routing during health heartbeat observation" >&2; exit 1; }
            heartbeat_node_record="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json |
              jq -r --arg key "node.${source_node}" '.data[$key] // empty')"
            if [[ -z "${heartbeat_node_record}" ]] || ! jq -e '
              .drainedAt == null and .omeGpuOccupantsPresent == true' <<<"${heartbeat_node_record}" >/dev/null; then
              echo "Alfred reported the unhealthy node drained during held heartbeat observation" >&2; exit 1
            fi
            health_condition="$("${kube[@]}" get node "${source_node}" -o json |
              jq -c '.status.conditions[] | select(.type == "GpuUnhealthy" and .status == "True")')"
            if [[ "$(jq -r '.lastTransitionTime' <<<"${health_condition}")" != "${health_transition}" ]]; then
              echo "heartbeat changed GPU-health lastTransitionTime" >&2; exit 1
            fi
            if [[ "$(jq -r '.lastHeartbeatTime' <<<"${health_condition}")" != "${health_heartbeat}" ]]; then heartbeat_advanced=true; break; fi
            sleep 0.2
          done
          if [[ "${heartbeat_advanced}" != "true" ]]; then echo "KWOK heartbeat never advanced" >&2; exit 1; fi
          health_json="$(jq -c --arg transition "${health_transition}" \
            '. + {conditionType:"GpuUnhealthy",conditionStatus:"True",transitionTime:$transition,
              transitionTimeAfterHeartbeat:$transition,heartbeatAdvanced:true}' <<<"${health_json}")"
        fi
        if [[ "${scenario}" == "restart-single" ]]; then
          echo "Restarting only ome-alfred with request ${request_uuid} held in-flight"
          before_dispatch="$(dispatch_for_request)"
          if ! jq -e '.phase == "submitted" or .phase == "acknowledged"' <<<"${before_dispatch}" >/dev/null; then
            echo "request was not durably in-flight before restart" >&2; exit 1
          fi
          printf '%s\n' "${before_dispatch}" >"${artifact_dir}/dispatch-before-restart.json"
          before_pod="$("${kube[@]}" -n "${alfred_namespace}" get pods -l app.kubernetes.io/name=ome-alfred -o json |
            jq -c '[.items[] | select(.metadata.deletionTimestamp == null)] | if length == 1 then .[0] else empty end')"
          old_pod_uid="$(jq -r '.metadata.uid' <<<"${before_pod}")"
          old_lease_holder="$("${kube[@]}" -n "${alfred_namespace}" get lease alfred.ome.io -o jsonpath='{.spec.holderIdentity}')"
          "${kube[@]}" -n "${alfred_namespace}" rollout restart deployment/ome-alfred >"${artifact_dir}/restart.txt"
          recovery_deadline=$((SECONDS + 120))
          recovered=false
          while (( SECONDS < recovery_deadline && SECONDS < request_deadline )); do
            assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
            assert_source_continuity || { echo "source lost readiness/routing during Alfred restart" >&2; exit 1; }
            if ! pods_json | jq -e --arg uid "$(jq -r '.uid' <<<"${replacement_json}")" \
              'any(.items[]; .metadata.uid == $uid and
                all(.status.conditions[]?; (.type != "Ready" and .type != "ome.io/serving") or .status != "True"))' >/dev/null; then
              echo "replacement was not continuously held during Alfred restart" >&2; exit 1
            fi
            after_pod="$("${kube[@]}" -n "${alfred_namespace}" get pods -l app.kubernetes.io/name=ome-alfred -o json |
              jq -c --arg old "${old_pod_uid}" '[.items[] | select(.metadata.uid != $old and .metadata.deletionTimestamp == null and
                any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | if length == 1 then .[0] else empty end')"
            new_lease_holder="$("${kube[@]}" -n "${alfred_namespace}" get lease alfred.ome.io -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)"
            after_dispatch="$(dispatch_for_request 2>/dev/null || true)"
            if [[ -n "${after_pod}" && -n "${new_lease_holder}" && "${new_lease_holder}" != "${old_lease_holder}" && -n "${after_dispatch}" ]] &&
               jq -e '.phase == "submitted" or .phase == "acknowledged"' <<<"${after_dispatch}" >/dev/null; then
              recovered=true; break
            fi
            sleep 0.2
          done
          if [[ "${recovered}" != "true" ]]; then echo "Alfred did not recover leadership and same in-flight journal UUID" >&2; exit 1; fi
          printf '%s\n' "${after_dispatch}" >"${artifact_dir}/dispatch-after-restart.json"
          restart_json="$(jq -cn --arg old "${old_pod_uid}" --arg new "$(jq -r '.metadata.uid' <<<"${after_pod}")" \
            --arg uuid "${request_uuid}" '{deployment:"ome-alfred",oldPodUID:$old,newPodUID:$new,
              journalUUIDBefore:$uuid,journalUUIDAfter:$uuid,leadershipRecovered:true,
              sourceReady:true,sourceServing:true,sourceEndpointReady:true}')"
        fi
        replacement_name="$(jq -r '.name' <<<"${replacement_json}")"
        "${kube[@]}" -n "${namespace}" annotate pod "${replacement_name}" \
          alfred-e2e.ome.io/readiness=immediate --overwrite >/dev/null
        replacement_released=true
        echo "Released replacement ${replacement_name} to KWOK readiness"
      fi
    fi
  fi

  migration="$(jq -c --arg uuid "${request_uuid}" \
    '[.status.migrations[]? | select(.requestUUID == $uuid)] | if length == 1 then .[0] else empty end' \
    <<<"${ir}")"
  if [[ -n "${migration}" && "$(jq -r '.phase' <<<"${migration}")" == "Failed" ]]; then
    echo "InferenceReplica reported migration failure: $(jq -r '.message // ""' <<<"${migration}")" >&2
    exit 1
  fi
  if [[ "${replacement_released}" == "true" && -n "${migration}" && "$(jq -r '.phase' <<<"${migration}")" == "Completed" && -n "${replacement:-}" ]]; then
    replacement_json="$(jq -c "${pod_summary_filter}" <<<"${replacement}")"
    completed_endpoints="$(routing_endpoints_json)"
    source_endpoint_present="$(jq --arg uid "${source_uid}" \
      'any(.items[].endpoints[]?; .targetRef.uid == $uid)' <<<"${completed_endpoints}")"
    replacement_endpoint_ready="$(jq --arg uid "$(jq -r '.uid' <<<"${replacement_json}")" \
      'any(.items[].endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true)' \
      <<<"${completed_endpoints}")"
    if [[ "${source_present}" == "false" ]] && jq -e --arg node "$(jq -r '.replacement.node' <<<"${surge_json}")" '.ready and .serving and .node != "" and .node == $node' <<<"${replacement_json}" >/dev/null &&
       [[ "${source_endpoint_present}" == "false" && "${replacement_endpoint_ready}" == "true" ]] &&
       jq -e --argjson expected "${initial_count}" '.status.readyReplicas == $expected and .status.servingReplicas == $expected and .status.availableReplicas == $expected' <<<"${ir}" >/dev/null; then
      decoded_ir="$("${ir_status}" <<<"${ir}")"
      if [[ "${scenario}" == "maintenance-columnar" ]] && ! jq -e \
        '.rawEncoding == "ColumnarV2" and (.rows | length) == 4 and all(.rows[]; .phase == "Ready")' \
        <<<"${decoded_ir}" >/dev/null; then
        sleep 0.2
        continue
      fi
      printf '%s\n' "${ir}" >"${artifact_dir}/ir-completed.raw.json"
      printf '%s\n' "${decoded_ir}" >"${artifact_dir}/ir-completed.decoded.json"
      completed_json="$(jq -cn --argjson migration "${migration}" \
        --argjson replacement "${replacement_json}" \
        --argjson ready "$(jq '.status.readyReplicas' <<<"${ir}")" \
        --argjson serving "$(jq '.status.servingReplicas' <<<"${ir}")" \
        --argjson available "$(jq '.status.availableReplicas' <<<"${ir}")" \
        '{migration:$migration, sourcePresent:false, sourceEndpointPresent:false,
          replacementPresent:true,
          replacementReady:$replacement.ready, replacementServing:$replacement.serving,
          replacementEndpointReady:true,
          irReadyReplicas:$ready, irServingReplicas:$serving, irAvailableReplicas:$available}')"
      break
    fi
  fi
  sleep 0.2
done
if [[ -z "${surge_json}" || -z "${completed_json}" ]]; then
  echo "migration did not produce an observed safe surge and healthy completion" >&2
  exit 1
fi

replacement_node="$(jq -r '.replacement.node' <<<"${surge_json}")"
if [[ "${replacement_node}" == "${source_node}" ]]; then
  echo "replacement was scheduled back onto the maintenance source" >&2
  exit 1
fi
if [[ "${delayed}" == true ]]; then
  delayed_request_evidence_json="$(jq -c --argjson replacement "$(jq '.replacement' <<<"${surge_json}")" \
    '.replacement=$replacement' <<<"${delayed_request_evidence_json}")"
  if [[ "${scenario}" == hint-exhaustion-single ]]; then
    jq -e '. as $e | .replacement.node == .fallback and (.request.payload.hint_target_nodes | index($e.replacement.node)) == null' <<<"${delayed_request_evidence_json}" >/dev/null
  else
    replacement_health="$("${kube[@]}" get node "${replacement_node}" -o json)"
    delayed_request_evidence_json="$(jq -c --argjson node "${replacement_health}" '.replacementNodeAfter=$node' <<<"${delayed_request_evidence_json}")"
    jq -e '. as $e | (.request.payload.hint_target_nodes | index($e.replacement.node)) != null and .replacementNodeAfter.metadata.name == .replacement.node and any(.replacementNodeAfter.status.conditions[]?;.type=="GpuUnhealthy" and .status=="True")' <<<"${delayed_request_evidence_json}" >/dev/null
  fi
  printf '%s\n' "${delayed_request_evidence_json}" >"${artifact_dir}/delayed-request-evidence.json"
fi

echo "Waiting for Alfred's durable completed dispatch and drained-node observation"
alfred_json=""
while (( SECONDS < request_deadline )); do
  assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
  recommendations_cm="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json 2>/dev/null || true)"
  dispatch_raw="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-dispatch-state \
    -o jsonpath='{.data.state\.json}' 2>/dev/null || true)"
  if [[ -z "${recommendations_cm}" || -z "${dispatch_raw}" ]]; then
    sleep 1
    continue
  fi
  node_record="$(jq -r --arg key "node.${source_node}" '.data[$key] // empty' <<<"${recommendations_cm}")"
  dispatch_entry="$(jq -c --arg uuid "${request_uuid}" \
    '[.entries[]? | select(.uuid == $uuid)] | if length == 1 then .[0] else empty end' \
    <<<"${dispatch_raw}" 2>/dev/null || true)"
  if [[ -n "${node_record}" && -n "${dispatch_entry}" ]] &&
     jq -e --arg scenario "${scenario}" '(if $scenario == "unhealthy-single" then
       .state == "Unhealthy" and .signaledAt != null and .drainedAt != null
       else .maintenance.requested == true and
       .maintenance.triggers == ["patching"] and
       .maintenanceRequestedAt != null and .maintenanceDrainedAt != null end) and
       .omeGpuOccupantsPresent == false' <<<"${node_record}" >/dev/null &&
     jq -e '.phase == "completed" and .completedAt != null' <<<"${dispatch_entry}" >/dev/null; then
    alfred_json="$(jq -cn --argjson recommendation "${recommendation_json}" \
      --argjson node "${node_record}" --argjson dispatch "${dispatch_entry}" \
      '{workload:$recommendation.workload, component:$recommendation.component,
        instance:$recommendation.instance, policy:$recommendation.policy,
        reason:$recommendation.reason, fromNode:$recommendation.fromNode,
        requestUUID:$dispatch.uuid, dispatchStatus:$dispatch.phase,
        maintenanceTriggers:$node.maintenance.triggers,
        maintenanceRequestedAt:$node.maintenanceRequestedAt,
        maintenanceDrainedAt:$node.maintenanceDrainedAt,
        healthState:$node.state, signaledAt:$node.signaledAt, drainedAt:$node.drainedAt,
        omeGPUOccupantsPresent:$node.omeGpuOccupantsPresent}')"
    break
  fi
  sleep 1
done
if [[ -z "${alfred_json}" ]]; then
  echo "Alfred never durably observed the completed request and drained maintenance node" >&2
  exit 1
fi

if [[ "${scenario}" == "restart-single" ]]; then
  # Observe two further decision-loop cycles before ruling out redispatch.
  duplicate_deadline=$((SECONDS + 5))
  while (( SECONDS < duplicate_deadline )); do
    assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
    ir="$("${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json)"
    dispatch_raw="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-dispatch-state -o jsonpath='{.data.state\.json}')"
    migration_count="$(jq '(.status.migrations // []) | length' <<<"${ir}")"
    dispatch_count="$(jq --arg iruid "$(jq -r '.metadata.uid' <<<"${ir}")" '[.entries[]? | select(.irUID == $iruid)] | length' <<<"${dispatch_raw}")"
    unique_request_count="$(jq -cs '[.[] | .metadata.annotations // {} | keys[] |
      select(startswith("ome.io/migration-request-v1-"))] | unique | length' "${request_watch_file}")"
    if [[ "${migration_count}" != "1" || "${dispatch_count}" != "1" || "${unique_request_count}" != "1" ]]; then
      echo "restart produced duplicate migrations, dispatches, or request UUIDs" >&2; exit 1
    fi
    sleep 0.2
  done
  restart_json="$(jq -c --argjson migrationCount "${migration_count}" --argjson dispatchCount "${dispatch_count}" \
    --argjson requestCount "${unique_request_count}" '. + {migrationCount:$migrationCount,
      workloadDispatchCount:$dispatchCount,uniqueRequestCount:$requestCount}' <<<"${restart_json}")"
fi

evidence="${artifact_dir}/evidence.json"
assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"
if [[ "${scenario}" == "maintenance-columnar" ]]; then
  unique_request_count="$(jq -cs '[.[] | .metadata.annotations // {} | keys[] |
    select(startswith("ome.io/migration-request-v1-"))] | unique | length' "${request_watch_file}")"
  columnar_json="$(jq -cn --argjson initialCount "${initial_count}" --argjson baselineUIDs "${baseline_uids}" \
    --argjson baselineNodes "${baseline_nodes}" --argjson uniqueRequestCount "${unique_request_count}" \
    --slurpfile before "${artifact_dir}/ir-before-trigger.raw.json" \
    --slurpfile completed "${artifact_dir}/ir-completed.raw.json" \
    --slurpfile decodedBefore "${artifact_dir}/ir-before-trigger.decoded.json" \
    --slurpfile decodedCompleted "${artifact_dir}/ir-completed.decoded.json" \
    '{initialCount:$initialCount,baselineUIDs:$baselineUIDs,baselineNodes:$baselineNodes,
      uniqueRequestCount:$uniqueRequestCount,rawBeforeTrigger:$before[0],rawCompleted:$completed[0],
      decodedBeforeTrigger:$decodedBefore[0],decodedCompleted:$decodedCompleted[0]}')"
fi
jq -n --arg scenario "${scenario}" --argjson source "${source_json}" \
  --argjson preTrigger "${pretrigger_json}" --argjson request "${request_json}" \
  --argjson surge "${surge_json}" --argjson completed "${completed_json}" \
  --argjson alfred "${alfred_json}" \
  --argjson health "${health_json}" --argjson restart "${restart_json}" \
  --argjson columnar "${columnar_json}" \
  --slurpfile handoff "${artifact_dir}/handoff-samples.jsonl" \
  '{scenario:$scenario, source:$source, preTrigger:$preTrigger, request:$request,
    surge:$surge, handoff:$handoff, completed:$completed, alfred:$alfred,health:$health,restart:$restart,columnar:$columnar}' >"${evidence}"

# Delayed variants retain the same complete maintenance handoff invariant;
# their separate evidence additionally characterizes the mailbox race.
if ! jq 'if .scenario == "hint-exhaustion-single" or .scenario == "target-health-race-single" then .scenario="maintenance-single" else . end' "${evidence}" | jq -e -f "${script_dir}/verify-evidence.jq" >/dev/null; then
  echo "captured evidence failed the acceptance verifier" >&2
  exit 1
fi
assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}"

dump_diagnostics
passed=true
if [[ "${scenario}" == target-health-race-single ]]; then
  echo "REPRODUCED: replacement landed on a target that became unhealthy after submission (not a safety pass)"
else
  echo "${scenario} passed"
fi
echo "evidence: ${evidence}"
