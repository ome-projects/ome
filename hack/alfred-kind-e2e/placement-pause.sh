#!/usr/bin/env bash
# Sourced only by scenario.sh placement-pause-single. Fixture authority is
# written through the public ISVC metadata contract with the harness identity.
# All IR spec/status, requests, journal entries and pods come from controllers.

placement_snapshot() {
  local service replica current_pods current_endpoints recommendations dispatch requests
  assert_annotation_watch_alive "${request_watch_pid}" "${request_watch_stderr}" || return 1
  service="$("${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" -o json)" || return 1
  replica="$("${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json)" || return 1
  current_pods="$(pods_json)" || return 1
  current_endpoints="$(routing_endpoints_json)" || return 1
  recommendations="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-recommendations -o json)" || return 1
  dispatch="$("${kube[@]}" -n "${alfred_namespace}" get configmap alfred-dispatch-state -o json)" || return 1
  # Keep the original complete watch in the artifacts too. A read/parse error
  # fails the scenario; it must never become a fabricated zero-request sample.
  requests="$(jq -cs '[.[] | .metadata.annotations // {} | to_entries[] |
    select(.key | startswith("ome.io/migration-request-v1-"))] | unique_by(.key)' "${request_watch_file}")" || return 1
  jq -cn --argjson isvc "${service}" --argjson ir "${replica}" \
    --argjson pods "${current_pods}" --argjson endpoints "${current_endpoints}" \
    --argjson recommendations "${recommendations}" --argjson dispatch "${dispatch}" \
    --argjson requests "${requests}" \
    '{isvc:$isvc,ir:$ir,pods:$pods,endpoints:$endpoints,recommendations:$recommendations,dispatch:$dispatch,requests:$requests}'
}

placement_check() {
  local phase="$1" sample="$2"
  jq -e --arg phase "${phase}" --argjson source "${source_json}" \
    --arg isvcUID "${placement_isvc_uid}" --arg irUID "${placement_ir_uid}" \
    --arg uuid "${request_uuid:-}" --argjson replacement "${replacement_json:-null}" \
    -L "${script_dir}" 'include "placement-pause-sample";
      placement_sample($phase;$source;$isvcUID;$irUID;$uuid;$replacement)' <<<"${sample}" >/dev/null
}

placement_set_policy() {
  local revision="$1" paused="$2" envelope
  envelope="$(jq -cn --argjson revision "${revision}" --argjson paused "${paused}" \
    '{planID:"alfred-e2e-pause",revision:$revision,sourceUID:"alfred-e2e-source",clusterUID:"alfred-e2e-member",pauseSurge:$paused}')"
  "${kube[@]}" -n "${namespace}" annotate inferenceservice "${isvc_name}" \
    "ome.io/placement-execution=${envelope}" --overwrite >/dev/null
}

placement_wait_projection() {
  local phase="$1" revision="$2" paused="$3" sample deadline
  deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    sample="$(placement_snapshot)"
    printf '%s\n' "${sample}" >>"${artifact_dir}/placement-${phase}-projection.jsonl"
    # The source must stay safe through projection; writes may be observed at
    # different resource versions, so allow the expected projection to settle.
    placement_check source-safe "${sample}" || return 1
    if jq -e --argjson revision "${revision}" --argjson paused "${paused}" '
      .ir.spec.placementExecution.revision == $revision and
      .ir.spec.placementExecution.pauseSurge == $paused and
      .ir.status.placementObservedGeneration == .ir.metadata.generation' <<<"${sample}" >/dev/null; then
      placement_check "${phase}" "${sample}" || return 1
      printf '%s\n' "${sample}" >"${artifact_dir}/placement-${phase}.json"
      return 0
    fi
    sleep 0.2
  done
  echo "placement ${phase} did not project and acknowledge within 60s" >&2
  return 1
}

placement_pause_observe_and_release() {
  local sample deadline cycles=0 previous='' timestamp
  placement_isvc_uid="$(jq -r '.metadata.uid' <<<"${isvc}")"
  placement_ir_uid="$(jq -r '.metadata.uid' <<<"${ir}")"
  [[ -n "${placement_isvc_uid}" && "${placement_isvc_uid}" != null && -n "${placement_ir_uid}" && "${placement_ir_uid}" != null ]] || return 1
  # The existing baseline saved the real fresh IR before maintenance. Require
  # pause to have existed there, not merely after a later annotation patch.
  jq -e --arg uid "${placement_ir_uid}" '
    .metadata.uid == $uid and .spec.placementReplicaLimit == 1 and
    .spec.placementExecution == {planID:"alfred-e2e-pause",revision:1,
      sourceUID:"alfred-e2e-source",clusterUID:"alfred-e2e-member",pauseSurge:true}' \
    "${artifact_dir}/ir-before-trigger.raw.json" >/dev/null
  echo 'Observing placement pause across three distinct Alfred decision cycles'
  deadline=$((SECONDS + 60))
  while (( SECONDS < deadline && cycles < 3 )); do
    sample="$(placement_snapshot)"
    printf '%s\n' "${sample}" >>"${artifact_dir}/placement-paused-api-samples.jsonl"
    # Check absence of effects on every sample, even before the first advisory.
    placement_check paused-safe "${sample}" || return 1
    timestamp="$(jq -r '.recommendations.data["last-cycle.json"] | fromjson | .timestamp' <<<"${sample}")"
    if [[ "${timestamp}" != "${previous}" ]] && placement_check paused "${sample}"; then
      printf '%s\n' "${sample}" >>"${artifact_dir}/placement-paused-cycles.jsonl"
      previous="${timestamp}"
      cycles=$((cycles + 1))
    fi
    sleep 0.2
  done
  (( cycles == 3 )) || { echo 'missing three placement-pause advisory cycles' >&2; return 1; }
  placement_set_policy 2 false
  placement_wait_projection released 2 false
}

placement_pause_allocated() {
  local sample deadline
  deadline=$((SECONDS + 30))
  # This proves controller allocation before publishing pause revision 3.
  # Seeing a request alone is insufficient: that would test a queued request.
  # Pod binding can race the journal/status and KWOK condition observations.
  # Retain every sample and wait for a complete witness with the source safe.
  while (( SECONDS < deadline )); do
    sample="$(placement_snapshot)"
    printf '%s\n' "${sample}" >>"${artifact_dir}/placement-allocation-api-samples.jsonl"
    placement_check source-safe "${sample}" || return 1
    if placement_check allocated "${sample}"; then break; fi
    sleep 0.2
  done
  placement_check allocated "${sample}" || { echo 'no allocated held surge witness' >&2; return 1; }
  printf '%s\n' "${sample}" >"${artifact_dir}/placement-allocated.json"
  placement_set_policy 3 true
  placement_wait_projection repaused 3 true
}

placement_pause_complete() {
  local sample deadline cycles=0 previous timestamp
  sample="$(placement_snapshot)"
  placement_check completed "${sample}" || return 1
  previous="$(jq -r '.recommendations.data["last-cycle.json"] | fromjson | .timestamp' <<<"${sample}")"
  deadline=$((SECONDS + 60))
  while (( SECONDS < deadline && cycles < 3 )); do
    sample="$(placement_snapshot)"
    printf '%s\n' "${sample}" >>"${artifact_dir}/placement-completed-api-samples.jsonl"
    placement_check completed "${sample}" || return 1
    timestamp="$(jq -r '.recommendations.data["last-cycle.json"] | fromjson | .timestamp' <<<"${sample}")"
    if [[ "${timestamp}" != "${previous}" ]]; then
      printf '%s\n' "${sample}" >>"${artifact_dir}/placement-completed-cycles.jsonl"
      previous="${timestamp}"
      cycles=$((cycles + 1))
    fi
    sleep 0.2
  done
  (( cycles == 3 )) || { echo 'missing three post-completion decision cycles' >&2; return 1; }
  jq -n --arg isvcUID "${placement_isvc_uid}" --arg irUID "${placement_ir_uid}" \
    --slurpfile baseline "${artifact_dir}/ir-before-trigger.raw.json" \
    --slurpfile paused "${artifact_dir}/placement-paused-cycles.jsonl" \
    --slurpfile released "${artifact_dir}/placement-released.json" \
    --slurpfile allocated "${artifact_dir}/placement-allocated.json" \
    --slurpfile repaused "${artifact_dir}/placement-repaused.json" \
    --slurpfile completed "${artifact_dir}/placement-completed-cycles.jsonl" \
    '{isvcUID:$isvcUID,irUID:$irUID,baseline:$baseline[0],paused:$paused,
      released:$released[0],allocated:$allocated[0],repaused:$repaused[0],completed:$completed}' \
    >"${artifact_dir}/placement-evidence.json"
}
