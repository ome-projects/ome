#!/usr/bin/env bash
# Real migration with a deterministic pause after the real scheduler result.
# All workload/status/placement changes still belong to OME and the schedulers.
set -euo pipefail
workload="${1:-}"
case "${workload}" in single|gang) ;; *) echo 'usage: namespace-churn.sh single|gang' >&2; exit 2 ;; esac
: "${STATE_DIR:?Set STATE_DIR to the dedicated absolute test-state directory}"
[[ "${STATE_DIR}" == /* && -d "${STATE_DIR}" && -f "${STATE_DIR}/kubeconfig" ]] || exit 2
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/namespace-churn-lib.sh"
for command in kubectl jq shasum perl; do command -v "${command}" >/dev/null || exit 2; done
churn_monotonic_time >/dev/null
kube=(kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e --request-timeout=15s)
"${kube[@]}" get node alfred-e2e-control-plane -o json | jq -e '.metadata.labels["alfred-e2e/infrastructure"] == "true"' >/dev/null
# Refuse to adopt or remove another run's workload. The nested runner creates
# its own source only after the barrier is installed and the namespace exists.
"${kube[@]}" -n alfred-e2e get inferenceservices,inferencereplicas,pods -o json | jq -e '.items | length == 0' >/dev/null
run_id="$(date -u +%Y%m%d%H%M%S)-$$"
artifact_dir="${STATE_DIR}/artifacts/namespace-churn-${workload}-${run_id}"
mkdir -p "${artifact_dir}"
noise_name="alfred-e2e-churn-${run_id}"
noise_key=alfred-e2e.ome.io/churn
registry_name="alfred-churn-${run_id}"
namespace=alfred-e2e
source_node=''
source_uids='[]'
owner_uid=''
ir_uid=''
routing_service=''
runner_pid=''
request_watch_pid=''
request_watch_file="${artifact_dir}/request-annotation-watch.jsonl"
registry_installed=false
passed=false
noise_uid=''
registry_uid=''

restore_registry() {
  local current patch
  current="$("${kube[@]}" -n ome get deployment ome-alfred -o json)" || return 1
  patch="$(churn_restore_patch "$(cat "${artifact_dir}/deployment-before.json")" "${current}" "${registry_name}")" || return 1
  [[ "${patch}" != '[]' ]] || return 0
  "${kube[@]}" -n ome patch deployment ome-alfred --type=json -p "${patch}" >"${artifact_dir}/restore.json" || return 1
  "${kube[@]}" -n ome rollout status deployment/ome-alfred --timeout=180s
}

cleanup() {
  local rc=$? cleared=true
  if [[ -n "${request_watch_pid}" ]]; then
    kill "${request_watch_pid}" 2>/dev/null || true
    wait "${request_watch_pid}" 2>/dev/null || true
  fi
  if [[ -n "${runner_pid}" ]] && kill -0 "${runner_pid}" 2>/dev/null; then
    kill "${runner_pid}" 2>/dev/null || true
    wait "${runner_pid}" 2>/dev/null || true
  fi
  if [[ -n "${source_node}" ]]; then
    "${kube[@]}" label node "${source_node}" maintenance.example.com/state- >/dev/null 2>&1 || cleared=false
  fi
  # Restoring must not release a migration that this failed test never proved.
  if [[ "${registry_installed}" == true ]]; then
    if [[ "${cleared}" == true ]] && "${kube[@]}" get nodes -l alfred-e2e/virtual=true -o json |
      jq -e 'all(.items[]; .metadata.labels["maintenance.example.com/state"] != "patching")' >/dev/null; then
      restore_registry || rc=1
    else
      echo 'Maintenance trigger not cleared; retaining fail-closed barrier registry' >&2
      rc=1
    fi
  fi
  if [[ "${passed}" != true ]]; then
    ((rc != 0)) || rc=1
    echo "namespace churn failed; retained fixture, registry and evidence: ${artifact_dir}" >&2
  elif ((rc == 0)); then
    # Delete only this run's empty noise namespace and temporary registry, with
    # UID preconditions. Workload fixtures and the durable journal are retained.
    jq -n --arg uid "${noise_uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' >"${artifact_dir}/noise-delete.json"
    "${kube[@]}" delete --raw "/api/v1/namespaces/${noise_name}" -f "${artifact_dir}/noise-delete.json" >"${artifact_dir}/noise-deleted.json" || rc=1
    jq -n --arg uid "${registry_uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' >"${artifact_dir}/registry-delete.json"
    "${kube[@]}" delete --raw "/api/v1/namespaces/ome/configmaps/${registry_name}" -f "${artifact_dir}/registry-delete.json" >"${artifact_dir}/registry-deleted.json" || rc=1
  fi
  exit "${rc}"
}
trap cleanup EXIT

"${kube[@]}" -n ome get deployment ome-alfred -o json >"${artifact_dir}/deployment-before.json"
normal_registry="$(jq -er '.spec.template.spec.volumes[] | select(.name=="simulation") | .configMap.name' "${artifact_dir}/deployment-before.json")"
"${kube[@]}" -n ome get configmap "${normal_registry}" -o json >"${artifact_dir}/registry-before.json"
backend=kind-default-v135
if [[ "${workload}" == gang ]]; then backend=kind-ome-v135; fi
profile="$(jq -cer --arg backend "${backend}" '.data["workers.json"]|fromjson|[.workers[]|select(.identity.backend==$backend)]|if length==1 then .[0].identity else error("profile missing") end' "${artifact_dir}/registry-before.json")"
jq -n --arg name "${noise_name}" --arg key "${noise_key}" '{apiVersion:"v1",kind:"Namespace",metadata:{name:$name,labels:{($key):"0"},annotations:{($key):"0"}}}' |
  "${kube[@]}" create -f - -o json >"${artifact_dir}/noise-created.json"
noise_uid="$(jq -er '.metadata.uid' "${artifact_dir}/noise-created.json")"
"${kube[@]}" wait namespace/"${noise_name}" --for=jsonpath='{.status.phase}'=Active --timeout=30s >/dev/null
jq -n --argjson original "$(cat "${artifact_dir}/registry-before.json")" --arg name "${registry_name}" --arg workload "${workload}" --argjson profile "${profile}" '
  {apiVersion:"v1",kind:"ConfigMap",metadata:{name:$name,namespace:"ome"},immutable:true,data:$original.data} |
  .data["workers.json"] |= (fromjson | .workers[].binaryPath="/alfred-simulator-barrier" | tojson) |
  .data["barrier.json"] = ({profile:$profile,namespace:"alfred-e2e",workload:$workload}|tojson)' |
  "${kube[@]}" create -f - -o json >"${artifact_dir}/registry-created.json"
registry_uid="$(jq -er '.metadata.uid' "${artifact_dir}/registry-created.json")"
patch="$(jq -c --arg registry "${registry_name}" '
  if (.spec.template.spec.containers|length)!=1 or .spec.template.spec.containers[0].name!="alfred" or
     ([.spec.template.spec.volumes[]|select(.name=="simulation")]|length)!=1 or
     ([.spec.template.spec.containers[0].args[]|select(startswith("--simulation-timeout="))]|length)!=1
  then error("unexpected deployment") else
  [{op:"test",path:"/metadata/uid",value:.metadata.uid},
   {op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
   {op:"replace",path:"/spec/template/spec/volumes",value:(.spec.template.spec.volumes|map(if .name=="simulation" then .configMap.name=$registry else . end))},
   {op:"replace",path:"/spec/template/spec/containers/0/args",value:(.spec.template.spec.containers[0].args|map(if startswith("--simulation-timeout=") then "--simulation-timeout=20s" else . end))}] end' "${artifact_dir}/deployment-before.json")"
# Mark before the request: a lost API response must still trigger reconciliation.
registry_installed=true
"${kube[@]}" -n ome patch deployment ome-alfred --type=json -p "${patch}" -o json >"${artifact_dir}/deployment-barrier.json"
"${kube[@]}" -n ome rollout status deployment/ome-alfred --timeout=180s
if [[ "${workload}" == single ]]; then
  STATE_DIR="${STATE_DIR}" ALFRED_E2E_MAINTENANCE_KEY=maintenance.example.com/state ALFRED_E2E_MAINTENANCE_VALUE=patching bash "${dir}/scenario.sh" maintenance-single >"${artifact_dir}/scenario.log" 2>&1 &
else
  STATE_DIR="${STATE_DIR}" ALFRED_E2E_MAINTENANCE_KEY=maintenance.example.com/state ALFRED_E2E_MAINTENANCE_VALUE=patching bash "${dir}/gang.sh" maintenance-gang >"${artifact_dir}/scenario.log" 2>&1 &
fi
runner_pid=$!

capture() {
  local isvc ir pods endpoints recommendations journal requests='[]'
  isvc="$("${kube[@]}" -n alfred-e2e get inferenceservice "${workload}" -o json)" || return 1
  ir="$("${kube[@]}" -n alfred-e2e get inferencereplica "${workload}-engine" -o json)" || return 1
  pods="$("${kube[@]}" -n alfred-e2e get pods -l "ome.io/inferenceservice=${workload},ome.io/managed-by=OMENative" -o json)" || return 1
  endpoints="$("${kube[@]}" -n alfred-e2e get endpointslices -l "kubernetes.io/service-name=${routing_service}" -o json)" || return 1
  recommendations="$("${kube[@]}" -n ome get configmap alfred-recommendations -o json)" || return 1
  journal="$("${kube[@]}" -n ome get configmap alfred-dispatch-state -o json)" || return 1
  if [[ -n "${request_watch_pid}" ]]; then
    kill -0 "${request_watch_pid}" 2>/dev/null || { echo 'request watch ended before evidence was sealed' >&2; return 1; }
    requests="$(churn_watch_requests "${request_watch_file}" "${owner_uid}")" || return 1
  fi
  jq -cn --argjson isvc "${isvc}" --argjson ir "${ir}" --argjson pods "${pods}" --argjson endpoints "${endpoints}" --argjson recommendations "${recommendations}" --argjson journal "${journal}" --argjson requests "${requests}" \
    '{isvc:$isvc,ir:$ir,pods:$pods,endpoints:$endpoints,recommendations:$recommendations,journal:$journal,requests:$requests}'
}

check_idle() {
  jq -e --arg owner "${owner_uid}" --arg ir "${ir_uid}" --argjson source "${source_uids}" '
    .isvc.metadata.uid==$owner and .ir.metadata.uid==$ir and
    ([.isvc.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))]|length)==0 and
    (.requests|length)==0 and
    (.ir.status.migrations // []|length)==0 and
    ([.journal.data["state.json"]|fromjson|.entries[]|select(.workloadUID==$owner)]|length)==0 and
    ([.pods.items[].metadata.uid]|sort)==($source|sort) and
    all(.pods.items[]; .metadata.deletionTimestamp==null and
      any(.status.conditions[]?;.type=="Ready" and .status=="True") and
      any(.status.conditions[]?;.type=="ome.io/serving" and .status=="True")) and
    any(.endpoints.items[].endpoints[]?; (.targetRef.uid as $uid|$source|index($uid))!=null and .conditions.ready==true and .conditions.terminating!=true)' >/dev/null
}

for index in 1 2 3 4; do
  mode=labels
  if ((index == 4)); then mode=annotations; fi
  echo "Waiting for real ${workload} preflight ${index} (${mode})"
  prefix="${artifact_dir}/barrier-${index}"
  # The bounded client runs inside this test Pod and talks only to loopback.
  # Port-forward would disconnect on the expected between-attempt refusals.
  "${kube[@]}" --request-timeout=190s -n ome exec deployment/ome-alfred -c alfred -- \
    /alfred-simulator-barrier --wait-held >"${prefix}-held.json"
  churn_decode_held "${prefix}-held.json" "${prefix}"
  jq -e --argjson profile "${profile}" --arg workload "${workload}" '
    .profile==$profile and .requestID=="preflight" and .migrationFromNode!="" and
    .excludedNodes==[.migrationFromNode] and
    all(.sourcePods[]; .metadata.namespace=="alfred-e2e" and .metadata.labels["ome.io/inferenceservice"]==$workload)' "${prefix}-request.json" >/dev/null
  source_node="$(jq -er '.migrationFromNode' "${prefix}-request.json")"
  source_uids="$(jq -c '[.sourcePods[].metadata.uid]|sort' "${prefix}-request.json")"
  routing_service="${workload}-engine-rev-$(jq -er '[.sourcePods[].metadata.labels["ome.io/revision-hash"]]|unique|if length==1 then .[0] else error("mixed revisions") end' "${prefix}-request.json")"
  before="$("${kube[@]}" get namespace "${noise_name}" -o json)"
  jq -e --arg uid "${noise_uid}" --argjson before "${before}" '
    [.clusterObjects[]|select(.kind=="Namespace" and .metadata.uid==$uid)] |
    length==1 and (.[0]|del(.metadata.resourceVersion,.metadata.managedFields))==($before|del(.metadata.resourceVersion,.metadata.managedFields))' "${prefix}-request.json" >/dev/null
  printf '%s\n' "${before}" >"${prefix}-namespace-before.json"
  for mutation in 1 2 3; do
    patch="$(jq -cn --arg uid "${noise_uid}" --arg rv "$(jq -r '.metadata.resourceVersion' <<<"${before}")" --arg path "/metadata/${mode}/alfred-e2e.ome.io~1churn" --arg value "${index}-${mutation}" \
      '[{op:"test",path:"/metadata/uid",value:$uid},{op:"test",path:"/metadata/resourceVersion",value:$rv},{op:"add",path:$path,value:$value}]')"
    after="$("${kube[@]}" patch namespace "${noise_name}" --type=json -p "${patch}" -o json)"
    churn_check_mutation "${before}" "${after}" "${mode}" "${noise_key}"
    printf '%s\n' "${after}" >>"${prefix}-namespace-mutations.jsonl"
    before="${after}"
  done
  printf '%s\n' "${after}" >"${prefix}-namespace-after.json"
  sample="$(capture)"
  printf '%s\n' "${sample}" >"${prefix}-before-release.json"
  if [[ -z "${owner_uid}" ]]; then
    owner_uid="$(jq -er '.isvc.metadata.uid' <<<"${sample}")"
    ir_uid="$(jq -er '.ir.metadata.uid' <<<"${sample}")"
    watch_timeout="$(churn_watch_timeout_seconds "${ALFRED_E2E_DEADLINE_SECONDS:-360}")"
    "${kube[@]}" -n alfred-e2e get inferenceservice "${workload}" --watch \
      --request-timeout="${watch_timeout}s" -o json >"${request_watch_file}" 2>"${artifact_dir}/request-annotation-watch.stderr" &
    request_watch_pid=$!
    # kubectl lists before watching from that list's resourceVersion. Require
    # the real initial object before releasing the first held simulation.
    watch_deadline=$((SECONDS + 3))
    until initial_requests="$(churn_watch_requests "${request_watch_file}" "${owner_uid}")"; do
      kill -0 "${request_watch_pid}" 2>/dev/null || { echo 'request watch failed to start' >&2; exit 1; }
      ((SECONDS < watch_deadline)) || { echo 'request watch did not produce its initial object' >&2; exit 1; }
    done
    [[ "${initial_requests}" == '[]' ]] || { echo 'request predated the first release' >&2; exit 1; }
  fi
  check_idle <<<"${sample}"
  jq -c '{nonce}' "${prefix}-held.json" >"${prefix}-release-request.json"
  release_started="$(churn_monotonic_time)"
  "${kube[@]}" -n ome exec -i deployment/ome-alfred -c alfred -- \
    /alfred-simulator-barrier --release-held <"${prefix}-release-request.json" >"${prefix}-receipt.json"
  jq -e --slurpfile held "${prefix}-held.json" -L "${dir}" 'include "placement-pause-sample";
    .requestSHA256==$held[0].requestSHA256 and .resultSHA256==$held[0].resultSHA256 and
    (.releasedAt|pp_time)>=($held[0].heldAt|pp_time) and (.releasedAt|pp_time)<($held[0].deadline|pp_time)' "${prefix}-receipt.json" >/dev/null
  released="$(jq -er '.releasedAt' "${prefix}-receipt.json")"
  deadline=$((SECONDS + 30))
  observed=false
  while ((SECONDS < deadline)); do
    sample="$(capture)"
    observation_finished="$(churn_monotonic_time)"
    printf '%s\n' "${sample}" >>"${prefix}-api.jsonl"
    jq -n --argjson start "${release_started}" --argjson finish "${observation_finished}" \
      --arg hash "$(jq -er '.requestSHA256' "${prefix}-receipt.json")" \
      '{releaseStarted:$start,observationFinished:$finish,budgetSeconds:30,requestSHA256:$hash}' >"${prefix}-timing.json"
    churn_within_window "${release_started}" "${observation_finished}" || { echo 'observation exceeded the 30-second release budget' >&2; exit 1; }
    if ((index < 4)); then
      check_idle <<<"${sample}"
      if jq -e -L "${dir}" --arg released "${released}" --arg workload "alfred-e2e/${workload}" 'include "placement-pause-sample";
        .recommendations.data["last-cycle.json"]|fromjson|
        (.timestamp|pp_time)>($released|pp_time) and any(.recommendations[]?;
        .workload==$workload and .dispatchStatus=="withheld" and .dispatchReason=="SchedulingStateChanged")' <<<"${sample}" >/dev/null; then observed=true; break; fi
    elif churn_request_published "${owner_uid}" "${ir_uid}" <<<"${sample}"; then
      observed=true; break
    fi
    sleep 0.1
  done
  [[ "${observed}" == true ]] || { echo "missing expected ${mode} outcome" >&2; exit 1; }
  printf '%s\n' "${sample}" >"${prefix}-observed.json"
done

wait "${runner_pid}"
runner_pid=''
base_evidence="$(sed -n 's/.*evidence: //p' "${artifact_dir}/scenario.log" | tail -n 1)"
[[ "${base_evidence}" == "${STATE_DIR}/artifacts/"* && -f "${base_evidence}" ]] || { echo 'nested runner did not seal evidence' >&2; exit 1; }
if [[ "${workload}" == single ]]; then verifier=verify-evidence.jq; else verifier=verify-gang-evidence.jq; fi
jq -e -f "${dir}/${verifier}" "${base_evidence}" >/dev/null
cp "${base_evidence}" "${artifact_dir}/handoff-evidence.json"
cp "$(dirname "${base_evidence}")/request-annotation-watch.jsonl" "${artifact_dir}/nested-request-annotation-watch.jsonl"
sample="$(capture)"
printf '%s\n' "${sample}" >"${artifact_dir}/completion-baseline.json"
baseline="$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' <<<"${sample}")"
previous="${baseline}"
cycles=0
deadline=$((SECONDS + 30))
while ((SECONDS < deadline && cycles < 3)); do
  sample="$(capture)"
  printf '%s\n' "${sample}" >>"${artifact_dir}/completion-api.jsonl"
  stamp="$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' <<<"${sample}")"
  if jq -en -L "${dir}" --arg stamp "${stamp}" --arg previous "${previous}" 'include "placement-pause-sample"; ($stamp|pp_time)>($previous|pp_time)' >/dev/null; then
    printf '%s\n' "${sample}" >>"${artifact_dir}/completion-cycles.jsonl"
    previous="${stamp}"; cycles=$((cycles + 1))
  fi
  sleep 0.2
done
((cycles == 3)) || { echo 'missing three new completion cycles' >&2; exit 1; }
kill -0 "${request_watch_pid}" 2>/dev/null || { echo 'request watch ended before completion evidence was sealed' >&2; exit 1; }
kill "${request_watch_pid}"
wait "${request_watch_pid}" 2>/dev/null || true
request_watch_pid=''
jq -n --arg workload "${workload}" --arg noiseUID "${noise_uid}" --arg ownerUID "${owner_uid}" --arg irUID "${ir_uid}" --argjson profile "${profile}" \
  --slurpfile handoff "${base_evidence}" --slurpfile baseline "${artifact_dir}/completion-baseline.json" --slurpfile completed "${artifact_dir}/completion-cycles.jsonl" \
  '{workload:$workload,noiseUID:$noiseUID,ownerUID:$ownerUID,irUID:$irUID,profile:$profile,handoff:$handoff[0],completionBaseline:$baseline[0],completed:$completed}' >"${artifact_dir}/evidence.json"
# Keep raw sibling artifacts; the verifier independently checks their outcomes.
bash "${dir}/verify-namespace-churn.sh" "${artifact_dir}"
passed=true
echo "namespace churn ${workload} passed; evidence: ${artifact_dir}/evidence.json"
