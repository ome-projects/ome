#!/usr/bin/env bash
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/no-capacity-ownership.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT

# Exercise the runner's current EXIT cleanup against a strict recording API
# boundary. A failed fixture setup must retain every object for diagnosis.
artifact_dir="${test_dir}"
namespace=alfred-e2e-no-capacity
runtime=alfred-e2e-no-capacity-runtime
alfred_namespace=ome
created=true
passed=false
triggered_node=''
stop_watches() { :; }
recording_kube() {
  local request="$*"
  if [[ "${request}" == 'get nodes -o yaml' ||
        "${request}" == "-n ${namespace} get inferenceservices.ome.io,inferencereplicas.ome.io,pods,endpointslices -o yaml" ||
        "${request}" == "-n ${namespace} get events -o yaml" ||
        "${request}" == "-n ${alfred_namespace} get configmap alfred-recommendations alfred-dispatch-state -o yaml" ]]; then
    return 0
  fi
  if [[ "${1:-}" == delete ]]; then
    printf '%s\n' "${request}" >>"${test_dir}/requests"
    return 0
  fi
  printf '%s\n' "${request}" >>"${test_dir}/unexpected-requests"
  return 97
}
kube=(recording_kube)
eval "$(awk '/^cleanup\(\) \{/ {copy=1} copy {print} copy && /^\}/ {exit}' "${dir}/no-capacity.sh")"

set +e
(trap cleanup EXIT; exit 17)
actual=$?
set -e
[[ "${actual}" == 17 ]] || { echo "expected original failure 17, got ${actual}" >&2; exit 1; }
if [[ -s "${test_dir}/requests" ]]; then
  echo 'failed fixture setup caused destructive cleanup' >&2
  cat "${test_dir}/requests" >&2
  exit 1
fi
if [[ -s "${test_dir}/unexpected-requests" ]]; then
  echo 'failed fixture cleanup issued an unexpected API operation' >&2
  cat "${test_dir}/unexpected-requests" >&2
  exit 1
fi

# A successful scenario may delete only the exact recorded identities.
: >"${test_dir}/requests"
passed=true
namespace_uid=namespace-uid
runtime_uid=runtime-uid
printf '%s\n' namespace-uid >"${test_dir}/namespace-live"
printf '%s\n' runtime-uid >"${test_dir}/runtime-live"
recording_kube() {
  local body path state uid kind
  if [[ "${1:-}" == get ]]; then
    case "${2:-}" in
      namespace) state="${test_dir}/namespace-live"; kind=Namespace ;;
      clusterservingruntimes.ome.io) state="${test_dir}/runtime-live"; kind=ClusterServingRuntime ;;
      *) return 1 ;;
    esac
    [[ -s "${state}" ]] || return 0
    uid="$(<"${state}")"
    jq -cn --arg kind "${kind}" --arg name "${3}" --arg uid "${uid}" \
      '{apiVersion:"v1",kind:$kind,metadata:{name:$name,uid:$uid}}'
    return
  fi
  [[ "${1:-}" == delete && "${2:-}" == --raw && "${4:-}" == -f ]] || return 1
  path="${3}"
  body="$(<"${5}")"
  printf '%s\t%s\n' "${path}" "$(jq -c . <<<"${body}")" >>"${test_dir}/requests"
  case "${path}" in
    /api/v1/namespaces/*) : >"${test_dir}/namespace-live" ;;
    /apis/ome.io/v1beta1/clusterservingruntimes/*) : >"${test_dir}/runtime-live" ;;
    *) return 1 ;;
  esac
  printf '{"kind":"Status","status":"Success"}\n'
}
set +e
(trap cleanup EXIT; exit 0)
actual=$?
set -e
[[ "${actual}" == 0 ]] || { echo "UID-fenced successful cleanup exited ${actual}" >&2; exit 1; }
[[ ! -s "${test_dir}/namespace-live" && ! -s "${test_dir}/runtime-live" ]] || {
  echo 'successful cleanup did not remove both recorded identities' >&2; exit 1;
}
jq -e -s '
  length == 2 and
  .[0].path == "/api/v1/namespaces/alfred-e2e-no-capacity" and
  .[0].body.preconditions.uid == "namespace-uid" and
  .[1].path == "/apis/ome.io/v1beta1/clusterservingruntimes/alfred-e2e-no-capacity-runtime" and
  .[1].body.preconditions.uid == "runtime-uid"
' < <(awk -F '\t' '{print "{\"path\":\"" $1 "\",\"body\":" $2 "}"}' "${test_dir}/requests") >/dev/null

fail() { echo "FAIL: $*" >&2; exit 1; }

# Exercise the runner's real observe function under the same conditional call
# used by its readiness loop. Each failed stage must stop before the next read;
# Bash suppresses automatic errexit for every command inside an if-condition.
observe_dir="${test_dir}/observe"
mkdir -p "${observe_dir}"
namespace=alfred-e2e-no-capacity-observe
namespace_uid=namespace-uid
name=no-capacity
observe_fail_at=none
pods_fixture='{"kind":"PodList","items":[{"metadata":{"uid":"source-uid","labels":{"ome.io/revision-hash":"revision-a"}}}]}'
ir_fixture='{"kind":"InferenceReplica","metadata":{"name":"no-capacity-engine","uid":"ir-uid"}}'
isvc_fixture='{"kind":"InferenceService","metadata":{"name":"no-capacity","uid":"isvc-uid"}}'
endpoints_fixture='{"kind":"EndpointSliceList","items":[{"metadata":{"name":"route-a"}}]}'
observe_kube() {
  local request="$*"
  printf '%s\n' "${request}" >>"${observe_dir}/calls"
  case "${request}" in
    "get namespace ${namespace} -o json")
      if [[ "${observe_fail_at}" == namespace ]]; then
        jq -cn --arg name "${namespace}" '{kind:"Namespace",metadata:{name:$name,uid:"successor-uid"}}'
      else
        jq -cn --arg name "${namespace}" --arg uid "${namespace_uid}" '{kind:"Namespace",metadata:{name:$name,uid:$uid}}'
      fi
      ;;
    "-n ${namespace} get pods -l ome.io/inferenceservice=${name},ome.io/managed-by=OMENative -o json")
      [[ "${observe_fail_at}" != pods ]] || return 1
      if [[ "${observe_fail_at}" == routing ]]; then printf '{invalid-json\n'; else printf '%s\n' "${pods_fixture}"; fi
      ;;
    "-n ${namespace} get inferencereplicas.ome.io ${name}-engine -o json")
      [[ "${observe_fail_at}" != ir ]] || return 1
      printf '%s\n' "${ir_fixture}"
      ;;
    "-n ${namespace} get inferenceservices.ome.io ${name} -o json")
      [[ "${observe_fail_at}" != isvc ]] || return 1
      printf '%s\n' "${isvc_fixture}"
      ;;
    "-n ${namespace} get endpointslices -l kubernetes.io/service-name=${name}-engine-rev-revision-a -o json")
      [[ "${observe_fail_at}" != endpoints ]] || return 1
      printf '%s\n' "${endpoints_fixture}"
      ;;
    *) return 97 ;;
  esac
}
kube=(observe_kube)
eval "$(sed -n '/^pods_json() /p; /^endpoints_json() /p' "${dir}/no-capacity.sh")"
eval "$(awk '/^observe\(\) \{/ {copy=1} copy {print} copy && /^}/ {exit}' "${dir}/no-capacity.sh")"

for case_spec in namespace:1 pods:2 routing:2 ir:3 isvc:4 endpoints:5; do
  observe_fail_at="${case_spec%%:*}"
  expected_calls="${case_spec##*:}"
  : >"${observe_dir}/calls"
  if observe >/dev/null 2>&1; then
    fail "observe accepted ${observe_fail_at} failure in conditional context"
  fi
  actual_calls="$(wc -l <"${observe_dir}/calls" | tr -d ' ')"
  [[ "${actual_calls}" == "${expected_calls}" ]] || fail "observe continued after ${observe_fail_at} failure: ${actual_calls} calls"
done

observe_fail_at=none
: >"${observe_dir}/calls"
if ! observe; then fail 'observe rejected complete successful snapshots'; fi
[[ "${routing_service}" == no-capacity-engine-rev-revision-a ]] || fail 'observe derived the wrong routing Service'
[[ "${pods}" == "${pods_fixture}" && "${ir}" == "${ir_fixture}" && "${isvc}" == "${isvc_fixture}" && "${endpoints}" == "${endpoints_fixture}" ]] || {
  fail 'observe did not retain successful snapshots'
}
[[ "$(wc -l <"${observe_dir}/calls" | tr -d ' ')" == 5 ]] || fail 'successful observe did not execute all reads'

echo 'no-capacity observe failure-propagation tests passed'

# CREATE is sequential and each child is bracketed by checks of the recorded
# Namespace UID. The boundary models actual API state, not only call presence.
create_dir="${test_dir}/create"
mkdir -p "${create_dir}"
artifact_dir="${create_dir}"
namespace=alfred-e2e-no-capacity-run
namespace_uid=namespace-uid
printf '%s\n' "${namespace_uid}" >"${create_dir}/namespace-live"
: >"${create_dir}/calls"
: >"${create_dir}/ownership.jsonl"
create_mode=success
creation_kube() {
  local desired uid
  printf '%s\n' "$*" >>"${create_dir}/calls"
  if [[ "${1:-}" == get && "${2:-}" == namespace && "${3:-}" == "${namespace}" ]]; then
    [[ -s "${create_dir}/namespace-live" ]] || return 0
    uid="$(<"${create_dir}/namespace-live")"
    jq -cn --arg name "${namespace}" --arg uid "${uid}" \
      '{apiVersion:"v1",kind:"Namespace",metadata:{name:$name,uid:$uid}}'
    return
  fi
  [[ "${1:-}" == create && "${2:-}" == -f && "${4:-}" == -o && "${5:-}" == json ]] || return 91
  desired="$(<"${3}")"
  case "${create_mode}" in
    lost) return 1 ;;
    malformed) printf '{not-json\n'; return 0 ;;
    wrong) jq -c '.metadata.name="someone-else" | .metadata.uid="wrong-uid"' <<<"${desired}" ;;
    already-exists) return 1 ;;
    replace-namespace)
      jq -c '.metadata.uid="isvc-uid"' <<<"${desired}"
      printf '%s\n' successor-uid >"${create_dir}/namespace-live"
      ;;
    success) jq -c '.metadata.uid=(.metadata.name+"-uid")' <<<"${desired}" ;;
    *) return 92 ;;
  esac
}
kube=(creation_kube)
jq -n --arg ns "${namespace}" '{apiVersion:"ome.io/v1beta1",kind:"InferenceService",metadata:{name:"no-capacity",namespace:$ns},spec:{}}' >"${create_dir}/isvc.json"
nc_create_owned "${create_dir}/isvc.json" "${create_dir}/isvc-created.json" \
  InferenceService no-capacity "${namespace}" || fail 'valid child CREATE rejected'
jq -e '.kind=="InferenceService" and .name=="no-capacity" and .namespace=="alfred-e2e-no-capacity-run" and .uid=="no-capacity-uid"' \
  "${create_dir}/ownership.jsonl" >/dev/null || fail 'validated ownership receipt not recorded'
[[ "$(grep -c '^get namespace ' "${create_dir}/calls")" == 2 ]] || fail 'Namespace was not checked before and after child CREATE'

for create_mode in lost malformed wrong; do
  : >"${create_dir}/calls"
  : >"${create_dir}/ownership.jsonl"
  printf '%s\n' "${namespace_uid}" >"${create_dir}/namespace-live"
  if nc_create_owned "${create_dir}/isvc.json" "${create_dir}/${create_mode}-response.json" \
    InferenceService no-capacity "${namespace}" >/dev/null 2>&1; then
    fail "accepted ${create_mode} CREATE response"
  fi
  [[ -f "${create_dir}/${create_mode}-response.json" ]] || fail "raw ${create_mode} response was not retained"
  [[ ! -s "${create_dir}/ownership.jsonl" ]] || fail "recorded ownership after ${create_mode} response"
  [[ "$(grep -c '^create ' "${create_dir}/calls")" == 1 ]] || fail "retried ${create_mode} CREATE"
  ! grep -q '^get inferenceservice' "${create_dir}/calls" || fail "guessed ownership after ${create_mode} CREATE"
done

create_mode=already-exists
: >"${create_dir}/calls"
jq -n --arg ns "${namespace}" '{apiVersion:"v1",kind:"Pod",metadata:{name:"block-node-a",namespace:$ns},spec:{}}' >"${create_dir}/blocker.json"
if nc_create_owned "${create_dir}/blocker.json" "${create_dir}/blocker-response.json" \
  Pod block-node-a "${namespace}" >/dev/null 2>&1; then
  fail 'adopted AlreadyExists blocker'
fi
[[ "$(grep -c '^create ' "${create_dir}/calls")" == 1 ]] || fail 'retried AlreadyExists blocker'
! grep -q '^get pod' "${create_dir}/calls" || fail 'looked up AlreadyExists blocker for adoption'

create_mode=replace-namespace
: >"${create_dir}/calls"
: >"${create_dir}/ownership.jsonl"
printf '%s\n' "${namespace_uid}" >"${create_dir}/namespace-live"
if nc_create_owned "${create_dir}/isvc.json" "${create_dir}/replacement-response.json" \
  InferenceService no-capacity "${namespace}" >/dev/null 2>&1; then
  fail 'accepted Namespace replacement around child CREATE'
fi
[[ "$(<"${create_dir}/namespace-live")" == successor-uid ]] || fail 'replacement model did not take effect'
jq -e '.uid=="isvc-uid"' "${create_dir}/ownership.jsonl" >/dev/null || fail 'successful child receipt was not retained'

# A parent replacement can happen after one child completed and before the next
# starts. The next child's precheck must stop before sending another CREATE,
# while the first raw receipt and inventory entry remain available for diagnosis.
create_mode=success
: >"${create_dir}/calls"
: >"${create_dir}/ownership.jsonl"
printf '%s\n' "${namespace_uid}" >"${create_dir}/namespace-live"
nc_create_owned "${create_dir}/isvc.json" "${create_dir}/first-child-created.json" \
  InferenceService no-capacity "${namespace}" || fail 'first child CREATE failed'
first_create_count="$(grep -c '^create ' "${create_dir}/calls")"
[[ "${first_create_count}" == 1 ]] || fail 'first child did not issue exactly one CREATE'
printf '%s\n' successor-uid >"${create_dir}/namespace-live"
if nc_create_owned "${create_dir}/blocker.json" "${create_dir}/second-child-created.json" \
  Pod block-node-a "${namespace}" >/dev/null 2>&1; then
  fail 'created second child after Namespace replacement'
fi
[[ "$(grep -c '^create ' "${create_dir}/calls")" == "${first_create_count}" ]] || fail 'second child CREATE reached the API'
[[ ! -e "${create_dir}/second-child-created.json" ]] || fail 'second child response was fabricated without CREATE'
jq -e '.metadata.uid=="no-capacity-uid"' "${create_dir}/first-child-created.json" >/dev/null || fail 'first child raw receipt was not retained'
jq -e -s 'length==1 and .[0].kind=="InferenceService" and .[0].uid=="no-capacity-uid"' \
  "${create_dir}/ownership.jsonl" >/dev/null || fail 'first child ownership inventory was not retained'
[[ "$(<"${create_dir}/namespace-live")" == successor-uid ]] || fail 'replacement Namespace state was not retained'

echo 'no-capacity sequential ownership CREATE tests passed'

# Maintenance writes use fresh Node identity/state plus JSON Patch tests. The
# fake changes real state at the PATCH boundary to model concurrent writers.
marker_dir="${test_dir}/marker"
mkdir -p "${marker_dir}"
artifact_dir="${marker_dir}"
source_node=alfred-kwok-gpu-a
source_node_uid=node-uid
maintenance_key=maintenance.example.com/state
maintenance_value=patching
node_mode=success
marker_node() {
  jq -cn '{apiVersion:"v1",kind:"Node",metadata:{name:"alfred-kwok-gpu-a",uid:"node-uid",resourceVersion:"10",labels:{"alfred-e2e/virtual":"true"}}}' >"${marker_dir}/node.json"
  : >"${marker_dir}/calls"
  trigger_active=false
  trigger_uncertain=false
  triggered_node=''
  triggered_node_uid=''
}
marker_kube() {
  local patch op path expected_uid expected_rv expected_labels live
  printf '%s\n' "$*" >>"${marker_dir}/calls"
  if [[ "${1:-}" == get && "${2:-}" == node && "${3:-}" == "${source_node}" && "${4:-}" == -o && "${5:-}" == json ]]; then
    cat "${marker_dir}/node.json"
    return
  fi
  [[ "${1:-}" == patch && "${2:-}" == node && "${3:-}" == "${source_node}" && "${4:-}" == --type=json && "${5:-}" == -p && "${7:-}" == -o && "${8:-}" == json ]] || return 91
  patch="${6}"
  case "${node_mode}" in
    replace-on-patch) jq '.metadata.uid="successor-node" | .metadata.resourceVersion="11"' "${marker_dir}/node.json" >"${marker_dir}/next" && mv "${marker_dir}/next" "${marker_dir}/node.json" ;;
    change-on-patch) jq '.metadata.labels["maintenance.example.com/state"]="someone-else" | .metadata.resourceVersion="11"' "${marker_dir}/node.json" >"${marker_dir}/next" && mv "${marker_dir}/next" "${marker_dir}/node.json" ;;
  esac
  live="$(<"${marker_dir}/node.json")"
  expected_uid="$(jq -r '.[0].value' <<<"${patch}")"
  expected_rv="$(jq -r '.[1].value' <<<"${patch}")"
  expected_labels="$(jq -c '.[2].value' <<<"${patch}")"
  jq -e --arg uid "${expected_uid}" --arg rv "${expected_rv}" --argjson labels "${expected_labels}" \
    '.metadata.uid==$uid and .metadata.resourceVersion==$rv and .metadata.labels==$labels' <<<"${live}" >/dev/null || return 1
  op="$(jq -r '.[3].op' <<<"${patch}")"
  path="$(jq -r '.[3].path' <<<"${patch}")"
  [[ "${path}" == /metadata/labels/maintenance.example.com~1state ]] || return 92
  case "${op}" in
    add)
      jq --arg value "$(jq -r '.[3].value' <<<"${patch}")" \
        '.metadata.labels["maintenance.example.com/state"]=$value | .metadata.resourceVersion="11"' \
        "${marker_dir}/node.json" >"${marker_dir}/next"
      ;;
    remove)
      jq 'del(.metadata.labels["maintenance.example.com/state"]) | .metadata.resourceVersion="12"' \
        "${marker_dir}/node.json" >"${marker_dir}/next"
      ;;
    *) return 93 ;;
  esac
  mv "${marker_dir}/next" "${marker_dir}/node.json"
  [[ "${node_mode}" != uncertain ]] || return 1
  cat "${marker_dir}/node.json"
}
kube=(marker_kube)

marker_node
node_mode=success
nc_set_maintenance "${source_node}" "${source_node_uid}" "${maintenance_key}" "${maintenance_value}" || fail 'fenced marker write failed'
jq -e '.metadata.labels["maintenance.example.com/state"]=="patching"' "${marker_dir}/node.json" >/dev/null || fail 'marker not applied'
nc_clear_maintenance || fail 'fenced marker clearance failed'
jq -e '.metadata.labels["maintenance.example.com/state"]==null' "${marker_dir}/node.json" >/dev/null || fail 'marker not cleared'

marker_node
node_mode=success
nc_set_maintenance "${source_node}" "${source_node_uid}" "${maintenance_key}" "${maintenance_value}" || fail 'failed-scenario marker setup failed'
passed=false
set +e
nc_cleanup 23 >/dev/null 2>&1
actual=$?
set -e
[[ "${actual}" == 23 ]] || fail "failed scenario exit changed while clearing trigger: ${actual}"
jq -e '.metadata.labels["maintenance.example.com/state"]==null' "${marker_dir}/node.json" >/dev/null || fail 'proven trigger was not cleared after scenario failure'
! grep -q '^delete ' "${marker_dir}/calls" || fail 'failed scenario deleted fixtures after trigger clearance'

marker_node
node_mode=success
nc_set_maintenance "${source_node}" "${source_node_uid}" "${maintenance_key}" "${maintenance_value}" || fail 'cleanup-failure marker setup failed'
node_mode=change-on-patch
passed=true
if nc_cleanup 0 >/dev/null 2>&1; then fail 'successful exit ignored trigger clearance failure'; fi
[[ "${trigger_uncertain}" == true ]] || fail 'failed trigger clearance was not tracked'
! grep -q '^delete ' "${marker_dir}/calls" || fail 'cleanup deleted fixtures after trigger clearance failure'

for node_mode in replace-on-patch change-on-patch; do
  marker_node
  if nc_set_maintenance "${source_node}" "${source_node_uid}" "${maintenance_key}" "${maintenance_value}" >/dev/null 2>&1; then
    fail "maintenance write overwrote ${node_mode} state"
  fi
  if [[ "${node_mode}" == replace-on-patch ]]; then
    jq -e '.metadata.uid=="successor-node" and .metadata.labels["maintenance.example.com/state"]==null' "${marker_dir}/node.json" >/dev/null || fail 'successor Node was mutated'
  else
    jq -e '.metadata.uid=="node-uid" and .metadata.labels["maintenance.example.com/state"]=="someone-else"' "${marker_dir}/node.json" >/dev/null || fail 'concurrent marker was overwritten'
  fi
done

marker_node
node_mode=uncertain
if nc_set_maintenance "${source_node}" "${source_node_uid}" "${maintenance_key}" "${maintenance_value}" >/dev/null 2>&1; then
  fail 'lost marker response was accepted'
fi
[[ "${trigger_uncertain}" == true ]] || fail 'lost marker response not tracked as uncertain'
jq -e '.metadata.labels["maintenance.example.com/state"]=="patching"' "${marker_dir}/node.json" >/dev/null || fail 'uncertain write model did not apply marker'
passed=true
namespace_uid=namespace-uid
runtime_uid=runtime-uid
if nc_cleanup 0 >/dev/null 2>&1; then fail 'cleanup ignored uncertain marker write'; fi
! grep -q '^delete ' "${marker_dir}/calls" || fail 'cleanup deleted blockers after uncertain marker write'

echo 'no-capacity fenced maintenance tests passed'

# Destructive cleanup is serial. The fake enforces the UID precondition against
# its current object, including a replacement racing between GET and DELETE.
cleanup_dir="${test_dir}/cleanup"
mkdir -p "${cleanup_dir}"
artifact_dir="${cleanup_dir}"
namespace=alfred-e2e-no-capacity-run
runtime=alfred-e2e-no-capacity-runtime-run
namespace_uid=namespace-uid
runtime_uid=runtime-uid
cleanup_mode=success
reset_cleanup() {
  printf '%s\n' namespace-uid >"${cleanup_dir}/namespace-live"
  printf '%s\n' runtime-uid >"${cleanup_dir}/runtime-live"
  : >"${cleanup_dir}/calls"
  passed=true
  trigger_active=false
  trigger_uncertain=false
  triggered_node=''
  triggered_node_uid=''
  nc_namespace_delete_timeout_seconds=0
  nc_runtime_delete_timeout_seconds=0
}
cleanup_kube() {
  local state uid kind path body
  printf '%s\n' "$*" >>"${cleanup_dir}/calls"
  if [[ "${1:-}" == get ]]; then
    case "${2:-}" in
      namespace) state="${cleanup_dir}/namespace-live"; kind=Namespace ;;
      clusterservingruntimes.ome.io) state="${cleanup_dir}/runtime-live"; kind=ClusterServingRuntime ;;
      *) return 91 ;;
    esac
    [[ -s "${state}" ]] || return 0
    uid="$(<"${state}")"
    jq -cn --arg kind "${kind}" --arg name "${3}" --arg uid "${uid}" '{kind:$kind,metadata:{name:$name,uid:$uid}}'
    return
  fi
  [[ "${1:-}" == delete && "${2:-}" == --raw && "${4:-}" == -f ]] || return 92
  path="${3}"
  body="$(<"${5}")"
  case "${path}" in
    /api/v1/namespaces/*) state="${cleanup_dir}/namespace-live" ;;
    /apis/ome.io/v1beta1/clusterservingruntimes/*) state="${cleanup_dir}/runtime-live" ;;
    *) return 93 ;;
  esac
  if [[ "${cleanup_mode}" == replace-on-delete && "${state}" == *namespace-live ]]; then
    printf '%s\n' successor-uid >"${state}"
  fi
  [[ "${cleanup_mode}" != reject-namespace || "${state}" != *namespace-live ]] || return 1
  uid="$(<"${state}")"
  [[ "$(jq -r '.preconditions.uid' <<<"${body}")" == "${uid}" ]] || return 1
  if [[ "${cleanup_mode}" != timeout-namespace || "${state}" != *namespace-live ]]; then
    : >"${state}"
  fi
  printf '{"kind":"Status","status":"Success"}\n'
}
kube=(cleanup_kube)

reset_cleanup
passed=false
if nc_cleanup 0 >/dev/null 2>&1; then fail 'successful exit without scenario acceptance was accepted'; fi
! grep -q '^delete ' "${cleanup_dir}/calls" || fail 'unaccepted scenario deleted fixtures'

reset_cleanup
passed=false
set +e
nc_cleanup 19 >/dev/null 2>&1
actual=$?
set -e
[[ "${actual}" == 19 ]] || fail "failed scenario exit changed from 19 to ${actual}"
! grep -q '^delete ' "${cleanup_dir}/calls" || fail 'partial creation failure deleted fixtures'

for cleanup_mode in replace-on-delete reject-namespace timeout-namespace; do
  reset_cleanup
  if nc_cleanup 0 >/dev/null 2>&1; then fail "cleanup accepted ${cleanup_mode}"; fi
  if [[ "${cleanup_mode}" == replace-on-delete ]]; then
    [[ "$(<"${cleanup_dir}/namespace-live")" == successor-uid ]] || fail 'Namespace successor was not preserved'
  fi
  [[ -s "${cleanup_dir}/runtime-live" ]] || fail "runtime deleted after ${cleanup_mode}"
  ! grep -q '^delete --raw /apis/ome.io' "${cleanup_dir}/calls" || fail "runtime delete attempted after ${cleanup_mode}"
done
[[ "$(<"${cleanup_dir}/namespace-live")" == namespace-uid ]] || fail 'namespace timeout changed identity'

echo 'no-capacity serial cleanup failure tests passed'

# Controller observations qualify only when the ISVC, IR and source Pod retain
# the exact recorded owner chain (Pod -> InferenceReplica -> InferenceService).
owned_isvc='{"kind":"InferenceService","metadata":{"name":"no-capacity","uid":"isvc-uid"}}'
owned_ir='{"kind":"InferenceReplica","metadata":{"name":"no-capacity-engine","uid":"ir-uid","ownerReferences":[{"kind":"InferenceService","uid":"isvc-uid","controller":true}]}}'
owned_pods='{"items":[{"kind":"Pod","metadata":{"name":"no-capacity-engine-0-default-0","uid":"source-uid","ownerReferences":[{"kind":"InferenceReplica","uid":"ir-uid","controller":true}]}}]}'
identity="$(nc_source_identity isvc-uid "${owned_isvc}" "${owned_ir}" "${owned_pods}")" || fail 'valid source owner chain rejected'
jq -e '.irUID=="ir-uid" and .sourceUID=="source-uid"' <<<"${identity}" >/dev/null || fail 'wrong source identities captured'
nc_source_owned isvc-uid ir-uid source-uid "${owned_isvc}" "${owned_ir}" "${owned_pods}" || fail 'recorded source owner chain rejected'
for mutation in isvc ir pod pod-owner; do
  isvc="${owned_isvc}"; ir="${owned_ir}"; pods="${owned_pods}"
  case "${mutation}" in
    isvc) isvc="$(jq '.metadata.uid="successor"' <<<"${isvc}")" ;;
    ir) ir="$(jq '.metadata.uid="successor"' <<<"${ir}")" ;;
    pod) pods="$(jq '.items[0].metadata.uid="successor"' <<<"${pods}")" ;;
    pod-owner) pods="$(jq '.items[0].metadata.ownerReferences[0].uid="successor"' <<<"${pods}")" ;;
  esac
  if nc_source_owned isvc-uid ir-uid source-uid "${isvc}" "${ir}" "${pods}"; then
    fail "accepted ${mutation} replacement in source observation"
  fi
done

watch_file="${test_dir}/owned-watch.json"
printf '%s\n' "$(jq -cn --argjson object "${owned_isvc}" '{type:"ADDED",object:$object}')" >"${watch_file}"
nc_watch_owned isvc isvc-uid ir-uid source-uid "${watch_file}" || fail 'valid ISVC watch rejected'
printf '%s\n' "$(jq -cn --argjson object "${owned_ir}" '{type:"MODIFIED",object:$object}')" >"${watch_file}"
nc_watch_owned ir isvc-uid ir-uid source-uid "${watch_file}" || fail 'valid IR watch rejected'
printf '%s\n' "$(jq -cn --argjson object "$(jq '.items[0]' <<<"${owned_pods}")" '{type:"MODIFIED",object:$object}')" >"${watch_file}"
nc_watch_owned pods isvc-uid ir-uid source-uid "${watch_file}" || fail 'valid Pod watch rejected'
for kind in isvc ir pods; do
  case "${kind}" in
    isvc) object="$(jq '.metadata.uid="successor"' <<<"${owned_isvc}")" ;;
    ir) object="$(jq '.metadata.ownerReferences[0].uid="successor"' <<<"${owned_ir}")" ;;
    pods) object="$(jq '.items[0].metadata.ownerReferences[0].uid="successor" | .items[0]' <<<"${owned_pods}")" ;;
  esac
  printf '%s\n' "$(jq -cn --argjson object "${object}" '{type:"MODIFIED",object:$object}')" >"${watch_file}"
  if nc_watch_owned "${kind}" isvc-uid ir-uid source-uid "${watch_file}"; then
    fail "accepted successor in ${kind} watch"
  fi
done

echo 'no-capacity observation ownership tests passed'
