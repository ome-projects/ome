#!/usr/bin/env bash
# Offline behavioral checks: no kubectl, credentials, VM or cluster access.
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail() { echo "$*" >&2; exit 1; }
[[ -f "${dir}/semantic-retry.sh" ]] || fail 'semantic retry helper is not implemented'
source "${dir}/semantic-retry.sh"
if [[ "${1:-}" == --case ]]; then
  test_case="$2" mode="$3" artifact_dir="$4"
  script_dir="${dir}"
  fixture="$(jq -L "${dir}" -f "${dir}/testdata/evidence-semantic-retry.jq" "${dir}/testdata/evidence-valid.json")"
  sr_baseline="$(jq -c '.semanticRetry.baseline' <<<"${fixture}")"
  sr_entry="$(jq -c '.semanticRetry.entry' <<<"${fixture}")"
  sr_owner_uid=owner-uid
  source_json="$(jq -c '.source' <<<"${fixture}")"
  replacement_json="$(jq -c '.surge.replacement' <<<"${fixture}")"
  sr_deadline=$((SECONDS + 3))
  case "${test_case}" in
    *prepared*) selector='.semanticRetry.prepared' ;;
    *paused*) selector='.semanticRetry.paused[0]' ;;
    *released*) selector='.semanticRetry.released' ;;
    *completed*) selector='.semanticRetry.completed[0]' ;;
  esac
  sr_snapshot() {
    if [[ "${test_case}" == unreadable-* ]]; then printf '{}\n'; return 1; fi
    jq -c "${selector} | .pods.items[0].status.conditions[0].status=\"False\"" <<<"${fixture}"
  }
  sr_set_policy() { touch "${artifact_dir}/unexpected-write"; }
  sr_delete_uid() { touch "${artifact_dir}/unexpected-write"; }
  if [[ "${mode}" == no-errexit ]]; then set +e; fi
  case "${test_case}" in
    *prepared*) sr_after_trigger ;;
    *paused*) sr_collect_cycles paused 3 paused ;;
    *released*) sr_wait_projection 3 false released ;;
    *completed*) sr_collect_cycles completed 3 completed ;;
  esac
  exit "$?"
fi
sr_name=alfred-e2e-retry-test
# Kubernetes labels a successfully evaluated false validation invalid_error,
# not no_error. Mirror the actual API-server metric, including label order.
metrics='process_start_time_seconds 1234
apiserver_validating_admission_policy_check_total{enforcement_action="deny",error_type="invalid_error",policy="alfred-e2e-retry-test",policy_binding="alfred-e2e-retry-test"} 2
apiserver_validating_admission_policy_check_total{enforcement_action="deny",error_type="invalid_error",policy="unrelated",policy_binding="unrelated"} 99'
parsed="$(sr_parse_metrics <<<"${metrics}")"
[[ "${parsed}" == '{"denyCount":2,"processStart":1234}' ]] || fail 'metric parser did not isolate fixture denials'
for bad in '' "${metrics/2/NaN}" "${metrics/invalid_error/compile_error}" "${metrics/invalid_error/no_error}" "${metrics/invalid_error/out_of_budget}" "${metrics}"$'\n'"${metrics}"; do
  if sr_parse_metrics <<<"${bad}" >/dev/null 2>&1; then fail 'metric parser accepted missing, invalid, or ambiguous evidence'; fi
done
# The request body is a JSON patch that preserves owner UID/RV and adds only
# the prepared annotation. Quotes and newlines in payload must round-trip.
sr_owner_uid=owner-uid
sr_entry='{"uuid":"01234567-89ab-4cde-8fab-0123456789ab","payload":"{\"schemaVersion\":\"v1\"}"}'
patch="$(sr_request_patch '{"metadata":{"uid":"owner-uid","resourceVersion":"123","annotations":{"existing":"kept"}}}' "${sr_entry}")"
jq -e 'length == 3 and .[0] == {op:"test",path:"/metadata/uid",value:"owner-uid"} and
  .[1] == {op:"test",path:"/metadata/resourceVersion",value:"123"} and
  .[2] == {op:"add",path:"/metadata/annotations/ome.io~1migration-request-v1-01234567-89ab-4cde-8fab-0123456789ab",value:"{\"schemaVersion\":\"v1\"}"}' <<<"${patch}" >/dev/null || fail 'probe patch changed identity or payload'
if sr_request_patch '{"metadata":{"uid":"successor","resourceVersion":"123","annotations":{}}}' "${sr_entry}" >/dev/null 2>&1; then
  fail 'probe patch accepted a successor owner'
fi
# Execute the actual cleanup helper against a boundary that records requests.
# Deletion must carry UID preconditions, and must never delete a successor.
calls=''
mock_kube() {
  if [[ "$*" == *'get validatingadmissionpolicybinding '* ]]; then
    printf '{"metadata":{"uid":"%s"}}\n' "${live_uid}"
  elif [[ "$*" == *'delete --raw='* ]]; then
    IFS= read -r body
    jq -e '.apiVersion == "v1" and .kind == "DeleteOptions" and .preconditions.uid == "binding-uid"' <<<"${body}" >/dev/null || return 1
    calls="${calls}delete"
  else
    fail "unexpected cleanup command: $*"
  fi
}
kube=(mock_kube)
live_uid=binding-uid
sr_delete_uid validatingadmissionpolicybinding binding-uid
[[ "${calls}" == delete ]] || fail 'cleanup did not send UID-fenced delete'
live_uid=successor
calls=''
if sr_delete_uid validatingadmissionpolicybinding binding-uid >/dev/null 2>&1; then
  fail 'cleanup accepted a successor admission object'
fi
[[ -z "${calls}" ]] || fail 'cleanup deleted a successor admission object'
# Exercise the actual admission object renderer. Keep namespace, owner UID,
# exact resource/update scope and Alfred identity fences when changing it.
policy="$(jq -cn -L "${dir}" 'include "semantic-retry-admission";
  retry_policy("alfred-e2e-retry-test";"owner-uid";"system:serviceaccount:ome:ome-alfred")')"
jq -e '.spec.failurePolicy == "Fail" and
  .spec.matchConstraints.namespaceSelector.matchLabels["kubernetes.io/metadata.name"] == "alfred-e2e" and
  .spec.matchConstraints.resourceRules == [{operations:["UPDATE"],apiGroups:["ome.io"],apiVersions:["v1beta1"],resources:["inferenceservices"],resourceNames:["single"],scope:"Namespaced"}] and
  (.spec.matchConditions|length) == 3 and
  any(.spec.matchConditions[]; .name == "alfred-identity" and .expression == "request.userInfo.username == \"system:serviceaccount:ome:ome-alfred\"") and
  any(.spec.matchConditions[]; .name == "fresh-fixture" and .expression == "object.metadata.uid == \"owner-uid\" && oldObject.metadata.uid == \"owner-uid\"") and
  any(.spec.matchConditions[]; .name == "new-migration-request" and (.expression|contains("oldObject.metadata.annotations"))) and
  .spec.validations == [{expression:"false",message:"semantic-retry publication held"}]' <<<"${policy}" >/dev/null || fail 'admission renderer broadened fixture denial'
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT
artifact_dir="${test_dir}"
namespace=alfred-e2e
isvc_name=single
sr_username=system:serviceaccount:ome:ome-alfred
mock_probe_kube() {
  if [[ "$*" == *'get inferenceservice '* ]]; then
    printf '{"metadata":{"uid":"owner-uid","resourceVersion":"123","annotations":{}}}\n'
  elif [[ "$*" == *'patch inferenceservice '* && "$*" == *'--dry-run=server'* && "$*" == *'--as=system:serviceaccount:ome:ome-alfred'* ]]; then
    case "${probe_mode}" in
      denied) echo 'alfred-e2e-retry-test: semantic-retry publication held' >&2; return 1 ;;
      forbidden) echo 'Forbidden by RBAC' >&2; return 1 ;;
      allowed) jq -cn --argjson entry "${sr_entry}" '{metadata:{uid:"owner-uid",annotations:{("ome.io/migration-request-v1-"+$entry.uuid):$entry.payload}}}' ;;
    esac
  else fail "probe attempted an unexpected or persistent API operation: $*"; fi
}
kube=(mock_probe_kube)
probe_mode=denied
sr_probe denied "${sr_entry}" || fail 'specific API denial was not recognized'
if sr_probe allowed "${sr_entry}"; then fail 'denied publication accepted as unblocked'; else [[ "$?" == 2 ]] || fail 'binding-cache delay not identified'; fi
probe_mode=forbidden
if sr_probe denied "${sr_entry}" 2>/dev/null; then fail 'RBAC denial mistaken for test policy'; fi
probe_mode=allowed
sr_probe allowed "${sr_entry}" || fail 'accepted dry-run payload was not recognized'
if sr_probe denied "${sr_entry}"; then fail 'unenforced policy accepted'; else [[ "$?" == 2 ]] || fail 'policy-cache delay not identified'; fi
# Preparation probes run before sr_after_trigger sets the shared retry deadline.
# Exercise the real wait/probe path under nounset with no optional deadline.
(
  unset sr_deadline
  probe_mode=denied
  sr_wait_probe denied "${sr_entry}"
  echo 'initial admission wait completed'
) >"${test_dir}/initial-wait.stdout" 2>"${test_dir}/initial-wait.stderr" || fail 'initial admission wait rejected an unset optional deadline'
grep -q '^initial admission wait completed$' "${test_dir}/initial-wait.stdout" || fail 'initial admission wait did not finish'
(
  sr_deadline=$((SECONDS - 1))
  ((sr_deadline > 0)) || sr_deadline=1
  SECONDS=$((sr_deadline + 1))
  sr_probe() { touch "${test_dir}/probe-after-deadline"; }
  if sr_wait_probe denied "${sr_entry}" 2>/dev/null; then fail 'expired shared deadline accepted'; fi
  [[ ! -f "${test_dir}/probe-after-deadline" ]] || fail 'expired shared deadline was ignored'
)
for mode in errexit no-errexit; do
  for stage in prepared paused released completed; do
    for failure in unreadable unsafe; do
      case_dir="${test_dir}/${mode}-${failure}-${stage}"
      mkdir "${case_dir}"
      if bash "$0" --case "${failure}-${stage}" "${mode}" "${case_dir}" >"${case_dir}/stdout" 2>"${case_dir}/stderr"; then
        fail "${stage} hook swallowed ${failure} (${mode})"
      fi
      [[ ! -f "${case_dir}/unexpected-write" ]] || fail "${stage} wrote after ${failure} (${mode})"
    done
  done
done
# The live cycle collector must not count the same timestamp twice, even when
# it reappears after another timestamp. Only the snapshot boundary is replaced.
(
  script_dir="${dir}"
  fixture="$(jq -L "${dir}" -f "${dir}/testdata/evidence-semantic-retry.jq" "${dir}/testdata/evidence-valid.json")"
  sr_baseline="$(jq -c '.semanticRetry.baseline' <<<"${fixture}")"
  sr_entry="$(jq -c '.semanticRetry.entry' <<<"${fixture}")"
  source_json="$(jq -c '.source' <<<"${fixture}")"
  artifact_dir="${test_dir}/cycles"
  mkdir "${artifact_dir}"
  printf '0\n' >"${artifact_dir}/cursor"
  sr_snapshot() {
    local cursor
    cursor="$(<"${artifact_dir}/cursor")"
    ((cursor < 4)) || return 1
    printf '%s\n' "$((cursor+1))" >"${artifact_dir}/cursor"
    jq -c --argjson index "${cursor}" '.semanticRetry.paused[([0,1,0,2][$index])]' <<<"${fixture}"
  }
  sleep() { :; }
  sr_deadline=$((SECONDS + 3))
  sr_collect_cycles paused 3 paused || fail 'three distinct cycles were not collected'
  [[ "$(<"${artifact_dir}/cursor")" == 4 ]] || fail 'collector counted a repeated timestamp'
  jq -es 'length == 3' "${artifact_dir}/retry-paused-cycles.jsonl" >/dev/null || fail 'wrong retained cycle count'
)
# The completed-state sample already present when collection starts is a
# baseline, not one of the three subsequent Alfred cycles.
(
  script_dir="${dir}"
  fixture="$(jq -L "${dir}" -f "${dir}/testdata/evidence-semantic-retry.jq" "${dir}/testdata/evidence-valid.json")"
  sr_baseline="$(jq -c '.semanticRetry.baseline' <<<"${fixture}")"
  sr_entry="$(jq -c '.semanticRetry.entry' <<<"${fixture}")"
  source_json="$(jq -c '.source' <<<"${fixture}")"
  replacement_json="$(jq -c '.surge.replacement' <<<"${fixture}")"
  artifact_dir="${test_dir}/completed-cycles"
  mkdir "${artifact_dir}"
  printf '0\n' >"${artifact_dir}/cursor"
  sr_snapshot() {
    local cursor
    cursor="$(<"${artifact_dir}/cursor")"
    ((cursor < 5)) || return 1
    printf '%s\n' "$((cursor+1))" >"${artifact_dir}/cursor"
    jq -c --argjson index "${cursor}" '
      if $index < 2 then .semanticRetry.completionBaseline else .semanticRetry.completed[$index-2] end' <<<"${fixture}"
  }
  sleep() { :; }
  sr_deadline=$((SECONDS + 3))
  sr_collect_cycles completed 3 completed || fail 'post-completion cycles were not collected'
  [[ "$(<"${artifact_dir}/cursor")" == 5 ]] || fail 'collector counted the pre-existing completion report'
  [[ -s "${artifact_dir}/retry-completion-baseline.json" ]] || fail 'completion boundary was not retained'
)
# Retry only after the same owner's resourceVersion advances, always rebuilding
# the UID/RV-fenced patch from a fresh API read. Persistent churn stays bounded.
for policy_case in transient churn stable-error successor missing-annotations; do
  (
    policy_live="${test_dir}/policy-${policy_case}.json"
    printf '%s\n' '{"metadata":{"uid":"owner-uid","resourceVersion":"123","annotations":{"existing":"kept"}}}' >"${policy_live}"
    patch_attempts=0
    sleep() { :; }
    mock_retry_policy_kube() {
      if [[ "$*" == *'get inferenceservice '* ]]; then
        cat "${policy_live}"
      elif [[ "$*" == *'patch inferenceservice '* ]]; then
        local patch='' previous='' arg
        patch_attempts=$((patch_attempts + 1))
        for arg in "$@"; do
          if [[ "${previous}" == -p ]]; then patch="${arg}"; fi
          previous="${arg}"
        done
        # The API changes after the GET but before evaluating the JSON patch.
        if [[ "${policy_case}" == churn || "${patch_attempts}" == 1 ]]; then
          case "${policy_case}" in
            transient|churn) mutation='.metadata.resourceVersion |= (tonumber + 1 | tostring)' ;;
            stable-error) return 1 ;;
            successor) mutation='.metadata.uid="successor" | .metadata.resourceVersion="124"' ;;
            missing-annotations) mutation='.metadata.resourceVersion="124" | del(.metadata.annotations)' ;;
          esac
          jq "${mutation}" "${policy_live}" >"${policy_live}.next"
          mv "${policy_live}.next" "${policy_live}"
        fi
        jq -e --slurpfile live "${policy_live}" '
          length == 3 and .[0] == {op:"test",path:"/metadata/uid",value:$live[0].metadata.uid} and
          .[1] == {op:"test",path:"/metadata/resourceVersion",value:$live[0].metadata.resourceVersion} and
          .[2].op == "add" and .[2].path == "/metadata/annotations/ome.io~1placement-execution"' <<<"${patch}" >/dev/null || return 1
        jq --argjson patch "${patch}" '.metadata.annotations["ome.io/placement-execution"]=$patch[2].value' \
          "${policy_live}" >"${policy_live}.next"
        mv "${policy_live}.next" "${policy_live}"
      else fail "unexpected retry authority operation: $*"; fi
    }
    kube=(mock_retry_policy_kube)
    if [[ "${policy_case}" == transient ]]; then
      sr_set_policy 3 false || fail 'authority write did not recover from one resourceVersion conflict'
      [[ "${patch_attempts}" == 2 ]] || fail 'authority conflict did not retry exactly once'
      jq -e '.metadata.uid == "owner-uid" and .metadata.annotations.existing == "kept" and
        (.metadata.annotations["ome.io/placement-execution"] | fromjson | .revision == 3 and .pauseSurge == false)' \
        "${policy_live}" >/dev/null || fail 'retried authority write changed identity or unrelated metadata'
    else
      if sr_set_policy 3 false 2>/dev/null; then fail "authority write accepted ${policy_case}"; fi
      if [[ "${policy_case}" == churn ]]; then expected_attempts=5; else expected_attempts=1; fi
      [[ "${patch_attempts}" == "${expected_attempts}" ]] || fail "wrong retry bound for ${policy_case}: ${patch_attempts}"
      jq -e '.metadata.annotations["ome.io/placement-execution"] == null' "${policy_live}" >/dev/null || fail "authority write mutated ${policy_case}"
    fi
  )
done
# A successor racing the cleanup pause must never receive that policy. Exercise
# the actual cleanup and authority writer; replace only the API boundary.
(
  source "${dir}/placement-pause.sh"
  sr_started=true
  sr_policy_uid=policy-uid
  sr_binding_uid=binding-uid
  sr_binding_removed=false
  sr_owner_uid=owner-uid
  mock_policy_kube() {
    if [[ "$*" == *'get inferenceservice '* ]]; then
      printf '{"metadata":{"uid":"owner-uid","resourceVersion":"123","annotations":{}}}\n'
    elif [[ "$*" == *'annotate inferenceservice '* ]]; then
      touch "${test_dir}/successor-mutated"
      return 1
    elif [[ "$*" == *'patch inferenceservice '* ]]; then
      local patch='' previous='' arg
      for arg in "$@"; do
        if [[ "${previous}" == -p ]]; then patch="${arg}"; fi
        previous="${arg}"
      done
      jq -e '.[0] == {op:"test",path:"/metadata/uid",value:"owner-uid"} and
        .[1] == {op:"test",path:"/metadata/resourceVersion",value:"123"} and
        .[2].path == "/metadata/annotations/ome.io~1placement-execution" and
        (.[2].value|fromjson|.revision == 4 and .pauseSurge == true)' <<<"${patch}" >/dev/null || return 1
      touch "${test_dir}/fenced-conflict"
      return 1
    else fail "unexpected authority-write boundary: $*"; fi
  }
  kube=(mock_policy_kube)
  sr_delete_uid() { touch "${test_dir}/admission-unblocked"; }
  if sr_cleanup; then fail 'cleanup ignored a raced owner conflict'; fi
  [[ ! -f "${test_dir}/successor-mutated" ]] || fail 'cleanup changed the successor owner'
  [[ -f "${test_dir}/fenced-conflict" ]] || fail 'cleanup did not enforce owner UID/resourceVersion'
  [[ ! -f "${test_dir}/admission-unblocked" ]] || fail 'cleanup unblocked admission after owner conflict'
)
echo 'semantic retry helper tests passed'
