#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Exercise the real hooks and jq safety checks. Only the API snapshot boundary
# is replaced; fixture status is never sent to a live cluster.
if [[ "${1:-}" == --case ]]; then
  test_case="$2" mode="$3" artifact_dir="$4"
  source "${script_dir}/placement-pause.sh"
  fixture="$(jq -L "${script_dir}" -f "${script_dir}/testdata/evidence-placement-valid.jq" \
    "${script_dir}/testdata/evidence-valid.json")"
  source_json="$(jq -c '.source' <<<"${fixture}")"
  replacement_json="$(jq -c '.surge.replacement' <<<"${fixture}")"
  request_uuid="$(jq -r '.request.uuid' <<<"${fixture}")"
  isvc="$(jq -c '.placement.paused[0].isvc' <<<"${fixture}")"
  ir="$(jq -c '.placement.baseline' <<<"${fixture}")"
  placement_isvc_uid=isvc-new
  placement_ir_uid=ir-new
  printf '%s\n' "${ir}" >"${artifact_dir}/ir-before-trigger.raw.json"
  sample_path='.placement.paused[0]'
  case "${test_case}" in
    *projection*) sample_path='.placement.released' ;;
    *allocated*) sample_path='.placement.allocated' ;;
    *completed*) sample_path='.placement.completed[0]' ;;
    missing-uid) isvc='{}' ;;
    baseline) jq '.spec.placementExecution.pauseSurge=false' <<<"${ir}" >"${artifact_dir}/ir-before-trigger.raw.json" ;;
  esac
  placement_snapshot() {
    if [[ "${test_case}" == *later && ! -f "${artifact_dir}/first-snapshot" ]]; then
      touch "${artifact_dir}/first-snapshot"
      jq -c "${sample_path}" <<<"${fixture}"
    elif [[ "${test_case}" == snapshot-* ]]; then
      printf '{}\n'
      return 1
    else
      jq -c "${sample_path} | .pods.items[0].status.conditions[0].status = \"False\"" <<<"${fixture}"
    fi
  }
  placement_set_policy() { touch "${artifact_dir}/unexpected-policy-write"; }
  if [[ "${mode}" == no-errexit ]]; then set +e; fi
  case "${test_case}" in
    *projection*) placement_wait_projection released 2 false ;;
    *allocated*) placement_pause_allocated ;;
    *completed*) placement_pause_complete ;;
    *) placement_pause_observe_and_release ;;
  esac
  exit "$?"
fi

test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT
failures=0
for mode in errexit no-errexit; do
  for test_case in snapshot-projection snapshot-observe snapshot-allocated \
    snapshot-completed-initial snapshot-completed-later missing-uid baseline \
    unsafe-projection unsafe-observe unsafe-allocated unsafe-completed-initial unsafe-completed-later; do
    case_dir="${test_dir}/${mode}-${test_case}"
    mkdir "${case_dir}"
    case "${test_case}" in
      snapshot-*) diagnostic='placement .*snapshot.*failed' ;;
      missing-uid) diagnostic='placement .*missing.*UID' ;;
      baseline) diagnostic='placement .*baseline.*failed' ;;
      unsafe-observe) diagnostic='placement .*paused.*check failed' ;;
      unsafe-completed*) diagnostic='placement .*completed.*check failed' ;;
      *) diagnostic='placement .*source.*check failed' ;;
    esac
    if bash "$0" --case "${test_case}" "${mode}" "${case_dir}" >"${case_dir}/stdout" 2>"${case_dir}/stderr"; then
      echo "hook accepted ${test_case} (${mode})" >&2
      failures=$((failures + 1))
    elif ! grep -Eq "${diagnostic}" "${case_dir}/stderr"; then
      echo "hook did not diagnose ${test_case} (${mode})" >&2
      failures=$((failures + 1))
    fi
    if [[ -f "${case_dir}/unexpected-policy-write" ]]; then
      echo "hook changed policy after ${test_case} (${mode})" >&2
      failures=$((failures + 1))
    fi
  done
done
(( failures == 0 )) || exit 1
echo 'placement pause hook failure tests passed'
