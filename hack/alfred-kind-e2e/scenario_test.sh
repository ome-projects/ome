#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scenario="${script_dir}/scenario.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

expect_rejected() {
  local name="$1"
  local want="$2"
  shift 2
  local output
  if output="$("$@" 2>&1)"; then
    echo "${name}: unexpectedly succeeded" >&2
    return 1
  fi
  if [[ "${output}" != *"${want}"* ]]; then
    echo "${name}: output did not contain ${want}: ${output}" >&2
    return 1
  fi
}

expect_rejected missing-state-dir \
  "STATE_DIR must be an absolute directory" \
  env -u STATE_DIR "${scenario}" maintenance-single
expect_rejected relative-state-dir \
  "STATE_DIR must be an absolute directory" \
  env STATE_DIR=relative "${scenario}" maintenance-single
expect_rejected missing-kubeconfig \
  "kubeconfig not found" \
  env STATE_DIR="${tmp_dir}" "${scenario}" maintenance-single

mkdir "${tmp_dir}/state with spaces"
expect_rejected state-dir-with-spaces \
  "kubeconfig not found" \
  env STATE_DIR="${tmp_dir}/state with spaces" "${scenario}" maintenance-single

touch "${tmp_dir}/kubeconfig"
expect_rejected unsupported-scenario \
  "unsupported scenario" \
  env STATE_DIR="${tmp_dir}" "${scenario}" something-else

# Exercise the actual EXIT cleanup without a cluster. Bash 3.2 may pass status
# zero after a fatal nounset expansion; an unsealed scenario must still fail.
cleanup_probe='
  set -euo pipefail
  scenario=maintenance-single
  artifact_dir=offline-diagnostics
  request_watch_pid=""
  triggered_node=""
  delayed=false
  passed="$2"
  dump_diagnostics() { :; }
  eval "$(sed -n "/^cleanup() {/,/^trap cleanup EXIT/p" "$1")"
  if [[ "$3" == nounset ]]; then
    (( ${missing_deadline:-0} > 0 && missing_deadline < 1 ))
  fi
  exit 0
'
for failure in zero nounset; do
  expect_rejected "unsealed-${failure}" \
    'maintenance-single failed; diagnostics: offline-diagnostics' \
    bash -c "${cleanup_probe}" -- "${scenario}" false "${failure}"
done
bash -c "${cleanup_probe}" -- "${scenario}" true zero

echo "scenario preflight tests passed"
