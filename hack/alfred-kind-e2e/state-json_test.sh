#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/state-json.sh"

# Shape and state match the live 2026-09-15 recovery-quarantine ConfigMap.
configmap='{"data":{"node.alfred-kwok-gpu-b":"{\"state\":\"Suspect\",\"conditions\":[{\"type\":\"GpuUnhealthy\",\"status\":\"False\",\"lastTransitionTime\":\"2026-09-15T08:57:31Z\"}],\"suspectUntil\":\"2026-09-15T08:58:31Z\",\"omeGpuOccupantsPresent\":true}"}}'
alfred_node_state_is "${configmap}" alfred-kwok-gpu-b Suspect || {
  echo 'failed to recognize the actual recovery-quarantine JSON shape' >&2; exit 1;
}
if alfred_node_state_is "${configmap}" alfred-kwok-gpu-b Unhealthy ||
   alfred_node_state_is "${configmap}" absent-node Suspect ||
   alfred_node_state_is '' alfred-kwok-gpu-b Suspect ||
   alfred_node_state_is "${configmap}}" alfred-kwok-gpu-b Suspect; then
  echo 'accepted absent, different or malformed node state' >&2; exit 1
fi
[[ "$(alfred_migration_phase '' 2>&1)" == '' ]] || {
  echo 'missing migration emitted a JSON parse error' >&2; exit 1;
}
[[ "$(alfred_migration_phase '{"phase":"SurgePending"}' 2>&1)" == SurgePending ]]
[[ "$(alfred_migration_phase '{"phase":"Failed"}' 2>&1)" == Failed ]]
echo 'optional state JSON tests passed'
