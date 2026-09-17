#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
verifier="${script_dir}/verify-evidence.jq"
valid="${script_dir}/testdata/evidence-valid.json"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

require_pass() {
  local name="$1"
  local input="$2"
  if ! jq -e -f "${verifier}" "${input}" >/dev/null; then
    echo "expected verifier to accept ${name}" >&2
    return 1
  fi
}

require_fail() {
  local name="$1"
  local filter="$2"
  local input="${tmp_dir}/${name}.json"
  jq "${filter}" "${valid}" >"${input}"
  if jq -e -f "${verifier}" "${input}" >/dev/null 2>&1; then
    echo "verifier falsely accepted ${name}" >&2
    return 1
  fi
}

require_pass valid "${valid}"

# Four healthy, distinct initial Instances must actually use columns on the
# wire. Relabeling the old dense evidence is not a ColumnarV2 qualification.
jq '.scenario = "maintenance-columnar" |
  .preTrigger.irReadyReplicas = 4 | .preTrigger.irServingReplicas = 4 |
  .preTrigger.irAvailableReplicas = 4 | .completed.irReadyReplicas = 4 |
  .completed.irServingReplicas = 4 | .completed.irAvailableReplicas = 4 |
  .surge.replacement.instance = 4 | .completed.migration.surgeInstance = 4 |
  .columnar = {initialCount:4, baselineUIDs:[.source.uid,"baseline-1","baseline-2","baseline-3"],
    baselineNodes:[.source.node,"alfred-gpu-b","alfred-gpu-c","alfred-gpu-d"],
    uniqueRequestCount:1,
    rawBeforeTrigger:{status:{readyReplicas:4,servingReplicas:4,availableReplicas:4,
      instanceStatusEncoding:"ColumnarV2",instanceStatusColumns:{members:"0-3",phases:[{value:"Ready",indexes:"0-3"}]}}},
    rawCompleted:{status:{readyReplicas:4,servingReplicas:4,availableReplicas:4,
      instanceStatusEncoding:"ColumnarV2",instanceStatusColumns:{members:"1-4",phases:[{value:"Ready",indexes:"1-4"}]}}},
    decodedBeforeTrigger:{rawEncoding:"ColumnarV2",rows:[range(0;4)|{index:.,phase:"Ready"}]},
    decodedCompleted:{rawEncoding:"ColumnarV2",rows:[range(1;5)|{index:.,phase:"Ready"}]}}
  ' "${valid}" >"${tmp_dir}/columnar.json"
require_pass maintenance-columnar "${tmp_dir}/columnar.json"
for mutation in \
  '.columnar.rawBeforeTrigger.status.instanceStatusEncoding = "DenseV1"' \
  '.columnar.rawCompleted.status.instanceStatusEncoding = "DenseV1"' \
  '.columnar.rawBeforeTrigger.status.instanceStatuses = [{index:0,phase:"Ready"}]' \
  '.columnar.rawCompleted.status.instanceStatuses = []' \
  'del(.columnar.rawBeforeTrigger.status.instanceStatusColumns)' \
  '.columnar.rawCompleted.status.instanceStatusColumns = null' \
  '.columnar.rawBeforeTrigger.status.readyReplicas = 1' \
  '.columnar.rawCompleted.status.availableReplicas = 1' \
  '.preTrigger.irReadyReplicas = 1' '.completed.irServingReplicas = 1' \
  '.columnar.initialCount = 1' '.columnar.uniqueRequestCount = 2' \
  '.columnar.baselineUIDs[1] = .surge.replacement.uid' \
  '.columnar.baselineNodes[1] = .source.node' \
  '.columnar.decodedBeforeTrigger.rows[0].phase = "Pending"' \
  '.columnar.decodedCompleted.rows |= .[0:1]'; do
  jq "${mutation}" "${tmp_dir}/columnar.json" >"${tmp_dir}/bad-columnar.json"
  if jq -e -f "${verifier}" "${tmp_dir}/bad-columnar.json" >/dev/null 2>&1; then
    echo "verifier falsely accepted columnar mutation: ${mutation}" >&2
    exit 1
  fi
done
require_fail single-replica-count-change '.preTrigger.irReadyReplicas = 4 | .completed.irReadyReplicas = 4'
require_fail pretrigger-already-migrating '.preTrigger.migrationRequestCount = 1'
require_fail source-not-serving '.source.serving = false'
require_fail source-not-in-endpoints '.source.endpointReady = false'
require_fail malformed-request-uuid '.request.uuid = "not-a-uuid"'
require_fail request-key-mismatch '.request.annotationKey = "ome.io/migration-request-v1-ffffffff-ffff-4fff-8fff-ffffffffffff"'
require_fail request-source-mismatch '.request.payload.from_node = "alfred-gpu-z"'
require_fail source-removed-before-surge-ready '.surge.sourcePresent = false'
require_fail source-drained-before-surge-ready '.surge.sourceServing = false'
require_fail source-not-routed-during-hold '.surge.sourceEndpointReady = false'
require_fail replacement-not-held '.surge.replacementReady = true'
require_fail replacement-routed-during-hold '.surge.replacementEndpointReady = true'
require_fail hold-too-short '.surge.holdSeconds = 0'
require_fail premature-drained-observation '.surge.alfredMaintenanceDrainedAt = "2026-09-14T12:00:03Z"'
require_fail alfred-missed-source-occupant '.surge.alfredOMEGPUOccupantsPresent = false'
require_fail replacement-reuses-source '.surge.replacement.uid = "source-uid"'
require_fail replacement-on-source-node '.surge.replacement.node = "alfred-gpu-a"'
require_fail replacement-unscheduled '.surge.replacement.node = ""'
require_fail replacement-node-null '.surge.replacement.node = null'
require_fail missing-handoff 'del(.handoff)'
require_fail unsafe-handoff '.handoff[0].sourceSafe = false | .handoff[0].replacementSafe = false'
require_fail wrong-handoff-replacement '.handoff[0].replacementUIDs = ["unrelated-pod"]'
require_fail unrouted-handoff '.handoff |= map(.routingService = "single-engine")'
require_fail nonterminal-migration '.completed.migration.phase = "Draining"'
require_fail failed-migration '.completed.migration.phase = "Failed"'
require_fail missing-completion-time 'del(.completed.migration.completedAt)'
require_fail completion-before-start '.completed.migration.completedAt = "2026-09-14T11:59:59Z"'
require_fail reversed-fractional-time '.alfred.maintenanceRequestedAt = "2026-09-14T12:00:00.123456790Z" | .alfred.maintenanceDrainedAt = "2026-09-14T12:00:00.123456789Z"'
require_fail source-still-present '.completed.sourcePresent = true'
require_fail source-endpoint-still-present '.completed.sourceEndpointPresent = true'
require_fail replacement-endpoint-not-ready '.completed.replacementEndpointReady = false'
require_fail workload-not-restored '.completed.irAvailableReplicas = 0'
require_fail alfred-uuid-mismatch '.alfred.requestUUID = "ffffffff-ffff-4fff-8fff-ffffffffffff"'
require_fail wrong-maintenance-trigger '.alfred.maintenanceTriggers = ["label:maintenance.example.com/state=patching"]'
require_fail no-drained-observation '.alfred.maintenanceDrainedAt = null'
require_fail source-reoccupied '.alfred.omeGPUOccupantsPresent = true'

jq '.alfred.maintenanceRequestedAt = "2026-09-14T12:00:00.123456789Z" |
  .alfred.maintenanceDrainedAt = "2026-09-14T05:00:00.123456790-07:00"' "${valid}" >"${tmp_dir}/fractional.json"
require_pass fractional-nanosecond-order "${tmp_dir}/fractional.json"

# A health signal is immediate when True. The one-minute suspicion window
# quarantines a recently recovered False node, without evacuating its source.
jq '.scenario = "unhealthy-single" |
 .request.payload.reason = "NodeUnhealthy" | .alfred.reason = "NodeUnhealthy" |
 .alfred.maintenanceTriggers = [] | .alfred.maintenanceRequestedAt = null |
 .alfred.maintenanceDrainedAt = null |
 .alfred.healthState = "Unhealthy" | .alfred.signaledAt = "2026-09-14T12:02:00Z" |
 .alfred.drainedAt = "2026-09-14T12:02:15Z" |
 .health = {conditionType:"GpuUnhealthy", conditionStatus:"True",heartbeatAdvanced:true,
   recoveryTransitionTime:"2026-09-14T12:01:00Z",
   transitionTime:"2026-09-14T12:02:00Z", transitionTimeAfterHeartbeat:"2026-09-14T12:02:00Z",
   recoveryWindowSeconds:60, recoveryObservedSeconds:60, recoveryRequestCount:0,
   recoveryMigrationCount:0, recoverySourceReady:true, recoverySourceServing:true,
   recoverySourceEndpointReady:true, recoverySuspectObserved:true}' "${valid}" >"${tmp_dir}/health.json"
require_pass unhealthy-single "${tmp_dir}/health.json"
jq '.health.recoveryTransitionTime="2026-09-14T12:01:00.250000001Z" |
  .health.transitionTime="2026-09-14T12:02:00.250000001Z" |
  .health.transitionTimeAfterHeartbeat=.health.transitionTime |
  .alfred.signaledAt="2026-09-14T12:02:00.250000001Z" |
  .alfred.drainedAt="2026-09-14T12:02:00.250000002Z"' "${tmp_dir}/health.json" >"${tmp_dir}/fractional-health.json"
require_pass fractional-health-boundary "${tmp_dir}/fractional-health.json"
if jq '.health.transitionTime="2026-09-14T12:02:00.250000000Z" |
  .health.transitionTimeAfterHeartbeat=.health.transitionTime' "${tmp_dir}/fractional-health.json" |
  jq -e -f "${verifier}" >/dev/null; then
  echo 'verifier accepted a recovery window shorter than 60 seconds' >&2; exit 1
fi
for mutation in '.health.recoveryRequestCount = 1' '.health.recoveryMigrationCount = 1' \
  '.health.recoveryObservedSeconds = 59' '.health.recoverySourceEndpointReady = false' \
  '.health.recoverySuspectObserved = false' '.health.conditionStatus = "False"' \
  '.health.recoveryTransitionTime = null' '.health.heartbeatAdvanced = false' \
  '.health.transitionTimeAfterHeartbeat = "2026-09-14T12:02:01Z"' \
  '.request.payload.reason = "NodeMaintenance"' '.alfred.drainedAt = null'; do
  jq "${mutation}" "${tmp_dir}/health.json" >"${tmp_dir}/bad-health.json"
  if jq -e -f "${verifier}" "${tmp_dir}/bad-health.json" >/dev/null 2>&1; then
    echo "verifier falsely accepted health mutation: ${mutation}" >&2
    exit 1
  fi
done
jq '.scenario = "restart-single" | .request.payload.reason = "NodeMaintenance" |
 .restart = {deployment:"ome-alfred", oldPodUID:"old-alfred", newPodUID:"new-alfred",
   journalUUIDBefore:"01234567-89ab-4cde-8fab-0123456789ab",
   journalUUIDAfter:"01234567-89ab-4cde-8fab-0123456789ab",
   leadershipRecovered:true, sourceReady:true, sourceServing:true, sourceEndpointReady:true,
   uniqueRequestCount:1, workloadDispatchCount:1, migrationCount:1}' "${valid}" >"${tmp_dir}/restart.json"
require_pass restart-single "${tmp_dir}/restart.json"
for mutation in '.restart.journalUUIDAfter = "different"' '.restart.uniqueRequestCount = 2' \
  '.restart.workloadDispatchCount = 2' '.restart.migrationCount = 2' \
  '.restart.newPodUID = "old-alfred"' '.restart.sourceEndpointReady = false' \
  '.restart.leadershipRecovered = false' '.restart.deployment = "ome-controller-manager"'; do
  jq "${mutation}" "${tmp_dir}/restart.json" >"${tmp_dir}/bad-restart.json"
  if jq -e -f "${verifier}" "${tmp_dir}/bad-restart.json" >/dev/null 2>&1; then
    echo "verifier falsely accepted restart mutation: ${mutation}" >&2
    exit 1
  fi
done

echo "verify-evidence tests passed"
