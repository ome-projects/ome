#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
valid="${dir}/testdata/evidence-gang-valid.json"
verifier="${dir}/verify-gang-evidence.jq"
jq -e -f "${verifier}" "${valid}" >/dev/null
for mutation in \
  '.preTrigger.migrationRequestCount=1' \
  '.source.podGroup.minMember=1' \
  '.source.pods[1].node=.source.pods[0].node' \
  '.source.pods[1].zone="wrong"' \
  '.source.pods[1].gpuRequest=1' \
  '.surge.pods=[.surge.pods[0]]' \
  '.surge.sourcePodsServing=1' \
  '.surge.replacementLeaderEndpointReady=true' \
  '.surge.alfredMaintenanceDrainedAt="2026-09-14T12:00:03Z"' \
  'del(.handoff)' \
  '.handoff[0].sourceSafe=false | .handoff[0].replacementSafe=false' \
  '.handoff[0].replacementUIDs=["unrelated-leader","unrelated-worker"]' \
  '.handoff |= map(.routingService="gang-engine")' \
  '.completed.sourcePodUIDsPresent=["source-worker"]' \
  '.completed.replacementPodsReady=1' \
  '.completed.migration.phase="Draining"' \
  '.completed.migration.phase="Failed"' \
  'del(.completed.migration.completedAt)' \
  '.completed.migration.completedAt="2026-09-14T11:59:59Z"' \
  '.alfred.maintenanceRequestedAt="2026-09-14T12:00:00.123456790Z" | .alfred.maintenanceDrainedAt="2026-09-14T12:00:00.123456789Z"' \
  '.alfred.requestUUID="ffffffff-ffff-4fff-8fff-ffffffffffff"'; do
  if jq "${mutation}" "${valid}" | jq -e -f "${verifier}" >/dev/null 2>&1; then
    echo "gang verifier falsely accepted ${mutation}" >&2
    exit 1
  fi
done
jq '.alfred.maintenanceRequestedAt="2026-09-14T12:00:00.123456789Z" |
  .alfred.maintenanceDrainedAt="2026-09-14T05:00:00.123456790-07:00"' "${valid}" |
  jq -e -f "${verifier}" >/dev/null
echo "gang evidence verifier tests passed"
