#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
baseline='{"scenario":"useful-defrag","before":{"metadata":{"uid":"pending-uid"},"spec":{"schedulerName":"alfred-default-scheduler"},"status":{"conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable","message":"Insufficient nvidia.com/gpu"}]}},"after":{"metadata":{"uid":"pending-uid"},"spec":{"nodeName":"alfred-kwok-gpu-a"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},"capacityBefore":[{"free":7},{"free":1},{"free":0},{"free":0}],"request":{"uuid":"request","payload":{"requested_by":"alfred","reason":"Fragmentation","from_node":"alfred-kwok-gpu-a","instance":0,"component":"engine"}},"migration":{"requestUUID":"request","phase":"Completed","sourceInstance":0,"surgeInstance":1,"completedAt":"2026-09-15T00:00:00Z"},"handoff":[{"sourceSafe":true,"replacementSafe":false},{"sourceSafe":false,"replacementSafe":true}]}'
jq -e -f "${dir}/verify-useful-defrag.jq" <<<"${baseline}" >/dev/null
for mutation in '.after.metadata.uid="new-pod"' '.before.spec.nodeName="already-fit"' '.before.status.conditions[0].reason="Unknown"' '.capacityBefore[0].free=8' '.request.payload.reason="NodeMaintenance"' '.migration.phase="Failed"' '.after.spec.nodeName="other-node"' '.handoff=[]' '.handoff[0].sourceSafe=false'; do
  if jq "${mutation}" <<<"${baseline}" | jq -e -f "${dir}/verify-useful-defrag.jq" >/dev/null; then
    echo "useful-defrag verifier accepted invalid evidence: ${mutation}" >&2; exit 1
  fi
done
echo 'useful-defrag evidence tests passed'
