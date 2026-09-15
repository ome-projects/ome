#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
baseline='{"requestUUID":"test-request","replacementUIDs":["leader","worker"],"samples":[{"sourceSafe":true,"leaderContainersReady":true,"workerContainersReady":false,"migrationCount":1,"requestUUID":"test-request","replacementUIDs":["leader","worker"],"migrationPhase":"SurgePending"},{"sourceSafe":true,"leaderContainersReady":true,"workerContainersReady":false,"migrationCount":1,"requestUUID":"test-request","replacementUIDs":["leader","worker"],"migrationPhase":"SurgePending"}],"recovery":{"oldPodUID":"old","newPodUID":"new","leadershipRecovered":true,"sameRequestObserved":true}}'
jq -e -f "${dir}/verify-partial-gang.jq" <<<"${baseline}" >/dev/null
for mutation in '.samples=[]' '.samples[0].sourceSafe=false' '.samples[0].workerContainersReady=true' '.samples[0].migrationCount=2' '.samples[0].replacementUIDs=["new","worker"]' '.recovery.newPodUID="old"' '.recovery.leadershipRecovered=false' '.samples[0].migrationPhase="Completed"'; do
  if jq "${mutation}" <<<"${baseline}" | jq -e -f "${dir}/verify-partial-gang.jq" >/dev/null; then
    echo "partial gang verifier accepted unsafe evidence: ${mutation}" >&2; exit 1
  fi
done
echo 'partial gang evidence tests passed'
