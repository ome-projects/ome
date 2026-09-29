#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
baseline="$(jq -n -f "${dir}/useful-defrag-fixture.jq")"
jq -e -L "${dir}" -f "${dir}/verify-useful-defrag.jq" <<<"${baseline}" >/dev/null
for mutation in \
  'del(.request.uuid,.migration.requestUUID)' \
  '.after.metadata.deletionTimestamp="2026-09-29T04:00:00Z"' \
  '.handoff=[{sourceSafe:false,replacementSafe:true}]' \
  '.after.metadata.uid="new-pod"' '.after.spec.containers[0].resources.requests["nvidia.com/gpu"]="1"' \
  '.before.spec.nodeName="already-fit"' '.before.status.conditions[0].reason="Unknown"' \
  '.capacityBefore[0].free=8' '.podsBefore.items[2].spec.containers[0].resources.requests["nvidia.com/gpu"]="6"' \
  '.request.payload.reason="NodeMaintenance"' '.migration.phase="Failed"' '.after.spec.nodeName="other-node"' \
  '.held.beneficiary.spec.nodeName="alfred-kwok-gpu-a"' '.held.pods.items[0].metadata.uid="not-source"' \
  '.held.pods.items[1].status.conditions[0].status="True"' '.held.endpoints.items[0].endpoints[0].targetRef.uid="wrong"' \
  '.handoff=[]' '.handoff[0].pods.items[0].metadata.deletionTimestamp="2026-09-29T04:00:00Z"' \
  '.handoff[1].endpoints.items[0].endpoints[0].conditions.terminating=true' \
  '.completed=[]' '.completed=.completed[0:2]' '.completed[1]=.completed[0]' \
  '.completed[0].recommendations=.completionBaseline.recommendations' \
  '.completed[0].isvc.metadata.uid="other-owner"' '.completed[0].ir.metadata.ownerReferences[0].uid="other-owner"' \
  '.completed[0].pods.items[0].metadata.ownerReferences[0].uid="other-replica"' \
  '(.completionBaseline,.completed[],.completionSamples[],.handoff[1]) |= (.allPods.items[1].metadata.uid="other-beneficiary") | .podsAfter=.completed[-1].allPods' \
  '.held.allPods.items[-1].metadata.deletionTimestamp="2026-09-29T04:00:00Z"' \
  '.held.allPods.items[-1].status.phase="Succeeded"' \
  '.completionSamples=[]' '.completionSamples[0].beneficiary.metadata.uid="different"' \
  '.completed[0].allPods.items[-1].metadata.uid="recreated-blocker"' \
  '.completed[0].dispatch.data["state.json"]|=(fromjson|.entries[0].phase="acknowledged"|tojson)' \
  '.completed[0].dispatch.data["state.json"]|=(fromjson|.entries+=[.entries[0]]|tojson)' \
  '.requestWatch=[]' '.requestWatch[0].metadata.uid="other-owner"' \
  '.requestWatch[1].metadata.annotations["ome.io/migration-request-v1-other"]="{}"' \
  '.podsAfter.items[2].metadata.uid="recreated-blocker"'; do
  if jq "${mutation}" <<<"${baseline}" | jq -e -L "${dir}" -f "${dir}/verify-useful-defrag.jq" >/dev/null; then
    echo "useful-defrag verifier accepted invalid evidence: ${mutation}" >&2; exit 1
  fi
done
echo 'useful-defrag evidence tests passed'
