#!/usr/bin/env bash
# Catch false acceptance of unsupported, absent, stale or side-effecting runs.
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
baseline="$(jq -n -f "${dir}/useful-defrag-fixture.jq" | jq -f "${dir}/no-benefit-defrag-fixture.jq")"
verify() { jq -e -L "${dir}" -f "${dir}/verify-no-benefit-defrag.jq" >/dev/null; }
verify <<<"${baseline}"
for mutation in \
  '.attempts=[]' '.attempts=.attempts[0:2]' '.attempts[1]=.attempts[0]' \
  '.attempts[0].result.decision="Unsupported"' '.attempts[0].result.placements[0].nodeName="alfred-kwok-gpu-b"' \
  '.attempts[0].result.placements[0].pod.uid="other"' '.attempts[0].result.snapshotID="other"' \
  '.attempts[0].request.sourcePods[0].metadata.uid="other"' '.attempts[0].request.replacementPods[0].spec.containers[0].resources.requests["nvidia.com/gpu"]="0"' \
  '.attempts[0].request.clusterObjects|=map(if .kind=="Pod" and .metadata.uid=="block-b" then .spec.containers[0].resources.requests["nvidia.com/gpu"]="6" else . end)' \
  '.attempts[0].request.clusterObjects|=map(if .kind=="Node" and .metadata.name=="alfred-kwok-gpu-b" then .spec.unschedulable=true else . end)' \
  '.attempts[0].request.clusterObjects|=map(if .kind=="Node" then .metadata.uid="recreated" else . end)' \
  '.config.policies.defragmentation.scoring.demandBlendLambda=0' '.config.policies.defragmentation.enabled=false' \
  '.attempts[0].receipt.requestSHA256="other"' '.attempts[0].receipt.releasedAt=.attempts[0].held.deadline' \
  '.attempts[0].observed=.attempts[0].beforeRelease' '.attempts[0].samples=[]' \
  '.attempts[0].observed.recommendations.data["last-cycle.json"]|=(fromjson|.recommendations=[]|tojson)' \
  '.attempts[0].observed.recommendations.data["last-cycle.json"]|=(fromjson|.recommendations[0].dispatchReason="SchedulingUnsupported"|tojson)' \
  '.attempts[0].observed.recommendations.data["last-cycle.json"]|=(fromjson|.recommendations[0].score=0|tojson)' \
  '.attempts[0].samples[0].isvc.metadata.annotations["ome.io/migration-request-v1-test"]="{}"' \
  '.attempts[0].samples[0].ir.status.migrations=[{phase:"Completed"}]' \
  '.attempts[0].samples[0].dispatch.data["state.json"]|=(fromjson|.entries=[{workloadUID:"other",irUID:"replica"}]|tojson)' \
  '.attempts[0].samples[0].pods.items[0].spec.nodeName="alfred-kwok-gpu-c"' \
  '.attempts[0].samples[0].allPods.items[-1].metadata.uid="recreated"' \
  '.attempts[0].samples[0].endpoints.items[0].endpoints[0].conditions.terminating=true' \
  '.requestWatch=[]' '.requestWatch[0].metadata.uid="other"' \
  '.requestWatch[0].metadata.annotations["ome.io/migration-request-v1-test"]="{}"' \
  '.podWatch=[]' '.podWatch[0].type="DELETED"' '.podWatch[0].object.metadata.uid="replacement"' \
  '.baseline.allPods.items[0].spec.affinity={}' '.attempts[0].samples[0].ir.spec.component="decoder"'; do
  mutated="$(jq "${mutation}" <<<"${baseline}")"
  if verify <<<"${mutated}"; then
    echo "no-benefit verifier accepted invalid evidence: ${mutation}" >&2; exit 1
  fi
done
echo 'no-benefit defrag evidence tests passed'
