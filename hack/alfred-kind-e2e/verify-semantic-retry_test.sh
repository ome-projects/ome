#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "${dir}/verify-semantic-retry.jq" ]] || { echo 'semantic retry verifier is not implemented' >&2; exit 1; }
valid="$(jq -L "${dir}" -f "${dir}/testdata/evidence-semantic-retry.jq" "${dir}/testdata/evidence-valid.json")"
verify() { jq -e -L "${dir}" -f "${dir}/verify-semantic-retry.jq" >/dev/null; }
verify <<<"${valid}" || { echo 'valid semantic retry witness rejected' >&2; exit 1; }
for mutation in \
  '.semanticRetry.admission.metricsRejected.denyCount=1' \
  '.semanticRetry.admission.metricsRejected.processStart=9999' \
  '.semanticRetry.admission.metricsRejected.apiServerUID="new-apiserver"' \
  '.semanticRetry.admission.metricsRejected.capturedAt="2026-09-14T11:59:50Z"' \
  '.semanticRetry.admission.metricsBefore.capturedAt="2026-09-14T12:00:20Z"' \
  '.semanticRetry.admission.policy.spec.matchConditions=[]' \
  '.semanticRetry.admission.policy.status.typeChecking.expressionWarnings=[{warning:"bad CEL"}]' \
  '.semanticRetry.admission.binding.spec.policyName="ome-alfred-migration-writes"' \
  '.semanticRetry.admission.allowedProbe.output="{}"' \
  '.semanticRetry.admission.deniedProbe.output="Forbidden by RBAC"' \
  '.semanticRetry.admission.allowedProbe.exitCode=1' \
  '.semanticRetry.admission.bindingRemoved=false' \
  '.semanticRetry.admission.guardAfter.policy.spec={changed:true}' \
  '.semanticRetry.prepared.requests=[{key:"unexpected",value:"{}"}]' \
  '.semanticRetry.prepared.ir.status.migrations=[{}]' \
  '.semanticRetry.prepared.dispatch.data["state.json"] |= (fromjson | .entries[0].phase="submitted" | tojson)' \
  '.semanticRetry.prepared.dispatch.data["state.json"] |= (fromjson | .entries[0].lastAttempt=null | tojson)' \
  '.semanticRetry.prepared.dispatch.data["state.json"] |= (fromjson | .entries[0].reason="OwnerChanged" | tojson)' \
  '.semanticRetry.paused |= .[0:2]' \
  '.semanticRetry.paused[1]=.semanticRetry.paused[0]' \
  '.semanticRetry.paused[0].ir.spec.placementExecution.pauseSurge=false' \
  '.semanticRetry.paused[0].ir.status.placementObservedGeneration=1' \
  '.semanticRetry.paused[0].pods.items[0].metadata.uid="replacement-source"' \
  '.semanticRetry.paused[0].pods.items[0].metadata.labels["ome.io/instance-incarnation"]="2"' \
  '.semanticRetry.paused[0].pods.items[0].spec.containers[0].image="different"' \
  '.semanticRetry.paused[0].pods.items[0].metadata.labels["fixture.example/change"]="different"' \
  '.semanticRetry.paused[0].pods.items[0].metadata.annotations["fixture.example/change"]="different"' \
  '.semanticRetry.paused[0].ir.spec.runners[0].template.metadata.annotations["fixture.example/stable"]="different"' \
  '.semanticRetry.paused[0].ir.spec.runners[0].template.metadata.annotations["ome.io/placement-execution"]="{}"' \
  '.semanticRetry.paused[0].ir.spec.runners[0].template.spec.containers[0].image="different"' \
  '.semanticRetry.completed[0].ir.spec.runners[0].template.metadata.annotations[.request.annotationKey]="{}"' \
  '.semanticRetry.completed[0].ir.spec.runners[0].template.metadata.annotations["ome.io/migration-request-v1-unexpected"]="{}"' \
  '.semanticRetry.paused[0].endpoints.items[0].endpoints[0].conditions.ready=false' \
  '.semanticRetry.paused[0].dispatch.data["state.json"] |= (fromjson | .entries += [.entries[0]] | tojson)' \
  '.semanticRetry.paused[1].dispatch.data["state.json"] |= (fromjson | .entries[0].lastAttempt="2026-09-14T12:00:07Z" | tojson)' \
  '.semanticRetry.released.ir.metadata.generation=1' \
  '.semanticRetry.released.isvc.metadata.generation=2' \
  '.semanticRetry.released.ir.spec.placementExecution.revision=2' \
  '.semanticRetry.completed[0].dispatch.data["state.json"] |= (fromjson | .entries[0].payload="{}" | tojson)' \
  '.semanticRetry.completed[0].dispatch.data["state.json"] |= (fromjson | .entries[0].uuid="new-uuid" | tojson)' \
  '.semanticRetry.completed[0].dispatch.data["state.json"] |= (fromjson | .entries[0].sourceFingerprint="new-fingerprint" | tojson)' \
  '.semanticRetry.completed[0].requests[0].value += " "' \
  '.semanticRetry.completed[0].requests += [{key:"another",value:"{}"}]' \
  '.semanticRetry.completed[0].ir.status.migrations += [.semanticRetry.completed[0].ir.status.migrations[0]]' \
  '.semanticRetry.completed[1]=.semanticRetry.completed[0]' \
  '.semanticRetry.completed[0].recommendations.data["last-cycle.json"] |= (fromjson | .timestamp="2026-09-14T12:00:10Z" | tojson)' \
  '.semanticRetry.completed[0].recommendations.data["last-cycle.json"] |= (fromjson | .timestamp="2026-09-14T12:00:16Z" | tojson)' \
  '.semanticRetry.completionBaseline=null' \
  '.semanticRetry.completed[0].endpoints.items[0].endpoints[0].conditions.ready=false' \
  '.semanticRetry.completed[0].dispatch.data["state.json"] |= (fromjson | .entries[0].phase="prepared" | tojson)'; do
  if jq "${mutation}" <<<"${valid}" | verify 2>/dev/null; then
    echo "semantic retry verifier accepted: ${mutation}" >&2; exit 1
  fi
done
# Exact symptom before the runtime fix: released authority, unchanged prepared
# intent, SourceChanged on subsequent cycles, and no migration publication.
old="$(jq '.semanticRetry.completed = [.semanticRetry.released] |
  .semanticRetry.completed[0].recommendations.data["last-cycle.json"] |=
    (fromjson | .recommendations[0].dispatchReason="SourceChanged" | tojson)' <<<"${valid}")"
if verify <<<"${old}"; then echo 'old-runtime failure witness accepted as success' >&2; exit 1; fi
echo 'semantic retry verifier tests passed; pre-fix witness rejected'
