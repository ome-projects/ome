#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
valid="$(jq -L "${dir}" -f "${dir}/testdata/evidence-placement-valid.jq" "${dir}/testdata/evidence-valid.json")"
verify() { jq -e -L "${dir}" -f "${dir}/verify-placement-pause.jq" >/dev/null; }
verify <<<"${valid}" || { echo 'valid pause fixture rejected' >&2; exit 1; }
# Go's nil slice is serialized as null on cycles with no recommendations.
jq '.placement.completed[].recommendations.data["last-cycle.json"] |= (fromjson | .recommendations=null | tojson)' \
  <<<"${valid}" | verify
# A held pod can lack OME's serving condition until its containers become ready.
jq '.placement.allocated.pods.items[1].status.conditions |= map(select(.type != "ome.io/serving")) |
    .placement.repaused.pods.items[1].status.conditions |= map(select(.type != "ome.io/serving"))' \
  <<<"${valid}" | verify
jq '.scenario="maintenance-single"' <<<"${valid}" | jq -e -f "${dir}/verify-evidence.jq" >/dev/null
for mutation in \
  '.scenario="maintenance-single"' \
  'del(.placement)' \
  '.placement.irUID="old-ir"' \
  '.placement.baseline.spec.placementExecution.pauseSurge=false' \
  '.placement.baseline.spec.placementReplicaLimit=0' \
  '.placement.paused=[]' \
  '.placement.paused |= .[0:2]' \
  '.placement.paused[1]=.placement.paused[0]' \
  '.placement.completed[2]=.placement.completed[0]' \
  '.placement.paused |= reverse' \
  '.placement.completed[0].recommendations.data["last-cycle.json"] |= (fromjson | .timestamp="stale" | tojson)' \
  '.placement.completed[0].recommendations.data["last-cycle.json"] |= (fromjson | del(.recommendations) | tojson)' \
  '.placement.completed[0].recommendations.data["last-cycle.json"] |= (fromjson | .recommendations={} | tojson)' \
  '.placement.paused[0].dispatch.data["state.json"]="unreadable"' \
  'del(.placement.paused[0].isvc.metadata.labels["ome.io/placement-origin"])' \
  '.placement.paused[0].isvc.metadata.annotations["ome.io/placement-origin-uid"]="other-source"' \
  '.placement.paused[0].ir.spec.placementExecution.sourceUID="other-source"' \
  '.placement.paused[0].ir.spec.placementExecution.revision=0' \
  '.placement.paused[0].ir.status.placementObservedGeneration=0' \
  '.placement.paused[0].ir.metadata.ownerReferences[0].uid="old-isvc"' \
  '.placement.paused[0].recommendations.data["last-cycle.json"] |= (fromjson | .recommendations=[] | tojson)' \
  '.placement.paused[0].recommendations.data["last-cycle.json"] |= (fromjson | .recommendations[0].advisoryReason="Other" | tojson)' \
  '.placement.paused[0].recommendations.data["last-cycle.json"] |= (fromjson | .recommendations[0].outcome="admitted" | tojson)' \
  '.placement.paused[0].recommendations.data["node.alfred-gpu-a"] |= (fromjson | .maintenanceDrainedAt="early" | tojson)' \
  '.placement.paused[0].requests=[{key:"ome.io/migration-request-v1-unexpected",value:"{}"}]' \
  '.placement.paused[0].isvc.metadata.annotations["ome.io/migration-request-v1-unexpected"]="{}"' \
  '.placement.paused[0].dispatch.data["state.json"] |= (fromjson | .entries=[{workloadUID:"isvc-new",irUID:"different-ir"}] | tojson)' \
  '.placement.paused[0].dispatch.data["state.json"] |= (fromjson | .entries=[{workloadUID:"different-isvc",irUID:"ir-new"}] | tojson)' \
  '.placement.paused[0].ir.status.migrations=[{}]' \
  '.placement.paused[0].pods.items += [.placement.allocated.pods.items[1]]' \
  '.placement.paused[0].pods.items[0].metadata.deletionTimestamp="early"' \
  '.placement.paused[0].pods.items[0].status.conditions[0].status="False"' \
  '.placement.paused[0].endpoints.items[0].endpoints[0].conditions.ready=false' \
  '.placement.released.ir.spec.placementExecution.revision=1' \
  '.placement.released.isvc.metadata.annotations["ome.io/placement-execution"] |= (fromjson | .revision=1 | tojson)' \
  '.placement.released.ir.spec.placementExecution.pauseSurge=true' \
  '.placement.allocated.ir.status.migrations[0].surgeInstance=null' \
  '.placement.allocated.ir.status.migrations[0].surgeInstance=-1' \
  '.placement.allocated.ir.status.migrations[0].requestUUID="different-request"' \
  '.placement.allocated.ir.status.migrations[0].trigger="Unhealthy"' \
  '.placement.repaused.ir.status.migrations[0].fromNode="other-node"' \
  '.placement.allocated.isvc.metadata.annotations[.request.annotationKey]="{}"' \
  '.placement.allocated.pods.items[1].status.conditions[0].status="True"' \
  '.placement.allocated.pods.items[1].status.conditions[1].status="True"' \
  '.placement.repaused.ir.spec.placementExecution.revision=2' \
  '.placement.repaused.ir.spec.placementExecution.pauseSurge=false' \
  '.placement.repaused.ir.status.placementObservedGeneration=2' \
  '.placement.completed[0].ir.status.migrations[0].phase="SurgePending"' \
  '.placement.completed[0].ir.status.migrations[0].trigger="Unhealthy"' \
  '.placement.completed[0].ir.status.migrations[0].fromNode="other-node"' \
  '.placement.completed[0].ir.spec.placementExecution.pauseSurge=false' \
  '.placement.completed[0].ir.status.migrations += [.placement.completed[0].ir.status.migrations[0]]' \
  '.placement.completed[0].requests += [{key:"ome.io/migration-request-v1-other",value:"{}"}]' \
  '.placement.completed[0].dispatch.data["state.json"] |= (fromjson | .entries += [.entries[0]] | tojson)' \
  '.placement.completed[0].dispatch.data["state.json"] |= (fromjson | .entries[0].phase="acknowledged" | tojson)' \
  '.placement.completed[0].dispatch.data["state.json"] |= (fromjson | .entries[0].payload="{}" | tojson)' \
  '.placement.completed[0].pods.items[0].metadata.uid="different-replacement"' \
  '.placement.completed[0].endpoints.items[0].endpoints[0].conditions.terminating=true' \
  '.placement.completed[0].ir.status.availableReplicas=0'; do
  if jq "${mutation}" <<<"${valid}" | verify 2>/dev/null; then
    echo "pause verifier accepted mutation: ${mutation}" >&2; exit 1
  fi
done
# Prior fixtures' durable entries are legitimate and must not contaminate a
# fresh UID-scoped zero-dispatch proof.
jq '.placement.paused[].dispatch.data["state.json"] |= (fromjson | .entries += [{workloadUID:"old-isvc",irUID:"old-ir"}] | tojson)' \
  <<<"${valid}" | verify
# Command substitutions do not inherit errexit on the supported Bash 3.2.
# Even syntactically valid output from a failed read must stop the snapshot.
source "${dir}/placement-pause.sh"
namespace=alfred-e2e
alfred_namespace=ome
isvc_name=single
ir_name=single-engine
request_watch_pid=mock
request_watch_stderr=mock
request_watch_file="${dir}/testdata/evidence-valid.json"
kube=(mock_placement_kube)
assert_annotation_watch_alive() { [[ "${failed_read}" != watch ]]; }
mock_placement_kube() {
  printf '{}\n'
  [[ "$*" != *"${failed_read}"* ]]
}
pods_json() { mock_placement_kube pods; }
routing_endpoints_json() { mock_placement_kube endpointslices; }
for failed_read in watch inferenceservice inferencereplica pods endpointslices alfred-recommendations alfred-dispatch-state; do
  if captured="$(placement_snapshot)"; then
    echo "snapshot swallowed failed read: ${failed_read}" >&2; exit 1
  fi
done
echo 'placement pause verifier tests passed'
