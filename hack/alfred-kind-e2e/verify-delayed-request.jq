# Public API evidence for the intentionally delayed mailbox boundary only.
# Final placement/handoff is verified separately by the scenario integration.
def nonempty: type == "string" and length > 0;
def instant: sub("\\.[0-9]+Z$";"Z") | fromdateiso8601;
def condition($type; $status): any(.status.conditions[]?; .type == $type and .status == $status);
def test_node:
  .metadata.name | IN("alfred-kwok-gpu-a","alfred-kwok-gpu-b","alfred-kwok-gpu-c","alfred-kwok-gpu-d");
def ready_target:
  test_node and .metadata.labels["alfred-e2e/virtual"] == "true" and
  condition("Ready";"True") and (.spec.unschedulable // false) == false and
  (.status.allocatable["nvidia.com/gpu"] | tonumber) == 8;
def gpu:
  ([.spec.containers[]?.resources.requests["nvidia.com/gpu"] // "0" | tonumber] | add // 0) +
  (.spec.overhead["nvidia.com/gpu"] // "0" | tonumber);
def live: .metadata.deletionTimestamp == null and (.status.phase | IN("Succeeded","Failed") | not);
def manager_paused:
  .managerPause as $m |
  ($m.originalArgs | type == "array") and
  $m.pausedArgs == ([$m.originalArgs[] | select(startswith("--enable-inferencereplica-controller=") | not)] +
    ["--enable-inferencereplica-controller=false"]) and
  ($m.pods.items | length) == 1 and
  ($m.oldPodUIDs | length) > 0 and
  all($m.pods.items[]; .metadata.deletionTimestamp == null and
    condition("Ready";"True") and
    (.metadata.uid as $uid | $m.oldPodUIDs | index($uid) == null) and
    any(.spec.containers[]; .name == "manager" and .args == $m.pausedArgs)) and
  ($m.lease.spec.holderIdentity | nonempty) and
  $m.lease.spec.holderIdentity == $m.renewedLease.spec.holderIdentity and
  ($m.lease.spec.renewTime | nonempty) and ($m.renewedLease.spec.renewTime | nonempty) and
  $m.lease.spec.renewTime != $m.renewedLease.spec.renewTime and
  any($m.pods.items[]; .metadata.name as $name | $m.lease.spec.holderIdentity | startswith($name + "_")) and
  $m.webhookReady == true;
def actual_unconsumed_request:
  .request as $r | .pausedISVC as $owner | .pausedIR as $ir |
  ($r.uuid | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")) and
  $r.annotationKey == ("ome.io/migration-request-v1-" + $r.uuid) and
  $r.payload.schemaVersion == "v1" and $r.payload.component == "engine" and
  $r.payload.instance >= 0 and $r.payload.from_node == .sourceNode and
  $r.payload.requested_by == "alfred" and $r.payload.reason == "NodeMaintenance" and
  ($r.payload.requested_at | nonempty) and
  ([$owner.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] == [$r.annotationKey]) and
  ($owner.metadata.annotations[$r.annotationKey] | fromjson) == $r.payload and
  ($owner.metadata.uid | nonempty) and ($ir.metadata.uid | nonempty) and
  any($ir.metadata.ownerReferences[]?; .kind == "InferenceService" and .controller == true and .uid == $owner.metadata.uid) and
  (($ir.status.migrations // []) | length) == 0 and
  .mutationIR.metadata.uid == $ir.metadata.uid and ((.mutationIR.status.migrations // []) | length) == 0 and
  .journal.uuid == $r.uuid and .journal.workloadUID == $owner.metadata.uid and .journal.irUID == $ir.metadata.uid and
  .journal.phase == "submitted" and .journal.acknowledgedAt == null and (.journal.payload | fromjson) == $r.payload and
  .elapsedSinceRequestSeconds >= 0 and .elapsedSinceRequestSeconds < 180 and
  (if has("resumedElapsedSeconds") then .resumedElapsedSeconds >= 0 and .resumedElapsedSeconds < 180 else true end);
def targets_proven:
  . as $e | .request.payload.hint_target_nodes as $hints |
  ($hints | length) == 2 and ($hints | unique | length) == 2 and
  ($hints | index($e.sourceNode)) == null and ($hints | index($e.fallback)) == null and
  .fallback != .sourceNode and .fallbackBefore.metadata.name == .fallback and
  .fallbackAfter.metadata.name == .fallback and
  .fallbackBefore.metadata.uid == .fallbackAfter.metadata.uid and
  .fallbackBefore.spec.unschedulable == true and
  .fallbackBefore.metadata.labels["nvidia.com/gpu.product"] == .fallbackAfter.metadata.labels["nvidia.com/gpu.product"] and
  ([.targets.nodes[].metadata.name] | sort) == ($hints | sort) and
  all(.targets.nodes[]; ready_target) and
  all(.targets.nodes[]; . as $target |
    any($e.originalNodes.items[]; .metadata.name == $target.metadata.name and
      .metadata.uid == $target.metadata.uid and .metadata.labels == $target.metadata.labels and
      .spec == $target.spec and .status.allocatable == $target.status.allocatable)) and
  all(.targets.pods.items[]; .spec.resources.requests["nvidia.com/gpu"] == null and
    all(.spec.initContainers[]?; (.resources.requests["nvidia.com/gpu"] // "0" | tonumber) == 0)) and
  (if .scenario == "hint-exhaustion-single" then
    (.fallbackAfter | ready_target) and
    (.targets.blockerUIDs | length) == 2 and (.targets.blockerUIDs | unique | length) == 2 and
    all(.targets.nodes[]; .metadata.name as $name |
      ([$e.targets.pods.items[] | select(.spec.nodeName == $name and live) | gpu] | add // 0) == 8 and
      any($e.targets.pods.items[]; .spec.nodeName == $name and live and gpu == 8 and
        (.metadata.uid as $uid | $e.targets.blockerUIDs | index($uid) != null) and
        .metadata.labels["alfred-e2e.ome.io/delayed-request"] == $e.runID and
        .spec.schedulerName == "alfred-default-scheduler" and condition("PodScheduled";"True")))
  elif .scenario == "target-health-race-single" then
    .fallbackAfter.spec.unschedulable == true and
    all(.targets.nodes[]; . as $target |
      .metadata.annotations["alfred-e2e.ome.io/gpu-health"] == "unhealthy" and
      any($e.originalNodes.items[]; .metadata.name == $target.metadata.name and
        all(.status.conditions[]? | select(.type == "GpuUnhealthy"); .status != "True") and
        . as $original |
        any($target.status.conditions[]; .type == "GpuUnhealthy" and .status == "True" and
          .reason == "AlfredE2EGpuUnhealthy" and
          (.lastTransitionTime | instant) >= ($e.request.payload.requested_at | instant) and
          .lastTransitionTime as $transition |
          all($original.status.conditions[]? | select(.type == "GpuUnhealthy"); .lastTransitionTime != $transition))))
  else false end);

try (manager_paused and actual_unconsumed_request and targets_proven and
  (.sourcePod.metadata.uid == .sourceUID and .sourcePod.spec.nodeName == .sourceNode and
    .sourcePod.metadata.deletionTimestamp == null and (.sourcePod | condition("Ready";"True")) and
    (.sourcePod | condition("ome.io/serving";"True")))) catch false
