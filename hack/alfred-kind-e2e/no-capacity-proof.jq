# Derive capacity from the captured objects, independently of the worker reason.
# This deliberately recognizes only this harness's simple one-GPU fixture.
def require($ok; $message): if $ok then . else error($message) end;
def gpu:
  if type == "string" and test("^[0-9]+$") then tonumber
  elif type == "number" and . >= 0 and floor == . then .
  else error("expected an integer GPU quantity") end;

require(.validated == true and .provenance == "deployed-worker" and
  (.result.placements // []) == [] and
  ((.result.decision == "Unsupported" and .result.reason == "Unsupported") or
   (.result.decision == "Infeasible" and .result.reason == "NoFeasiblePlacement"));
  "worker did not return a validated fail-closed result") |
.request as $r |
require(($r.requestID | type == "string" and length > 0) and
  ($r.snapshotID | type == "string" and length > 0) and
  .result.requestID == $r.requestID and .result.snapshotID == $r.snapshotID;
  "worker result does not identify the captured request") |
require(($r.sourcePods | type == "array" and length == 1) and
  ($r.replacementPods | type == "array" and length == 1) and
  ($r.clusterObjects | type == "array"); "expected a single-Pod snapshot") |
$r.sourcePods[0] as $sourcePod | $r.replacementPods[0] as $replacement |
require($sourcePod.metadata.uid == $uid and $sourcePod.spec.nodeName == $source and
  $r.migrationFromNode == $source and $r.excludedNodes == [$source] and
  $replacement.spec.nodeSelector == {"alfred-e2e/virtual":"true"} and
  ($replacement.spec.nodeName // "") == "" and
  $replacement.spec.schedulerName == "alfred-default-scheduler" and
  ($replacement.spec.containers | length) == 1 and
  ($replacement.spec.initContainers // []) == [] and
  ($replacement.spec.containers[0].resources.requests["nvidia.com/gpu"] | gpu) == 1;
  "source identity, exclusion or one-GPU replacement does not match the fixture") |
($replacement.spec.priority // 0) as $priority |
require(($priority | type == "number" and floor == .);
  "replacement priority is not an integer") |
[$r.clusterObjects[] | select(.kind == "Pod" and .metadata.uid == $uid)] as $sourceCopies |
require(($sourceCopies | length) == 1 and
  $sourceCopies[0].metadata.name == $sourcePod.metadata.name and
  $sourceCopies[0].metadata.namespace == $sourcePod.metadata.namespace and
  $sourceCopies[0].spec.nodeName == $source;
  "source Pod UID is absent or mismatched in the captured objects") |
[$r.clusterObjects[] | select(.kind == "Node" and
  .metadata.labels["alfred-e2e/virtual"] == "true")] as $nodes |
require(($nodes | length) == 4 and
  ([$nodes[].metadata.name] | unique | length) == 4 and
  any($nodes[]; .metadata.name == $source);
  "expected exactly four distinct virtual nodes including the source") |
[$nodes[] | select(.metadata.name != $source) | . as $node |
  [$r.clusterObjects[] | select(.kind == "Pod" and
    .metadata.namespace == $sourcePod.metadata.namespace and
    .metadata.name == ("block-" + $node.metadata.name) and
    .metadata.labels["alfred-e2e/capacity-blocker"] == "true")] as $blockers |
  require(($blockers | length) == 1; "destination does not have one capacity blocker") |
  $blockers[0] as $blocker |
  require(($blocker.metadata.uid | type == "string" and length > 0) and
    $blocker.metadata.deletionTimestamp == null and $blocker.status.phase == "Running" and
    $blocker.spec.nodeName == $node.metadata.name and
    $blocker.spec.nodeSelector["kubernetes.io/hostname"] == $node.metadata.name and
    $blocker.spec.schedulerName == "alfred-default-scheduler" and
    ($blocker.spec.containers | length) == 1 and
    ($blocker.spec.initContainers // []) == [] and
    (($blocker.spec.priority // 0) | type == "number" and floor == .) and
    ($blocker.spec.priority // 0) >= $priority;
    "capacity blocker is unbound, terminating, malformed or preemptible") |
  ($node.status.allocatable["nvidia.com/gpu"] | gpu) as $allocatable |
  ($blocker.spec.containers[0].resources.requests["nvidia.com/gpu"] | gpu) as $occupied |
  require($allocatable > 0 and $occupied >= $allocatable;
    "destination still has unallocated GPU capacity") |
  {node:$node.metadata.name, allocatableGPU:$allocatable, blockerGPU:$occupied,
   unallocatedGPU:($allocatable-$occupied), blockerUID:$blocker.metadata.uid,
   blockerPriority:($blocker.spec.priority // 0)}] as $destinations |
require(([$destinations[].blockerUID] | unique | length) == 3;
  "capacity blockers do not have distinct identities") |
{sourceUID:$uid, sourceNode:$source, requestID:$r.requestID, snapshotID:$r.snapshotID,
 replacementGPU:1, replacementPriority:$priority,
 eligibleNodes:([$nodes[].metadata.name] | sort), destinations:$destinations}
