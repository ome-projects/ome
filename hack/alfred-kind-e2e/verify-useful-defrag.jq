include "useful-defrag-sample";
include "placement-pause-sample";
. as $e |
.scenario == "useful-defrag" and
all(.before.metadata.uid,.baseline.isvc.metadata.uid,.baseline.ir.metadata.uid,.source.metadata.uid,.replacement.metadata.uid,.routingService; pp_nonempty) and
.before.metadata.name == "beneficiary" and .before.metadata.namespace == "alfred-e2e-defrag" and
.before.metadata.deletionTimestamp == null and .before.spec.nodeName == null and
.before.spec.schedulerName == "alfred-default-scheduler" and
(.before.spec.containers|length == 1 and .[0].resources.requests["nvidia.com/gpu"] == "8") and
any(.before.status.conditions[]?; .type == "PodScheduled" and .status == "False" and
  .reason == "Unschedulable" and (.message|contains("Insufficient nvidia.com/gpu"))) and
.source.metadata.uid != .replacement.metadata.uid and
.source.spec.nodeName == "alfred-kwok-gpu-a" and .source.metadata.labels["ome.io/instance-index"] == "0" and
.replacement.spec.nodeName == "alfred-kwok-gpu-b" and .replacement.metadata.labels["ome.io/instance-index"] == "1" and
all(.source,.replacement; (.spec.containers|length == 1 and .[0].resources.requests["nvidia.com/gpu"] == "1")) and
(.baseline|df_identity($e) and df_safe($e.source;$e) and .beneficiary == $e.before and
  [.pods.items[].metadata.uid] == [$e.source.metadata.uid] and (df_entries($e)|length) == 0 and
  (.ir.status.migrations // []|length) == 0) and
(.held|df_held($e)) and
(.handoff|type == "array" and length > 0) and all(.handoff[]; df_handoff($e)) and
(.handoff[-1]|df_safe($e.replacement;$e)) and
(.request.uuid|type == "string" and test("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")) and
.request.payload.schemaVersion == "v1" and .request.payload.requested_by == "alfred" and
.request.payload.reason == "Fragmentation" and .request.payload.from_node == "alfred-kwok-gpu-a" and
.request.payload.instance == 0 and .request.payload.component == "engine" and
.migration.requestUUID == .request.uuid and .migration.phase == "Completed" and
.migration.trigger == "Manual" and .migration.fromNode == "alfred-kwok-gpu-a" and
.migration.sourceInstance == 0 and .migration.surgeInstance == 1 and (.migration.completedAt|pp_nonempty) and
(.completionBaseline|df_completed($e)) and
(.completed|type == "array" and length >= 3) and all(.completed[]; df_completed($e)) and
(.completionSamples|type == "array" and length >= 3) and
all(.completionSamples[]; df_completed($e) and (df_capacity(.nodes;.allPods)|map(.free)) == [0,0,0,0]) and
all(.completed[]; . as $selected|any($e.completionSamples[]; . == $selected)) and
([.completed[]|pp_cycle.timestamp|pp_time]|. == sort and length == (unique|length)) and
all(.completed[]; df_new_cycle($e;($e.completionBaseline|pp_cycle.timestamp))) and
.after == .completed[-1].beneficiary and
.nodesBefore == .baseline.nodes and .podsBefore == .baseline.allPods and
.nodesAfter == .completed[-1].nodes and .podsAfter == .completed[-1].allPods and
(.requestWatch|type == "array" and length >= 2) and
all(.requestWatch[]; .metadata.uid == $e.baseline.isvc.metadata.uid and
  .metadata.namespace == "alfred-e2e-defrag" and .metadata.name == "single") and
([.requestWatch[]|.metadata.annotations // {}|to_entries[]|select(.key|startswith("ome.io/migration-request-v1-"))]|unique|
  length == 1 and .[0].key == ("ome.io/migration-request-v1-"+$e.request.uuid) and (.[0].value|fromjson) == $e.request.payload) and
all(.nodesBefore,.nodesAfter;
  ([.items[].metadata.name]|sort) == ["alfred-kwok-gpu-a","alfred-kwok-gpu-b","alfred-kwok-gpu-c","alfred-kwok-gpu-d"] and
  all(.items[]; .metadata.deletionTimestamp == null and .spec.unschedulable != true and
    .status.allocatable["nvidia.com/gpu"] == "8" and
    any(.status.conditions[]?; .type == "Ready" and .status == "True"))) and
([.nodesBefore.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)) ==
  ([.nodesAfter.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)) and
(df_capacity(.nodesBefore;.podsBefore)) == (.capacityBefore|sort_by(.name)) and
(.capacityBefore|sort_by(.name)|map(.free)) == [7,1,0,0] and
(df_capacity(.nodesAfter;.podsAfter)|map(.free)) == [0,0,0,0] and
([.podsBefore.items[]|select(.metadata.labels["alfred-e2e/blocker"] == "true")]|length == 3) and
([.podsBefore.items[]|select(.metadata.labels["alfred-e2e/blocker"] == "true")|{uid:.metadata.uid,spec}]|sort_by(.uid)) ==
  ([.podsAfter.items[]|select(.metadata.labels["alfred-e2e/blocker"] == "true")|{uid:.metadata.uid,spec}]|sort_by(.uid))
