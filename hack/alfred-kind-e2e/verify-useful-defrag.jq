. as $e |
.scenario == "useful-defrag" and
(.before.metadata.uid | type == "string" and length > 0) and
.before.metadata.uid == .after.metadata.uid and
.before.spec.nodeName == null and .before.spec.schedulerName == "alfred-default-scheduler" and
any(.before.status.conditions[]?; .type == "PodScheduled" and .status == "False" and
  .reason == "Unschedulable" and (.message | contains("Insufficient nvidia.com/gpu"))) and
.after.spec.nodeName == "alfred-kwok-gpu-a" and
any(.after.status.conditions[]?; .type == "Ready" and .status == "True") and
(.capacityBefore | map(.free) | sort) == [0,0,1,7] and
.request.payload.requested_by == "alfred" and .request.payload.reason == "Fragmentation" and
.request.payload.from_node == "alfred-kwok-gpu-a" and
.request.payload.instance == 0 and .request.payload.component == "engine" and
.migration.requestUUID == .request.uuid and .migration.phase == "Completed" and
.migration.sourceInstance == 0 and .migration.surgeInstance == 1 and
(.migration.completedAt | type == "string" and length > 0) and
(.handoff | length) > 0 and all(.handoff[]; .sourceSafe == true or .replacementSafe == true) and
.handoff[-1].replacementSafe == true
