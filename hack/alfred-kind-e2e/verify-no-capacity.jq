def capacity_matches($uid; $scheduling):
  try (. as $p |
    .sourceUID == $uid and .requestID == $scheduling.requestID and
    .snapshotID == $scheduling.snapshotID and .replacementGPU == 1 and
    (.replacementPriority | type == "number" and floor == .) and
    (.eligibleNodes | type == "array" and length == 4 and (unique | length) == 4) and
    (.eligibleNodes | index($p.sourceNode)) != null and
    (.destinations | length) == 3 and
    ([.destinations[].node] | sort) == ((.eligibleNodes - [.sourceNode]) | sort) and
    ([.destinations[].blockerUID] | unique | length) == 3 and
    all(.destinations[];
      (.blockerUID | type == "string" and length > 0) and
      (.allocatableGPU | type == "number" and floor == . and . > 0) and
      (.blockerGPU | type == "number" and floor == .) and
      (.blockerPriority | type == "number" and floor == .) and
      .blockerPriority >= $p.replacementPriority and .blockerGPU >= .allocatableGPU and
      .unallocatedGPU == (.allocatableGPU - .blockerGPU) and .unallocatedGPU <= 0)) catch false;

.requestCount == 0 and .migrations == [] and .watchComplete == true and
(.sourceUID | type == "string" and length > 0) and
(.samples | length >= 2) and
((.samples[-1].timestamp | fromdateiso8601) -
 (.samples[0].timestamp | fromdateiso8601) >= 15) and
(.sourceUID as $uid | all(.samples[];
  .sourceReady == true and .sourceServing == true and .sourceRouted == true and
  .podUIDs == [$uid] and .drained == false and
  .maintenanceRequested == true and .occupantsPresent == true and
  .requestCount == 0 and .migrations == [] and
  .scheduling.provenance == "deployed-worker" and .scheduling.validated == true and
  (.scheduling.requestID | type == "string" and length > 0) and
  (.scheduling.snapshotID | type == "string" and length > 0) and
  .scheduling.placements == [] and
  ((.scheduling.status == "Unsupported" and .scheduling.reason == "Unsupported") or
   (.scheduling.status == "Infeasible" and .scheduling.reason == "NoFeasiblePlacement")) and
  (.scheduling as $scheduling | .capacityProof | capacity_matches($uid; $scheduling))))
