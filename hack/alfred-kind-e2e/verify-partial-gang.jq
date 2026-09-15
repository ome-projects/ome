. as $e |
(.requestUUID | type == "string" and length > 0) and
(.replacementUIDs | length) == 2 and
(.replacementUIDs | unique | length) == 2 and
all(.replacementUIDs[]; type == "string" and length > 0) and
(.samples | length) >= 2 and
all(.samples[];
  .sourceSafe == true and .leaderContainersReady == true and
  .workerContainersReady == false and .migrationCount == 1 and
  .requestUUID == $e.requestUUID and
  (.replacementUIDs | sort) == ($e.replacementUIDs | sort) and
  .migrationPhase == "SurgePending") and
.recovery.oldPodUID != .recovery.newPodUID and
(.recovery.oldPodUID | type == "string" and length > 0) and
(.recovery.newPodUID | type == "string" and length > 0) and
.recovery.leadershipRecovered == true and
.recovery.sameRequestObserved == true
