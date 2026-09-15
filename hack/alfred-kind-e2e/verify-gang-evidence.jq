def uuid:
  type == "string" and
  test("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
def nonempty: type == "string" and length > 0;
# Preserve nanosecond ordering without adding fractions to epoch floats.
def instant:
  capture("^(?<whole>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\\.(?<fraction>[0-9]{1,9}))?(?<zone>Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])$") as $t |
  (if $t.zone == "Z" then 0 else
    (($t.zone[1:3] | tonumber) * 3600 + ($t.zone[4:6] | tonumber) * 60) *
    (if $t.zone[0:1] == "+" then 1 else -1 end) end) as $offset |
  [(($t.whole + "Z" | fromdateiso8601) - $offset),
    ((($t.fraction // "") + "000000000")[0:9] | tonumber)];
def two_whole_nodes($zone; $ready; $serving):
  length == 2 and
  (map(.uid) | unique | length) == 2 and
  (map(.node) | unique | length) == 2 and
  (map(.zone) | unique) == [$zone] and
  (map(.runner) | sort) == ["leader", "worker"] and
  all(.[]; (.uid|nonempty) and (.node|nonempty) and .gpuRequest == 8 and
    .ready == $ready and .serving == $serving);
def pod_group($name):
  .name == $name and .minMember == 2 and
  .topologyKey == "topology.kubernetes.io/zone";

.source.zone as $sourceZone |
.surge.zone as $surgeZone |
.request.payload.from_node as $fromNode |
(.source.pods | map(.uid) | sort) as $sourceUIDs |
(.surge.pods | map(.uid) | sort) as $replacementUIDs |
.source.routingService as $service |
.scenario == "maintenance-gang" and
.source.instance == 0 and
(.source.zone|nonempty) and
(.source.podGroup | pod_group("gang-engine-0")) and
(.source.pods | two_whole_nodes($sourceZone; true; true)) and
.source.leaderEndpointReady == true and
.preTrigger.migrationRequestCount == 0 and
.preTrigger.irReadyReplicas == 1 and .preTrigger.irServingReplicas == 1 and
.preTrigger.irAvailableReplicas == 1 and
(.request.uuid|uuid) and
.request.annotationKey == ("ome.io/migration-request-v1-" + .request.uuid) and
.request.payload.schemaVersion == "v1" and .request.payload.component == "engine" and
.request.payload.instance == .source.instance and
([.source.pods[].node] | index($fromNode)) != null and
.request.payload.requested_by == "alfred" and .request.payload.reason == "NodeMaintenance" and
.surge.instance == 1 and .surge.zone != .source.zone and
(.surge.podGroup | pod_group("gang-engine-1")) and
(.surge.pods | two_whole_nodes($surgeZone; false; false)) and
.surge.holdSeconds >= 2 and .surge.sourcePodsPresent == 2 and
.surge.sourcePodsReady == 2 and .surge.sourcePodsServing == 2 and
.surge.sourceLeaderEndpointReady == true and .surge.replacementLeaderEndpointReady == false and
.surge.alfredMaintenanceDrainedAt == null and .surge.alfredOMEGPUOccupantsPresent == true and
($service | type == "string" and startswith("gang-engine-rev-") and length > 16) and
(.handoff | type == "array" and length > 0) and
all(.handoff[]; .routingService == $service and
  (.sourceUIDs | sort) == $sourceUIDs and (.replacementUIDs | sort) == $replacementUIDs and
  (.sourceSafe == true or .replacementSafe == true)) and
.handoff[-1].replacementSafe == true and
.completed.migration.requestUUID == .request.uuid and
.completed.migration.trigger == "Manual" and
.completed.migration.sourceInstance == 0 and .completed.migration.surgeInstance == 1 and
.completed.migration.fromNode == .request.payload.from_node and
.completed.migration.phase == "Completed" and
(.completed.migration.startedAt|nonempty) and (.completed.migration.completedAt|nonempty) and
(.completed.migration.completedAt|instant) >= (.completed.migration.startedAt|instant) and
.completed.sourcePodUIDsPresent == [] and .completed.sourceEndpointPresent == false and
.completed.replacementPodsReady == 2 and .completed.replacementPodsServing == 2 and
.completed.replacementLeaderEndpointReady == true and
.completed.irReadyReplicas == 1 and .completed.irServingReplicas == 1 and
.completed.irAvailableReplicas == 1 and
.alfred.workload == "alfred-e2e/gang" and .alfred.component == "engine" and
.alfred.instance == 0 and .alfred.policy == "nodehealth" and .alfred.reason == "NodeMaintenance" and
.alfred.fromNode == .request.payload.from_node and .alfred.requestUUID == .request.uuid and
.alfred.dispatchStatus == "completed" and .alfred.maintenanceTriggers == ["patching"] and
(.alfred.maintenanceRequestedAt|nonempty) and (.alfred.maintenanceDrainedAt|nonempty) and
(.alfred.maintenanceRequestedAt|instant) < (.alfred.maintenanceDrainedAt|instant) and
.alfred.omeGPUOccupantsPresent == false
