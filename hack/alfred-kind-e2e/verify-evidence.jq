def uuid:
  type == "string" and
  test("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

def nonempty: type == "string" and length > 0;

def expected_replicas: if .scenario == "maintenance-columnar" then 4 else 1 end;

# Keep seconds and nanoseconds separate: epoch floating-point addition would
# lose sub-microsecond ordering. Accept Go RFC3339/RFC3339Nano and UTC offsets.
def instant:
  capture("^(?<whole>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\\.(?<fraction>[0-9]{1,9}))?(?<zone>Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])$") as $t |
  (if $t.zone == "Z" then 0 else
    (($t.zone[1:3] | tonumber) * 3600 + ($t.zone[4:6] | tonumber) * 60) *
    (if $t.zone[0:1] == "+" then 1 else -1 end) end) as $offset |
  [(($t.whole + "Z" | fromdateiso8601) - $offset),
    ((($t.fraction // "") + "000000000")[0:9] | tonumber)];

def source_healthy:
  expected_replicas as $expected |
  (.source.name | nonempty) and
  (.source.uid | nonempty) and
  (.source.node | nonempty) and
  .source.instance >= 0 and
  .source.incarnation >= 1 and
  .source.ready == true and
  .source.serving == true and
  .source.endpointReady == true and
  .preTrigger.migrationRequestCount == 0 and
  .preTrigger.irReadyReplicas == $expected and
  .preTrigger.irServingReplicas == $expected and
  .preTrigger.irAvailableReplicas == $expected;

def request_is_real:
  (.request.uuid | uuid) and
  .request.annotationKey == ("ome.io/migration-request-v1-" + .request.uuid) and
  .request.payload.schemaVersion == "v1" and
  .request.payload.component == "engine" and
  .request.payload.instance == .source.instance and
  .request.payload.from_node == .source.node and
  .request.payload.requested_by == "alfred";

def surge_preserved_source:
  .surge.observed == true and
  .surge.holdSeconds >= 2 and
  .surge.sourcePresent == true and
  .surge.sourceReady == true and
  .surge.sourceServing == true and
  .surge.sourceEndpointReady == true and
  .surge.replacementReady == false and
  .surge.replacementServing == false and
  .surge.replacementEndpointReady == false and
  .surge.alfredMaintenanceDrainedAt == null and
  .surge.alfredOMEGPUOccupantsPresent == true and
  (.surge.replacement.name | nonempty) and
  (.surge.replacement.uid | nonempty) and
  .surge.replacement.uid != .source.uid and
  (.surge.replacement.node | nonempty) and
  .surge.replacement.node != .source.node and
  .surge.replacement.instance != .source.instance and
  .surge.replacement.incarnation >= 1;

def migration_completed:
  expected_replicas as $expected |
  .completed.migration.requestUUID == .request.uuid and
  .completed.migration.trigger == "Manual" and
  .completed.migration.sourceInstance == .source.instance and
  .completed.migration.surgeInstance == .surge.replacement.instance and
  .completed.migration.fromNode == .source.node and
  .completed.migration.phase == "Completed" and
  (.completed.migration.startedAt | nonempty) and
  (.completed.migration.completedAt | nonempty) and
  (.completed.migration.completedAt | instant) >= (.completed.migration.startedAt | instant) and
  .completed.sourcePresent == false and
  .completed.sourceEndpointPresent == false and
  .completed.replacementPresent == true and
  .completed.replacementReady == true and
  .completed.replacementServing == true and
  .completed.replacementEndpointReady == true and
  .completed.irReadyReplicas == $expected and
  .completed.irServingReplicas == $expected and
  .completed.irAvailableReplicas == $expected;

def raw_columnar:
  .status.instanceStatusEncoding == "ColumnarV2" and
  (.status.instanceStatusColumns | type == "object") and
  (.status | has("instanceStatuses") | not) and
  .status.readyReplicas == 4 and .status.servingReplicas == 4 and
  .status.availableReplicas == 4;

def columnar_proven:
  .source.uid as $sourceUID | .source.node as $sourceNode |
  .source.instance as $sourceIndex | .surge.replacement.uid as $replacementUID |
  .columnar.initialCount == 4 and .columnar.uniqueRequestCount == 1 and
  (.columnar.baselineUIDs | length == 4 and (unique | length) == 4 and
    index($sourceUID) != null and index($replacementUID) == null) and
  (.columnar.baselineNodes | length == 4 and (unique | length) == 4 and index($sourceNode) != null) and
  (.columnar.rawBeforeTrigger | raw_columnar) and
  (.columnar.rawCompleted | raw_columnar) and
  all(.columnar.decodedBeforeTrigger, .columnar.decodedCompleted;
    .rawEncoding == "ColumnarV2" and (.rows | length) == 4 and
    all(.rows[]; .phase == "Ready")) and
  any(.columnar.decodedBeforeTrigger.rows[]; .index == $sourceIndex);

def handoff_preserved_routing:
  .source.uid as $sourceUID | .surge.replacement.uid as $replacementUID |
  .source.routingService as $service |
  ($service | type == "string" and startswith("single-engine-rev-") and length > 18) and
  (.handoff | type == "array" and length > 0) and
  all(.handoff[]; .routingService == $service and
    .sourceUIDs == [$sourceUID] and .replacementUIDs == [$replacementUID] and
    (.sourceSafe == true or .replacementSafe == true)) and
  .handoff[-1].replacementSafe == true;

def alfred_observed_drain:
  .alfred.workload == "alfred-e2e/single" and
  .alfred.component == "engine" and
  .alfred.instance == .source.instance and
  .alfred.policy == "nodehealth" and
  .alfred.reason == .request.payload.reason and
  .alfred.fromNode == .source.node and
  .alfred.requestUUID == .request.uuid and
  .alfred.dispatchStatus == "completed" and
  .alfred.omeGPUOccupantsPresent == false and
  (if .scenario == "unhealthy-single" then
    .alfred.reason == "NodeUnhealthy" and
    .alfred.healthState == "Unhealthy" and
    (.alfred.signaledAt | nonempty) and (.alfred.drainedAt | nonempty) and
    (.alfred.signaledAt | instant) < (.alfred.drainedAt | instant)
  else
  .alfred.reason == "NodeMaintenance" and
  .alfred.maintenanceTriggers == ["patching"] and
  (.alfred.maintenanceRequestedAt | nonempty) and
  (.alfred.maintenanceDrainedAt | nonempty) and
  (.alfred.maintenanceRequestedAt | instant) <
    (.alfred.maintenanceDrainedAt | instant) and
  .alfred.omeGPUOccupantsPresent == false end);

def health_window_observed:
  .health.recoveryWindowSeconds as $window |
  .health.conditionType == "GpuUnhealthy" and .health.conditionStatus == "True" and
  (.health.transitionTime | nonempty) and
  .health.transitionTime == .health.transitionTimeAfterHeartbeat and
  (.health.recoveryTransitionTime | nonempty) and .health.heartbeatAdvanced == true and
  (.health.transitionTime | instant) >=
    (.health.recoveryTransitionTime | instant | .[0] += $window) and
  .health.recoveryWindowSeconds == 60 and
  .health.recoveryObservedSeconds >= .health.recoveryWindowSeconds and
  .health.recoveryRequestCount == 0 and .health.recoveryMigrationCount == 0 and
  .health.recoverySourceReady == true and .health.recoverySourceServing == true and
  .health.recoverySourceEndpointReady == true and .health.recoverySuspectObserved == true;

def restart_recovered_same_request:
  .restart.deployment == "ome-alfred" and
  (.restart.oldPodUID | nonempty) and (.restart.newPodUID | nonempty) and
  .restart.oldPodUID != .restart.newPodUID and
  .restart.journalUUIDBefore == .request.uuid and .restart.journalUUIDAfter == .request.uuid and
  .restart.leadershipRecovered == true and .restart.sourceReady == true and
  .restart.sourceServing == true and .restart.sourceEndpointReady == true and
  .restart.uniqueRequestCount == 1 and .restart.workloadDispatchCount == 1 and
  .restart.migrationCount == 1;

(.scenario == "maintenance-single" or .scenario == "maintenance-columnar" or .scenario == "unhealthy-single" or .scenario == "restart-single") and
source_healthy and
request_is_real and
surge_preserved_source and
handoff_preserved_routing and
migration_completed and
alfred_observed_drain and
(if .scenario == "unhealthy-single" then health_window_observed
 elif .scenario == "restart-single" then restart_recovered_same_request
 elif .scenario == "maintenance-columnar" then columnar_proven
 else true end)
