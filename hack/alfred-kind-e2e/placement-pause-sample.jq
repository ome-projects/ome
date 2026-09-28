# Shared by live assertions and the independently rerunnable evidence verifier.
def pp_nonempty: type == "string" and length > 0;
def pp_policy($revision; $paused):
  {planID:"alfred-e2e-pause",revision:$revision,sourceUID:"alfred-e2e-source",
   clusterUID:"alfred-e2e-member",pauseSurge:$paused};
def pp_cycle: .recommendations.data["last-cycle.json"] | fromjson;
def pp_time:
  capture("^(?<whole>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\\.(?<fraction>[0-9]{1,9}))?Z$") as $t |
  [($t.whole + "Z" | fromdateiso8601), ((($t.fraction // "") + "000000000")[0:9] | tonumber)];
def pp_journal: .dispatch.data["state.json"] | fromjson;
def pp_entries($isvcUID; $irUID):
  [pp_journal.entries[] | select(.workloadUID == $isvcUID or .irUID == $irUID)];
def pp_annotations:
  [.isvc.metadata.annotations | to_entries[] | select(.key | startswith("ome.io/migration-request-v1-"))];
def pp_node($node): .recommendations.data["node." + $node] | fromjson;
def pp_endpoint($uid; $service):
  any(.endpoints.items[]; .metadata.labels["kubernetes.io/service-name"] == $service and
    any(.endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true and .conditions.terminating != true));
def pp_pod($uid; $ready):
  any(.pods.items[]; .metadata.uid == $uid and .metadata.deletionTimestamp == null and
    any(.status.conditions[]?; .type == "Ready" and .status == (if $ready then "True" else "False" end)) and
    (if $ready then any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True")
     else all(.status.conditions[]?; .type != "ome.io/serving" or .status != "True") end));
def pp_source($source):
  pp_pod($source.uid; true) and pp_endpoint($source.uid; $source.routingService) and
  any(.pods.items[]; .metadata.uid == $source.uid and .spec.nodeName == $source.node and
    .metadata.labels["ome.io/instance-index"] == ($source.instance | tostring));
def pp_projection($revision; $paused):
  (.isvc.metadata.annotations["ome.io/placement-execution"] | fromjson) == pp_policy($revision;$paused) and
  .ir.spec.placementExecution == pp_policy($revision;$paused) and
  .ir.status.placementObservedGeneration == .ir.metadata.generation and
  (if $paused then .ir.spec.placementReplicaLimit == 1 else .ir.spec.placementReplicaLimit == null end);
def pp_identity($isvcUID; $irUID):
  ($isvcUID | pp_nonempty) and ($irUID | pp_nonempty) and
  .isvc.metadata.uid == $isvcUID and .isvc.metadata.name == "single" and
  .isvc.metadata.namespace == "alfred-e2e" and .isvc.metadata.deletionTimestamp == null and
  .isvc.metadata.labels["ome.io/placement-origin"] == "alfred-e2e-source" and
  .isvc.metadata.annotations["ome.io/placement-origin-uid"] == "alfred-e2e-source" and
  .ir.metadata.uid == $irUID and .ir.metadata.name == "single-engine" and
  .ir.metadata.namespace == "alfred-e2e" and .ir.metadata.deletionTimestamp == null and
  any(.ir.metadata.ownerReferences[]?; .uid == $isvcUID and .kind == "InferenceService" and .controller == true) and
  (.ir.metadata.generation | type == "number" and . >= 1) and
  (.pods.items | type == "array") and (.endpoints.items | type == "array") and
  (.requests | type == "array") and
  (pp_journal | .version == "v1" and (.entries | type == "array")) and
  (pp_cycle | (.timestamp | pp_nonempty) and (.timestamp | pp_time | length == 2) and
    .mode == "execute" and has("recommendations") and
    (.recommendations == null or (.recommendations | type == "array")));
def pp_paused_safe($source; $isvcUID; $irUID):
  pp_projection(1;true) and pp_source($source) and
  .ir.status.readyReplicas == 1 and .ir.status.servingReplicas == 1 and .ir.status.availableReplicas == 1 and
  ((.ir.status.migrations // []) | length == 0) and
  ([.pods.items[].metadata.uid] == [$source.uid]) and
  (pp_annotations | length == 0) and (.requests | length == 0) and
  (pp_entries($isvcUID;$irUID) | length == 0);
def pp_one_request($source; $uuid; $isvcUID; $irUID):
  .requests[0].value as $payload |
  ($uuid | type == "string" and test("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")) and
  (.requests | length == 1) and
  .requests[0].key == ("ome.io/migration-request-v1-" + $uuid) and
  (.requests[0].value | fromjson | .schemaVersion == "v1" and .component == "engine" and
    .instance == $source.instance and .from_node == $source.node and .requested_by == "alfred" and .reason == "NodeMaintenance") and
  all(pp_annotations[]; .key == ("ome.io/migration-request-v1-" + $uuid) and
    (.value | fromjson) == ($payload | fromjson)) and
  (pp_entries($isvcUID;$irUID) | length == 1 and .[0].uuid == $uuid and
    .[0].workloadUID == $isvcUID and .[0].irUID == $irUID and
    (.[0].payload | fromjson) == ($payload | fromjson));
def pp_allocated($source; $replacement; $uuid):
  ($replacement.uid | pp_nonempty) and $replacement.uid != $source.uid and
  $replacement.node != $source.node and ($replacement.node | pp_nonempty) and
  ([.pods.items[].metadata.uid] | sort) == ([$source.uid,$replacement.uid] | sort) and
  pp_source($source) and pp_pod($replacement.uid;false) and
  (pp_endpoint($replacement.uid;$source.routingService) | not) and
  any(.pods.items[]; .metadata.uid == $replacement.uid and .spec.nodeName == $replacement.node and
    .metadata.labels["ome.io/instance-index"] == ($replacement.instance | tostring)) and
  (.ir.status.migrations | length == 1 and .[0].requestUUID == $uuid and
    .[0].trigger == "Manual" and .[0].fromNode == $source.node and
    .[0].phase == "SurgePending" and .[0].sourceInstance == $source.instance and
    .[0].surgeInstance == $replacement.instance and (.[0].surgeInstance | type == "number" and . >= 0));
def placement_sample($phase; $source; $isvcUID; $irUID; $uuid; $replacement):
  pp_identity($isvcUID;$irUID) and
  if $phase == "source-safe" then pp_source($source)
  elif $phase == "paused-safe" then pp_paused_safe($source;$isvcUID;$irUID)
  elif $phase == "paused" then
    pp_paused_safe($source;$isvcUID;$irUID) and
    (pp_node($source.node) | .maintenance.requested == true and .maintenanceDrainedAt == null and .omeGpuOccupantsPresent == true) and
    (pp_cycle | any(.recommendations[]; .workload == "alfred-e2e/single" and .component == "engine" and
      .instance == $source.instance and .fromNode == $source.node and .policy == "nodehealth" and
      .reason == "NodeMaintenance" and .outcome == "advisory" and .advisoryReason == "OMENativeStateIneligible" and
      (.requestUUID // "") == ""))
  elif $phase == "released" then pp_projection(2;false) and pp_source($source)
  elif $phase == "allocated" or $phase == "repaused" then
    pp_projection((if $phase == "allocated" then 2 else 3 end);$phase == "repaused") and
    pp_one_request($source;$uuid;$isvcUID;$irUID) and pp_allocated($source;$replacement;$uuid)
  elif $phase == "completed" then
    pp_projection(3;true) and pp_one_request($source;$uuid;$isvcUID;$irUID) and
    ([.pods.items[].metadata.uid] == [$replacement.uid]) and pp_pod($replacement.uid;true) and
    pp_endpoint($replacement.uid;$source.routingService) and
    (any(.endpoints.items[].endpoints[]?; .targetRef.uid == $source.uid) | not) and
    .ir.status.readyReplicas == 1 and .ir.status.servingReplicas == 1 and .ir.status.availableReplicas == 1 and
    (.ir.status.migrations | length == 1 and .[0].requestUUID == $uuid and .[0].phase == "Completed" and
      .[0].trigger == "Manual" and .[0].fromNode == $source.node and
      .[0].sourceInstance == $source.instance and .[0].surgeInstance == $replacement.instance) and
    (pp_entries($isvcUID;$irUID) | .[0].phase == "completed" and (.[0].completedAt | pp_nonempty))
  else false end;
