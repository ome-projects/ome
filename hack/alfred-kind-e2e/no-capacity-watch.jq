# Input is slurped kubectl --output-watch-events JSON, never line-delimited text.
def objects: (.object // .) | (.items // [.])[];
length > 0 and all(.[];
  .type != "DELETED" and
  all(objects;
    .metadata.deletionTimestamp == null and
    if $kind == "pods" then
      .metadata.uid == $uid and
      any(.status.conditions[]?; .type == "Ready" and .status == "True") and
      any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True")
    elif $kind == "endpoints" then
      any(.endpoints[]?; .targetRef.uid == $uid and .conditions.ready == true)
    elif $kind == "isvc" then
      ([.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] | length) == 0
    elif $kind == "ir" then
      (.status.migrations // []) == [] and .status.readyReplicas == 1 and
      .status.servingReplicas == 1 and .status.availableReplicas == 1
    elif $kind == "recommendations" then
      (.data[$key] // "null" | fromjson) as $record |
      $record == null or ($record.maintenanceDrainedAt == null and $record.drainedAt == null and
        ($record.maintenance.requested != true or $record.omeGpuOccupantsPresent == true))
    else false end))
