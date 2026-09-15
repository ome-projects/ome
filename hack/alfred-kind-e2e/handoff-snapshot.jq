# Summarize one public-API observation after releasing KWOK readiness.
# A source may leave only when the exact held replacement is healthy and routed.
def healthy_pods($uids):
  .pods.items as $pods |
  ($uids | length) > 0 and all($uids[]; . as $uid |
    any($pods[]; .metadata.uid == $uid and .metadata.deletionTimestamp == null and
      any(.status.conditions[]?; .type == "Ready" and .status == "True") and
      any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True")));
def routed($uid):
  .routingService as $service |
  any(.endpoints.items[] |
    select(.metadata.labels["kubernetes.io/service-name"] == $service) | .endpoints[]?;
    .targetRef.uid == $uid and .conditions.ready == true and .conditions.terminating != true);
{
  routingService, sourceUIDs, replacementUIDs,
  sourceSafe: (healthy_pods(.sourceUIDs) and routed(.sourceRoutingUID)),
  replacementSafe: (healthy_pods(.replacementUIDs) and routed(.replacementRoutingUID))
}
