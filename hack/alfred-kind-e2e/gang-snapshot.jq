def yes($type): any(.status.conditions[]?; .type == $type and .status == "True");
($pods.items | map(select((.metadata.labels["ome.io/instance-index"] // "-1" | tonumber) == $instance)) |
  map(. as $pod | {
    name:.metadata.name, uid:.metadata.uid, terminating:(.metadata.deletionTimestamp != null),
    node:(.spec.nodeName // ""),
    zone:([$nodes.items[] | select(.metadata.name == $pod.spec.nodeName) | .metadata.labels["topology.kubernetes.io/zone"]] | first // ""),
    runner:.metadata.labels["ome.io/runner"],
    podGroup:.metadata.labels["scheduling.x-k8s.io/pod-group"],
    scheduler:.spec.schedulerName,
    gpuRequest:([.spec.containers[].resources.requests["nvidia.com/gpu"] // "0" | tonumber] | add),
    ready:yes("Ready"), serving:yes("ome.io/serving")
  }) | sort_by(.runner)) as $members |
([ $groups.items[] | select(.metadata.name == ("gang-engine-" + ($instance|tostring))) |
  {name:.metadata.name,minMember:.spec.minMember,topologyKey:.metadata.annotations["ome.io/topology-key"]}] | first // {}) as $group |
([$members[] | select(.runner == "leader") | .uid] | first // "") as $leader |
{instance:$instance,zone:($members[0].zone // ""),podGroup:$group,pods:$members,
 leaderEndpointReady:any($endpoints.items[].endpoints[]?; .targetRef.uid == $leader and .conditions.ready == true and .conditions.terminating != true)}
