---
title: Traffic map
description: "A TrafficMap is the alpha routing table that OME writes for a multi-cluster InferenceService: each workload cluster's share of the requests, and the reasons behind it."
status: preview
since: v1.3
---

[Placement](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-PlacementSpec) decides which [workload clusters](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster) run a multi-cluster [InferenceService](../serving/inference-services.md). Multi-cluster placement and routing are alpha, still in development, and off by default. With routing on, OME writes a TrafficMap that gives each of those clusters a weight. A publisher writes the weights into your global gateway, by default as a [Gateway API](https://gateway-api.sigs.k8s.io/) HTTPRoute. Read the map to see why a cluster gets less traffic, or none, and whether the publisher has written the latest weights.

## Turn on routing

Routing runs only on the control-plane cluster, and it's off by default. Turn it on in that cluster's `ome-resources` values:

```yaml
ome:
  multicluster:
    enabled: true
    role: control-plane
    config:
      routing:
        enabled: true
```

`helm upgrade` restarts the manager to apply them. If you edit the `inferenceservice-config` ConfigMap instead, restart the manager yourself.

Routing then writes the maps. To publish them to your gateway, set the `endpoint` values in [Publish a global endpoint](../../guides/multi-cluster/publish-a-global-endpoint.md#step-1-configure-publishing). Until you do, `Published` is `False` with reason `Withdrawn`.

OME writes a map, with the same name and namespace, for each InferenceService that sets `spec.placement.requirements` or `spec.placement.clusterSelector`. Without `spec.placement`, the annotation `ome.io/accelerator-requirements` or `ome.io/cluster-selector` counts too. To opt an InferenceService out, set `spec.routing.enabled: false`. Setting it to `true` doesn't turn routing on when the installation has it off.

OME deletes the map when the InferenceService stops being routed, for example because you turn routing off. When you delete the InferenceService, the finalizer `ome.io/trafficmap-publisher` holds the map until the publisher has removed the route. Placement keeps the InferenceService's copies on the workload clusters until the map is gone.

## Read a TrafficMap

The examples use the InferenceService `llama-chat` in `llama-demo`, placed in mode `Split` on the workload clusters `cluster-a` and `cluster-b`.

To see a map, run `kubectl get trafficmap`, or its short names `tm` and `tmap`, on the control-plane cluster:

```bash
kubectl get trafficmap llama-chat -n llama-demo
```

```output
NAME         MODE    PUBLISHED   ROUTABLE   OVERRIDE   REASON     AGE
llama-chat   Split   True        True       False      Routable   3h
```

`MODE` is the InferenceService's placement mode: `Single` runs it on one cluster, `All` on every cluster that admits it, and `Split` spreads its replicas across clusters. The other columns come from the map's [conditions](#conditions).

[`kubectl ome get trafficmaps`](../../reference/kubectl-ome/get.md#trafficmaps-columns) lists the maps too, with their entry counts. `-o wide` adds the route and whether the publisher is current.

This is the map with four replicas ready on `cluster-a` and two on `cluster-b`, without its conditions and `status.publisher`:

```yaml
apiVersion: ome.io/v1beta1
kind: TrafficMap
metadata:
  name: llama-chat
  namespace: llama-demo
  generation: 4
spec:
  service: llama-chat
  mode: Split
  observedISVCGeneration: 7
  entries:
    - cluster: cluster-a
      endpoint: http://llama-chat.llama-demo.cluster-a.example.com
      weight: 2
      healthy: true
      capacity:
        allocated: 4
        ready: 4
        source: ControlPlane
    - cluster: cluster-b
      endpoint: http://llama-chat.llama-demo.cluster-b.example.com
      weight: 1
      healthy: true
      capacity:
        allocated: 2
        ready: 2
        source: ControlPlane
status:
  sourceUID: 0f8e2c1a-5b7d-4e39-9c61-2d4a8b3f7e10
  published: true
  observedTrafficMapGeneration: 4
  gatewayRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: llama-chat-global
    namespace: llama-demo
```

Each entry is a home of the InferenceService: a workload cluster that has admitted it and reports an endpoint for it. Condition reasons call the entries homes, as in `AllHomesUnready`. The entries hold these fields:

| Field | What it holds |
| --- | --- |
| `cluster` | The WorkloadCluster's name. |
| `endpoint` | The URL of the InferenceService's copy on that cluster. |
| `weight` | The cluster's share of the requests, relative to the other entries. |
| `healthy` | `true` when the cluster has a ready replica and no probe gates it. It ignores capacity, so a healthy entry can still have weight 0. |
| `capacity.allocated` | The replicas that placement admitted on the cluster, lowered to `capacity.reported` when that's lower. |
| `capacity.ready` | The ready replicas. |
| `capacity.factor` | The capacity factor, present only when you set one. |
| `capacity.source` | `Endpoint` when a capacity report lowered `allocated`, otherwise `ControlPlane`. |
| `capacity.reported`, `capacity.fallbackReason` | While capacity polling is on: the ceiling that the cluster reports, or why it has none. See [Watch the capacity state](../../guides/multi-cluster/tune-routing-capacity-polling.md#step-3-watch-the-capacity-state). |
| `probe` | The routing health probe's last verdict for the cluster, present once a configured probe has run. See [Check the probe state](../../guides/multi-cluster/routing-health-probes.md#step-3-check-the-probe-state). |
| `drainRefs` | The IDs of the drains that hold the cluster at weight 0. |

The map leaves out `allocated` and `ready` when they're 0. [`TrafficMapEntry`](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-TrafficMapEntry) in the API reference describes every field.

Treat the map as read-only: OME undoes a hand edit right away.

## How OME sets the weights

OME works out an entry's weight in four steps:

1. It counts the replicas that can serve: the lower of `capacity.allocated` and `capacity.ready`, times the cluster's capacity factor, which is 1 unless you set one. For an InferenceService with an engine and a decoder, both counts are the lower of the two [components'](../serving/inference-services.md#components) counts.
2. An entry that a routing health probe gates gets 0, with one exception that [Routable](#routable) describes.
3. It reduces the weights to their smallest whole-number ratio, so 4 and 2 become 2 and 1.
4. An entry that a drain holds gets 0. The other weights stay as they are.

Divide a weight by the sum of the weights for its share: in the example, `cluster-a` gets two thirds of the requests.

Four settings change the weights:

| Setting | What it does | How to use it |
| --- | --- | --- |
| Capacity factor | Counts a cluster's replicas for more or less. | [Weight traffic for heterogeneous clusters](../../guides/multi-cluster/weight-traffic-for-heterogeneous-clusters.md) |
| Capacity polling | Can lower a cluster's `capacity.allocated` to what the cluster reports. | [Tune routing capacity polling](../../guides/multi-cluster/tune-routing-capacity-polling.md) |
| Routing health probe | Holds a cluster at 0 after repeated failures. | [Configure routing health probes](../../guides/multi-cluster/routing-health-probes.md) |
| Drain | Holds a cluster at 0, whatever its capacity. | [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md) |

## Why a cluster's weight is zero

Compare the entry with these rows, and take the first that matches:

| What the entry shows | Why the weight is 0 |
| --- | --- |
| `drainRefs` lists an ID | A drain holds the cluster at 0, whatever its capacity. Remove the drain by its ID, as [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md) shows. |
| No `capacity.ready`, and `healthy: false` | The cluster has no ready replica, or the InferenceService's copy there isn't `IngressReady` yet. |
| No `capacity.allocated`, `capacity.source: Endpoint` and `capacity.reported: 0` | The cluster reports that it can serve nothing, although its replicas are ready. |
| No `capacity.allocated`, and `capacity.source: ControlPlane` | Placement admitted no replicas on the cluster. |
| `probe.gated: true`, and `healthy: false` | The probe gated the cluster after repeated failures. `probe.message` holds the last one. |

If every entry has weight 0, or the table is empty, the [Routable](#routable) condition names the cause for the whole map.

## Conditions

`Routable`, `OverrideActive` and `CapacityFallback` describe the table. `Published` says whether the publisher has written it to the gateway.

`kubectl describe` also shows `PublicationFallback`, which is `False` with reason `Unsupported` after the built-in publisher publishes successfully. It's `Unknown` while a publish is under way or failing. When `Published` has reason `Withdrawn`, `Unpublished` or `Finalizing`, `PublicationFallback` is `False` with one of those reasons too.

### Routable

`Routable` says whether a gateway can send the InferenceService's requests anywhere:

| Status | Reason | What it means |
| --- | --- | --- |
| `True` | `Routable` | At least one entry has a positive weight. |
| `True` | `AllHomesProbeFailed` | The probe gates every cluster, and `allFailedPolicy: PreserveTraffic` keeps the weights of the clusters with ready replicas and capacity. |
| `False` | `NotPlaced` | The InferenceService's `status.placement.phase` isn't `Placed`, so the table is empty. |
| `False` | `NoAddressableHome` | The InferenceService is placed, but no admitted cluster reports an endpoint, so the table is empty. |
| `False` | `TrafficDrain` | Drains hold every cluster that had a positive weight. |
| `False` | `AllHomesUnready` | No cluster has a ready replica. |
| `False` | `NoRoutableCapacity` | No cluster that the probe leaves open has both `capacity.allocated` and `capacity.ready` above 0. |
| `False` | `AllHomesProbeFailed` | The probe gates every cluster, and its `allFailedPolicy` is `Drain`. |

### OverrideActive

`OverrideActive` says whether drains hold any entry:

| Status | Reason | What it means |
| --- | --- | --- |
| `True` | `OverridesApplied` | At least one drain holds an entry. |
| `False` | `OverridesPending` | The InferenceService has drains, but none names a cluster in the table. `kubectl ome traffic drain` doesn't check the name against the map, so look for a misspelling. |
| `False` | `NoOverrides` | The InferenceService has no drains. |

### CapacityFallback

`CapacityFallback` is `True` when capacity polling is on and an entry has no usable report:

| Status | Reason | What it means |
| --- | --- | --- |
| `True` | `EndpointCapacityUnavailable` | At least one entry has no usable report and uses the control plane's count. Its `capacity.fallbackReason` says why. |
| `False` | `EndpointCapacityAvailable` | Every entry has a usable capacity report. |
| `False` | `CapacityPollingDisabled` | Capacity polling is off. |
| `False` | `NoCapacityTargets` | The table is empty, so there's nothing to poll. |

### Published

The built-in publisher writes the table into an HTTPRoute, by default `llama-chat-global` in the InferenceService's namespace, and `status.gatewayRef` names it. The route has a backend for each entry, at the entry's weight, entries at weight 0 included. [How publishing works](../../guides/multi-cluster/publish-a-global-endpoint.md#how-publishing-works) lists what else it writes.

`Published` means that the publisher wrote the route. The HTTPRoute's own status says whether the gateway accepted it, as [Check the publication](../../guides/multi-cluster/publish-a-global-endpoint.md#step-2-check-the-publication) shows.

| Status | Reason | What it means |
| --- | --- | --- |
| `True` | `Published` | The publisher wrote the current table. |
| `False` | `Transitioning` | The publisher is writing the current table. |
| `False` | `Withdrawn` | One of `globalGateway`, `backendPort` or the InferenceService's global hostname is missing, so the publisher deleted its route. See [Withdrawn](../../guides/multi-cluster/publish-a-global-endpoint.md#published-is-false-with-reason-withdrawn). |
| `False` | `InvalidPlan` | The table can't become a route, for example because it has more than 16 entries. See [InvalidPlan](../../guides/multi-cluster/publish-a-global-endpoint.md#published-is-false-with-reason-invalidplan). |
| `False` | `InvalidOptions` | `spec.routing.publisher.options` sets options, and the built-in publisher takes none. |
| `False` | `ClaimRejected` | The publisher can't claim the route or one of its objects, for example because another route already publishes the global hostname. See [ClaimRejected](../../guides/multi-cluster/publish-a-global-endpoint.md#published-is-false-with-reason-claimrejected). |
| `False` | `ApplyFailed` | Writing or deleting the route, or one of its objects, failed. See [ApplyFailed](../../guides/multi-cluster/publish-a-global-endpoint.md#published-is-false-with-reason-applyfailed). |
| `False` | `DrainFailed` | After the route moved, for example because `routeNamespace` changed, setting the old route's weights to 0 failed. |
| `False` | `InvalidOwner` | The map doesn't match its InferenceService, for example until OME rewrites it after a spec change. See [When the map stops updating](#where-it-comes-from). |
| `False` | `LegacyLifecycleActive` | The publisher waits for the legacy endpoint publisher to finish its cleanup. See [LegacyLifecycleActive](../../guides/multi-cluster/publish-a-global-endpoint.md#published-is-false-with-reason-legacylifecycleactive). |
| `False` | `PublisherChanged` | `status.publisher` lists targets that another publisher claimed, for example after `ome.multicluster.config.routing.publisher.name` changed. |
| `False` | `Finalizing` | The map is being deleted, and the publisher is removing its route. |
| `False` | `Unpublished` | The InferenceService stopped being routed or is being deleted. If the map is being deleted, the publisher has removed its route. |
| `False` | `UnpublishFailed` | OME doesn't set this reason. When removing the route fails, `Published` stays `Finalizing`, and the manager logs `TrafficMap publisher finalization failed` and retries. |

When publishing fails, the route from the last success stays, and `status.gatewayRef` still names it. The condition's message is generic. The error is in the manager's log, on a line that reads `TrafficMap publisher reconciliation failed`. The publisher retries on its own.

## Staleness checks

Before you rely on a map, check that it's current:

| Compare | With | A mismatch means |
| --- | --- | --- |
| `spec.observedISVCGeneration` | The InferenceService's `metadata.generation` | OME hasn't built a table from the latest spec yet. If the mismatch stays, see [When the map stops updating](#where-it-comes-from). |
| `status.observedTrafficMapGeneration` | The map's `metadata.generation` | The publisher hasn't published the latest table. After a failure, it still holds the generation of the last success. |
| Each condition's `observedGeneration` | The map's `metadata.generation` | The condition describes an older table. |
| `status.sourceUID` | The InferenceService's `metadata.uid` | The map is left from an earlier InferenceService with the same name. |
| `probe.lastProbeTime` | The current time | A time much older than the probe's `period` means that the prober has stopped. |

To compare the map's generation with the one that the publisher last published:

```bash
kubectl get trafficmap llama-chat -n llama-demo \
  -o jsonpath='{.metadata.generation} {.status.observedTrafficMapGeneration}{"\n"}'
```

```output
4 4
```

The numbers match, so the publisher has published the latest table.

## When the map stops updating {#where-it-comes-from}

Some errors stop OME from updating a map. The map keeps its last table and conditions, and the route keeps its last published weights. When the error follows a spec change, `Published` turns `False` with reason `InvalidOwner`. The error shows only in the manager's log. Read every replica's log with `kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1`.

| Log error | Cause |
| --- | --- |
| `validate routing policy:` | A factor in `spec.routing.capacityFactors` is 0 or negative. See [The weights don't change](../../guides/multi-cluster/weight-traffic-for-heterogeneous-clusters.md#the-weights-dont-change). |
| `parse ome.io/traffic-drain:` | The drain annotation is malformed. See [The TrafficMap stops changing after a hand edit](../../guides/multi-cluster/drain-a-workload-cluster.md#the-trafficmap-stops-changing-after-a-hand-edit). |
| `TrafficMap llama-demo/llama-chat exists but is not controlled by InferenceService UID` | A TrafficMap with that name belongs to something else, for example an earlier InferenceService with the same name. |

## Next steps

- [Publish a global endpoint](../../guides/multi-cluster/publish-a-global-endpoint.md): write the map's weights into your global gateway.
- [Weight traffic for heterogeneous clusters](../../guides/multi-cluster/weight-traffic-for-heterogeneous-clusters.md): count one cluster's replicas for more with a capacity factor.
- [Tune routing capacity polling](../../guides/multi-cluster/tune-routing-capacity-polling.md): lower a cluster's weight to the capacity it reports.
- [Configure routing health probes](../../guides/multi-cluster/routing-health-probes.md): gate a cluster's weight on an end-to-end probe.
- [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md): hold a cluster at weight 0 with kubectl ome traffic drain.
