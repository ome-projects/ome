---
title: Weight traffic for heterogeneous clusters
description: Give a workload cluster whose replicas serve more requests a larger share of an alpha multi-cluster InferenceService's traffic, with capacity factors in spec.routing.capacityFactors.
status: preview
since: v1.3
---

A capacity factor gives a workload cluster whose replicas serve more requests a larger share of a multi-cluster [InferenceService](../../concepts/serving/inference-services.md)'s traffic. By default, the service's [TrafficMap](../../concepts/rollouts-and-traffic/traffic-map.md) counts every ready replica the same, which suits clusters that run the same accelerators. In the examples, a replica on `worker-b` serves three times the requests of one on `worker-a`, so `worker-b` gets a factor of 3 in `spec.routing.capacityFactors`.

Multi-cluster routing is alpha, still in development, and off by default. Its API, capacity factors included, can change without notice.

<div class="prerequisites" markdown>

- A control-plane cluster with OME in the namespace `ome`, and `kubectl` access to it. Its `ome-resources` values set `ome.multicluster.enabled=true` and `ome.multicluster.role=control-plane`, as in [Controller manager flags](../../reference/operate-ome/controller-manager-flags.md#multi-cluster), and `ome.multicluster.config.routing.enabled=true`, as in [Turn on routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing).
- Two [WorkloadClusters](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster) with different accelerators, `worker-a` and `worker-b`.
- The InferenceService `chat` in the namespace `prod` on the control-plane cluster, [eligible for routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing), with [`spec.placement.mode`](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-PlacementSpec) `All` or `Split`. Once it's placed on both workload clusters, `kubectl get trafficmap chat -n prod -o jsonpath='{.spec.entries[*].cluster}'` prints `worker-a worker-b`.
- How many requests one replica serves on each workload cluster at the same latency target, measured for example with a [BenchmarkJob](../../concepts/serving/benchmarks.md) against the cluster's endpoint.

</div>

## How factors change the weights

OME counts the replicas that can serve on a cluster: the lower of its admitted and its ready replicas. It multiplies the count by the cluster's factor, 1 unless you set one, and reduces the results across clusters to their smallest whole-number ratio.

For example, `chat` has four replicas on `worker-a` and two on `worker-b`, whose replicas each serve three times as many requests:

| Cluster | Replicas that can serve | Factor | Replicas × factor | Weight |
| --- | --- | --- | --- | --- |
| `worker-a` | 4 | 1, the default | 4 | 2 |
| `worker-b` | 2 | 3 | 6 | 3 |

`worker-b` gets three of every five requests with a third of the replicas. Without the factor, the weights would be 2 and 1.

- A factor counts per replica, so a cluster's share still moves as it scales or as its replicas stop being ready.
- Only the ratios between factors matter: `worker-a: "2"` with `worker-b: "6"` gives the same weights as `worker-b: "3"` alone.
- Factors change only the weights. OME ignores them when it places the InferenceService, in mode `Split` too: to change where replicas run, change `spec.placement`.
- A capacity poll, a routing health probe or a drain can lower a cluster's weight, or hold it at 0, whatever its factor. See [How OME sets the weights](../../concepts/rollouts-and-traffic/traffic-map.md#how-ome-sets-the-weights).

## Step 1: Declare capacity factors

OME doesn't derive factors from the hardware: you choose them. Work out a cluster's factor as what one of its replicas serves divided by what one replica serves on your baseline cluster. If a `worker-a` replica sustains 20 requests per second at your latency target and a `worker-b` replica sustains 60, `worker-b`'s factor is 3. A slower cluster gets a factor below 1, such as `"0.5"`. Set factors only for the clusters that differ from the baseline.

Add [`spec.routing.capacityFactors`](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-RoutingSpec) to the InferenceService on the control-plane cluster. Its keys are WorkloadCluster names, as `kubectl get workloadclusters` lists them. Its values are positive, quoted Kubernetes quantities, such as `"3"`, `"1.5"` or `"500m"`. The control-plane cluster accepts 0 or a negative value, but OME then stops updating the TrafficMap, as [The weights don't change](#the-weights-dont-change) shows.

```yaml title="chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  model:
    name: llama-3-1-8b-instruct
  runtime:
    name: srt-llama-3-1-8b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 4
  placement:
    mode: All
    clusterSelector: "metadata.name in (worker-a,worker-b)"
  routing:
    capacityFactors:
      worker-b: "3"
```

In mode `All`, each workload cluster scales on its own, so `worker-a` can run four replicas while `worker-b` runs two.

```bash
kubectl apply -f chat.yaml
```

```output
inferenceservice.ome.io/chat configured
```

OME rebuilds the TrafficMap when the InferenceService changes. Check that it has built the map from your change: the map's `spec.observedISVCGeneration` equals the InferenceService's `metadata.generation`.

```bash
kubectl get inferenceservice chat -n prod -o jsonpath='{.metadata.generation}{"\n"}'
```

```output
6
```

```bash
kubectl get trafficmap chat -n prod -o jsonpath='{.spec.observedISVCGeneration}{"\n"}'
```

```output
6
```

If the map's number stays lower, OME has stopped updating the map: see [The weights don't change](#the-weights-dont-change).

## Step 2: Check the weights

List the entries' replica counts, factors and weights:

```bash
kubectl get trafficmap chat -n prod \
  -o jsonpath='{range .spec.entries[*]}{.cluster}{" allocated="}{.capacity.allocated}{" ready="}{.capacity.ready}{" factor="}{.capacity.factor}{" weight="}{.weight}{"\n"}{end}'
```

```output
worker-a allocated=4 ready=4 factor= weight=2
worker-b allocated=2 ready=2 factor=3 weight=3
```

- `allocated` and `ready` are the admitted and ready replicas. An empty value means 0.
- `factor` is the factor you set, in canonical form: `"1.5"` shows as `1500m`. It's empty for `worker-a`, which counts as 1.
- `weight` is the cluster's share, relative to the other entries.

If a weight is 0, or isn't what you expect, compare the entry with [Why a cluster's weight is zero](../../concepts/rollouts-and-traffic/traffic-map.md#why-a-clusters-weight-is-zero).

If you [publish a global endpoint](publish-a-global-endpoint.md), check that the publisher has written these weights with the map's [staleness checks](../../concepts/rollouts-and-traffic/traffic-map.md#staleness-checks).

## Move from the deprecated field

`spec.placement.capacityFactors` is a deprecated alias of `spec.routing.capacityFactors`, with the same form and meaning. Move the map to `spec.routing` in one change, because the API server rejects an InferenceService that sets both:

```diff
 spec:
   placement:
     mode: All
     clusterSelector: "metadata.name in (worker-a,worker-b)"
-    capacityFactors:
-      worker-b: "3"
+  routing:
+    capacityFactors:
+      worker-b: "3"
```

If you manage the InferenceService with `kubectl apply`, apply the edited file: kubectl removes the old field in the same update that adds the new one.

## Troubleshooting

### The weights don't change

After you apply a factor, the weights stay as they were, and the map's `spec.observedISVCGeneration` stays below the InferenceService's `metadata.generation` in [Step 1's check](#step-1-declare-capacity-factors). OME has stopped updating the map, for example because a factor in `spec.routing.capacityFactors` is 0 or negative.

With the `kubectl ome` plugin, `kubectl ome placement explain chat -n prod` shows [`Routing intent`](../../reference/kubectl-ome/placement.md#explain-output-fields) as `Invalid` when a factor is the cause. The command reads alpha features.

The error message shows only in the manager's log. Read every replica's log:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'validate routing policy'
```

The log shows an error such as `validate routing policy: spec.routing.capacityFactors["worker-b"] must be positive, got "0"`. While it lasts, the TrafficMap and your gateway's route keep their last weights, and the map's `Published` condition turns `False` with reason `InvalidOwner`.

Set a positive factor, or remove the key, and apply the file again. OME then builds the map from the fixed spec. For the other errors that stop the map, see [When the map stops updating](../../concepts/rollouts-and-traffic/traffic-map.md#where-it-comes-from).

### A cluster's factor has no effect

Find the cluster's entry in [Step 2](#step-2-check-the-weights):

| What the entry shows | Cause and fix |
| --- | --- |
| Empty `factor` | The key doesn't match the WorkloadCluster's name. OME accepts any key, so compare yours with `kubectl get workloadclusters`. |
| The only entry, with weight 1 | The InferenceService serves from one workload cluster, which gets all the traffic whatever its factor. |
| Weight 0 | A drain, a probe gate or no ready replicas. See [Why a cluster's weight is zero](../../concepts/rollouts-and-traffic/traffic-map.md#why-a-clusters-weight-is-zero). |

### The API server rejects the InferenceService

`kubectl apply` fails with an error that ends with `spec.routing.capacityFactors and deprecated spec.placement.capacityFactors must not both be set`. The InferenceService sets both fields. Remove `spec.placement.capacityFactors`, and keep the factors in `spec.routing.capacityFactors`.

## Clean up

To go back to plain replica counts, remove `spec.routing.capacityFactors` from `chat.yaml`, and apply the file again.

## Next steps

- [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md): the inputs of a weight, and how to tell whether the table is current.
- [Tune routing capacity polling](tune-routing-capacity-polling.md): lower a cluster's count to the capacity that it reports.
- [Configure routing health probes](routing-health-probes.md): take a cluster's traffic away when its endpoint fails.
- [Drain a workload cluster](drain-a-workload-cluster.md): hold a cluster at weight 0 for maintenance.
- [Publish a global endpoint](publish-a-global-endpoint.md): write the weights into your global gateway.
