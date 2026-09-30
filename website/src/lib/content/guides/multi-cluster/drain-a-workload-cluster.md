---
title: Drain a workload cluster
description: Use the alpha kubectl ome traffic drain to take a workload cluster out of an InferenceService's traffic for maintenance, and undrain to give it back.
status: preview
since: v1.3
---

Drain a workload cluster to move an [InferenceService](../../concepts/serving/inference-services.md)'s traffic off it, for example during maintenance, while its replicas there keep running. OME holds the drained cluster at weight 0 in the service's [TrafficMap](../../concepts/rollouts-and-traffic/traffic-map.md), and the other clusters take its share. Start a drain with `kubectl ome traffic drain`, and end it with `kubectl ome traffic undrain`. A drain applies to one InferenceService: to take a cluster out of every service's traffic, drain each service. Multi-cluster routing is alpha and still in development, and so are both commands. Routing is off by default.

<div class="prerequisites" markdown>

- A control-plane cluster with routing on, and `kubectl` access to it. Routing needs the `ome-resources` values `ome.multicluster.enabled=true` and `ome.multicluster.role=control-plane`, which [Controller manager flags](../../reference/operate-ome/controller-manager-flags.md#multi-cluster) describes, and `ome.multicluster.config.routing.enabled=true`, which [Turn on routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing) shows. The examples use its kubeconfig context, `hub`, as the current context.
- The InferenceService `chat` in the namespace `prod`, placed on the WorkloadClusters `worker-a` and `worker-b`: its `spec.placement.mode` is `All` or `Split`, not the default `Single`. Check that it has a TrafficMap with `kubectl get trafficmap chat -n prod`. If not, routing is off or skips the service: see [Turn on routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing).
- A global endpoint that publishes `chat`'s TrafficMap, as [Publish a global endpoint](publish-a-global-endpoint.md) sets up. Without one, a drain changes only the TrafficMap.
- The `kubectl ome` plugin on your `PATH`. See [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md).
- Permission to `get` and `patch` `inferenceservices.ome.io` in `prod`, and to `get` `trafficmaps.ome.io`. Check with `kubectl auth can-i patch inferenceservices.ome.io -n prod`.

</div>

## How a drain is recorded

`kubectl ome traffic drain` adds an entry, keyed by a drain ID that you choose, to the `ome.io/traffic-drain` annotation on the InferenceService in the control-plane cluster:

```yaml
metadata:
  annotations:
    ome.io/traffic-drain: '{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"}}'
```

OME gives the drained cluster's [TrafficMap entry](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-TrafficMapEntry) weight 0, and lists the drain's ID in its `drainRefs`. The other clusters keep their weights, and share the traffic in the same ratio as before. Several drains can hold one cluster, and it gets its traffic back when the last one is removed. A drain stays until you undrain it.

## Step 1: Drain the cluster

First check that another cluster can take the traffic:

```bash
kubectl get trafficmap chat -n prod \
  -o jsonpath='{range .spec.entries[*]}{.cluster}{" weight="}{.weight}{" drainRefs="}{.drainRefs}{"\n"}{end}'
```

Both clusters have the same capacity, so each has weight 1:

```output
worker-a weight=1 drainRefs=
worker-b weight=1 drainRefs=
```

!!! danger "Draining the last serving cluster cuts off traffic"
    If `worker-a` is the only entry with a positive weight, the drain takes all of `chat`'s traffic away. See [Routable is False with reason TrafficDrain](#routable-is-false-with-reason-trafficdrain).

`kubectl ome traffic drain` takes three required flags:

| Flag | What to pass |
| --- | --- |
| `--workload-cluster` | The WorkloadCluster to drain. The global `--cluster` flag picks a cluster from your kubeconfig instead. |
| `--id` | A name for the drain, in lowercase letters, digits and hyphens. `undrain` takes it later. |
| `--reason` | Why you drain the cluster. Anyone who can read the service sees it, so keep secrets out of it. |

Drain `worker-a`. To try it first, add `--dry-run=server`: the API server checks the change and your permission, and stores nothing.

```bash
kubectl ome traffic drain chat -n prod \
  --workload-cluster worker-a \
  --id maintenance-a \
  --reason "planned maintenance"
```

The command prints a preview to stderr. Check that its `Cluster` row names one of the entries you listed, since the command accepts any valid name, then type `y` and press Enter:

```output
ALPHA guarded traffic action (not TrafficMap convergence)
FIELD             VALUE
Action            traffic drain
Context           hub
Workload NS       prod
Target            InferenceService/chat
UID               2f6c1d9e-4b7a-4c1e-9f3d-8a5b6c7d0e12
ResourceVersion   1284711
Dry-run           none
Override ID       maintenance-a
Cluster           worker-a
Reason            "planned maintenance"
Override count    0 -> 1
Annotation        ome.io/traffic-drain
Follow-up         kubectl ome traffic status chat -n prod --context=hub
API acceptance is not TrafficMap convergence or data-plane realization.
The exact InferenceService UID/resourceVersion is tested atomically.
Other annotations and traffic-drain override IDs are preserved.
Run the previewed traffic status command to observe controller evidence.
Confirm this exact action? [y/N] y
```

In a script, pass `--yes` to skip the prompt. The result goes to stdout:

```output
FIELD         VALUE
action        traffic drain
target        InferenceService/prod/chat
dry-run       none
accepted      Yes
applied       Yes
override-id   maintenance-a
cluster       worker-a
overrides     0 -> 1
message       API accepted traffic annotation request; not TrafficM...
follow-up     kubectl ome traffic status chat -n prod --context=hub
hint          Use -o json or -o yaml for full values.
```

## Step 2: Check the drain

`applied` `Yes` means that the API server stored the drain, and OME then applies it to the TrafficMap. The result suggests `kubectl ome traffic status`, but that command doesn't show drains, so list the TrafficMap's entries:

```bash
kubectl get trafficmap chat -n prod \
  -o jsonpath='{range .spec.entries[*]}{.cluster}{" weight="}{.weight}{" drainRefs="}{.drainRefs}{"\n"}{end}'
```

```output
worker-a weight=0 drainRefs=["maintenance-a"]
worker-b weight=1 drainRefs=
```

`worker-a` is held at 0, and `worker-b` gets all the traffic. Then check the TrafficMap's [conditions](../../concepts/rollouts-and-traffic/traffic-map.md#conditions):

```bash
kubectl get trafficmap chat -n prod
```

```output
NAME   MODE   PUBLISHED   ROUTABLE   OVERRIDE   REASON     AGE
chat   All    True        True       True       Routable   3h
```

`OVERRIDE` is `True` because the drain holds an entry, and `ROUTABLE` is `True` because `worker-b` can still serve. This `PUBLISHED` can still be `True` from before the drain. Before you start the maintenance, wait until `kubectl ome get trafficmap chat -n prod` shows `PUBLISHED` `True`. That command shows `True` only once the publisher has written the new weights to your gateway's route. If it stays `Unknown` or `False`, see [Check the publication](publish-a-global-endpoint.md#step-2-check-the-publication). Clients that call `worker-a`'s own endpoint, not the global one, still reach it.

To see every drain on the service, with its reason, read the annotation:

```bash
kubectl get inferenceservice chat -n prod -o jsonpath='{.metadata.annotations.ome\.io/traffic-drain}{"\n"}'
```

```output
{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"}}
```

## Step 3: Undrain the cluster

When the maintenance is done, remove the drain by its ID:

```bash
kubectl ome traffic undrain chat -n prod --id maintenance-a
```

Check the drain's cluster and reason in the preview, and type `y`. The result shows `applied` `Yes` and `overrides` `1 -> 0`:

```output
FIELD         VALUE
action        traffic undrain
target        InferenceService/prod/chat
dry-run       none
accepted      Yes
applied       Yes
override-id   maintenance-a
cluster       worker-a
overrides     1 -> 0
message       API accepted traffic annotation request; not TrafficM...
follow-up     kubectl ome traffic status chat -n prod --context=hub
hint          Use -o json or -o yaml for full values.
```

OME then sets `worker-a`'s weight from its capacity and health again. If the maintenance left it with fewer ready replicas, its weight stays lower, or at 0, until they're ready: see [Why a cluster's weight is zero](../../concepts/rollouts-and-traffic/traffic-map.md#why-a-clusters-weight-is-zero). List the entries again:

```bash
kubectl get trafficmap chat -n prod \
  -o jsonpath='{range .spec.entries[*]}{.cluster}{" weight="}{.weight}{" drainRefs="}{.drainRefs}{"\n"}{end}'
```

```output
worker-a weight=1 drainRefs=
worker-b weight=1 drainRefs=
```

## Troubleshooting

The commands print errors to stderr as `error: <message>`. The reference lists their [exit codes](../../reference/kubectl-ome/traffic.md#exit-codes).

### The command refuses to run

These are the messages you're most likely to see. [Refusals](../../reference/kubectl-ome/traffic.md#drain-and-undrain-refusals) lists the rest.

| Message | Cause and fix |
| --- | --- |
| `traffic drain requires --workload-cluster; --cluster selects the Kubernetes API cluster` | Name the cluster to drain with `--workload-cluster`, not `--cluster`. |
| `required Kubernetes API request failed; check access and connectivity` | The service isn't in that namespace, you lack permission, or the API server is unreachable. After the preview, see [The command fails after you confirm](#the-outcome-is-unknown). |
| `action refused: placement sources and derived services cannot be mutated` | The target is placement's copy on a workload cluster. Switch to the control-plane cluster's context. |
| `traffic action refused: target is not eligible for cross-cluster traffic routing` | Routing skips the service: its `spec.placement` has no `requirements` or `clusterSelector`. |
| `traffic drain refused: override ID already exists` | The service already has a drain with this ID. Choose another ID, or undrain it first. |
| `traffic undrain refused: override ID does not exist` | The service has no drain with this ID. List its drains as in [Step 2](#step-2-check-the-drain). |
| `action not confirmed; noninteractive input requires --yes` | Answer `y` at the prompt. In a script, pass `--yes`. |
| `context deadline exceeded` | The command has 45 seconds in all, including the prompt. Run it again, or, if it came after you confirmed, see [The command fails after you confirm](#the-outcome-is-unknown). |

### The patch is rejected as stale

The command fails with this message and exit code `3`:

```output
error: guarded annotation patch rejected; refresh traffic status and retry explicitly
```

`chat` changed after the command read it, often through a status update while the prompt waited, so the API server applied nothing. Run the command again to see a fresh preview.

### The command fails after you confirm {#the-outcome-is-unknown}

When the command fails after you confirm, or after the preview when you pass `--yes`, the API server may still have stored the change. This applies to a message that ends in `check traffic status`, and to `required Kubernetes API request failed; check access and connectivity` or `context deadline exceeded`. To see whether it did, list the service's drains, as in [Step 2](#step-2-check-the-drain). Running the command again is also safe. If the first one went through, a repeated drain fails with `traffic drain refused: override ID already exists`. A repeated undrain fails with `traffic undrain refused: override ID does not exist`.

### The drain stays pending

In Step 2, no entry lists the drain in `drainRefs`. `OVERRIDE` is `False` too, unless another drain holds an entry. The drain names a cluster that has no entry in the TrafficMap, usually because of a typo. Undrain it, then drain the right cluster: until you do, it holds any cluster with that name that later serves the service.

### Routable is False with reason TrafficDrain

The drains hold every cluster that had a positive weight, so the gateway has nowhere to send `chat`'s requests. You drained them all, or the others were already at 0, for example with no ready replicas. Undrain a cluster that can serve, or fix the others.

### The TrafficMap stops changing after a hand edit

Nothing validates a hand edit of `ome.io/traffic-drain`. When the value is invalid, OME keeps the last valid TrafficMap, so drained clusters stay drained. Capacity and health changes stop reaching the TrafficMap too, and `kubectl ome traffic drain` and `undrain` refuse with `traffic action refused: existing traffic-drain annotation is malformed or unsafe`. OME logs the error:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'parse ome.io/traffic-drain'
```

Each line holds the parser's error after `parse ome.io/traffic-drain:`. Fix the value with `kubectl annotate --overwrite`, in the form that [How a drain is recorded](#how-a-drain-is-recorded) shows, with IDs, clusters and reasons that `kubectl ome traffic drain` accepts. Removing the annotation also works, but it ends every drain on the service.

## Next steps

- [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md): how OME sets each cluster's weight, and what the conditions mean.
- [kubectl ome traffic](../../reference/kubectl-ome/traffic.md): every flag of `drain`, `undrain`, `status` and `explain`.
- [Guarded actions](../../reference/kubectl-ome/guarded-actions.md): the preview and confirmation that mutating `kubectl ome` commands share.
