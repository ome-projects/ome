---
title: Configure pod batching
description: Pace OMENative scale-up and scale-down work to reduce bursts of API requests while keeping each Instance's pods in the same batch.
since: v1.3
---

Use smaller pod batches when large scale changes put pressure on your Kubernetes API server. [OMENative](../../concepts/omenative/overview.md) applies the budgets to each component: it creates missing pods in batches and limits how much scale-down work it advances at once. A multi-pod [Instance](../../concepts/omenative/instances.md), such as a leader and its workers, stays together in either direction.

<div class="prerequisites" markdown>

- OME built from `main` and [installed from source](../../getting-started/install.md#install-from-source), using the `ome-resources` chart as release `ome` in namespace `ome`. These settings are part of the unreleased v1.3 work; v1.2.2 doesn't support them.
- The same source checkout and values file used for that installation, plus Helm, `kubectl` and `jq`.
- Permission to upgrade the release and read its ConfigMap, Deployment and pod logs.
- An InferenceService with an OMENative component whose normal scaling you can observe. [Serve a model on OMENative](../omenative/serve-a-model-on-omenative.md) creates one.

</div>

## Step 1: Choose the budgets

All three settings live under `ome.controller.lifecycle` in the chart's values:

| Setting | Chart default | What it controls |
| --- | --- | --- |
| `scaleUpPodBatchSize` | `100` | Missing pods selected for creation during one pass over a component. |
| `scaleDownPodBatchSize` | `100` | Pod-equivalent cost selected for draining, deletion and cleanup. An Instance costs its live and Terminating pods, or 1 when its pods are gone but cleanup remains. |
| `scaleDownRequeueInterval` | `5s` | How often OME checks scale-down work while it is still in progress. Watches also trigger checks. |

Lower a budget to reduce the size of each batch; raise it when batches are limiting progress and the API server has room for more work. These are per-component budgets, so several components can advance at once. They don't set a cluster-wide requests-per-second limit; see [Tune reconcile throughput](configure-the-controller.md#tune-reconcile-throughput) for the manager's API rate limit.

OME selects all the missing pods of an Instance together during scale-up, and keeps an Instance together during scale-down. Once the next Instance doesn't fit in a batch, later ones wait too. If the first eligible Instance exceeds the entire budget, it proceeds alone. For example, a 16-pod Instance still progresses with a budget of 8; the budget doesn't split its gang. During scale-down, OME resumes existing deletion work before admitting another batch, including when you reduce the desired count again.

Scale-down batching also applies when an InferenceReplica is being deleted. Changing these budgets doesn't change desired replica counts, [rollout availability budgets](../../concepts/architecture/omenative-update-strategies.md), or [readiness deadlines](../omenative/set-instance-readiness-deadlines.md).

## Step 2: Apply the settings

Merge the following into your existing values file to try smaller batches, keeping its other settings, including your image registry and tags:

```yaml title="values.yaml (excerpt)"
ome:
  controller:
    lifecycle:
      scaleUpPodBatchSize: 32
      scaleDownPodBatchSize: 32
      scaleDownRequeueInterval: 5s
```

From the checkout used to install OME, upgrade the release:

```bash
helm upgrade ome ./charts/ome-resources --namespace ome -f values.yaml
```

With Helm 4, add `--server-side=false`, as in [Install from source](../../getting-started/install.md#install-from-source). Helm reports the release's upgrade. The chart changes the `lifecycle` entry in the `inferenceservice-config` ConfigMap and automatically rolls the manager pods.

These three values are read only at manager startup. Wait for all replicas to finish restarting:

```bash
kubectl rollout status deployment/ome-controller-manager -n ome --timeout=5m
```

Continue once the command reports that the Deployment successfully rolled out. Then check the stored values:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.lifecycle}' |
  jq '{scaleUpPodBatchSize, scaleDownPodBatchSize, scaleDownRequeueInterval}'
```

Confirm that the two budgets are `32` and the interval is `"5s"`. If you change the ConfigMap directly instead of upgrading Helm, restart the manager yourself with `kubectl rollout restart deployment/ome-controller-manager -n ome`. A later Helm upgrade can replace that direct edit.

## Step 3: Observe a scale change

During your next scale-down, read the manager's batch summaries:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=500 |
  grep 'OMENative scale-down wave'
```

Each matching line identifies the `namespace`, `isvc` and `component`, followed by `podBudget`, `activePodCost`, `activeInstances`, `admittedInstances` and `deferredInstances`. No matching lines means no batch summary appears in the log window you read.

When `activePodCost` stays near the budget and `deferredInstances` remains high, the component has more work waiting behind the batch. Check that the active Instances are completing their drains and deletions before raising the budget. If the same pods remain Terminating, follow [Recover stuck deletions](../omenative/recover-stuck-deletions.md); a larger batch won't unblock those pods.

If you [collect controller metrics](metrics.md), these series show scale-down progress:

| Metric | What to watch |
| --- | --- |
| `ome_omenative_scale_down_active_pods` | Active cost, by namespace, InferenceService and component. |
| `ome_omenative_scale_down_deferred_instances` | Instances still waiting outside the active batch, with the same labels. |
| `ome_omenative_scale_down_batch_pods` | Histogram of admitted batch cost, by component. |
| `ome_omenative_scale_down_oversized_batch_total` | Batches that admitted one Instance larger than the budget, by component. An increase can be expected for large gangs. |

Scale-up has no dedicated batch metric. Follow the Instances' `Creating` and `Ready` phases with [`kubectl ome instance list`](../../reference/kubectl-ome/instance.md#list), or inspect the InferenceReplica's status. Newly created pods don't need to become Ready before the next batch can start, so the scale-up budget doesn't cap all starting pods across the component.

`scaleDownRequeueInterval` is a fallback check interval. Pod and other resource events can advance work sooner, and configured [force-delete and teardown deadlines](../omenative/recover-stuck-deletions.md) still wake the controller when due. This interval doesn't pace scale-up.

## Troubleshooting

### New manager pods fail to start

The manager rejects a zero or negative budget, or a malformed, zero or negative interval, with `Failed to initialize lifecycle scale configuration` in its startup log. Correct the value in your values file, upgrade again, and wait for the Deployment rollout.

The binary supplies no fallback batch sizes when a key is absent from the ConfigMap. An omitted budget leaves that direction unbounded; an omitted interval disables only periodic scale-down checks. Removing a key from your Helm values file restores the chart's default, because Helm merges your values with the chart's values. Use positive values to tune the budgets.

### A Deployment or LeaderWorkerSet scales as before

These settings apply only to OMENative components. RawDeployment and the deprecated MultiNode mode use their own workload controllers. Check the component's [deployment mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode).

## Next steps

- [Configure the controller](configure-the-controller.md): adjust reconcile concurrency and API request limits.
- [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md): control how an update replaces serving pods.
- [Collect metrics](metrics.md): scrape the manager and follow scale-down progress over time.
