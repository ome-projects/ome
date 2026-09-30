---
title: Set Instance readiness deadlines
description: Bound how long OMENative waits for a new Instance to become Ready with instanceReadyTimeout and the stuck-pod and unschedulable grace periods, and see what OME does when one runs out.
since: v1.3
---

On [OMENative](../../concepts/omenative/overview.md) components, each attempt to create, update or restart an [Instance](../../concepts/omenative/instances.md) has a deadline to become Ready. A bad image, a crashing server or a pod that no node can take then ends in a `Failed` Instance that says why, instead of a wait with no end. The deadline acts on the whole Instance: one pod, or a leader and its workers.

The `ome-resources` chart sets all three limits by default. You'll give one component more time, watch its Instance against the deadline, and change the cluster defaults.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed with the `ome-resources` chart, and `kubectl` access that lets you create namespaces and InferenceServices. See [Install OME](../../getting-started/install.md).
- The ClusterBaseModel `qwen3-0-6b` in the `Ready` state, and the ClusterServingRuntime `srt-qwen3-0-6b`, from [Serve your first model](../../getting-started/serve-your-first-model.md).
- A node where the model is `Ready`, with a free NVIDIA GPU, 10 CPUs and 30 GiB of memory for the engine pod. If the InferenceService from Serve your first model still holds the GPU, delete it with `kubectl delete inferenceservice qwen3-0-6b -n qwen3-0-6b`, and keep the model and the runtime.
- The [kubectl ome](../../reference/kubectl-ome/overview.md) plugin, for Step 2.
- For [Change the cluster defaults](#change-the-cluster-defaults): access to the `ome` Helm release, and `jq`.

</div>

## How the deadlines work

| Setting | What it does | Where you set it | Chart default |
| --- | --- | --- | --- |
| `instanceReadyTimeout` | Time for an attempt to become Ready, including scheduling, the image pull and the server start. With none set anywhere, a stuck Instance waits for you. | A component's `lifecycle`, or the cluster | `30m` |
| `stuckPodGracePeriod` | Fails the attempt early when a container or init container is stuck, as in `ImagePullBackOff`, and its pod is older than this. Unset, only the deadline fails it. | The cluster | `60s` |
| `unschedulableGracePeriod` | Fails the attempt when the scheduler can't place a pod for this long. Unset, the pod waits until you free capacity. | The cluster | `15m` |

A component's value in the InferenceService wins over the runtime's, and either wins over the cluster's. The same timeout sets the deadline of an [Instance migration](../../concepts/omenative/migration-and-transient-scale.md#deadline), and a [drain](recover-stuck-deletions.md) that runs past it raises `DrainOverdue`. OME also derives a multi-pod Instance's [gang schedule timeout](../../concepts/serving/gang-scheduling.md#bound-the-gang-schedule-timeout) from it.

The clock stops while an attempt waits on something outside the Instance: a scheduling gate, a quota refusal, an unschedulable pod, an unreachable node or a [paused rollout](../roll-out-changes/pause-and-resume-a-rollout.md#what-a-pause-holds). When the wait ends, the attempt gets a full timeout again. A changed timeout applies to new attempts, and to running ones that have no deadline yet. A changed grace period applies at once. [Operations](../../concepts/omenative/instances.md#operations) has the details.

## Step 1: Give a component more time

Create a namespace for the example:

```bash
kubectl create namespace qwen3-deadlines
```

```output
namespace/qwen3-deadlines created
```

This InferenceService runs the engine on OMENative and gives it 45 minutes, to leave room for a first image pull on a new node:

```yaml title="qwen3-0-6b-deadlines.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-deadlines
spec:
  deploymentMode: OMENative
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 1
    maxReplicas: 1
    lifecycle:
      instanceReadyTimeout: 45m
```

Apply it:

```bash
kubectl apply -f qwen3-0-6b-deadlines.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b created
```

To set a default for every InferenceService that uses a runtime, put the same `lifecycle` block under the runtime's `engineConfig`, `decoderConfig` or `routerConfig`.

Once OME has created the engine's InferenceReplica, `qwen3-0-6b-engine`, check the timeout that applies:

```bash
kubectl get inferencereplica qwen3-0-6b-engine -n qwen3-deadlines \
  -o jsonpath='{.status.conditions[?(@.type=="InstanceReadyTimeoutUnconfigured")].message}{"\n"}'
```

```output
Instance readiness deadline is 45m0s
```

Without a timeout, the condition is `True`, and its message starts with `no instanceReadyTimeout is set`: see [The condition says no timeout is set](#the-condition-says-no-timeout-is-set). OME creates an InferenceReplica only for components on OMENative. If kubectl can't find it, see [Opt in to OMENative](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative).

## Step 2: Watch the Instance against its deadline

Right after you apply the InferenceService, OME starts a Create operation for its Instance. Show the Instance in the wide format, which includes the operation's times:

```bash
kubectl ome instance status qwen3-0-6b 0 --component engine -n qwen3-deadlines -o wide
```

These rows show the attempt and its deadline:

| Row | What it shows |
| --- | --- |
| `phase` | `Creating` while the Instance starts, then `Ready`, or `Failed` if a limit runs out. |
| `operation started`, `operation deadline` | When the attempt began, and when it fails if it isn't Ready, in UTC. Here they're 45 minutes apart. While the clock is stopped, the deadline shows `-`. |
| `op hold` | What the attempt waits on, after `waiting=`. |
| `failure pod`, `failure reason`, `failure time` | Which pod failed, why and when. |

Once the Instance is Ready, the operation rows are gone.

### When a limit runs out {#when-a-deadline-runs-out}

OME marks the Instance `Failed`, and the `failure reason` row says why:

| Reason | What it means | What to do |
| --- | --- | --- |
| `ImagePullBackOff`, `ErrImagePull`, `InvalidImageName` | The image can't be pulled. | Fix the image, and publish a new revision. |
| `CreateContainerConfigError` | A Secret or ConfigMap that the container needs is missing, or its configuration is broken. | Fix the configuration, and publish a new revision. |
| `CrashLoopBackOff`, `CreateContainerError`, `RunContainerError` | The container can't start, or keeps exiting. | Read the logs. With a `60s` `stuckPodGracePeriod`, one crash after the pod's first minute fails the attempt. Raise it for a server that needs a restart or two. |
| `ContainersNotReady` | The pod runs, but its containers never passed their readiness checks. | Read the logs. If the model needs longer to load, raise `instanceReadyTimeout`. |
| `ReadinessGateNotSatisfied` | The containers are ready, but a readiness gate keeps the pod out of its Service. | Run `kubectl describe pod` to see which gate stays `False`. |
| `Unschedulable` | The scheduler couldn't place the pod within `unschedulableGracePeriod`. | Free capacity, or change the spec. |
| `DeadlineExceeded` | The deadline passed with none of the evidence above. | Read the logs. If the start needs longer, raise `instanceReadyTimeout`. |

What OME does next depends on the cause and the operation:

- Only an image or configuration error counts against the revision. OME retries the revision with backoff, then holds it until you publish a fixed one or [release it](../roll-out-changes/release-a-held-revision.md).
- A failed Create or single-pod update then starts a new attempt. If the revision isn't at fault and the pods all ran on one node, OME can [build it on another node](../../concepts/omenative/migration-and-transient-scale.md#automatic-relocation), up to 3 times with the chart.
- When a multi-pod update fails under the default SurgeThenDrain strategy, OME removes the replacement, and the Instance goes back to `Ready` on its old revision. Under `RecreatePod` or an in-place strategy, the Instance stays `Failed` until a new revision reaches it, and a reset skips it.
- A failed restart leaves the Instance `Failed`, with its pods, until you [reset it](reset-failed-instances.md) or a new revision reaches it.

[What OME does with a failed Instance](../../concepts/omenative/instances.md#what-ome-does-with-a-failed-instance) has the details. OME also records an `InstanceFailed` Warning event on the InferenceService. List those events:

```bash
kubectl get events -n qwen3-deadlines --field-selector reason=InstanceFailed
```

While no Instance has failed, kubectl prints:

```output
No resources found in qwen3-deadlines namespace.
```

Each message names the Instance, its pod and the reason, and often what OME did next.

## Change the cluster defaults

Set the cluster values under `ome.controller.lifecycle`, in the values file you install `ome-resources` with:

```yaml title="values.yaml"
ome:
  controller:
    lifecycle:
      instanceReadyTimeout: 30m
      stuckPodGracePeriod: 2m
      unschedulableGracePeriod: 15m
```

Give each duration a unit, as in `90s` or `1h`. A bare number, like `30`, stops new controller pods from starting: see [The controller doesn't start after you change the settings](#the-controller-doesnt-start-after-you-change-the-settings). To turn a setting off, set it to `null`.

With no timeout at all, a migration request fails as soon as OME accepts it, with `deadline exceeded in phase Accepted: surge never allocated`. This is a known bug, so keep a timeout set while you use migrations.

Keep your other values in the file: `helm upgrade` resets the values that aren't in the file to the chart's defaults. Upgrade with the chart version that you already run, which `helm list -n ome` shows in its `CHART` column:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm reports that the release has been upgraded, and the controller's pods restart with the new settings. Wait for the rollout:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

```output
deployment "ome-controller-manager" successfully rolled out
```

Then read the settings back from the `inferenceservice-config` ConfigMap:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.lifecycle}' \
  | jq '{instanceReadyTimeout, stuckPodGracePeriod, unschedulableGracePeriod}'
```

```output
{
  "instanceReadyTimeout": "30m",
  "stuckPodGracePeriod": "2m",
  "unschedulableGracePeriod": "15m"
}
```

A setting you turned off shows as `null`. A kustomize install sets only `instanceReadyTimeout`. To edit the ConfigMap directly instead, see [Change ConfigMap settings](../operate-ome/configure-the-controller.md#tune-the-config-cache).

## Troubleshooting

### The condition says no timeout is set

The `InstanceReadyTimeoutUnconfigured` condition is `True`, though you set a timeout. Check the places OME reads:

- In the InferenceService, the value goes under the component, `spec.engine.lifecycle`, not under `spec.lifecycle`.
- A `0s` in the InferenceService counts as unset, and hides the runtime's value.
- In the cluster settings, the value needs a unit and must be above zero. OME treats `"30"`, `0s` and `-5m` as unset.

To read the cluster settings, use the ConfigMap check in [Change the cluster defaults](#change-the-cluster-defaults).

### The Instance is stuck and never fails

An Instance stays in `Creating`, or another phase, past its deadline. Check its rows, as in [Step 2](#step-2-watch-the-instance-against-its-deadline):

| What you see | What to do |
| --- | --- |
| `operation deadline` is `-`, with an `op hold` row | Clear the wait it names: free quota or capacity, bring back the node, or resume the rollout. An unschedulable pod fails the attempt after `unschedulableGracePeriod`. |
| `operation deadline` is `-`, and `admitted` is `false` | A pod still has scheduling gates. The clock starts when Kueue, or whichever controller added them, removes them. |
| `operation deadline` is `-`, with no `op hold` row, and `admitted` is `true` | Check the condition, as in [Step 1](#step-1-give-a-component-more-time). If it names a timeout, the rollout is paused, which stops every Instance's clock until you [resume it](../roll-out-changes/pause-and-resume-a-rollout.md#step-3-resume-the-rollout). |
| `operation surge index` shows a number | The wait can be on the Instance at that index. Check its `op hold` and `admitted` rows. |
| All the Instance's pods serve traffic | Nothing: OME doesn't fail an Instance while all its pods serve. |

### The controller doesn't start after you change the settings

After a `helm upgrade`, the rollout doesn't finish, and the new controller pods crash-loop. Read the logs of every replica:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1
```

A pod that failed to start logs `Failed to initialize lifecycle scale configuration`, with an error that contains `unable to parse lifecycle config json`. The `lifecycle` entry isn't valid JSON, or a duration in it is a bare number. Give each duration a unit, and upgrade again. Until then, the replicas that were already running ignore the whole `lifecycle` entry, so only a component's own `instanceReadyTimeout` applies.

## Clean up

Delete the InferenceService and its namespace:

```bash
kubectl delete inferenceservice qwen3-0-6b -n qwen3-deadlines
kubectl delete namespace qwen3-deadlines
```

```output
inferenceservice.ome.io "qwen3-0-6b" deleted from qwen3-deadlines namespace
namespace "qwen3-deadlines" deleted
```

If you changed the chart values, put back the ones you want to keep in your values file, and upgrade the release again with the same `--version`.

## Next steps

- [Reset failed Instances](reset-failed-instances.md): start over an Instance that failed during a create or a restart.
- [Release a held revision](../roll-out-changes/release-a-held-revision.md): let OME try a revision again after it's held.
- [Instances](../../concepts/omenative/instances.md): the phases and operations of an Instance.
- [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md): find why an InferenceService doesn't serve.
