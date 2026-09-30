---
title: Instances
description: "An Instance is one replica of an OMENative component, a single pod or a leader and its workers, whose identity, phase, operation and failures OME records on the component's InferenceReplica."
since: v1.3
---

An Instance is one replica of an OMENative component: a single pod, or a leader pod and its workers. OME creates, updates, repairs, moves and removes each Instance as one unit. On the component's InferenceReplica, it reports the Instances' phases, revisions, pod counts and current operations, so you can see what each one is doing and why one failed. The autoscaler, `minReplicas` and `maxReplicas` count Instances, not pods.

By default, an update brings an Instance's new pods up and serving before it drains the old ones, even when the Instance is a multi-node gang. A revision that keeps failing for a reason of its own is retried with backoff and then held. Readiness deadlines, restart policies, migration off a node, rollout pauses, scale-down and teardown also act on whole Instances.

## What an Instance is

A single-pod Instance runs one pod. When the component has a `leader` and a `worker`, each Instance runs a leader pod and `worker.size` worker pods, which share one phase, one revision and one operation. When the scheduler-plugins PodGroup CRD is installed, OME also creates a PodGroup for each multi-pod Instance, so that a gang-aware scheduler places all its pods or none. [Gang scheduling](../serving/gang-scheduling.md#bring-a-gang-aware-scheduler) explains how to choose one, such as the alpha [OME scheduler](../scheduling/ome-scheduler.md).

OME writes a component's InferenceReplica, named `{isvc}-{component}`, from the InferenceService, and records the Instances in its status. You change the component in the InferenceService, and get or describe the InferenceReplica to see what OME is doing.

For example, this InferenceService serves the model and runtime from [Serve your first model](../../getting-started/serve-your-first-model.md) on OMENative, with two engine Instances:

```yaml title="qwen3-0-6b-isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-0-6b
spec:
  deploymentMode: OMENative
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 2
    maxReplicas: 2
```

OME creates the InferenceReplica `qwen3-0-6b-engine` and two single-pod Instances, 0 and 1. Pod names follow the pattern `{isvc}-{component}-{index}-{runner}-{ordinal}`, so the Instances run `qwen3-0-6b-engine-0-default-0` and `qwen3-0-6b-engine-1-default-0`. The ordinal numbers a runner's pods from `0`. When the default [SurgeThenDrain](../architecture/omenative-update-strategies.md#surgethendrain) strategy updates a single-pod Instance, the new pod takes the other ordinal, so after one update, Instance 0 runs `qwen3-0-6b-engine-0-default-1`.

### Index

An Instance keeps its index for life, in its pod names, its `ome.io/instance-index` label and `OME_INSTANCE_INDEX`. A new Instance takes the lowest free index. A migration, or a SurgeThenDrain update of a multi-pod Instance, starts the replacement at a new index and then removes the old Instance, so the indices can have gaps.

On scale-down, OME first keeps the Instances that a migration or a multi-pod surge is handing over, with their replacements. It fills the remaining places with `Ready` Instances before the others, lowest indices first, and removes the rest.

### Incarnation

A new Instance starts at incarnation `1`, and so does the replacement in a migration or a multi-pod surge. A restart raises the incarnation by one, and so does an update that deletes the pods and creates them again under the same names. It stays the same through an in-place update, a SurgeThenDrain update of a single-pod Instance, and the rebuild of a failed or `Pending` Instance. The pods carry it in the `ome.io/instance-incarnation` label, which tells a recreated pod from the one it replaced.

### Runners

Each pod belongs to a runner, the part it plays in its Instance: `default` in a single-pod Instance, or `leader` and `worker` in a multi-pod one. The `ome.io/runner` label names it. Through `OME_*` environment variables, a pod learns its runner and position and, in a multi-pod Instance, the leader's address, its rank and the pod count. [Pod labels and environment](../architecture/deployment-modes.md#pod-labels-and-environment) lists them.

## Instance status {#where-instance-status-lives}

OME keeps a status row for each Instance, an [OMENativeInstanceStatus](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-OMENativeInstanceStatus), on the component's InferenceReplica only. The InferenceService gets the component's totals, under `status.components.{component}.lifecycle`: see [Lifecycle status](../architecture/deployment-modes.md#lifecycle-status). List the example's InferenceReplica:

```bash
kubectl get inferencereplica qwen3-0-6b-engine -n qwen3-0-6b
```

```output
NAME                COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
qwen3-0-6b-engine   engine      2         2         2       2           5m
```

`DESIRED` is the number of Instances the component should run, `CURRENT` the number with a status row, in any phase, and `READY` and `AVAILABLE` the counts from [Readiness and availability](#readiness-and-availability). The short name `irep` works too.

[kubectl ome instance](../../reference/kubectl-ome/instance.md) reads the rows: `list` prints a line for each Instance, and `status` shows one Instance with its operation, last failure, pods and warning events. With the chart, OME stores the rows in the compact `ColumnarV2` encoding when that's smaller than a plain list, and the command reads either: see [Change the status encoding](../../guides/omenative/change-the-status-encoding.md).

## Instance phases

An Instance's phase says what it's doing. In `Creating`, `Updating`, `Restarting`, `Migrating` and `Deleting`, an operation is in flight. `Ready`, `Pending` and `Failed` are resting phases. A `Failed` Instance can still hold the operation that failed.

| Phase | What it means | What comes next |
| --- | --- | --- |
| `Creating` | OME is creating the pods of a new, `Pending` or failed Instance, or of the replacement in a migration or a multi-pod surge. | `Ready`, or `Failed` |
| `Ready` | The last operation finished with every pod ready, and nothing is in flight. From then on, the pod counts track readiness, not the phase. | `Updating`, `Restarting`, `Migrating`, `Deleting` or `Pending` |
| `Updating` | OME is moving the Instance to a new revision, as its [update strategy](../architecture/omenative-update-strategies.md) says. | `Ready` on the new revision, or `Failed` |
| `Restarting` | OME is recreating all the pods at the next incarnation, after a crash loop or, under `RecreateInstanceOnPodRestart`, when a pod restarts, fails or goes missing. See [Instance restart policy](instance-restart-policy.md). | `Ready`, or `Failed` |
| `Migrating` | The Instance keeps serving while its replacement starts on other nodes. See [Migration and transient scale](migration-and-transient-scale.md). | Removed once the replacement is `Ready`. If the migration fails, `Ready`, or `Failed` when its pods aren't all ready. |
| `Pending` | A `Ready` Instance lost all its pods. OME uses this phase only under the `None` restart policy, the default for single-pod Instances. | `Creating` |
| `Failed` | An operation on the Instance failed: see [When an Instance fails](#when-an-instance-fails). | `Creating`, `Ready` or `Updating`: see [What OME does with a failed Instance](#what-ome-does-with-a-failed-instance). |
| `Deleting` | The component is scaling down, and OME is draining and deleting the pods. | Removed once its pods are gone |

A `Deleting` Instance whose drain runs past its deadline doesn't fail. OME sets the InferenceReplica's `DrainOverdue` condition and records one `DrainOverdue` warning event: see [Recover stuck deletions](../../guides/omenative/recover-stuck-deletions.md).

## Readiness and availability

OME counts an Instance's pods in three states, and the InferenceReplica counts the Instances that have enough pods in each:

| State | A pod counts when | Instance count |
| --- | --- | --- |
| Ready | All its containers are ready: its `ContainersReady` condition is `True`. | `readyReplicas` |
| Serving | It's ready, and OME has set its `ome.io/serving` readiness gate to `True`. | `servingReplicas` |
| Available | It's listed on the component's headless Service and isn't terminating, and, when `minReadySeconds` is more than `0`, its `Ready` condition has been `True` for at least that long. | `availableReplicas` |

An Instance counts toward a state when at least its desired number of pods do, so it still counts during a surge. The `maxUnavailable` budget of the [update strategy](../architecture/omenative-update-strategies.md#pacing-with-rollingupdate) and the InferenceReplica's `Ready` condition count serving Instances.

The headless Service lists pods as soon as they have an IP address, so that a leader and its workers can find each other while they start. With `minReadySeconds` at `0`, `availableReplicas` can therefore be higher than `readyReplicas` while pods start.

OME controls rotation with the `ome.io/serving` readiness gate: a pod is `Ready`, and in rotation, only while the gate is `True`. OME sets the gate once the containers of all the Instance's pods are ready, so a gang enters rotation together. It marks the Instance `Ready` once all its pods have been `Ready` for `lifecycle.minReadySeconds`, `0` by default. It clears the gate before it drains a pod or, by default, updates one in place. [Ready versus available](../../guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md#ready-versus-available) explains how `minReadySeconds` paces rollouts.

## Operations

Before OME creates, deletes or replaces an Instance's pods, it records the work as the Instance's `operation`, with a type and a step, and clears it when the work finishes. The record is in the InferenceReplica's status, so a restarted manager carries the work on from the step it reached. [InstanceOperation](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InstanceOperation) lists its fields. The types and their steps are:

| Type | Steps |
| --- | --- |
| `Create` | `CreatePods` |
| `Update` | `InPlace`, `Drain`, `Surge`, `SurgeDrain`, `GangSurgeTarget` or `GangSurgeTargetCleanup` |
| `Restart` | `Drain` |
| `Migrate` | `CreateSurge`, on the source and its replacement |
| `Delete` | `Drain` |

In an update, `InPlace` updates the containers in their pods, `Drain` recreates the pods, and the steps with `Surge` in their names belong to a SurgeThenDrain update.

An operation's deadline is its start time plus the component's `lifecycle.instanceReadyTimeout`. OME takes the timeout from the InferenceService, then the runtime, then the operator's settings, where the chart sets `30m`. Left unset everywhere, operations get no deadline, so a stuck Instance waits for you, and the InferenceReplica's `InstanceReadyTimeoutUnconfigured` condition is `True`. [Set Instance readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md) shows how to set it.

While something outside the Instance holds an operation, such as a scheduling gate that Kueue sets until it admits the workload, OME stops the clock. When the hold ends, the operation gets a full window again. The operation's `waiting` field names most holds:

| `waiting` | What holds the operation |
| --- | --- |
| `QuotaExceeded` | The API server refused the pods for lack of ResourceQuota. |
| `Unschedulable` | The scheduler can't place a pod. |
| `PodGroupTerminating` | The Instance's PodGroup from an earlier attempt is still being deleted. |
| `NodeUnknown` | A pod that OME must replace is on a node that stopped reporting. |
| `Paused` | The rollout is [paused](../../guides/roll-out-changes/pause-and-resume-a-rollout.md). |
| `SourceUnrouted` | Nothing: during a surge, the old Instance left rotation before OME drained it. The clock keeps running. |

## When an Instance fails

OME moves an Instance to `Failed` when an operation can't finish. The chart sets the three limits under `ome.controller.lifecycle`:

| Cause | When it fails the Instance | Setting (chart value) |
| --- | --- | --- |
| Stuck pod | A container waits in a state such as `CrashLoopBackOff` or `ImagePullBackOff`, and its pod is older than the grace period. | `stuckPodGracePeriod` (`60s`) |
| Deadline | The deadline passes before the Instance is ready, as when a large image pulls slowly or the server never passes its readiness checks. | `instanceReadyTimeout` (`30m`) |
| No room | The scheduler can't place a pod for longer than the grace period. OME treats this as a problem with the cluster, not the revision. | `unschedulableGracePeriod` (`15m`) |
| Rejected pods | The API server rejects the pods as invalid, or because the namespace is being deleted. | None |
| PodGroup name in use | Another controller holds the PodGroup name the Instance needs. | None |

Without `stuckPodGracePeriod`, only the deadline catches a stuck pod. Without `unschedulableGracePeriod`, an Instance waits for room as long as it takes. OME never fails an Instance whose pods all serve, at the desired count.

While OME creates or restarts an Instance, it deletes and recreates any pod that has ended, such as one the kubelet evicted, until the operation's deadline. It records a `TerminalPodRecycled` warning event each time.

OME records the most recent failure in the Instance's `lastFailure`, with the pod, container, reason, exit code and message. That includes a recycled pod or a restart, and the record stays after the Instance recovers. While an Instance is `Failed`, the InferenceReplica's `Ready` condition has the reason `InstanceFailed`, and `RolloutStalled` is `True` when the failure came during a rollout: see [Conditions](../architecture/deployment-modes.md#conditions).

### What OME does with a failed Instance

What happens next depends on the operation that failed:

| Operation that failed | What happens next |
| --- | --- |
| Create, or update of a single-pod Instance | OME clears the operation and retries when the backoff allows. An Instance that already ran the target revision stays `Failed` until its pods are ready, and OME replaces lost ones. |
| Update of a multi-pod Instance under SurgeThenDrain, the default | OME deletes the replacement Instance, returns the Instance to `Ready` on its old revision, and records a `GangSurgeAbandoned` warning event. The retry backoff decides when the update tries again. |
| Restart, or update of a multi-pod Instance under `RecreatePod`, `InPlaceIfPossible` or `InPlaceOnly` | The Instance keeps its operation. OME retries failed updates at once with new pods. A failed restart leaves it `Failed` until its pods are ready, and OME replaces lost ones. |
| Migration | OME ends the migration, removes the replacement and clears the operation on both Instances. See [Migration and transient scale](migration-and-transient-scale.md). |

The chart's `ome.controller.lifecycle.updateRetry` sets the retry backoff. When the revision caused the failure, as with an image that can't be pulled, the revision gets three attempts, the second after one minute and the third after two more. Then OME holds it until a new revision replaces it or you [release it](../../guides/roll-out-changes/release-a-held-revision.md). Without `updateRetry`, the first such failure holds it. A crash loop never counts toward a hold, since a broken node or GPU can cause one too.

When a failure didn't come from the revision, and the attempt's pods all ran on one node, OME steers the next attempt off that node. The moves need `autoMigrate.maxAttempts` in the operator's settings, where the chart allows three for each Instance. A component's `lifecycle.migrationPolicy.mode: Never` turns them off. [When a deadline runs out](../../guides/omenative/set-instance-readiness-deadlines.md#when-a-deadline-runs-out) has the details.

To start a failed Instance over once you've fixed the cause, set the `ome.io/reset-instances` annotation on its InferenceReplica, as [Reset failed Instances](../../guides/omenative/reset-failed-instances.md) describes. The reset skips an Instance that still holds its failed `Update` operation.

## Next steps

- [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md): run an InferenceService on OMENative and check its Instances.
- [OMENative overview](overview.md): what OMENative manages for a component.
- [Instance restart policy](instance-restart-policy.md): when OME restarts a whole Instance.
- [Migration and transient scale](migration-and-transient-scale.md): move an Instance to other nodes, or add Instances for a while.
- [OMENative update strategies](../architecture/omenative-update-strategies.md): how an update replaces an Instance's pods.
- [Set Instance readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md): bound how long an operation may take.
- [Reset failed Instances](../../guides/omenative/reset-failed-instances.md): start failed Instances over.
