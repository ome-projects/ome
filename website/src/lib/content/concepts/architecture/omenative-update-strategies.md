---
title: OMENative update strategies
description: "Choose how an OMENative rollout replaces an Instance's pods, by starting new ones first, recreating them or updating them in place, and pace it with partition, maxSurge and maxUnavailable."
since: v1.3
---

When the pod template of an [OMENative](../omenative/overview.md) component changes, OME records a new [revision](deployment-modes.md#what-ome-creates) and moves each of the component's [Instances](../omenative/instances.md) onto it. The component's `lifecycle.updateStrategy` sets how OME replaces an Instance's pods, and how many Instances move at once. By default, the new pods come up and serve before the old ones drain, even for a leader and its workers.

A new image in the [InferenceService](../serving/inference-services.md) changes the pod template. So can an edit to its [serving runtime](../runtimes/serving-runtimes.md), unless the InferenceService [pins the runtime](../runtimes/runtime-revisions.md). A RawDeployment component uses `deploymentStrategy` instead, and [rollout groups](../rollouts-and-traffic/rollout-groups.md) in `spec.rollout` coordinate several components.

## The four strategies

Set the type in the component's `lifecycle.updateStrategy`, for example `spec.engine.lifecycle.updateStrategy.type`. [UpdateStrategy](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-UpdateStrategy) lists every field.

| Strategy | How OME replaces an Instance's pods | Choose it when |
| --- | --- | --- |
| `SurgeThenDrain` | The default. Starts new pods next to the old ones, and drains the old pods once the new ones are ready. The Instance keeps serving. | The cluster has spare accelerators for the extra pods. |
| `RecreatePod` | Drains and deletes the old pods, then creates new ones. The Instance serves nothing until they're ready. | The cluster has no room for extra pods. |
| `InPlaceIfPossible` | Changes the images in a single-pod Instance's running pod, which leaves rotation while its containers restart. Recreates the pods for any other change. | Most changes are image updates, and the cluster has no room for extra pods. |
| `InPlaceOnly` | Changes the images in a single-pod Instance's running pod, as `InPlaceIfPossible` does. Refuses any other change. | Single-pod Instances must keep their node and `emptyDir` volumes, and you'd rather stop the rollout than recreate a pod. |

A multi-pod Instance always moves as a whole. A change to its worker pods alone rolls the leader too, and the in-place strategies recreate its pods.

This version of the InferenceService from [Opt in to OMENative](deployment-modes.md#opt-in-to-omenative) runs two engine Instances and updates them in place when it can:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-3b-instruct
  runtime:
    name: srt-llama-3-2-3b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 2
    lifecycle:
      updateStrategy:
        type: InPlaceIfPossible
```

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-3b-instruct configured
```

Changing the strategy doesn't start a rollout. Instance updates that start after the change use it.

### SurgeThenDrain

For a single-pod Instance, the new pod takes the Instance's other ordinal: `llama-3-2-3b-instruct-engine-0-default-1` replaces `llama-3-2-3b-instruct-engine-0-default-0`. If the new pod fails, the old one keeps serving while OME tries the update again. For a leader and its workers, OME starts a whole replacement Instance at a new [index](../omenative/instances.md#index), so the indices can have gaps. If the replacement fails, OME deletes it, records a `GangSurgeAbandoned` warning event, and the old Instance keeps serving.

When OME drains the old pods, their `preStop` hook and `terminationGracePeriodSeconds` cover the requests they're still serving.

While the extra pods can't be scheduled, the old pods keep serving. After [`unschedulableGracePeriod`](../../guides/omenative/set-instance-readiness-deadlines.md), 15 minutes with the chart, OME fails the update and tries it again later.

### In-place updates

When only the images of regular containers change, `InPlaceIfPossible` and `InPlaceOnly` update a single-pod Instance in place. OME takes the pod out of rotation, sets the new images, and puts the pod back once its restarted containers are ready. The pod keeps its name, its node and its `emptyDir` volumes. For any other change, such as an environment variable, a resource request or an init container's image, `InPlaceIfPossible` recreates the pods.

A label that you add to the pod template reaches the pod only when OME recreates it. With `inPlaceUpdateStrategy.markNotReadyDuringLifecycle: false`, the pod stays in rotation while its containers restart, so requests to it can fail. See [InPlaceUpdateStrategy](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InPlaceUpdateStrategy).

`InPlaceOnly` refuses any other change. The Instance stays on its old revision, the rollout stalls, and OME records an `InPlaceUpdateNotPossible` warning event on the InferenceService. For example, after you change an environment variable of an engine that runs `InPlaceOnly`, list the refusals:

```bash
kubectl get events -n llama-demo --field-selector reason=InPlaceUpdateNotPossible \
  -o custom-columns=KIND:.involvedObject.kind,NAME:.involvedObject.name,MESSAGE:.message
```

```output
KIND               NAME                    MESSAGE
InferenceService   llama-3-2-3b-instruct   OMENative component=engine instance=0 rejected update: InPlaceOnly strategy but diff exceeds container images
```

To go ahead, change the type, for example to `InPlaceIfPossible`, or undo the change.

## Pacing with rollingUpdate

`lifecycle.updateStrategy.rollingUpdate` limits how many Instances move at once, and can hold some on the old revision:

```yaml
spec:
  engine:
    lifecycle:
      updateStrategy:
        type: InPlaceIfPossible
        rollingUpdate:
          partition: 1
          maxUnavailable: 1
```

| Field | What it does | Default with the chart |
| --- | --- | --- |
| `maxSurge` | Read by `SurgeThenDrain`: how many Instances can have extra pods at once. | `25%` |
| `maxUnavailable` | Read by the other strategies: how many Instances can be out of service at once. Under any strategy, it also caps [crash-loop repairs](../omenative/instance-restart-policy.md#the-crash-loop-repair). | `25%`, or none with `SurgeThenDrain` |
| `partition` | How many Instances stay on the old revision, lowest indices first, so you can try a change on the others. | None |
| `lifecycle.minReadySeconds` | How long a new pod must stay ready before it counts as available, and before `SurgeThenDrain` drains the old pod. See [Pace rollouts with minReadySeconds](../../guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md). | None |

A budget is a whole number, or a percentage of the component's Instances rounded up: `25%` of two Instances is one. When nothing sets the budget that the strategy reads, every Instance can move at once, and with `RecreatePod` that takes the whole component offline. A [manifest install](../../guides/operate-ome/move-to-the-helm-charts.md) sets no default budgets, so set them in the InferenceService or the runtime. In a [rollout group](../rollouts-and-traffic/rollout-groups.md), the group's limits apply too, and a canary's current step replaces your `partition`. See [RollingUpdate](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-RollingUpdate).

!!! warning "A zero budget stalls the rollout"
    When the budget that the strategy reads is `0`, as with `maxUnavailable: 0` and `InPlaceIfPossible`, no Instance moves. The admission webhook rejects an InferenceService that sets both budgets to `0`, but a zero on just one of them, or in the runtime, gets through.

With the settings above and two Instances, the next change to the pod template moves one Instance and holds the other. Once the moved Instance is ready, the component's [InferenceReplica](../omenative/overview.md#how-omenative-runs-a-component), which OME writes from the InferenceService, reports `Staged` in its `Ready` condition:

```bash
kubectl get inferencereplica llama-3-2-3b-instruct-engine -n llama-demo \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}'
```

```output
Staged at partition 1: 1/2 Instances on llama-3-2-3b-instruct-engine-7b4c2e19, 1 held on the prior revision
```

To finish the rollout, set `partition` to `0` or remove it. See [Conditions](deployment-modes.md#conditions).

## Where the defaults come from

OME fills in each field of a component's update strategy from the first of these that sets it:

1. The component in the InferenceService, such as `spec.engine.lifecycle.updateStrategy`.
2. The runtime's `engineConfig`, `decoderConfig` or `routerConfig`, such as `spec.engineConfig.lifecycle.updateStrategy`. OME merges it field by field, so an InferenceService that sets only the type keeps the runtime's budgets.
3. The operator default for the component, from the chart's `ome.controller.updateStrategy`. It covers `type`, `maxSurge` and `maxUnavailable`.
4. `SurgeThenDrain` for the type.

To give every InferenceService that uses a runtime the same strategy, set it in the runtime:

```yaml
spec:
  engineConfig:
    lifecycle:
      updateStrategy:
        type: RecreatePod
        rollingUpdate:
          maxUnavailable: 1
```

The `ome-resources` chart sets these defaults for the `engine`, and the same ones for the `router` and `decoder`:

```yaml title="values.yaml"
ome:
  controller:
    updateStrategy:
      engine:
        type: SurgeThenDrain
        maxSurge: 25%
        maxUnavailable: 25%
```

OME fills in only the budget that the strategy reads. The InferenceService in [The four strategies](#the-four-strategies) sets only the type, so its engine gets `maxUnavailable` from the chart:

```bash
kubectl get inferencereplica llama-3-2-3b-instruct-engine -n llama-demo \
  -o jsonpath='{.spec.lifecycle.updateStrategy}'
```

```output
{"rollingUpdate":{"maxUnavailable":"25%"},"type":"InPlaceIfPossible"}
```

An invalid default, such as a zero budget, stops the manager from starting. See [The manager rollout doesn't finish](../../guides/operate-ome/configure-the-controller.md#the-manager-rollout-doesnt-finish).

OME keeps the revisions that a component uses and, with the chart, its 10 most recent other revisions. The chart's `ome.controller.lifecycle.revisionHistoryLimit` sets that number, and the InferenceService's `ome.io/revision-history-limit` annotation overrides it. With neither set, OME keeps every revision.

## When a rollout doesn't move

While no Instance of a component can move, the component's rollout hold names the gate that stops it. For an engine that runs `InPlaceIfPossible` with `maxUnavailable: 0`:

```bash
kubectl get inferenceservice llama-3-2-3b-instruct -n llama-demo \
  -o jsonpath='{.status.components.engine.lifecycle.rolloutHold}'
```

```output
{"gate":"Budget","reason":"per-Component unavailability budget 0 exhausted (would become 1)","since":"2026-09-27T08:14:02Z","target":"llama-3-2-3b-instruct-engine-7b4c2e19"}
```

| What you see | Cause | What to do |
| --- | --- | --- |
| Gate `Budget` | The budget that the strategy reads is `0`, or other Instances already use it up. | Raise that budget. |
| New pods stay `Pending` | The cluster has no room for the extra pods of a [SurgeThenDrain](#surgethendrain) update. | Free capacity, or switch to `RecreatePod`. |
| `InPlaceUpdateNotPossible` events | `InPlaceOnly` refused a change beyond container images. | Change the type, or undo the change. |
| Gate `RetryBlock` | An update failed, for example because a leader and its workers weren't ready within `lifecycle.instanceReadyTimeout`. OME tries the revision again after a backoff. | Fix the cause. |
| Gate `Held` | The revision itself kept failing, for example on an image that can't be pulled, and used up its attempts, three with the chart. | Change the pod template, or [release the revision](../../guides/roll-out-changes/release-a-held-revision.md). |

[RolloutHold](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-RolloutHold) lists the gates that rollout groups add.

## Changing the strategy mid-rollout

An Instance that's already moving finishes with the strategy it started with, which its [`operation.strategy`](../omenative/instances.md#operations) records. Every other Instance uses the new strategy when it next moves, including one that `InPlaceOnly` refused or whose update failed. Changing the strategy doesn't reset the revision's retries.

## Next steps

- [Instances](../omenative/instances.md): pod names, indices, and the phases an update moves an Instance through.
- [Rollout groups](../rollouts-and-traffic/rollout-groups.md): roll several components out together.
- [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md): stop rollout work and continue it later.
- [Release a held revision](../../guides/roll-out-changes/release-a-held-revision.md): retry a revision that OME stopped retrying.
