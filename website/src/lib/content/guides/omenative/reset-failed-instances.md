---
title: Reset failed Instances
description: Rebuild the OMENative Instances that OME marked Failed, once you've fixed the cause, with the ome.io/reset-instances annotation.
since: v1.3
---

A reset rebuilds the [Instances](../../concepts/omenative/instances.md) of an [OMENative](../../concepts/architecture/deployment-modes.md#omenative) component that OME has marked `Failed`. OME deletes an Instance's pods, whether one pod or a leader and its workers, and builds it again from the component's target revision. You request a reset with the `ome.io/reset-instances` annotation, once you've fixed the cause. The examples reset Instance 0 of the `engine` component of the [InferenceService](../../concepts/serving/inference-services.md) `qwen3-0-6b`, in the namespace `qwen3-native`.

<div class="prerequisites" markdown>

- OME v1.3 or later.
- An InferenceService with an OMENative component that has a `Failed` Instance. The examples use the one from [Serve a model on OMENative](serve-a-model-on-omenative.md).
- The kubectl ome plugin, and the permissions that its read commands need. See [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md).
- `get` and `patch` on `inferencereplicas` in the `ome.io` API group, and read access to pods, pod logs and events, in the InferenceService's namespace.

</div>

## When to reset

OME marks an Instance `Failed` when it can't create or restart it, and leaves the pods in place so that you can find the cause. [When an Instance fails](../../concepts/omenative/instances.md#when-an-instance-fails) lists the causes. Once you know the cause, pick the fix:

| Situation | What to do |
| --- | --- |
| The cause is in the InferenceService or its runtime, such as a wrong image tag | Fix it there. The new revision replaces the `Failed` Instances along with the others. |
| You fixed a registry or a Secret | Wait. The kubelet retries, and once the pods are ready, OME sets the Instance back to `Ready`. Reset it if it stays `Failed`. |
| You fixed a node or another cause outside the spec | Reset the Instance. |
| `kubectl ome instance retry-blocks qwen3-0-6b --component engine -n qwen3-native` shows `HELD` | [Release the held revision](../roll-out-changes/release-a-held-revision.md) too, with the alpha `release-held` action. A reset's rebuild waits until you do. |

OME repairs `Ready` Instances on its own, as the component's [Instance restart policy](../../concepts/omenative/instance-restart-policy.md) sets. It also rebuilds some failed Instances without a reset, as [What OME does with a failed Instance](../../concepts/omenative/instances.md#what-ome-does-with-a-failed-instance) describes.

## Step 1: Find the failed Instances {#step-1-find-the-parked-instances}

List the Instances of the InferenceService:

```bash
kubectl ome instance list qwen3-0-6b -n qwen3-native
```

A failed Instance shows `Failed` in the `PHASE` column. In the `AOF` column, `F` means that OME recorded a failure, and `O` that the Instance still holds an operation.

Show one of them, here Instance 0 of the engine. `-o wide` prints each field on its own row:

```bash
kubectl ome instance status qwen3-0-6b 0 --component engine -n qwen3-native -o wide
```

The `failure reason`, `failure pod` and `failure container` rows name the last failure, such as `ImagePullBackOff`. [Output fields](../../reference/kubectl-ome/instance.md#status-output-fields) describes the other rows.

The pods are still there, so `kubectl describe pod` and `kubectl logs` show their events and output. Find the cause, and fix it as [When to reset](#when-to-reset) describes.

## Step 2: Request the reset

!!! warning "A reset deletes the pods"
    Save the logs and events that you need first. Once the pods are gone, `kubectl logs` can't read them.

OME creates an InferenceReplica for each OMENative component, named `<InferenceService>-<component>`, so the engine's is `qwen3-0-6b-engine`. Set the `ome.io/reset-instances` annotation on it to the index of the Instance to reset:

```bash
kubectl annotate inferencereplica qwen3-0-6b-engine -n qwen3-native \
  ome.io/reset-instances=0
```

```output
inferencereplica.ome.io/qwen3-0-6b-engine annotated
```

The value is an index, a comma-separated list of indices, or `all` for every `Failed` Instance of the component. On the InferenceService, the annotation does nothing. To reset another component's Instances, annotate its InferenceReplica instead.

OME answers a request once. It resets the Instances that it can, records an event, and deletes the annotation, even when it resets nothing. An Instance that fails later needs a new request.

Check that OME has answered:

```bash
kubectl get inferencereplica qwen3-0-6b-engine -n qwen3-native \
  -o jsonpath='{.metadata.annotations.ome\.io/reset-instances}'
```

The command prints nothing once OME has deleted the annotation. If it still prints the value, see [The annotation stays](#the-annotation-stays).

## Step 3: Watch the outcome

OME records an `InstancesReset` event on the InferenceService for the Instances that it resets:

```bash
kubectl get events -n qwen3-native --field-selector reason=InstancesReset \
  -o custom-columns=TYPE:.type,OBJECT:.involvedObject.name,MESSAGE:.message
```

```output
TYPE     OBJECT       MESSAGE
Normal   qwen3-0-6b   InferenceReplica qwen3-native/qwen3-0-6b-engine component=engine: reset instance(s) 0 at operator request (ome.io/reset-instances annotation): pods deleted and preserved operation cleared; the lifecycle passes rebuild them
```

When OME skips the Instance or rejects the request, it records another event instead: see [Troubleshooting](#troubleshooting).

Then watch the Instance:

```bash
kubectl ome instance list qwen3-0-6b -n qwen3-native
```

The Instance stays `Failed`, with no `O` in the `AOF` column, until OME starts to rebuild it. Its phase then becomes `Creating`, and `Ready` once the pods are ready. If it stays `Failed`, see [The Instance stays Failed after the reset](#the-instance-stays-failed-after-the-reset).

## Troubleshooting

### kubectl can't find the InferenceReplica

`kubectl annotate` fails with `Error from server (NotFound): inferencereplicas.ome.io "qwen3-0-6b-engine" not found` when the name is wrong. List the InferenceReplicas with `kubectl get inferencereplicas -n qwen3-native`. Only OMENative components have one: a component that runs as a Deployment, or in the `MultiNode` mode (deprecated), has no Instances to reset. To move a `MultiNode` component to OMENative, see [Move from LeaderWorkerSet to OMENative](move-from-leaderworkerset.md).

### The annotation stays

The request is still waiting. Check that the OME manager is running, then look for errors on the InferenceReplica in the logs of every manager replica:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 \
  | grep 'Reconciler error' | grep 'qwen3-0-6b-engine'
```

Each line's error names its cause. An error that contains `consume reset-instances request:` comes from the reset itself, and OME tries the request again. While a request waits, `kubectl annotate` refuses a new value unless you pass `--overwrite`, which replaces the waiting request.

### The reset skips an Instance

List the skips:

```bash
kubectl get events -n qwen3-native --field-selector reason=InstancesResetSkipped \
  -o custom-columns=TYPE:.type,OBJECT:.involvedObject.name,MESSAGE:.message
```

```output
TYPE     OBJECT       MESSAGE
Normal   qwen3-0-6b   InferenceReplica qwen3-native/qwen3-0-6b-engine component=engine: reset skipped for instance(s) 0 (still serving)
```

The reason follows each skipped index:

| Reason | What to do |
| --- | --- |
| `still serving` | A reset leaves an Instance alone while any of its pods serves. Request it again once no `pod[N] serving ready` row in the [Step 1](#step-1-find-the-parked-instances) status is `True`. |
| `owned by Update`, `owned by Migrate` or `owned by Delete` | The rollout, migration or deletion that started the operation handles the Instance. If the rollout's revision is held, [release it](../roll-out-changes/release-a-held-revision.md). |
| `nothing to reset` | The Instance is already waiting for its rebuild: see [The Instance stays Failed after the reset](#the-instance-stays-failed-after-the-reset). If its pods stay `Terminating`, see [Recover stuck deletions](recover-stuck-deletions.md). |
| `Phase=<phase>` | The Instance is in another phase, and needs no reset. |
| `no such instance` | Check the index with `kubectl ome instance list`. |

For `all`, when no Instance is `Failed`, the message ends `none is Failed; nothing to reset`.

### The request is rejected

For any value other than `all` or a list of indices, OME resets nothing and records an `InstancesResetRejected` warning that quotes the value:

```bash
kubectl get events -n qwen3-native --field-selector reason=InstancesResetRejected \
  -o custom-columns=TYPE:.type,OBJECT:.involvedObject.name,MESSAGE:.message
```

```output
TYPE      OBJECT       MESSAGE
Warning   qwen3-0-6b   InferenceReplica qwen3-native/qwen3-0-6b-engine component=engine: ome.io/reset-instances="All" rejected: "All" is not a non-negative instance index; nothing reset
```

The value is case-sensitive. Correct it, and annotate the InferenceReplica again.

### The Instance stays Failed after the reset

When the reset event names the Instance but it stays `Failed` with no pods, a retry block or a pause holds the rebuild.

Show the retry blocks with `kubectl ome instance retry-blocks qwen3-0-6b --component engine -n qwen3-native`. The rebuild waits until the time in the `NEXT` column for a `BACKOFF` block, and until another Instance's attempt at the revision ends for a `RUNNING` block. For a `HELD` block, it waits until you [release the revision](../roll-out-changes/release-a-held-revision.md).

A paused rollout holds the rebuild too. `kubectl get inferenceservice qwen3-0-6b -n qwen3-native -o jsonpath='{.metadata.annotations.ome\.io/rollout-paused}'` prints `true` or `freeze` while the rollout is paused. To resume it, see [Pause and resume a rollout](../roll-out-changes/pause-and-resume-a-rollout.md).

### The Instance fails again

The cause remains, or it's in the spec: see [When to reset](#when-to-reset), and read the new failure as in [Step 1](#step-1-find-the-parked-instances). A failure that the revision causes, such as `ImagePullBackOff`, counts against the revision and can leave it held, as [How a revision gets held](../roll-out-changes/release-a-held-revision.md#how-holds-and-releases-work) describes.

## Next steps

- [Instances](../../concepts/omenative/instances.md): the phases, operations and incarnations of an Instance.
- [Set Instance readiness deadlines](set-instance-readiness-deadlines.md): when OME fails an Instance that doesn't become ready.
- [Instance restart policy](../../concepts/omenative/instance-restart-policy.md): how OME repairs `Ready` Instances on its own.
- [Release a held revision](../roll-out-changes/release-a-held-revision.md): let OME retry a held revision, with the alpha `release-held` action.
- [kubectl ome instance](../../reference/kubectl-ome/instance.md): every flag of `list`, `status` and `retry-blocks`.
