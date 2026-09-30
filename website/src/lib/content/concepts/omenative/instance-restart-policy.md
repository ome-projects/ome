---
title: Instance restart policy
description: "Choose whether OMENative repairs only a failed pod or rebuilds its whole Instance, and see how a restart runs."
since: v1.3
---

When a pod of a ready [Instance](instances.md) fails, its component's `lifecycle.restartPolicy` decides what OME repairs: only that pod, or the whole Instance. By default, OME rebuilds a multi-pod Instance whole, so that a leader and its workers come back as one group. For a single-pod Instance, it repairs only the failed pod. Only [OMENative](overview.md) components read the field.

## The two policies

| Policy | When a pod of a `Ready` Instance fails | Default for |
| --- | --- | --- |
| `None` | OME repairs only that pod: it replaces a pod that's deleted or `Failed`. | Single-pod Instances, including routers |
| `RecreateInstanceOnPodRestart` | OME drains and deletes all the Instance's pods, then creates them again at the next incarnation. | Multi-pod Instances |

The policy is separate from the pod's own `restartPolicy`, which is `Always` by default, so the kubelet restarts a crashed container in place under either policy.

The pods of a multi-pod Instance run one copy of the model as a single process group, and a restart of any of them breaks the group. A single-pod Instance has no group to break, and a restart in place is the quicker repair.

Set `RecreateInstanceOnPodRestart` on a single-pod component when a crashed engine should start over in a new pod, which runs its init containers again and gets fresh `emptyDir` volumes. Set `None` on a multi-pod component only if its engine can take a restarted or replaced pod back into a running group.

## Set the policy

The engine in [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md) has one pod, so its default policy is `None`. This version of the guide's InferenceService rebuilds the engine's Instance in a new pod when its container restarts after the Instance is ready:

```yaml title="qwen3-0-6b-native.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-native
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
      restartPolicy: RecreateInstanceOnPodRestart
```

If you still run the guide's InferenceService, apply the file:

```bash
kubectl apply -f qwen3-0-6b-native.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

Changing the policy doesn't start a rollout. OME uses the new value from then on, and a restart already under way still finishes.

OME takes `lifecycle.restartPolicy` from the first of these that has one:

1. The component in the InferenceService: `spec.engine`, `spec.decoder` or `spec.router`.
2. The component's config in the serving runtime: `spec.engineConfig`, `spec.decoderConfig` or `spec.routerConfig`.
3. The default for the Instance's shape, from the table above.

There's no cluster-wide setting. A runtime sets the policy for the InferenceServices that use it and don't set their own:

```yaml
spec:
  engineConfig:
    lifecycle:
      restartPolicy: None
```

[LifecycleSpec](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-LifecycleSpec) lists the other lifecycle fields.

## Choose a policy per component

Each component takes its own policy. This prefill-decode InferenceService serves `llama-3-2-1b-instruct` with the runtime `srt-llama-3-2-1b-instruct-pd`, both from OME's catalog. The runtime needs a GPU node with RDMA networking for each engine and decoder pod: see [PD mode](../../reference/operate-ome/ome-serving-values.md#pd-mode).

```yaml title="llama-pd.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-pd
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  runtime:
    name: srt-llama-3-2-1b-instruct-pd
  engine:
    minReplicas: 2
    maxReplicas: 2
    lifecycle:
      restartPolicy: RecreateInstanceOnPodRestart
      updateStrategy:
        type: SurgeThenDrain
        rollingUpdate:
          maxUnavailable: 1
  decoder:
    minReplicas: 1
    maxReplicas: 1
    lifecycle:
      restartPolicy: None
  router:
    minReplicas: 1
    maxReplicas: 1
```

- The engine runs prefill. When a pod of a ready prefill Instance fails, OME rebuilds the Instance in a new pod. `maxUnavailable: 1` lets the [crash-loop repair](#the-crash-loop-repair) rebuild one prefill Instance at a time.
- The decoder states its default, `None`, which also overrides any value in the runtime. OME repairs only the decoder pod that fails.
- The router keeps its default, `None`.

## What triggers a restart

Under `RecreateInstanceOnPodRestart`, OME restarts a `Ready` Instance when:

- The main container of one of its pods, `ome-container`, restarts. Restarts of sidecars and init containers don't count.
- One of its pods is in phase `Failed`.
- It has fewer pods than it needs, for example because a pod was deleted or evicted.

So by default, a node drain that evicts one pod of a multi-pod Instance rebuilds the whole Instance: see [Configure pod disruption budgets](../../guides/deploy-models/configure-pod-disruption-budgets.md).

Before an Instance is `Ready`, a crashing container is part of an ordinary start, and the Instance's [readiness deadline](../../guides/omenative/set-instance-readiness-deadlines.md) decides when it fails.

## The crash-loop repair

Some pod states, such as `CrashLoopBackOff` and `ImagePullBackOff`, [don't clear on their own](../../guides/omenative/set-instance-readiness-deadlines.md#when-a-deadline-runs-out). When a pod of a `Ready` Instance is in one of them and is older than the stuck-pod grace period, OME restarts the whole Instance, under either policy. Its `RestartTriggered` event says the pod is `wedged in <reason> past the stuck-pod grace`.

The ome-resources chart sets the grace period, `ome.controller.lifecycle.stuckPodGracePeriod`, to `60s`. Without the setting, the repair is off.

!!! warning "Repairs can rebuild a whole component"
    One cause can wedge every Instance of a component at once. Under the default `SurgeThenDrain` strategy, and outside a [rollout group](../rollouts-and-traffic/rollout-groups.md), OME can then rebuild them all together. To pace the repairs, set `maxUnavailable` in the component's `lifecycle.updateStrategy.rollingUpdate`, as the engine in [Choose a policy per component](#choose-a-policy-per-component) does.

With `maxUnavailable: 1`, OME starts a repair only while no other Instance of the component is restarting or out of service for an update. A repair that waits records a `RepairHeld` warning event that names what holds it: `Budget` for `maxUnavailable`, or the rollout group's gate. Restarts that the policy starts don't wait for either, and `SurgeThenDrain` updates ignore `maxUnavailable`.

## What a restart does

OME sets the Instance's phase to `Restarting`. It takes the pods out of the Service, deletes them once the Service stops routing to them, and creates them again on the same revision, one [incarnation](instances.md#incarnation) higher. Once the new pods are ready and have passed the component's [minReadySeconds](../../guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md), the Instance is `Ready` again.

The old pods' logs go with them. When a pod failed, the Instance's [`lastFailure`](instances.md#when-an-instance-fails) keeps its container, reason, exit code and time, even after the Instance is `Ready` again.

A restart has until the Instance readiness timeout to get its new pods serving: the component's `lifecycle.instanceReadyTimeout`, or `30m` with the ome-resources chart. When the time runs out, or a new pod stays stuck past the stuck-pod grace period, the Instance becomes `Failed`. OME keeps a `Failed` Instance and its pods for you to inspect until you [reset it](../../guides/omenative/reset-failed-instances.md) or a new revision reaches it.

## Observe a restart

OME records a restart's events on the InferenceService:

| Event | Type | When |
| --- | --- | --- |
| `RestartTriggered` | Warning | A restart starts. The message names the Instance, the trigger and the new incarnation. |
| `RestartCompleted` | Normal | The Instance is `Ready` again. |
| `RepairHeld` | Warning | A crash-loop repair waits for `maxUnavailable` or its rollout group. |
| `FoundOrphan` | Warning | A pod without the `ome.io/instance-incarnation` label holds up the restart until the pod is gone. |
| `InstanceFailed` | Warning | The restart ran out of time, or a new pod stayed stuck, and the Instance is `Failed`. |

For example, after the engine's container is killed for running out of memory, list the restarts:

```bash
kubectl get events -n qwen3-native --field-selector reason=RestartTriggered \
  -o custom-columns=KIND:.involvedObject.kind,NAME:.involvedObject.name,MESSAGE:.message
```

```output
KIND               NAME         MESSAGE
InferenceService   qwen3-0-6b   OMENative component=engine instance=0 restart triggered: pod qwen3-0-6b-engine-0-default-0 container ome-container restarted after Ready: OOMKilled (exit 137) (incarnation=2)
```

[kubectl ome instance](../../reference/kubectl-ome/instance.md) shows the restart too. In the output of `kubectl ome instance list qwen3-0-6b -n qwen3-native`, `PHASE` is `Restarting` and `IDX/INC` shows the new incarnation, such as `0/2`. The `kubectl ome instance status` command adds the operation, the last failure and each pod's restarts.

By default, while an engine Instance restarts, the InferenceService's `EngineReady` condition is `False`, with the reason `InsufficientAvailable`. With `maxUnavailable` set, it stays `True`, with `MinimumAvailable`, while no more than that many Instances are out of service: see [Conditions](../architecture/deployment-modes.md#conditions).

## Updates, migrations and pauses

| Case | What happens |
| --- | --- |
| A change to the pod template, such as a new image | It rolls out under the [update strategy](../architecture/omenative-update-strategies.md), not the restart policy. |
| An Instance that's being migrated | Its [migration](migration-and-transient-scale.md) handles it, not the restart policy. |
| A paused InferenceService | At `ome.io/rollout-paused: true`, repairs go on, but under `None`, OME holds off creating missing pods until you resume. At `freeze`, no repair starts. See [What a pause holds](../../guides/roll-out-changes/pause-and-resume-a-rollout.md#what-a-pause-holds). |

## Next steps

- [Instances](instances.md): phases, incarnations and `lastFailure`.
- [Set Instance readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md): decide when a slow or stuck Instance fails.
- [Reset failed Instances](../../guides/omenative/reset-failed-instances.md): start over Instances that OME marked `Failed`.
- [OMENative update strategies](../architecture/omenative-update-strategies.md): how a change to the pod template replaces an Instance's pods.
- [Serve a multi-node model](../../guides/omenative/serve-a-multi-node-model.md): run a multi-pod engine, which OME rebuilds whole by default.
