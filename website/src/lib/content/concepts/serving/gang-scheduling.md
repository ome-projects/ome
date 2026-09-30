---
title: Gang scheduling
description: With the scheduler-plugins PodGroup CRD installed, OME creates a PodGroup for each multi-pod OMENative Instance, so a gang-aware scheduler places its leader and workers together or not at all.
since: v1.3
---

Gang scheduling places all the pods of a multi-pod [Instance](../omenative/instances.md) together, or none of them. An [OMENative](../omenative/overview.md) Instance with a leader and workers serves only when all its pods run, and gang scheduling stops some of them from holding GPUs while the rest wait.

When the [scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins) PodGroup CRD is installed, OME creates a PodGroup for each multi-pod Instance of an OMENative engine or decoder, and none for RawDeployment or [MultiNode](../architecture/deployment-modes.md#the-deployment-modes) (deprecated) components. A gang-aware scheduler reads the PodGroups: the alpha [OME scheduler](../scheduling/ome-scheduler.md) or the scheduler-plugins Coscheduling plugin. The default Kubernetes scheduler ignores them and places pods one at a time.

## Install the PodGroup CRD {#when-the-podgroup-crd-is-missing}

No OME chart installs the PodGroup CRD. Install it from scheduler-plugins v0.34.7, the release OME is built against. If you run the OME scheduler, install the CRD version from [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md) instead. Then restart the controller, which looks for the CRD only when it starts. This example assumes OME runs in the `ome` namespace:

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/scheduler-plugins/v0.34.7/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
customresourcedefinition.apiextensions.k8s.io/podgroups.scheduling.x-k8s.io created
deployment.apps/ome-controller-manager restarted
```

## Bring a gang-aware scheduler

A PodGroup holds a gang together only when the scheduler that places the pods reads it. The pods use the scheduler that the [runtime](../runtimes/serving-runtimes.md) or the [InferenceService](inference-services.md) names, and the default scheduler otherwise. The default scheduler can bind part of an Instance while the rest waits, and the bound pods hold their GPUs with nothing serving. Use one of these:

| Scheduler | What it does | `schedulerName` |
| --- | --- | --- |
| [OME scheduler](../scheduling/ome-scheduler.md) | Places each gang in one topology domain, all or nothing. Alpha, and needs Kubernetes 1.35. | `ome-scheduler` |
| scheduler-plugins Coscheduling | Places each gang all or nothing, in your default scheduler or as a second scheduler. | Unset in your default scheduler, or `scheduler-plugins-scheduler` from the scheduler-plugins Helm chart |

The OME scheduler also needs a topology key: the component's `topologyKey`, or its chart's `scheduler.plugin.topologyKey` for every gang. Without either, the gang's pods stay `Pending`. [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md) installs the scheduler and sets the key.

Set `spec.schedulerName` in the runtime to use one scheduler for all its components:

```yaml
spec:
  schedulerName: ome-scheduler
```

A `schedulerName` in an InferenceService component, or in a runtime component config such as `engineConfig`, overrides it for that component's leader and workers:

```yaml
spec:
  engine:
    schedulerName: ome-scheduler
```

When the pods of a component with PodGroups use the default scheduler, OME records a `MaybeNoGangScheduler` Warning event on the InferenceService. If you added Coscheduling to your default scheduler, ignore it.

## One PodGroup per multi-pod Instance

OME creates an Instance's PodGroup before its pods, and names it `<isvc>-<component>-<index>`. Its `minMember` is the Instance's pod count: the leader plus `worker.size` workers. Its `scheduleTimeoutSeconds` comes from the [gang schedule timeout](#bound-the-gang-schedule-timeout). When the component sets `topologyKey`, the PodGroup carries it in the `ome.io/topology-key` annotation. OME labels the Instance's pods with `scheduling.x-k8s.io/pod-group`, set to the PodGroup's name, which gang-aware schedulers read.

For example, the InferenceService in [Serve a multi-node model](../../guides/omenative/serve-a-multi-node-model.md) runs one engine Instance of a leader and one worker, so OME creates the PodGroup `qwen3-multi-node-engine-0`. Without its owner reference and status, it looks like this:

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: qwen3-multi-node-engine-0
  namespace: qwen3-multi-node
  labels:
    component: engine
    ome.io/inferenceservice: qwen3-multi-node
    ome.io/instance-index: "0"
    ome.io/managed-by: OMENative
spec:
  minMember: 2
  scheduleTimeoutSeconds: 600
```

List the InferenceService's PodGroups:

```bash
kubectl get podgroups -n qwen3-multi-node -l ome.io/inferenceservice=qwen3-multi-node \
  -o custom-columns=NAME:.metadata.name,MIN-MEMBER:.spec.minMember,TIMEOUT:.spec.scheduleTimeoutSeconds
```

```output
NAME                        MIN-MEMBER   TIMEOUT
qwen3-multi-node-engine-0   2            600
```

List the Instance's pods by their PodGroup label:

```bash
kubectl get pods -n qwen3-multi-node -l scheduling.x-k8s.io/pod-group=qwen3-multi-node-engine-0 -o name
```

```output
pod/qwen3-multi-node-engine-0-leader-0
pod/qwen3-multi-node-engine-0-worker-0
```

## Bound the gang schedule timeout

A gang-aware scheduler holds a gang's placed pods until the whole gang fits, for up to the PodGroup's `scheduleTimeoutSeconds`. At the timeout, it frees their resources for other pods and tries the gang again later.

OME sets `scheduleTimeoutSeconds` to the component's [ready timeout](../../guides/omenative/set-instance-readiness-deadlines.md), `instanceReadyTimeout`, clamped between the `min` and `max` of the controller's `gangScheduleTimeout` setting. The `ome-resources` chart sets both:

```yaml
ome:
  controller:
    lifecycle:
      instanceReadyTimeout: 30m
      gangScheduleTimeout:
        min: 60s
        max: 600s
```

With these values, the 30-minute ready timeout becomes 600 seconds, as in the PodGroup above.

| Ready timeout | `gangScheduleTimeout` | `scheduleTimeoutSeconds` |
| --- | --- | --- |
| Between `min` and `max` | Set | The ready timeout |
| Above `max` | Set | `max` |
| Below `min`, or unset | Set | `min` |
| Set | Unset | The ready timeout: 1800 in the kustomize install |
| Unset | Unset | Unset: the scheduler's default applies |

The scheduler's default is `defaultPermitTimeoutSeconds` for the OME scheduler, 600 in its chart, or `permitWaitingTimeSeconds` for Coscheduling. `gangScheduleTimeout` needs both `min` and `max`, positive, with `min` no larger than `max`. OME treats an invalid one as unset.

[Change the cluster defaults](../../guides/omenative/set-instance-readiness-deadlines.md#change-the-cluster-defaults) shows how to change these settings. OME updates existing PodGroups to the new timeout.

## What you see

| What you see | What it means |
| --- | --- |
| The component's `GangSchedulingUnavailable` condition is `True`, with the reason `PodGroupCRDNotInstalled` | The CRD is missing, or was when the controller started. The default scheduler may place part of an Instance, and the OME scheduler keeps its pods `Pending`. [Install the CRD](#when-the-podgroup-crd-is-missing). |
| An `InstanceFailed` Warning event with the scheduler's message | A pod stayed unschedulable longer than the controller's [`unschedulableGracePeriod`](../../guides/omenative/set-instance-readiness-deadlines.md), 15 minutes with the chart. |
| An `InstanceFailed` Warning event that names `PodGroupOwnershipConflict` | Another owner controls a PodGroup with the Instance's PodGroup name. OME fails the Instance and creates none of its pods. |
| A `PodGroupReset` Warning event on the InferenceService | A pod of a running gang failed, so the scheduler-plugins controller marked the PodGroup `Failed`. OME replaced it without failing the Instance. The Instance's [restart policy](../omenative/instance-restart-policy.md) decides what happens next. |

The condition is on the component's InferenceReplica, and in the InferenceService's `status.components.<component>.lifecycle.conditions`. When it's `False`, its reason is `GangSchedulingAvailable`. Read its reason for the engine, here without the CRD:

```bash
kubectl get inferenceservice qwen3-multi-node -n qwen3-multi-node \
  -o jsonpath='{.status.components.engine.lifecycle.conditions[?(@.type=="GangSchedulingUnavailable")].reason}'
```

```output
PodGroupCRDNotInstalled
```

## Next steps

- [The OME scheduler](../scheduling/ome-scheduler.md): how OME's alpha scheduler places each gang in one topology domain.
- [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md): install the OME scheduler and point your pods at it.
- [Serve a multi-node model](../../guides/omenative/serve-a-multi-node-model.md): run an engine whose Instances are a leader and workers.
- [Set Instance readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md): the ready timeout that the gang schedule timeout comes from.
