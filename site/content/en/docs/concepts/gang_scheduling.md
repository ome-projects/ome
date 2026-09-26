---
title: "Gang Scheduling"
linkTitle: "Gang Scheduling"
weight: 34
description: >
  How OME gang-schedules multi-pod OMENative Instances with scheduler-plugins PodGroups, what happens when the PodGroup CRD or a gang-aware scheduler is missing, and how to bound the gang schedule timeout.
---

A multi-node [OMENative](/ome/docs/concepts/omenative) Instance is one leader pod plus its worker pods, and the LLM runtime inside it only boots when **all** of those pods are running. If the scheduler places the pods one at a time, the leader can land on a node while the workers stay `Pending` — and the runtime hangs waiting for peer connections, holding GPUs the whole time. Gang scheduling makes placement all-or-nothing: either every pod of the Instance can be scheduled, or none is bound to a node.

OME expresses the gang through the [scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins) coscheduling API: for every multi-pod Instance, the controller creates one `scheduling.x-k8s.io/v1alpha1` **PodGroup** and labels the member pods to reference it. Any scheduler that implements the coscheduling contract can then enforce the gang.

Gang scheduling applies only to OMENative components. Single-pod Instances (including the Router, which is always single-pod) get no PodGroup and no pod-group label — there is nothing to gang.

## One PodGroup per multi-pod Instance

For each multi-pod Instance in the plan, the controller reconciles a PodGroup named `<isvc>-<component>-<index>` (truncated to 63 characters, because the same string is stamped on member pods as a label value). The name matches the Instance's DNS subdomain shape, so you can pivot between the gang and its pods without consulting status:

```bash
kubectl get podgroups.scheduling.x-k8s.io
NAME                  AGE
llama-chat-engine-0   5m
llama-chat-engine-1   5m

# Members of one gang:
kubectl get pods -l scheduling.x-k8s.io/pod-group=llama-chat-engine-0
```

The PodGroup's key fields:

| Field | Value |
|-------|-------|
| `spec.minMember` | The Instance's total pod count (leader + workers) — e.g. a leader with `worker.size: 2` gives `minMember: 3`. All members must be schedulable before any is bound. |
| `spec.scheduleTimeoutSeconds` | Derived from the component's `instanceReadyTimeout` and clamped by operator config — see [Bounding the gang schedule timeout](#bounding-the-gang-schedule-timeout). |
| `metadata.annotations["ome.io/topology-key"]` | Present when the component configures a topology key; advertises the topology domain a topology-aware gang scheduler should place all members in. |
| `metadata.ownerReferences` | The owning workload resource, so PodGroups are garbage-collected with it. |

Ordering matters: the PodGroup is created **before** the Instance's first pod. A coscheduling-capable scheduler that sees a pod referencing a PodGroup that does not exist yet falls back to scheduling that pod individually, which defeats the gang. Every member pod carries the upstream coscheduling label `scheduling.x-k8s.io/pod-group: <podgroup-name>`; that label is the entire pod-side contract.

## When the PodGroup CRD is missing

The PodGroup CRD is **optional**. The OME controller manager discovers it once at startup; when it is absent, gang scheduling degrades softly rather than blocking workloads:

- Pods are still created — multi-pod Instances come up, but nothing prevents partial placement (the leader may schedule while workers stay `Pending`).
- No PodGroup objects are written.
- The component reports the condition `GangSchedulingUnavailable=True` with reason `PodGroupCRDNotInstalled` and message `scheduler-plugins scheduling.x-k8s.io/v1alpha1 PodGroup CRD is not installed; multi-pod Instances may schedule partially`.

The condition is written to the owning InferenceReplica's `status.conditions` and mirrored into the InferenceService's `status.components.<component>.lifecycle.conditions`:

```bash
kubectl get inferencereplica llama-chat-engine -o jsonpath='{.status.conditions}' | jq
```

When the CRD is present — or the component has no multi-pod Instances at all — the condition is `False` with reason `GangSchedulingAvailable` ("Gang scheduling is available or not required").

> **Note:** Because the CRD check runs at controller startup, installing the PodGroup CRD on a running cluster does not enable gang scheduling until the OME controller manager restarts. The reverse direction is handled gracefully: if the CRD is deleted at runtime, the controller observes the API as unavailable on the next pass and degrades to the soft-fail behavior instead of blocking the Instance lifecycle.

## A gang-aware scheduler is your responsibility

Creating PodGroups changes nothing by itself: the stock kube-scheduler does not read `scheduling.x-k8s.io` objects. The gang is enforced only when the pods' `spec.schedulerName` points at a scheduler that implements the coscheduling contract — the scheduler-plugins coscheduler, OME's own secondary scheduler (the `ome-scheduler` Helm chart, opted into with `schedulerName: ome-scheduler`), or an equivalent.

OME deliberately does **not** set or enforce `schedulerName`. Set it where you set other pod-spec fields:

- on the ServingRuntime (`spec.schedulerName`), as a default for every service using that runtime, or
- on the InferenceService component (`spec.engine.schedulerName`, or inside the `leader`/`worker` blocks) — the merged component spec wins over the runtime.

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-multinode
spec:
  schedulerName: ome-scheduler   # a gang-aware scheduler
  # ...
```

When a PodGroup is created but the rendered pod template's effective `schedulerName` (the leader's value, falling back to the worker's) is empty or `default-scheduler`, the controller emits a one-shot Warning event, reason `MaybeNoGangScheduler`, against the InferenceService:

```bash
kubectl describe isvc llama-chat
# Warning  MaybeNoGangScheduler  OMENative component=engine created scheduler-plugins
#          PodGroup objects but pod template's spec.schedulerName is "" (default
#          kube-scheduler does not enforce PodGroup gang); ...
```

The event is a heuristic, emitted once per Instance incarnation. It is a false positive if you have installed the coscheduling plugin **inside** the default scheduler — in that configuration `default-scheduler` is gang-aware, and the controller cannot detect that from inside the cluster. Setting any non-default `schedulerName` suppresses the warning.

## Bounding the gang schedule timeout

`spec.scheduleTimeoutSeconds` tells the gang scheduler how long a PodGroup may wait for all members to become schedulable before it gives up on the attempt. OME derives it from the component's Instance readiness backstop and lets the operator bound it cluster-wide:

1. **Derivation** — the component's effective `instanceReadyTimeout`: the per-component `spec.<component>.lifecycle.instanceReadyTimeout` when set, otherwise the operator-wide `lifecycle.instanceReadyTimeout` in the `inferenceservice-config` ConfigMap (the Helm chart default is `30m`).
2. **Clamp** — the `lifecycle.gangScheduleTimeout` block in the same ConfigMap (Helm value `ome.controller.lifecycle.gangScheduleTimeout`) bounds the derived value: raised to `min` when below it (or when no usable `instanceReadyTimeout` exists at either level), lowered to `max` when above it.

```yaml
# ome-resources Helm values (rendered into inferenceservice-config)
ome:
  controller:
    lifecycle:
      instanceReadyTimeout: 30m
      gangScheduleTimeout:
        min: 60s
        max: 600s
```

With these chart defaults, the derived 30-minute timeout is clamped to the ceiling, so every gang PodGroup gets `scheduleTimeoutSeconds: 600` — a stuck gang releases its admission attempt after 10 minutes instead of holding it for the full readiness window.

Rules for the clamp block:

- Both `min` and `max` are **required** when the block is present, must parse as positive durations, and `min` must not exceed `max`. An invalid block is logged and ignored for that pass — the derived timeout reaches the scheduler unclamped, never patched up with fallback bounds.
- Removing the block entirely passes the derived timeout through unclamped. If, additionally, no `instanceReadyTimeout` is configured at either level, `scheduleTimeoutSeconds` is left unset and the scheduler's own default applies.
- The controller reconciles `scheduleTimeoutSeconds` on existing PodGroups, so a ConfigMap change propagates to live gangs without recreating them.

When the scheduler gives up on a gang it marks the PodGroup's phase `Failed`. That verdict is absorbing — the group can never be admitted again under that name — so OME deletes its own Failed PodGroup and builds a fresh one on a later pass, emitting a Warning event, reason `PodGroupReset`, that carries the scheduler's explanation. The Instance itself is not failed for a Failed gang; the event is the operator-facing trace of the rebuild.

## Related resources

- [Deployment Modes and OMENative](/ome/docs/concepts/omenative) — how a component resolves to OMENative and what an Instance is
- [Inference Service](/ome/docs/concepts/inference_service) — leader/worker component specs
