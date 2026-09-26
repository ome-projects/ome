---
title: "Deployment Modes and OMENative"
linkTitle: "Deployment Modes"
weight: 31
description: >
  How OME resolves the deployment mode for each InferenceService component, how to opt in with spec.deploymentMode or the ome.io/deploymentMode annotation, and what the OMENative mode provides.
---

Every InferenceService component (Engine, Decoder, Router) is dispatched to a **deployment mode** — the workload backend that creates and manages its pods. OME resolves the mode per component from your spec; you rarely need to name it explicitly, but you can.

| Mode            | Backing workload                                          | How it is selected                                                                    |
|-----------------|-----------------------------------------------------------|----------------------------------------------------------------------------------------|
| `RawDeployment` | Kubernetes Deployment + HPA/KEDA                          | The default; or `spec.deploymentMode: RawDeployment`; or the per-component annotation.   |
| `OMENative`     | OME-managed pods via an InferenceReplica                  | `spec.deploymentMode: OMENative`; the per-component annotation; or inferred from a `leader`/`worker` block. |
| `MultiNode`     | [LeaderWorkerSet](https://github.com/kubernetes-sigs/lws) | Only via the `ome.io/deploymentMode: MultiNode` annotation — never inferred from shape.  |

Two related names are **not** dispatch modes you set per component: `PDDisaggregated` is a service-level shape descriptor derived from declaring both an Engine and a Decoder, and `VirtualDeployment` is a legacy mode retained for backward compatibility.

## What OMENative is

OMENative is OME's native pod-lifecycle engine. Instead of delegating to a Deployment or a LeaderWorkerSet, the InferenceService controller projects each OMENative-mode component onto an **InferenceReplica** — a per-component workload resource (one per InferenceService/component pair) whose spec is written by the InferenceService controller and whose status is written by the InferenceReplica controller:

```bash
kubectl get inferencereplicas        # short name: irep
NAME                  COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
llama-chat-engine     engine      2         2         2       2           5m
```

The InferenceReplica controller manages pods directly, grouped into **Instances**. An Instance is one unit of serving capacity: a single pod for a single-pod component, or a leader pod plus its worker pods when the component declares `leader` and `worker` blocks — OMENative handles multi-node serving natively, without a LeaderWorkerSet. Each Instance moves through a lifecycle phase:

`Pending` → `Creating` → `Ready`, with `Updating`, `Restarting`, `Migrating`, `Failed`, and `Deleting` covering updates, recovery, and teardown.

Because OMENative owns the pod lifecycle, it can offer capabilities a Deployment cannot, such as revision-tracked in-place updates and per-Instance rollout pacing. Those behaviors are configured through the component's `lifecycle` block and the rollout API, which are beyond the scope of this page.

## Opting in

### The typed field: `spec.deploymentMode`

The top-level `spec.deploymentMode` field selects the dispatch backend for **every** component on the InferenceService. It accepts only `OMENative` or `RawDeployment` (the CRD enum rejects other values at admission):

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-70b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 10
```

When set, the field propagates to the Engine, Decoder, and Router at resolution time without mutating per-component annotations, so `kubectl get -o yaml` shows exactly what you wrote.

### The annotation: `ome.io/deploymentMode`

The `ome.io/deploymentMode` annotation is the explicit override and the only way to select LeaderWorkerSet-backed `MultiNode`. Set it in a component's `annotations` field to control that component alone — useful for mixed-mode setups where, say, the Engine runs OMENative while the Router stays on a plain Deployment:

```yaml
spec:
  engine:
    annotations:
      ome.io/deploymentMode: "OMENative"
    minReplicas: 1
    maxReplicas: 3
  router:
    annotations:
      ome.io/deploymentMode: "RawDeployment"
    minReplicas: 1
```

Accepted values are `RawDeployment`, `MultiNode`, and `OMENative` (plus the legacy `VirtualDeployment`). An unrecognized value is ignored and resolution falls through to the next rule below. `PDDisaggregated` is not an accepted annotation value.

The annotation is read from the **merged** component spec — the runtime's `engineConfig`/`decoderConfig`/`routerConfig` merged with your InferenceService component, with the InferenceService's fields taking precedence. A ServingRuntime can therefore carry the annotation as a default for services that use it, and your own component annotation overrides it.

## How the mode is resolved per component

For each component, the first rule that matches wins:

1. **Per-component annotation** — `ome.io/deploymentMode` on the merged component spec.
2. **Typed field** — `spec.deploymentMode`, applied to every component.
3. **Shape inference** — an Engine or Decoder that declares a `leader` or `worker` block resolves to **OMENative**. The Router has no leader/worker shape and skips this rule.
4. **Default** — `RawDeployment`.

Two consequences are worth calling out:

- **Shape inference selects OMENative, not MultiNode.** Declaring `engine.leader`/`engine.worker` gives you a multi-node OMENative component. LeaderWorkerSet-backed `MultiNode` is chosen only when you name it explicitly with the annotation.
- **Shape inference runs on the merged spec**, so a `leader`/`worker` block contributed by the ServingRuntime's `engineConfig` also triggers OMENative, even if your InferenceService does not declare one.

The admission webhook rejects a `leader` without a `worker` (and vice versa), and a non-positive `worker.size`, so the only valid multi-pod shape is a complete leader/worker pair.

Per-component support:

| Component | `RawDeployment` | `OMENative` | `MultiNode` (annotation only) |
|-----------|-----------------|-------------|-------------------------------|
| Engine    | ✓               | ✓           | ✓                             |
| Decoder   | ✓               | ✓           | ✓                             |
| Router    | ✓               | ✓ (always single-pod) | —                   |

### Example: multi-node OMENative engine

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: deepseek-r1-native
spec:
  model:
    name: deepseek-r1
  engine:
    minReplicas: 1
    maxReplicas: 2
    leader:
      runner:
        resources:
          limits:
            nvidia.com/gpu: "8"
    worker:
      size: 1
      runner:
        resources:
          limits:
            nvidia.com/gpu: "8"
```

No mode is named anywhere: the leader/worker shape resolves the Engine to OMENative, and each Instance is one leader pod plus one worker pod. To render the same spec as a LeaderWorkerSet instead, add `ome.io/deploymentMode: "MultiNode"` to `engine.annotations`.

## The service-level mode

Separately from the per-component dispatch, OME resolves one mode for the InferenceService **as a whole**, used for service-level handling such as the legacy VirtualDeployment path. The chain is:

1. The `ome.io/deploymentMode` annotation in `metadata.annotations`.
2. `spec.deploymentMode`.
3. Declared shape: Engine **and** Decoder present → `PDDisaggregated`; an Engine with both `leader` and `worker` → `OMENative`.
4. The operator default — `defaultDeploymentMode` in the `deploy` block of the `inferenceservice-config` ConfigMap, which the operator requires to be `RawDeployment`.

This is where `PDDisaggregated` comes from: it is a description of the service's shape, not a workload backend. In a prefill-decode disaggregated service, each component still dispatches through its own per-component mode.

## Observing OMENative in status

A component that resolves to OMENative reports an aggregated `lifecycle` block under `status.components.<component>` (the block is absent for other modes):

```yaml
status:
  components:
    engine:
      lifecycle:
        currentRevision: llama-chat-engine-6f8b49
        updateRevision: llama-chat-engine-6f8b49
        replicas: 2                # Instances in any phase
        readyReplicas: 2           # Instances with every pod Ready
        servingReplicas: 2         # Instances actually in the load balancer's rotation
        availableReplicas: 2
        updatedReplicas: 2
        conditions:
          - type: Available
            status: "True"
          - type: Progressing
            status: "False"
```

`servingReplicas` counts the Instances whose pods are in the traffic rotation; it can dip below `readyReplicas` during in-place updates. Per-Instance detail (index, phase, revisions) is not mirrored to the InferenceService — read the owning InferenceReplica's status for that:

```bash
kubectl get inferencereplica llama-chat-engine -o jsonpath='{.status}' | jq
```

## Related resources

- [Inference Service](/ome/docs/concepts/inference_service) — components, multi-node leader/worker specs, autoscaling
- [Labels and Annotations](/ome/docs/reference/labels-and-annotations) — the full annotation reference
