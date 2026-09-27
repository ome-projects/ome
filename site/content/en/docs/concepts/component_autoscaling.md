---
title: "Component Autoscaling"
linkTitle: "Component Autoscaling"
weight: 32
description: >
  Configure autoscaling per InferenceService component with an inline autoscaler block — choose the HPA or KEDA class, supply metrics or triggers, and know which configuration layer wins.
---

Every InferenceService component (`engine`, `decoder`, `router`) gets its own autoscaler. You configure it with a typed `autoscaler` block on the component spec; OME renders the block into a real `autoscaling/v2` HorizontalPodAutoscaler or KEDA ScaledObject, owns it, and reconciles it on every pass. The block is the only per-component autoscaling surface on the InferenceService — the legacy `scaleTarget`/`scaleMetric` fields were removed, and the legacy `ome.io/autoscalerClass` annotation cannot be combined with it.

> **Alpha:** The `ComponentAutoscaler` API is alpha and may change without notice.

Autoscaler dispatch runs for RawDeployment components (the scaler targets the component's `Deployment`) and OMENative components (the scaler targets the component's `InferenceReplica` through its `/scale` subresource). MultiNode components have no autoscaler dispatch.

## The block at a glance

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
spec:
  model:
    name: llama-3-70b-instruct
  runtime:
    name: srt-llama-3
  engine:
    minReplicas: 2        # replica bounds live on the component, not in the block
    maxReplicas: 16
    autoscaler:
      class: HPA          # HPA | KEDA | External | None
      hpa:                # optional when class: HPA
        metrics: [...]
      # keda:             # required when class: KEDA
      #   triggers: [...]
```

| Field | Meaning |
|-------|---------|
| `class` | Required. Selects the implementation: `HPA`, `KEDA`, `External`, or `None`. |
| `hpa` | HPA configuration. Optional when `class: HPA`; omitted, the controller emits a default CPU=80% HPA. |
| `keda` | KEDA configuration. Required when `class: KEDA` — at least one trigger. |

The four classes:

- **`HPA`** — OME emits and reconciles a HorizontalPodAutoscaler named after the component workload (`<isvc>-<component>`).
- **`KEDA`** — OME emits and reconciles a KEDA ScaledObject named `scaledobject-<workload-name>` (long workload names keep their last 50 characters). KEDA must be installed on the cluster; admission does not probe for it.
- **`External`** — OME deletes any scaler it manages for the component and stays out. It keeps publishing `status.components.<component>.scaleTargetRef` — the canonical `{apiVersion, kind, name}` your own scaler should target (the InferenceReplica for OMENative, the Deployment for RawDeployment).
- **`None`** — no autoscaler at all. Used by proportional scaling-policy followers, where a coordinator writes replicas directly.

`External` and `None` reconcile identically (both remove OME-managed scalers and leave foreign objects untouched); the status field `managedBy` is what distinguishes them (`external` vs `none`).

## Replica bounds

`minReplicas` and `maxReplicas` sit beside the block on the component spec and are forwarded to the generated HPA (`minReplicas`/`maxReplicas`) or ScaledObject (`minReplicaCount`/`maxReplicaCount`):

- `minReplicas` defaults to 1 when unset.
- `maxReplicas` is clamped up to `minReplicas` when it is lower or unset.
- Admission rejects `minReplicas < 0`, `maxReplicas < 1` (when set), and `minReplicas > maxReplicas`.
- `minReplicas: 0` is only accepted when that component opts into KEDA (`autoscaler.class: KEDA`, or the whole-service legacy `ome.io/autoscalerClass: keda` annotation); with an HPA it would leave the workload sitting at zero replicas.

## HPA configuration

```yaml
spec:
  engine:
    minReplicas: 2
    maxReplicas: 16
    autoscaler:
      class: HPA
      hpa:
        metrics:
          - type: Resource
            resource:
              name: cpu
              target:
                type: Utilization
                averageUtilization: 60
        behavior:
          scaleDown:
            stabilizationWindowSeconds: 300
```

Both fields are standard `autoscaling/v2` shapes passed through verbatim to the generated HPA:

| Field | Behavior |
|-------|----------|
| `hpa.metrics` | Any `autoscalingv2.MetricSpec` list — `Resource`, `ContainerResource`, `Pods`, `Object`, and `External` sources are all accepted. Empty or omitted, the controller emits a single CPU metric targeting 80% average utilization. |
| `hpa.behavior` | Standard `HorizontalPodAutoscalerBehavior` (stabilization windows, per-direction policies). Omitted, the Kubernetes API server's defaults apply. |

Admission checks each metric entry's shape: the declared `type` must have its matching sub-object populated (`type: Resource` requires `resource`, `type: Pods` requires `pods`, and so on). A mismatched entry is rejected with reason `HPAMetricMalformed` instead of silently producing a no-op HPA.

## KEDA configuration

```yaml
spec:
  engine:
    minReplicas: 1
    maxReplicas: 8
    autoscaler:
      class: KEDA
      keda:
        triggers:
          - type: prometheus
            metricType: AverageValue
            metadata:
              serverAddress: http://prometheus.monitoring.svc:9090
              query: sum(rate(request_success_total{isvc="chat"}[2m]))
              threshold: "20"
        pollingInterval: 15
        cooldownPeriod: 120
        fallback:
          failureThreshold: 3
          replicas: 8
```

| Field | Behavior |
|-------|----------|
| `keda.triggers` | Required, at least one entry. Verbatim KEDA `ScaleTriggers` — the full trigger surface (`prometheus`, `cron`, `kafka`, `external`, ...) is supported. |
| `keda.pollingInterval` | Seconds between trigger checks. Unset, KEDA's own default applies. |
| `keda.cooldownPeriod` | Seconds KEDA waits before scaling down after the last trigger fires. Unset, KEDA's own default applies. |
| `keda.idleReplicaCount` | Fixed replica count when no trigger fires (commonly `0`). Must be strictly less than the component's effective `minReplicas`. |
| `keda.fallback` | Verbatim KEDA fallback applied when the metric source becomes unavailable. |
| `keda.advanced` | Verbatim KEDA advanced block: `horizontalPodAutoscalerConfig` and `restoreToOriginalReplicaCount`. |

Everything is forwarded verbatim to the ScaledObject spec, so KEDA's own documentation for each field applies directly. Two OME-specific rules:

- A `class: KEDA` block without triggers never dispatches — admission rejects it (`KedaTriggersRequired`), and the controller refuses to act on one that slips through, so an invalid configuration can never remove a working scaler.
- `advanced.horizontalPodAutoscalerConfig.name` must not collide with the component workload name (`<isvc>-<component>`) — that name is reserved for the HPA OME itself would manage, and the collision is rejected at dispatch.

Unlike the [AutoscalerPolicy](/ome/docs/concepts/autoscaler_policy) template surface, inline triggers are raw KEDA triggers: you supply `serverAddress` and any authentication yourself, and there is no `providerRef` indirection or template variable rendering.

## Which layer wins

The per-component autoscaler resolves through a fixed chain; the first layer that provides a block wins, and `status.components.<component>.autoscaler.specSource` reports which one did:

| Priority | Layer | `specSource` |
|----------|-------|--------------|
| 1 | Inline `spec.<component>.autoscaler` on the InferenceService | `isvc` |
| 2 | The rendered [`autoscalerPolicyRef`](/ome/docs/concepts/autoscaler_policy) | `policy` |
| 3 | The resolved ServingRuntime's component-level `autoscaler` block | `runtime` |
| 4 | Legacy `ome.io/autoscalerClass` annotation (RawDeployment only) | `legacy` |
| 5 | Built-in default: an HPA with one CPU=80% utilization metric | `default` |

Points worth knowing:

- **The default always produces an HPA.** A component with no autoscaler configuration anywhere still gets a CPU=80% HPA between its replica bounds. To have no OME-managed scaler you must say so explicitly with `class: External` or `class: None`.
- **The runtime layer** is a ServingRuntime or ClusterServingRuntime declaring the same `autoscaler` block inside `engineConfig` / `decoderConfig` / `routerConfig` — a vendor-family default every InferenceService on that runtime inherits unless a higher layer overrides it. The runtime webhook validates those blocks with the same shape rules as the InferenceService webhook.
- **The legacy layer** translates the deprecated whole-service annotations ([`ome.io/autoscalerClass`](/ome/docs/reference/labels-and-annotations) with lowercase values `hpa`/`keda`/`external`, and `ome.io/targetUtilizationPercentage` into an explicit CPU metric). It only substitutes for the default — any typed block or policy ref outranks it — and only on RawDeployment.
- **Annotation conflict:** an InferenceService that carries the `ome.io/autoscalerClass` annotation *and* a typed `autoscaler` block on any component is rejected at admission (`AutoscalerAnnotationConflict`); remove the annotation.

## What admission rejects

The InferenceService webhook (and, for the runtime layer, the ServingRuntime/ClusterServingRuntime webhook) denies:

- `class` outside `HPA | KEDA | External | None` (`AutoscalerClassUnknown`).
- `class: KEDA` without at least one trigger (`KedaTriggersRequired`).
- An `hpa.metrics` entry whose `type` doesn't match the populated sub-object (`HPAMetricMalformed`).
- `keda.idleReplicaCount` not strictly less than the component's `minReplicas` (`KedaIdleBelowMin`) — a friendlier version of the rejection KEDA itself would issue.
- Invalid replica bounds, and `minReplicas: 0` on a component that isn't KEDA-autoscaled.
- The legacy `ome.io/autoscalerClass` annotation combined with any typed block (`AutoscalerAnnotationConflict`).
- The removed `scaleTarget` / `scaleMetric` fields, with a migration message pointing at the `autoscaler` block (`LegacyAutoscalerFieldsRejected`).

## Switching classes safely

Changing `class` (say `HPA` → `KEDA`) is an ordinary spec edit. The controller reconciles the new scaler *first* and deletes the stale sibling only afterwards, so a failed switch — an invalid trigger, KEDA missing — leaves the previous working scaler in place. Both objects are keyed off the same component workload name (the ScaledObject carries the `scaledobject-` prefix), which makes the cross-class cleanup idempotent.

For the managed classes, the controller also preflights ownership of both canonical objects: a foreign or ownerless HPA/ScaledObject occupying the canonical name holds reconciliation with an error rather than being adopted or deleted. `None` and `External` leave such foreign objects untouched.

## Reading the result in status

The controller mirrors each component's autoscaler state onto the InferenceService at `status.components.<component>.autoscaler`:

| Field | Meaning |
|-------|---------|
| `class` | The resolved class that drove dispatch. |
| `managedBy` | `ome` (OME reconciles the scaler), `external`, or `none`. |
| `specSource` | Which precedence layer won: `isvc`, `policy`, `runtime`, `legacy`, or `default`. |
| `currentReplicas` / `desiredReplicas` / `lastScaleTime` | Mirrored from the live HPA / ScaledObject when `managedBy: ome`; zero/empty otherwise. |
| `conditions` | The scaler's conditions verbatim — HPA: `AbleToScale`, `ScalingActive`, `ScalingLimited`; ScaledObject: `Ready`, `Active`, `Fallback`, `Paused`. |

`status.components.<component>.scaleTargetRef` is published alongside for every reconciled RawDeployment or OMENative component — including with `class: External`, where it is the handoff point for the operator-owned scaler.

The kubectl-ome plugin reads this surface for you: [`kubectl ome autoscale explain`](/ome/docs/tasks/kubectl-ome-autoscale-explain) shows which layer won and whether the reported evidence matches the declared intent, and [`kubectl ome autoscale status`](/ome/docs/tasks/kubectl-ome-autoscale-status) reports the live per-component scaler state.

## Reference

- Reusable templates instead of inline blocks: [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy) (an inline block always outranks a policy ref)
- Legacy annotations: [Labels and Annotations](/ome/docs/reference/labels-and-annotations)
- API fields: [`ComponentAutoscaler` and `ComponentExtensionSpec`](/ome/docs/reference/ome.v1beta1)
