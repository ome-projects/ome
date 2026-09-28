---
title: "Scale a Component to Zero with KEDA"
linkTitle: "Scale to Zero (KEDA)"
weight: 20
date: 2026-09-27
description: >
  Why the admission webhook rejects minReplicas 0 without KEDA, and how to configure scale-to-zero with a typed KEDA autoscaler — minReplicaCount 0 on RawDeployment, or idleReplicaCount 0.
---

This page shows you how to let an InferenceService component scale to zero replicas. Scale-to-zero in OME is a KEDA capability: the admission webhook rejects any component that explicitly sets `minReplicas: 0` unless that component is KEDA-autoscaled, and the actual idle-at-zero behavior is carried out by the generated KEDA ScaledObject.

If you set `minReplicas: 0` without the KEDA opt-in, the write is denied with:

```text
InvalidScaleToZero: engine.minReplicas=0 requires KEDA autoscaling
(set spec.engine.autoscaler.class=KEDA or the ome.io/autoscalerClass=keda annotation);
Serverless mode no longer auto-promotes scale-to-zero
```

> **Alpha:** The typed per-component `autoscaler` block is alpha and may change without notice.

## Why the gate exists

Before Serverless mode was removed, `minReplicas: 0` auto-promoted the component to Knative Serving, which natively supports scale-to-zero. Today the same spec falls through to a plain workload, and the default autoscaler — an HPA with a single CPU 80% metric — cannot scale a Deployment from zero: a RawDeployment admitted at `minReplicas: 0` would simply sit at zero replicas with nothing to wake it. The webhook therefore rejects the shape at admission unless a KEDA autoscaler, which *can* scale from zero, is configured for that component.

Two configurations satisfy the gate:

- **The typed per-component field** — `spec.<component>.autoscaler.class: KEDA`. This is the preferred surface. It is evaluated per component: the specific component that sets `minReplicas: 0` must itself carry the block.
- **The legacy whole-service annotation** — `ome.io/autoscalerClass: "keda"`. It satisfies the gate for every component, but it cannot coexist with any inline `autoscaler` block (the webhook rejects the pair with `AutoscalerAnnotationConflict`), and it carries no trigger configuration of its own — on its own it never produces a working scaler (see [Troubleshooting](#troubleshooting)). Note the intentional case difference: the typed enum value is `KEDA`, the legacy annotation value is lowercase `keda`.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- KEDA installed in the cluster. OME generates ScaledObjects but does not install KEDA, and the webhook does not verify that KEDA is present
- `kubectl` configured to communicate with your cluster

## Two shapes of scale-to-zero

The generated ScaledObject can reach zero replicas in two distinct ways, and they cannot be combined:

1. **`minReplicas: 0`** — the ScaledObject gets `minReplicaCount: 0`, so KEDA scales the workload to zero whenever no trigger reports activity. Supported on **RawDeployment** components only (see [OMENative](#omenative-use-idlereplicacount) below).
2. **`keda.idleReplicaCount: 0`** with `minReplicas` ≥ 1 (or unset, which defaults to 1) — the ScaledObject keeps `minReplicaCount` at your floor and adds `idleReplicaCount: 0`: the workload idles at zero when no trigger fires and jumps back to at least `minReplicas` when one does. Works on both RawDeployment and OMENative components. KEDA itself only supports `0` as the idle value.

The webhook enforces that `keda.idleReplicaCount` is strictly less than the component's `minReplicas` when both are set — the same rule KEDA applies against its effective `minReplicaCount`, surfaced up front as a friendlier error. That is why the shapes are exclusive: `minReplicas: 0` together with `idleReplicaCount: 0` is rejected.

| `minReplicas` | `keda.idleReplicaCount` | Admission | Generated ScaledObject |
|---------------|-------------------------|-----------|------------------------|
| `0` (with KEDA opt-in) | unset | accepted | `minReplicaCount: 0` (RawDeployment) |
| ≥ 1 or unset | `0` | accepted | `minReplicaCount: <floor, default 1>`, `idleReplicaCount: 0` |
| `0` | `0` | **rejected** — `KedaIdleBelowMin` | — |
| `0` (no KEDA opt-in) | — | **rejected** — `InvalidScaleToZero` | — |

## Scale to zero on RawDeployment

Set `minReplicas: 0` and give the same component a typed KEDA autoscaler with at least one trigger (`class: KEDA` with zero triggers is itself rejected, with `KedaTriggersRequired`):

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 0
    maxReplicas: 4
    autoscaler:
      class: KEDA
      keda:
        triggers:
          - type: prometheus
            metadata:
              serverAddress: http://prometheus.monitoring.svc:9090
              query: sum(rate(request_success_total{namespace="llama-demo",isvc="llama-chat"}[2m]))
              threshold: "10"
        cooldownPeriod: 300
EOF
```

Inline trigger `metadata` is passed through verbatim to the ScaledObject, so — unlike [AutoscalerPolicy templates](/ome/docs/concepts/autoscaler_policy/), where endpoints are provider-owned — the `serverAddress` goes directly in the trigger. The full KEDA trigger surface (prometheus, cron, kafka, external, ...) is available.

The controller generates a ScaledObject named `scaledobject-<isvc>-<component>` targeting the component's Deployment:

```yaml
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: llama-chat-engine
  minReplicaCount: 0
  maxReplicaCount: 4
  cooldownPeriod: 300
  triggers:
    - type: prometheus
      # ...
```

From here the behavior is KEDA's: once every trigger has been inactive for `cooldownPeriod` (KEDA's default is 300 seconds; the OME field is optional and forwarded verbatim), KEDA scales the Deployment to zero. When a trigger reports activity again, KEDA scales it back up and hands control to the underlying HPA within the `minReplicaCount`–`maxReplicaCount` range.

Two things to be aware of at zero:

- **Wake-up is trigger-driven, not request-driven.** There is no activator buffering requests for a scaled-to-zero component; a request that arrives while the component is at zero fails until the trigger metric wakes it. Choose a trigger that observes demand from *outside* the component itself (a gateway or router metric, a queue depth, a cron window) — a metric served by the scaled-to-zero pods can never fire again.
- **The controller does not fight the scaler.** With any autoscaler class other than `None`, the ISVC controller leaves the Deployment's `spec.replicas` to the scaler, so the zero count persists across reconciles.

## Idle at zero with a non-zero active floor

To guarantee at least N replicas *while there is traffic* but still drop to zero when idle, keep `minReplicas` at your floor and set `idleReplicaCount: 0`:

```yaml
spec:
  engine:
    minReplicas: 2
    maxReplicas: 8
    autoscaler:
      class: KEDA
      keda:
        idleReplicaCount: 0
        triggers:
          - type: prometheus
            metadata:
              serverAddress: http://prometheus.monitoring.svc:9090
              query: sum(rate(request_success_total{namespace="llama-demo",isvc="llama-chat"}[2m]))
              threshold: "10"
```

When no trigger fires, KEDA scales the component to zero; on activity it scales straight back to the `minReplicaCount` floor of 2 and up to 8 under load. Because `minReplicas` here is not `0`, this shape never trips the `InvalidScaleToZero` gate at all — the only rule in play is `idleReplicaCount < minReplicas`.

## OMENative: use idleReplicaCount

On components running in [OMENative deployment mode](/ome/docs/concepts/omenative/), the generated ScaledObject targets the component's InferenceReplica, and the dispatch **floors `minReplicaCount` at 1** — an explicit `minReplicas: 0` passes admission (with the KEDA opt-in) but renders as `minReplicaCount: 1` and never reaches zero. [`kubectl ome autoscale explain`](/ome/docs/tasks/kubectl-ome-autoscale-explain) reports this as `ZERO unsupported`.

Scale-to-zero on OMENative therefore goes exclusively through `idleReplicaCount: 0` with `minReplicas` ≥ 1, exactly as in the previous section.

## Verify

Check the generated ScaledObject:

```bash
kubectl get scaledobject scaledobject-llama-chat-engine -n llama-demo \
  -o jsonpath='{.spec.minReplicaCount} {.spec.idleReplicaCount}'
```

The `ZERO` row of [`kubectl ome autoscale explain`](/ome/docs/tasks/kubectl-ome-autoscale-explain) gives the declared-side verdict: `yes` (eligible: KEDA with triggers), `-` (not requested), `unsupported` (requested on a mode that cannot honor it), or `invalid` (requested without the KEDA gate — `zero-invalid` — or with an idle count that is not below the minimum — `keda-idle-invalid`).

Once the component has actually scaled down, the Deployment (or InferenceReplica) reports zero replicas. In the explain report the mirrored `CUR/DES` counters carry a trailing `?` (for example `0/0?`): from status alone, a zero can mean deliberately scaled-to-zero or never started, so the report marks the evidence as ambiguous rather than guessing.

## Troubleshooting

**`InvalidScaleToZero: ... requires KEDA autoscaling`** — the component sets `minReplicas: 0` but neither carries `autoscaler.class: KEDA` itself nor is covered by the `ome.io/autoscalerClass: "keda"` annotation. Add the typed block to the same component that sets the zero.

**`engine: KedaIdleBelowMin: keda.idleReplicaCount must be < minReplicas`** — you combined `minReplicas: 0` with `idleReplicaCount`, or set an idle count at or above the floor. Use one shape or the other: `minReplicas: 0` alone, or `idleReplicaCount: 0` under a floor of at least 1.

**`engine: KedaTriggersRequired: class=keda requires at least 1 trigger`** — a typed KEDA block must declare its triggers inline; there is no default trigger.

**`AutoscalerAnnotationConflict`** — the legacy `ome.io/autoscalerClass` annotation is present together with an inline `autoscaler` block on some component. Remove the annotation; the typed block is authoritative.

**Admission passed but reconciliation fails with `minReplicas=0 requires typed KEDA with at least one trigger`** — the service relies on the legacy annotation alone. The annotation satisfies the admission gate but resolves to a KEDA autoscaler with no triggers, which the dispatch refuses. Move to the typed `autoscaler` block (or have the resolved ServingRuntime's component config supply one).

**On OMENative the ScaledObject shows `minReplicaCount: 1` despite `minReplicas: 0`** — expected: the OMENative dispatch floors the minimum at 1. Use `idleReplicaCount: 0` instead.

## Next steps

- [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy/) — reusable autoscaler templates and the full inline > policy > runtime > legacy > default precedence chain
- [Explain Effective Autoscaling](/ome/docs/tasks/kubectl-ome-autoscale-explain) — decode the `ZERO` column and the `zero-invalid` / `keda-idle-invalid` codes
- [KEDA ScaledObject documentation](https://keda.sh/docs/latest/reference/scaledobject-spec/) — upstream semantics of `minReplicaCount`, `idleReplicaCount`, and `cooldownPeriod`
