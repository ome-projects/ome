---
title: "Bring Your Own Autoscaler"
linkTitle: "Bring Your Own Autoscaler"
weight: 20
date: 2026-09-27
description: >
  Disable OME's autoscaler for a component with autoscaler class External and drive replicas from an operator-owned scaler via the published scaleTargetRef.
---

This page shows you how to take over scaling of one InferenceService component with a scaler you operate yourself — your own HorizontalPodAutoscaler, your own KEDA ScaledObject, or a custom controller. You declare `class: External` on the component's `autoscaler` block, and OME steps aside: it removes any scaler it manages for that component, stops writing the replica count, and publishes the canonical scale target on `status.components.<component>.scaleTargetRef` for your scaler to point at.

> **Alpha:** The per-component `autoscaler` block, the `External` class, and the `scaleTargetRef` status contract are alpha and may change without notice.

The workflow applies to components in **RawDeployment** or **OMENative** deployment mode. MultiNode components have no autoscaler dispatch and publish no scale target.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- An InferenceService whose component runs in RawDeployment or OMENative mode
- `kubectl` configured to communicate with your cluster
- The scaler you plan to run — an HPA or ScaledObject you author yourself, or a controller with RBAC on the target's `scale` subresource (see below)

## What `class: External` changes

Three things, all per component:

- **OME removes its own scaler and creates none.** Any OME-managed HorizontalPodAutoscaler or KEDA ScaledObject for the component is deleted. Objects OME does not own are never touched — an HPA or ScaledObject without OME's controller owner reference is silently preserved, so your scaler is safe even if it occupies OME's canonical object name.
- **OME stops owning the replica count.** On OMENative, the component's InferenceReplica is created with `spec.replicas` = the component's effective `minReplicas` (default 1); on every later reconcile the live value is preserved, so what your scaler writes through the `/scale` subresource survives. On RawDeployment, the live `Deployment.spec.replicas` is carried into the reconciler's target state and excluded from its diff. Your scaler is the authoritative writer.
- **Status reports the hand-off.** `status.components.<component>.autoscaler` reports `class: External` and `managedBy: external`. The mirrored scaler fields — `conditions`, `currentReplicas`, `desiredReplicas`, `lastScaleTime` — stay empty or zero: OME has no way to read an external scaler's state. Read scaling state from your own scaler or from the target object.

`minReplicas` and `maxReplicas` are **not enforced** for an External component — they are forwarded only to scalers OME generates, and here there is none. `minReplicas` still seeds the initial count on OMENative create; after that, bounds live in your scaler's own configuration.

The inline block sits at the top of the per-component resolution chain (inline > policy > runtime > legacy > default), so it wins over everything else. `External` is an inline-only statement — an [AutoscalerPolicy](/ome/docs/concepts/autoscaler_policy/) can only render `KEDA` or `HPA` templates.

## Step 1: Declare the class

Set the component's inline `autoscaler` block:

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 2        # initial count on create; not enforced afterwards
    maxReplicas: 16       # not enforced for class External
    autoscaler:
      class: External
EOF
```

No other fields are required — `class: External` takes no `keda` or `hpa` configuration. The same block works under `spec.decoder.autoscaler` and `spec.router.autoscaler`; each component hands off independently.

If the component previously ran with `class: HPA` or `class: KEDA` (or the CPU=80% HPA default), the next reconcile deletes that OME-managed scaler.

## Step 2: Read the published scale target

The controller publishes the canonical scale target on the InferenceService status:

```bash
kubectl get inferenceservice chat -n prod \
  -o jsonpath='{.status.components.engine.scaleTargetRef}' | jq
```

```json
{
  "apiVersion": "ome.io/v1beta1",
  "kind": "InferenceReplica",
  "name": "chat-engine"
}
```

What it refers to depends on the component's deployment mode:

| Deployment mode | `apiVersion` | `kind` | `name` |
|-----------------|--------------|--------|--------|
| OMENative | `ome.io/v1beta1` | `InferenceReplica` | `<isvc>-<component>` |
| RawDeployment | `apps/v1` | `Deployment` | the component's workload name (`<isvc>-<component>`) |
| MultiNode | — | — | field absent: no target is published |

The namespace is always the InferenceService's own. The field is published for **every** reconciled RawDeployment or OMENative component, whatever the autoscaler class — so you can read the target before switching classes, and it stays stable afterwards.

The reference is the same GroupKind `kubectl scale` would target. Both kinds expose a standard `scale` subresource, which is the supported write path — the InferenceReplica is a controller-only resource (its admission webhook and RBAC gate direct spec edits, and the projector overwrites user-set fields on reconcile), but `/scale` writes to `spec.replicas` go through and survive:

```bash
kubectl scale inferencereplica chat-engine -n prod --replicas=4
```

## Step 3: Point your scaler at the target

Any scaler that speaks the scale subresource works. For example, an HPA you own and author yourself:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: chat-engine-byo        # not OME's canonical name — see the note below
  namespace: prod
spec:
  scaleTargetRef:
    apiVersion: ome.io/v1beta1
    kind: InferenceReplica
    name: chat-engine
  minReplicas: 2
  maxReplicas: 16
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

The built-in HPA controller and KEDA already hold RBAC on every `scale` subresource. A custom controller needs it granted explicitly — the subresource must be named in the rule:

```yaml
- apiGroups: ["ome.io"]
  resources: ["inferencereplicas/scale"]
  verbs: ["get", "update", "patch"]
# RawDeployment components instead:
# - apiGroups: ["apps"]
#   resources: ["deployments/scale"]
#   verbs: ["get", "update", "patch"]
```

> **Note on names:** OME's canonical scaler names are `<target name>` for the HPA and `scaledobject-<target name>` for the ScaledObject. With `class: External` OME never deletes an object it does not own, even at those names — but if you later switch the component back to `class: HPA` or `class: KEDA`, the dispatch refuses to adopt or delete a foreign object at its canonical name and holds reconciliation with an ownership error until you remove it. Give your own scaler a different name.

## Verify

Confirm the hand-off on the InferenceService status:

```bash
kubectl get inferenceservice chat -n prod \
  -o jsonpath='{.status.components.engine.autoscaler}' | jq
```

```json
{
  "class": "External",
  "managedBy": "external",
  "specSource": "isvc"
}
```

`managedBy: external` means OME publishes the target and stays out. Zero `currentReplicas` / `desiredReplicas` and empty `conditions` are by design here, not a fault — watch your own scaler and the target object instead:

```bash
kubectl get inferencereplica chat-engine -n prod
kubectl get hpa chat-engine-byo -n prod
```

With the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome), [`kubectl ome autoscale status`](/ome/docs/tasks/kubectl-ome-autoscale-status/) shows the same evidence per component (`CLASS External`, `MANAGED-BY external`), and [`kubectl ome autoscale explain`](/ome/docs/tasks/kubectl-ome-autoscale-explain/) cross-checks the declared class against the reported target.

## `External` vs `None`

Both classes make OME create no scaler and delete only its own HPA / ScaledObject, leaving foreign objects alone. They differ in who owns the replica count:

| | `External` | `None` |
|---|---|---|
| `status.…autoscaler.managedBy` | `external` | `none` |
| Who writes the replica count | Your scaler, via `/scale`; OME preserves the live value on every reconcile | OME: on OMENative it re-stamps `minReplicas` every reconcile; on RawDeployment it re-asserts an explicitly declared positive `minReplicas` floor |
| Meaning | An operator-owned scaler drives this component | Autoscaling is disabled; OME (or the proportional-policy coordinator) pins the count |

The practical consequence: a scaler pointed at a `class: None` component **gets overwritten** — on OMENative the projector re-stamps `minReplicas` on every reconcile, and on RawDeployment the controller re-asserts any declared floor. `External` is the class that makes bring-your-own scaling durable.

## Troubleshooting

**`scaleTargetRef` is absent:** the component runs in a mode that publishes no target (MultiNode), or the controller has not reconciled the component yet. Check the deployment mode first.

**Replicas snap back to `minReplicas`:** the resolved class is not `External` for this component. Check `status.components.<component>.autoscaler.class` and `specSource` — an `autoscaler` block may be resolving from another layer than you expect.

**Switching back to `class: HPA` or `class: KEDA` holds with an ownership error:** your own scaler occupies OME's canonical object name. Delete or rename it; the dispatch never adopts or deletes an object it does not own.

## Next steps

- [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy/) — reusable KEDA / HPA templates for OME-managed scaling
- [Check Autoscaling Status with kubectl-ome](/ome/docs/tasks/kubectl-ome-autoscale-status/) — read the per-component evidence, with optional exact live reads
- [Request a Transient Scale](/ome/docs/tasks/request-a-transient-scale/) — one guarded manual `/scale` request, and why it is transient under every ownership class
