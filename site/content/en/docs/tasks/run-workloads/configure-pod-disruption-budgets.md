---
title: "Configure PodDisruptionBudgets"
linkTitle: "PodDisruptionBudgets"
weight: 20
date: 2026-09-27
description: >
  Control the PodDisruptionBudget OME generates for each InferenceService component: the per-component minAvailable/maxUnavailable fields, the per-mode operator defaults, and how OMENative pod counts and mode switches are handled.
---

OME generates one [PodDisruptionBudget](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) (PDB) for every InferenceService component whose deployment mode resolves to **RawDeployment** or **OMENative**, protecting the component's pods against voluntary disruptions such as node drains. The PDB lives in the service's namespace, is named after the component workload (`<isvc>-engine`, `<isvc>-decoder`, `<isvc>-router`), and carries a controller owner reference to the InferenceService, so it is deleted with the service.

A default `ome-resources` chart install ships `maxUnavailable: 1` for both modes, so every RawDeployment and OMENative component gets a PDB out of the box. **MultiNode** components get no OME-managed PDB.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- An InferenceService whose components run in `RawDeployment` or `OMENative` deployment mode — see [Deployment Modes and OMENative](/ome/docs/concepts/omenative/)
- `kubectl` configured to communicate with your cluster

## How the budget resolves

Each component's budget comes from exactly one of two sources — they are never merged:

1. **The component spec.** `minAvailable` and `maxUnavailable` sit directly on `spec.engine`, `spec.decoder`, and `spec.router`, alongside `minReplicas` (not in a nested block). Because a ServingRuntime's `engineConfig` / `decoderConfig` / `routerConfig` is merged under the InferenceService's component spec, a runtime can also supply these fields; the InferenceService's own values win field by field.
2. **The operator's per-mode default** — the `rawDeployment` / `omeNative` policies in the `podDisruptionBudget` key of the `inferenceservice-config` ConfigMap. This applies only when the merged component spec sets *neither* field.

If neither source configures a budget, the component gets no PDB — and an OME-owned PDB left over from earlier configuration is deleted.

Whichever source wins must set **exactly one** of the two fields. Each value is either a non-negative integer or a percentage from `0%` to `100%`. The admission webhook rejects an InferenceService that sets both fields on one component, and the controller rejects a ConfigMap policy that sets both or neither. There is no default injected in code; the shipped defaults come entirely from the chart values below.

## Set a per-component budget

Add either field to the component spec:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 3
    maxReplicas: 3
    minAvailable: 2
  router:
    minReplicas: 2
    maxUnavailable: 50%
```

Each declared component resolves independently: here the engine keeps at least 2 pods through voluntary disruptions, the router tolerates half its pods being evicted, and a decoder (if declared) would fall back to the operator default.

Setting either field — even `minAvailable: 0`, which permits every eviction — makes the component ignore the operator default entirely. That is the per-service opt-out when a cluster default exists.

## Set the per-mode operator defaults

Through the `ome-resources` Helm chart:

```yaml
ome:
  controller:
    # The chart's shipped defaults. Set either mode to null to disable
    # its default; components with their own fields are unaffected.
    podDisruptionBudget:
      rawDeployment:
        maxUnavailable: 1
      omeNative:
        maxUnavailable: 1
```

Or directly in the `podDisruptionBudget` key of the `inferenceservice-config` ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  podDisruptionBudget: |-
    {
      "rawDeployment": {"maxUnavailable": 1},
      "omeNative": {"minAvailable": "90%"}
    }
```

The key is parsed strictly: unknown or duplicate fields, and a mode policy that sets both fields or neither, fail the config load — which fails every InferenceService reconcile until fixed. Omitting a mode (or setting it to `null`) disables that mode's default. The ConfigMap is re-read on reconcile (through a short-lived cache), so changes take effect without restarting the controller.

## What OME writes for RawDeployment

The resolved budget is copied verbatim onto the PDB — `minAvailable` or `maxUnavailable`, integer or percentage — and Kubernetes evaluates it against the Deployment's pods. The selector matches the pod label the raw Deployment stamps, `app: <component workload name>`:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: llama-chat-engine
  namespace: llama-demo
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: llama-chat-engine
```

## What OME writes for OMENative

OMENative pods are managed directly by OME rather than by a built-in workload controller, and Kubernetes only honors an integer `minAvailable` for such pods. OME therefore normalizes whichever budget form you configured into an absolute `minAvailable`, computed from the component's desired pod count:

```
desired pods = InferenceReplica spec.replicas × pods per instance
```

A single-pod component contributes 1 pod per instance. A multi-pod (leader/worker) component contributes 1 leader plus the worker size — an engine with `worker.size: 2` and 4 replicas has 12 desired pods.

The forms convert as follows:

- **Integer `minAvailable`** is used as is.
- **Percentage `minAvailable`** is resolved against the desired pod count, rounding up: `90%` of 12 pods → `minAvailable: 11`.
- **`maxUnavailable`** (integer or percentage, the percentage rounding up) is subtracted from the desired pod count, floored at 0: `25%` of 12 pods → `minAvailable: 9`; the shipped default `maxUnavailable: 1` → `minAvailable: 11`.

The result is recomputed every reconcile against the committed replica count, so the PDB follows scale events, including autoscaler-driven ones. The selector matches every pod OMENative manages for the component:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: llama-chat-engine
  namespace: llama-demo
spec:
  minAvailable: 9
  selector:
    matchLabels:
      ome.io/inferenceservice: llama-chat
      component: engine
      ome.io/managed-by: OMENative
```

During a surge rollout the selector also matches surge pods while `minAvailable` stays derived from the desired count, so rollouts get extra eviction headroom rather than less.

## Switching between RawDeployment and OMENative

Both modes use the same PDB name, so switching a component's deployment mode reuses the object — only the selector must flip from one mode's pods to the other's. The reconciler treats this as a cutover:

- While the PDB's live selector still matches the **old** mode's pods, it is held in place — updates and even removal (when no budget resolves for the new mode) are deferred — so the pods still serving keep their disruption protection.
- The hold releases once the new mode's workload is fully ready: the Deployment or InferenceReplica reports every desired replica Ready and Available with its status caught up to its spec. The selector (and normalized budget) then flip in place.

The corollary: the new mode's pods are not covered by the OME-managed PDB until the cutover completes.

## Verify

```bash
kubectl get pdb -n llama-demo
NAME                MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS   AGE
llama-chat-engine   9               N/A               1                     5m
```

`kubectl get pdb <name> -o yaml` shows the resolved spec, and `status.disruptionsAllowed` tells you whether an eviction would currently be admitted.

## Troubleshooting

**No PDB appears:** the component's deployment mode is MultiNode (never gets one), or no budget resolves — the component sets neither field and the ConfigMap's policy for the component's mode is absent or `null`.

**Reconcile fails with `exactly one of minAvailable or maxUnavailable must be set` although your InferenceService sets only one field:** the ServingRuntime's component config supplies the *other* field, and the runtime merge leaves both set on the merged spec (the webhook validates only your InferenceService, not the merge result). Use the same field the runtime uses, or remove the runtime's.

**Reconcile fails with a conflict naming another owner:** a PDB with the component's name already exists and is not controlled by this InferenceService. OME never adopts or overwrites foreign objects — the whole component reconcile stops at this preflight check until you delete or rename the conflicting PDB.

**Manual edits to the generated PDB don't stick:** the PDB is reconciled back to the resolved budget and canonical selector. Change the component spec or the ConfigMap instead.

**Every InferenceService fails with `unable to parse "podDisruptionBudget" config json`:** the ConfigMap key is malformed — a typo'd or duplicate field, or a mode policy setting both fields or neither. Fix the key; no controller restart is needed.

## Next steps

- [Deployment Modes and OMENative](/ome/docs/concepts/omenative/) — how a component resolves to RawDeployment, MultiNode, or OMENative
- [OMENative Update Strategies](/ome/docs/concepts/omenative-update-strategies/) — rollout pacing budgets (`maxUnavailable` there bounds rollout batches, not evictions)
- [Specifying a Disruption Budget](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) — Kubernetes PDB semantics
