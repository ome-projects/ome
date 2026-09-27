---
title: "Replica Defaults"
linkTitle: "Replica Defaults"
weight: 8
date: 2026-09-27
description: >
  Where a component's effective minReplicas and maxReplicas come from when the InferenceService and its runtime leave them unset — the deploy.replicas block of the inferenceservice-config ConfigMap — and why the filled values never appear on the stored object.
---

Every InferenceService component (`engine`, `decoder`, `router`) carries an
optional pair of replica bounds, `minReplicas` and `maxReplicas`, that the
workload generators and the autoscaler dispatch consume. Most services never
author them. This page documents the single defaulting chain that produces the
effective values in that case, and where to look for the result — it is **not**
on the object `kubectl get` shows you.

## The resolution chain

At reconcile time, after merging the InferenceService with its resolved
ServingRuntime, the controller fills each component's replica bounds in this
order — first present value wins:

1. **Authored on the InferenceService** — `spec.<component>.minReplicas` /
   `spec.<component>.maxReplicas`.
2. **Authored on the runtime** — the same fields on the ServingRuntime's
   `engineConfig` / `decoderConfig` / `routerConfig`, carried in by the
   runtime merge.
3. **Operator configuration** — the `deploy.replicas` block of the
   `inferenceservice-config` ConfigMap in the OME control-plane namespace.
4. **Nothing.** The OME binary has no built-in replica defaults. If the
   operator configuration omits a value, the field simply stays unset and
   each downstream consumer applies its own reading — the autoscaler
   dispatch, for example, floors an unset `minReplicas` at `1` and collapses
   an unset `maxReplicas` to the minimum.

Two details of "authored" are worth knowing:

- `minReplicas` is a pointer field, so an explicit `minReplicas: 0` (a
  scale-to-zero floor under KEDA) counts as authored and is never overwritten
  by the default.
- `maxReplicas` is a plain integer where `0` means unset, so there is no way
  to author a zero maximum: an explicit `maxReplicas: 0` is indistinguishable
  from an absent field and gets filled like one, and the admission webhook
  rejects negative values.

## Configuring the defaults

The `ome-resources` chart renders `ome.controller.replicas` verbatim into the
`deploy` key of the `inferenceservice-config` ConfigMap. The chart ships:

```yaml
ome:
  controller:
    replicas:
      defaultMinReplicas: 1
      defaultMaxReplicas:
        engine: 3
        decoder: 3
        router: 2
```

which lands in the ConfigMap as:

```yaml
deploy: |-
  {
    "defaultDeploymentMode": "RawDeployment",
    "replicas": {
      "defaultMinReplicas": 1,
      "defaultMaxReplicas": {"engine": 3, "decoder": 3, "router": 2}
    }
  }
```

`defaultMinReplicas` is one shared floor for all three components;
`defaultMaxReplicas` is per component. Every field is optional and each one
defaults independently: omitting a field disables defaulting of that value
only, and omitting the whole `replicas` block disables replica defaulting
entirely (case 4 above). There is no silent fallback to baked-in numbers.

Two constraints on configured values:

- **Every configured value must be greater than zero.** The controller
  validates the block when it loads the ConfigMap; a non-positive value fails
  the load with an error such as `invalid deploy config,
  replicas.defaultMinReplicas must be > 0`, and InferenceService reconciles
  fail until the ConfigMap is fixed.
- **A filled maximum is raised to the effective minimum.** If the component's
  `minReplicas` (authored, or just filled) exceeds the configured
  `defaultMaxReplicas`, the filled maximum is raised to match, so the
  defaulting never manufactures a `min > max` conflict that validation would
  reject. An *authored* `maxReplicas` is never adjusted — authoring
  `minReplicas: 5` with `maxReplicas: 3` is simply rejected at admission.

The reconcile path reads the ConfigMap through a short-lived cache
(`ome.controller.configCacheTTL`, default `30s`), so an edit takes effect
within that window without restarting the controller.

## Why `kubectl get` doesn't show the filled values

The defaulting runs at **reconcile time**, on the reconcile-local merged
component specs — after the runtime merge and before workload generation, the
autoscaler dispatch, and the canary partition math, so every consumer in the
same pass sees the same resolved bounds. Nothing is written back to the stored
InferenceService: `kubectl get inferenceservice -o yaml` keeps showing exactly
the fields you authored, and changing the operator defaults later changes the
effective bounds of existing services on their next reconcile without touching
their specs.

Earlier OME versions instead stamped these defaults into the object at
admission time through a mutating webhook. That defaulter has been removed —
but an InferenceService created under such a version may still carry the
stamped values in its stored spec, and the chain above treats them as authored
on the InferenceService (layer 1), so they keep winning over runtime and
operator configuration until you remove them.

To see the effective bounds, read the generated scaler objects — they carry
the filled values directly: the HorizontalPodAutoscaler's
`minReplicas`/`maxReplicas` on RawDeployment, or the KEDA ScaledObject's
`minReplicaCount`/`maxReplicaCount`. With the chart defaults above, a service
that authors no bounds gets an engine HPA of `1`/`3`.

Note that the `RANGE` row of
[`kubectl ome autoscale explain`](/ome/docs/tasks/kubectl-ome-autoscale-explain/)
does **not** include this layer: the plugin is a read-only client that never
reads the operator ConfigMap, so its declared range covers only the
InferenceService and runtime layers plus the dispatch floor (an unset pair
shows as `1..1`, while the controller actually dispatches `1..3` under the
chart defaults). The generated HPA or ScaledObject is the authoritative view
of the config-filled bounds.

## Related pages

- [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy/) — how autoscaler
  configuration resolves. Replica bounds are a separate chain: policies
  consume the effective bounds documented here, they don't set them.
- [Explain Effective Autoscaling](/ome/docs/tasks/kubectl-ome-autoscale-explain/)
  — inspect the resolved bounds and scaler evidence per component.
