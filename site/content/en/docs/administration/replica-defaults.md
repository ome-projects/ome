---
title: "Replica Defaults"
linkTitle: "Replica Defaults"
weight: 8
date: 2026-09-27
description: >
  Where a component's effective minReplicas and maxReplicas come from when the InferenceService and its runtime leave them unset — the deploy.replicas block of the inferenceservice-config ConfigMap — why the filled values never appear on the stored object, and the sibling deploy.terminationGracePeriodSeconds default behind the 600-second termination grace on generated serving pods.
---

Every InferenceService component (`engine`, `decoder`, `router`) carries an
optional pair of replica bounds, `minReplicas` and `maxReplicas`, that the
workload generators and the autoscaler dispatch consume. Most services never
author them. This page documents the single defaulting chain that produces the
effective values in that case, and where to look for the result — it is **not**
on the object `kubectl get` shows you. The `deploy` block carries one more
pod-level default with the identical contract — the termination grace behind
the `terminationGracePeriodSeconds: 600` on generated serving pods — covered
in [its own section below](#the-sibling-default-pod-termination-grace).

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
`deploy` key of the `inferenceservice-config` ConfigMap, next to the
[sibling termination-grace default](#the-sibling-default-pod-termination-grace)
it ships in the same block. The chart ships:

```yaml
ome:
  controller:
    replicas:
      defaultMinReplicas: 1
      defaultMaxReplicas:
        engine: 3
        decoder: 3
        router: 2
    terminationGracePeriodSeconds: 600
```

which lands in the ConfigMap as:

```yaml
deploy: |-
  {
    "defaultDeploymentMode": "RawDeployment",
    "replicas": {
      "defaultMinReplicas": 1,
      "defaultMaxReplicas": {"engine": 3, "decoder": 3, "router": 2}
    },
    "terminationGracePeriodSeconds": 600
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

## The sibling default: pod termination grace

The `deploy` block carries a second pod-level default with the identical
contract: `terminationGracePeriodSeconds`, the window Kubernetes gives a pod
between SIGTERM and SIGKILL. It is why generated serving pods show a
`terminationGracePeriodSeconds: 600` nobody authored: the chart ships
`ome.controller.terminationGracePeriodSeconds: 600` (rendered into the
ConfigMap as shown above), and the same reconcile-time pass that fills the
replica bounds fills this value into every component pod spec that neither
the InferenceService nor its ServingRuntime set.

The resolution chain is [the one above](#the-resolution-chain), applied per
pod spec — first present value wins:

1. **Authored on the InferenceService** — each component spec embeds a full
   pod spec, so the field sits directly under the component:
   `spec.<component>.terminationGracePeriodSeconds`. In a multi-node shape
   the `leader` and `worker` blocks carry their own pod specs, and each one
   resolves independently — authoring the worker's grace still leaves the
   leader's and the component's own to be filled.
2. **Authored on the runtime** — the same field on the ServingRuntime's
   `engineConfig` / `decoderConfig` / `routerConfig`, carried in by the
   runtime merge.
3. **Operator configuration** — `deploy.terminationGracePeriodSeconds` in
   the `inferenceservice-config` ConfigMap.
4. **Nothing.** With the key unconfigured the field stays unset and the
   generated pod gets the standard Kubernetes default of `30` seconds.
   There is no baked-in OME fallback.

The field is a pointer at every level, so any explicit value counts as
authored and is never overwritten — none of the zero-value ambiguity of
`maxReplicas`. And unlike the OMENative-only `lifecycle` defaults that share
the `deploy` block, this fill is not gated on deployment mode or component
type: engine, decoder, and router pod specs all get it.

### Sizing it

The grace bounds how long a component may keep finishing in-flight work
after Kubernetes asks it to stop — on rollouts, scale-downs, node drains,
and evictions alike. The Kubernetes default of 30 seconds kills a pod
mid-request on every one of those events, which is why the chart ships ten
minutes instead. Size the value above the longest request the component is
expected to serve, and if the runtime performs an in-process drain on
SIGTERM, keep that drain strictly shorter than the grace so the runtime
finishes first.

A generous grace does not slow healthy shutdowns — SIGKILL at the deadline
is a ceiling, and a pod whose processes exit after SIGTERM is gone
immediately. It also does not delay the
[stuck-deletion escalation](/ome/docs/administration/stuck-deletion-recovery/):
its overdue clock starts from the pod's `deletionTimestamp`, which already
includes the pod's own grace, so the escalation never races a graceful
shutdown however long the window is.

### Overriding and removing the default

For one service, author the field on the component — it is part of the pod
spec, so it sits at the top level of the component block:

```yaml
spec:
  engine:
    terminationGracePeriodSeconds: 1800
```

For every service on a runtime, set the same field on the runtime's
`engineConfig` / `decoderConfig` / `routerConfig`. For the cluster default,
change `ome.controller.terminationGracePeriodSeconds` in the chart values —
or set it to `null`, which omits the key from the rendered `deploy` JSON
entirely and returns unauthored pods to the Kubernetes 30-second default
(case 4 above).

A configured value must be greater than zero. Like the replica defaults, the
controller validates it when it loads the ConfigMap: a non-positive value
fails the load with an error such as `invalid deploy config,
terminationGracePeriodSeconds must be > 0, got 0`, and InferenceService
reconciles fail until the ConfigMap is fixed. The cached ConfigMap read
described above applies here too, so an edit takes effect within the
`configCacheTTL` window.

### Where to see the effective value

Same contract as the replica bounds: the fill happens on reconcile-local
copies and is never written back, so `kubectl get inferenceservice -o yaml`
shows only what you authored. The webhook history above applies to this
field too — a service created under an older OME may carry a stamped
`terminationGracePeriodSeconds` in its stored spec, which counts as authored
(layer 1) and keeps winning over the operator default until you remove it.

The effective value is on the generated workload's pod spec:

```bash
# RawDeployment: the Deployment's pod template
kubectl get deployment llama-chat-engine -n llama-demo \
  -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}'

# any mode: the serving pods themselves
kubectl get pod <pod> -n llama-demo \
  -o jsonpath='{.spec.terminationGracePeriodSeconds}'
```

On MultiNode, the LeaderWorkerSet's leader and worker templates each carry
their own resolved value. With the chart default and nothing authored every
one of these reads `600`; with the operator default removed, `30`.

## Related pages

- [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy/) — how autoscaler
  configuration resolves. Replica bounds are a separate chain: policies
  consume the effective bounds documented here, they don't set them.
- [Explain Effective Autoscaling](/ome/docs/tasks/kubectl-ome-autoscale-explain/)
  — inspect the resolved bounds and scaler evidence per component.
