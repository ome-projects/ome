---
title: "Pace Rollouts with minReadySeconds"
linkTitle: "Pace Rollouts (minReadySeconds)"
weight: 20
date: 2026-09-26
description: >
  Require a newly Ready pod to hold Ready for a warm-up window before OMENative counts it as Available and lets a rollout drain the old pod or promote the Instance.
---

This page shows you how to use `lifecycle.minReadySeconds` to pace OMENative rollouts. The field requires a newly Ready pod to stay Ready for a minimum window before OMENative treats it as **Available** — and rollouts drain and promote only on Available pods. It is the OMENative counterpart of [`Deployment.spec.minReadySeconds`](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#min-ready-seconds) and applies the same per-pod rule.

The field lives in the `lifecycle` block of a component spec, so it applies only to components whose deployment mode resolves to **OMENative** — see [Deployment Modes and OMENative](/ome/docs/concepts/omenative/). On `RawDeployment` or `MultiNode` components the whole `lifecycle` block is a no-op.

Use it when Ready is a weaker signal than "safe to proceed": a model server whose readiness probe passes before caches are warm, or one that flaps Ready shortly after startup. Without a window, a SurgeThenDrain rollout drains the old pod the moment the new one turns Ready; with a window, the old pod keeps serving until the new one has *held* Ready for the whole window.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- An InferenceService component running in OMENative deployment mode (`spec.deploymentMode: OMENative`, the per-component `ome.io/deploymentMode` annotation, or an inferred leader/worker shape)
- `kubectl` configured to communicate with your cluster

## Ready vs Available

OMENative keeps two separate signals per pod:

- **Ready** is the health signal: the pod's `PodReady` condition is `True` (which includes the OME-managed serving readiness gate).
- **Available** is the pacing signal: the pod is in rotation on the component's headless Service **and**, when a positive `minReadySeconds` applies, its `PodReady` condition has been `True` continuously for at least that long. The check is per pod, against the `PodReady` condition's `lastTransitionTime` — exactly the `Deployment.spec.minReadySeconds` rule.

Unset or `0` means a pod is Available as soon as it is Ready.

Two things follow from Available being a pacing signal, not a traffic gate:

- **The window does not delay traffic to the new pod.** A pod enters the Service rotation when it turns Ready. What the window delays is the controller's willingness to act on it.
- **The rollout budgets stay held for the whole window.** A SurgeThenDrain rollout keeps its surge slot occupied (counted against `maxSurge`) while the replacement waits out the window, with old and new pods both serving; the old pod is drained and deleted only once the replacement has held Ready through the window. Non-surge strategies (`RecreatePod`, in-place) stamp an Instance complete only after its new pods clear the same bar, so the Instance keeps counting against `maxUnavailable` until then. Either way, a longer window means a slower, more conservative rollout — not a wider one.

The same bar gates newly created Instances (initial deploy and scale-up): an Instance's phase reaches `Ready` only after its pods have held Ready through the window.

A Ready flap inside the window re-arms it — the `lastTransitionTime` moves, so the pod must hold Ready for the full window again before the rollout proceeds. Once the drain of the old pod has started, however, a later flap on the replacement does not hold it out of service for another window; the window applies to the promotion decision, not to recovery afterwards.

## How the value is resolved

Highest priority first:

1. **The InferenceService** — `spec.<component>.lifecycle.minReadySeconds`. An explicit `0` counts as authored and wins, which is how you opt a single service out of a runtime or cluster default.
2. **The ServingRuntime** — the same `lifecycle.minReadySeconds` field inside the runtime's `engineConfig`, `decoderConfig`, or `routerConfig`. The runtime's component config is merged under the InferenceService's component spec, with the InferenceService's fields taking precedence.
3. **The cluster default** — `minReadySeconds` in the `deploy` block of the `inferenceservice-config` ConfigMap, filled in by the controller at reconcile time on every OMENative component that neither of the above sets.

There is no built-in default: with nothing configured at any level, pods are Available as soon as they are Ready. The cluster default is applied to a reconcile-local copy of the spec and is **never written back to your InferenceService** — `kubectl get inferenceservice -o yaml` always shows exactly what you authored. To see the resolved value, read the generated InferenceReplica (below).

Negative values are rejected: the CRD schema and the admission webhook enforce `>= 0` on the InferenceService, and the controller rejects a negative `deploy.minReadySeconds` when loading its configuration.

## Set it on an InferenceService

Add the field to the component's `lifecycle` block:

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 2
    lifecycle:
      minReadySeconds: 120
EOF
```

Every rollout of this engine now keeps each old pod serving until its replacement has held Ready for two minutes. The same field works under `spec.decoder.lifecycle` and `spec.router.lifecycle`; each component resolves independently.

## Set a default in the ServingRuntime

A runtime author can set the window once for every InferenceService that uses the runtime:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: my-runtime
spec:
  # ...
  engineConfig:
    lifecycle:
      minReadySeconds: 60
    # ...
```

An InferenceService that sets its own `lifecycle.minReadySeconds` (including an explicit `0`) overrides this.

## Set a cluster-wide default

Through the `ome-resources` Helm chart — the value ships commented out, so pacing is opt-in:

```yaml
ome:
  controller:
    # Applied to every OMENative component whose InferenceService and
    # ServingRuntime both leave lifecycle.minReadySeconds unset.
    minReadySeconds: 30
```

Or directly in the `deploy` key of the `inferenceservice-config` ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  deploy: |-
    {
      "defaultDeploymentMode": "RawDeployment",
      "minReadySeconds": 30
    }
```

Leaving the Helm value unset omits the field from the ConfigMap entirely; setting it to `0` renders an explicit `"minReadySeconds": 0`, which is a valid value that leaves pods Available as soon as Ready.

## Verify

The resolved value is projected onto the component's InferenceReplica as top-level `spec.minReadySeconds` (`0` means no window at any level):

```bash
kubectl get inferencereplica llama-chat-engine -n llama-demo \
  -o jsonpath='{.spec.minReadySeconds}'
```

During a rollout, watch the `AVAILABLE` column lag `READY` by the window:

```bash
kubectl get inferencereplicas -n llama-demo
NAME                COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
llama-chat-engine   engine      2         2         2       1           15m
```

The window-filtered counters appear in three places:

- `status.availableReplicas` on the InferenceReplica — Instances whose desired pod count is Available;
- `availablePodCounts` in the InferenceReplica's per-Instance status columns;
- `status.components.<component>.lifecycle.availableReplicas` on the InferenceService.

```bash
kubectl get inferenceservice llama-chat -n llama-demo \
  -o jsonpath='{.status.components.engine.lifecycle}' | jq
```

A pod crossing the end of its window generates no Kubernetes event, so the controller schedules its own re-reconcile for that instant — the counters catch up on their own, within the usual reconcile latency.

## Troubleshooting

**The field has no effect:** the component is not running in OMENative mode. The `lifecycle` block is only read by the OMENative backend; check the mode with the rules in [Deployment Modes and OMENative](/ome/docs/concepts/omenative/).

**`kubectl get isvc -o yaml` doesn't show the cluster default:** by design. The default is applied at reconcile time and never written to your object. Read the resolved value from the InferenceReplica's `spec.minReadySeconds`.

**A rollout looks stuck with the new pod Ready:** it is most likely waiting out the window — compare the `AVAILABLE` column (`status.availableReplicas`) against `READY` (`status.readyReplicas`) on the InferenceReplica. The wait ends `minReadySeconds` after the pod's last Ready transition; a pod that keeps flapping Ready restarts its window on every flap and will hold the rollout until it stays up.

## Next steps

- [Deployment Modes and OMENative](/ome/docs/concepts/omenative/) — how a component resolves to OMENative and what the `lifecycle` status block reports
- [Pause and Resume a Rollout](/ome/docs/tasks/pause-and-resume-a-rollout/) — holding a rollout entirely instead of pacing it
