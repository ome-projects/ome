---
title: "OMENative Update Strategies"
linkTitle: "Update Strategies"
weight: 31
description: >
  How OMENative physically replaces an Instance's pods on a template change — SurgeThenDrain, RecreatePod, InPlaceIfPossible, InPlaceOnly — how partition, maxSurge, and maxUnavailable pace the roll, and where the defaults come from.
---

When the pod template of an [OMENative](/ome/docs/concepts/omenative) component changes — a new image, a runtime edit, an environment tweak — OMENative rolls each **Instance** from its current revision to the new one. `lifecycle.updateStrategy` on the component controls the **physical replacement mechanism** for one Instance's pods and how many Instances may be moving at once:

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
    minReplicas: 4
    maxReplicas: 8
    lifecycle:
      updateStrategy:
        type: SurgeThenDrain
        rollingUpdate:
          maxSurge: "25%"
```

The block is read only for components that resolve to the OMENative deployment mode; on any other mode it is ignored (`RawDeployment` components use the standard `deploymentStrategy` field instead). It is also a different layer from `spec.rollout`: a rollout group's progression (blue-green, canary, its own `rollingUpdate` budgets) decides *when* revisions advance and how traffic shifts, while `lifecycle.updateStrategy` decides *how each Instance's pods are physically swapped* once an update is admitted.

## The four strategies

| `type` | Mechanism | Capacity during the swap | Paced by |
|--------|-----------|--------------------------|----------|
| `SurgeThenDrain` (default) | Create a replacement pod, wait for it to be Ready, drain the old pod out of traffic, delete it | No dip; +1 pod per moving Instance | `maxSurge` |
| `RecreatePod` | Drain and delete the pods, then recreate them | The Instance is offline for the swap | `maxUnavailable` |
| `InPlaceIfPossible` | Patch container images on the live pods when the change is image-only; otherwise recreate | No extra pod; brief traffic withdrawal during the patch | `maxUnavailable` |
| `InPlaceOnly` | Patch in place when the change is image-only; **refuse** the update otherwise | Same as above | `maxUnavailable` |

### SurgeThenDrain

The default, and what an unset `type` runs as. For a single-pod Instance, OMENative creates the new-revision pod at the Instance's alternate pod-name slot, waits for it to become Ready (and, when `lifecycle.minReadySeconds` is set, to stay Ready for that long — Available), then drains the old pod out of the traffic rotation and deletes it. The Instance keeps serving throughout, at the cost of one extra pod while it moves.

For a multi-pod (leader/worker) Instance, the surge is per gang: a whole replacement gang is created at a fresh Instance index with its own PodGroup, gang-scheduled, brought to Ready, and only then is the source gang drained. This is why Instance indices can become sparse over time — the replacement keeps its new index.

### RecreatePod

Always tears down and rebuilds: the Instance's pods are drained, deleted, and recreated from the new revision, and the Instance's `incarnation` counter is bumped. The Instance is out of service between the delete and the new pods becoming Ready, so the roll is paced by `maxUnavailable`.

### InPlaceIfPossible and InPlaceOnly

An update is **in-place capable** only when the difference between the recorded running revision and the target is regular-container images and nothing else. The comparison uses the recorded revision's PodSpec, not the live pod. Two consequences:

- An init-container image change is *not* in-place capable (kubelet cannot re-run init containers), and routes to recreate.
- On a multi-pod (leader/worker) Instance, both in-place strategies always run as recreate: eligibility compares only the leader's PodSpec, so an in-place patch could not safely roll a worker-only change.

`InPlaceIfPossible` falls back to `RecreatePod` for any change that is not in-place capable. `InPlaceOnly` instead refuses such an update on a single-pod Instance: the controller emits a Warning event with reason `InPlaceUpdateNotPossible` and the Instance stays on its current revision until you either make an image-only change or switch the strategy.

Before patching a pod in place, OMENative flips the `ome.io/serving` readiness gate to `False` so EndpointSlice withdraws the pod from traffic first, then restores it once the patched containers are Ready. This is controlled by:

```yaml
lifecycle:
  updateStrategy:
    type: InPlaceIfPossible
    inPlaceUpdateStrategy:
      markNotReadyDuringLifecycle: true   # the default
```

During that window the component's `status.components.<component>.lifecycle.servingReplicas` dips below `readyReplicas` — the pods are technically Ready but deliberately out of rotation. In-place updates do **not** bump the Instance's `incarnation`; recreates do.

## Pacing: `rollingUpdate`

```yaml
lifecycle:
  updateStrategy:
    type: SurgeThenDrain
    rollingUpdate:
      partition: 2
      maxSurge: "25%"
      maxUnavailable: 1
```

**Each strategy reads exactly one budget.** `SurgeThenDrain` never takes a serving pod offline before its replacement is up, so it is gated on `maxSurge` — the number of extra Instances allowed above the component's replica count — and never consults `maxUnavailable`. Every non-surge strategy (`RecreatePod`, `InPlaceIfPossible`, `InPlaceOnly`) takes pods out of service to move them, so it is gated on `maxUnavailable` — the number of Instances allowed to be out of service at once — and never consults `maxSurge`. A budget on the arm your strategy does not read is inert.

Both budgets accept an absolute integer (`2`) or a percent string (`"25%"`). Percentages resolve at reconcile time as `ceil(replicas × percent / 100)`, so `"25%"` on 4 replicas allows 1 Instance in flight and the budget scales as the component scales. An unset budget means **no per-component cap** from this layer — though when the component participates in a rollout coordination group, the group-wide pacing ceiling still applies, and the effective cap is the minimum of the two layers.

When a fresh start would exceed the budget, the Instance simply waits; the component reports why under `status.components.<component>.lifecycle.rolloutHold`:

```yaml
rolloutHold:
  gate: Budget
  reason: 'per-Component surge budget 1 exhausted (would become 2)'
  target: llama-chat-engine-7c9f21
  since: "2026-09-25T08:14:02Z"
```

### `partition`

`partition` holds back that **number of Instances** on their current (old) revision — a count, not an index threshold, because Instance indices go sparse after migrations and gang surges. The lowest-indexed old-revision Instances are held; an Instance already converging to the target is never held (it must finish). `0` or unset holds nothing.

Partition is primarily driven for you: a canary rollout group stamps a projected partition each step to stage the old/new split, and that projected value overrides an authored `partition` for the duration of the run (an explicit projected `0` releases every Instance). Author it directly only when you want a manual hold outside the rollout machinery.

## Where the defaults come from

For each field, the first layer that sets it wins:

1. **The InferenceService component** — `spec.engine.lifecycle.updateStrategy` (likewise `decoder`, `router`).
2. **The ServingRuntime** — the same block on the runtime's `engineConfig` / `decoderConfig` / `routerConfig`. The merge is field-level: an InferenceService that sets only `type` inherits the runtime's budgets.
3. **The cluster default** — the `deploy.updateStrategy` block of the `inferenceservice-config` ConfigMap, keyed per component. The reconciler applies it to the merged spec at reconcile time; it is **never written back** to your object, so `kubectl get -o yaml` keeps showing exactly what you wrote.
4. **Fixed fallback** — with nothing configured anywhere, an unset `type` runs as `SurgeThenDrain` and unset budgets mean no per-component cap.

The `ome-resources` chart ships the cluster default under `ome.controller.updateStrategy` — `SurgeThenDrain` with 25% budgets for all three components:

```yaml
# charts/ome-resources/values.yaml
ome:
  controller:
    updateStrategy:
      router:
        type: SurgeThenDrain
        maxSurge: 25%
        maxUnavailable: 25%
      engine:
        type: SurgeThenDrain
        maxSurge: 25%
        maxUnavailable: 25%
      decoder:
        type: SurgeThenDrain
        maxSurge: 25%
        maxUnavailable: 25%
```

Note the config block is flat (`maxSurge` directly under the component entry), unlike the API, where budgets nest under `rollingUpdate`. Both budgets are configured even though a strategy only reads one, so that overriding a component's `type` — on the runtime or the service — does not also require supplying the other arm's budget: the reconciler fills **only the budget the resolved strategy reads** (the surge arm for `SurgeThenDrain` or an unset type, the unavailability arm otherwise), and only where the merged spec left it unset. A field that would never be read is left off your effective spec rather than sitting there looking like a bound.

The block is validated when the operator loads its configuration: an unrecognized `type` is rejected, as is any budget that is not an integer or percentage or that resolves to zero or less — a zero budget would deny every Instance and no rollout could ever start. Removing a component's entry (or the whole block) disables cluster defaulting for it; there is no built-in default behind it beyond the fixed fallback above.

## Editing the strategy mid-roll

`updateStrategy` is not part of the revision payload, so editing it **triggers no rollout** and retargets nothing in flight. Each in-flight update attempt pins the strategy it opened with on the Instance's `operation.strategy` and runs to completion on that mechanism — a surge already spent is not unwound, an in-place patch under way is not re-dispatched as a recreate. The edit takes effect on each Instance at its next admitted attempt.

The one exception is a **Failed** Instance: its preserved operation pins nothing, precisely so that editing the strategy works as the rescue lever — an Instance wedged by `InPlaceOnly` on a non-image change retries under `SurgeThenDrain` as soon as you switch the type.

## Related resources

- [Deployment Modes and OMENative](/ome/docs/concepts/omenative) — what OMENative and Instances are, and how a component opts in
- [Rollout Policy](/ome/docs/concepts/rollout_policy) — the group/progression layer that decides when revisions advance and how traffic shifts
- [Traffic Policy](/ome/docs/concepts/traffic_policy) — traffic splitting across revisions
