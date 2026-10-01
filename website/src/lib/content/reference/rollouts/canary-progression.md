---
title: Canary progression
description: Canary steps move a share of each component's Instances to a new revision, then advance at once, on a timer, on a promote or on Prometheus analysis.
since: v1.3
---

A canary moves a new revision onto a share of a component's [Instances](../../concepts/omenative/instances.md) at a time, and waits at a gate after each step. The gate opens at once, after a timed pause, when you promote the step, or when Prometheus analysis passes. A rollback, from you or from a failed analysis, returns every Instance to the stable revision. You declare a canary in the `canary` block of a [rollout group](../../concepts/rollouts-and-traffic/rollout-groups.md) in an [InferenceService](../../concepts/serving/inference-services.md). To act on a running canary, see [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md).

!!! warning "Capacity sets the canary's share of requests"
    Requests reach every ready Instance of the component, whatever its revision, so the new revision's share of requests follows its share of ready Instances. OME records each step's `traffic` weight in status, but doesn't route by it. To limit the requests a new revision gets, limit its `capacity`. [Traffic weight in status](#traffic-split) has the details.

A canary group's components must run on [OMENative](../../concepts/architecture/deployment-modes.md). Set `spec.deploymentMode: OMENative`, or `ome.io/deploymentMode: OMENative` in the component's `annotations`. A multi-node component needs this too, even though it runs on OMENative by default.

## Example

The InferenceService `chat` rolls its engine out in three steps:

```yaml title="chat.yaml"
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
    minReplicas: 4
    maxReplicas: 4
  rollout:
    groups:
      - components: [engine]
        canary:
          scaleDownDelaySeconds: 60
          readyTimeout: 20m
          steps:
            - capacity: "25%"
              traffic: 10
              pause:
                duration: 10m
            - capacity: "50%"
              traffic: 50
              pause: {}
            - capacity: "100%"
              traffic: 100
```

| Step | `capacity` | Instances on the new revision | `traffic` | Gate |
| --- | --- | --- | --- | --- |
| 1 | `"25%"` | 1 of 4 | 10 | Timed: the step advances 10 minutes after its new Instance is ready. |
| 2 | `"50%"` | 2 of 4 | 50 | Manual: the step waits for the alpha `kubectl ome rollout promote`. |
| 3 | `"100%"` | 4 of 4 | 100 | Immediate: the canary completes 60 seconds after all 4 new Instances are ready. |

Each step first waits for its new Instances to be ready, and the canary fails if a step waits longer than 20 minutes. [`kubectl ome rollout status`](../kubectl-ome/rollout.md#status) shows the canary's step, gate and phase.

A canary runs when a change, such as a new image, gives the engine a new revision while an earlier one serves. So the first revision of `chat` rolls out without a canary, and adding the group starts nothing until the next change. The canary keeps the plan it started with: an edit to the `canary` block waits for the next run. The alpha `kubectl ome rollout repin` applies it to the running canary, as [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md) shows.

### Canary prefill and decode together

When an InferenceService declares both an engine and a decoder, a canary group that names one must name both, and the two roll out as one canary. The InferenceService `chat-pd` uses the model and the prefill-decode runtime that [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md) creates, on GPU nodes with RDMA NICs:

```yaml title="chat-pd.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat-pd
  namespace: prod
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  runtime:
    name: srt-llama-3-2-1b-instruct-pd
  engine:
    minReplicas: 4
    maxReplicas: 4
  decoder:
    minReplicas: 2
    maxReplicas: 2
  router:
    minReplicas: 1
    maxReplicas: 1
  rollout:
    groups:
      - components: [engine, decoder]
        canary:
          steps:
            - capacity: "50%"
              traffic: 50
              pause: {}
            - capacity: "100%"
              traffic: 100
```

Each step's `capacity` applies to each component. Step 1 moves 2 of the 4 engine Instances and 1 of the 2 decoder Instances to the new revision. It waits for all three to be ready, then for a promote. The router is in no group, so it rolls out on its own and balances requests across both revisions.

## Fields

| Field | Default | What it sets |
| --- | --- | --- |
| `steps` | Required | The steps, in order. |
| `steps[].capacity` | Required | How many of each component's Instances run the new revision: a percentage such as `"25%"`, or an integer. The last step's percentage must be `"100%"`. See [New-revision capacity](#new-revision-capacity). |
| `steps[].traffic` | Required | The new revision's weight in status, from 0 to 100. It can't fall from one step to the next, must be 100 at the last step, and above 0 needs a `capacity` above zero. |
| `steps[].pause` | None | A `duration` makes a timed gate, and `pause: {}` a manual one. See [How a step advances](#how-a-step-advances). |
| `steps[].analysis` | None | Prometheus checks that gate the step. See [Canary metric analysis](canary-analysis.md). |
| `readyTimeout` | The operator's, `15m` with the chart | How long a step can wait for its new Instances. Above zero. See [`readyTimeout`](#readytimeout). |
| `scaleDownDelaySeconds` | None | The canary completes at least this many seconds after the last step's new Instances are ready. Until then, a rollback still works and a last-step analysis keeps sampling. It isn't a drain window: a `"100%"` last step has already replaced the old Instances, and a rollback doesn't wait for it. |
| `prometheus` | The operator's default source | Where analysis queries go: `providerRef` or `serverAddress`. See [Canary metric analysis](canary-analysis.md#where-the-metrics-come-from). |

## New-revision capacity

`capacity` takes one of these forms:

| Form | Example | Instances on the new revision |
| --- | --- | --- |
| A quoted whole-number percentage, from `"0%"` to `"100%"` | `"25%"` | That share of the desired replicas, rounded up: `"25%"` of 3 is 1. |
| An unquoted integer | `2` | That many, up to the desired replicas. |
| A quoted number without `%` | `"2"` | None: admission rejects it. |

A component's desired replicas are its `minReplicas`. Set `minReplicas` on each component in the group, in the InferenceService itself. Without it, a step can wait for Instances that never start until its [ready timeout](#readytimeout), or start its gate before any new Instance is ready. [Set replica defaults](../../guides/operate-ome/set-replica-defaults.md) explains why defaults from the runtime or the chart don't count.

A step replaces Instances; it doesn't add any. The rest of the desired replicas stay on the previous revision, and Instances that an autoscaler adds above them run the new revision. Each component works out its own count, as the [prefill-decode example](#canary-prefill-and-decode-together) shows. For a multi-node component, capacity counts whole Instances, each a leader and its workers.

## How a step advances

A step starts by moving its capacity to the new revision, and waits in `Pending` until every component in the group has that many new Instances ready. At that moment, OME records the step's traffic, and the step's gate decides when the next step starts:

| Gate | The step sets | The step advances when |
| --- | --- | --- |
| Immediate | Only `capacity` and `traffic` | Its new Instances are ready. |
| Timed | `pause` with a `duration` | The duration has passed since its new Instances were ready. |
| Manual | `pause: {}` | You request promotion with `kubectl ome rollout promote`, subject to the [final-step limitation](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#final-step-promotion). |
| Analysis | `analysis` | Its metric checks pass, after the optional `initialDelay` and no sooner than the step's `pause.duration`. See [Canary metric analysis](canary-analysis.md). |

GATE in `kubectl ome rollout status` shows these names. If a serving step's ready Instances drop below its count, the step keeps its phase and recorded traffic while it waits for capacity to recover. Its warm-up and bake clocks keep running. A separate capacity-wait clock bounds the outage, as [`readyTimeout`](#readytimeout) describes. At the last step, a gate holds the canary in `Paused`; after it passes, `Promoting` waits for any remaining `scaleDownDelaySeconds`.

## Phases

The canary's phase is in `status.components.<component>.rolloutPhase` of the group's primary component: the router when the group includes it, otherwise the engine. PHASE in `kubectl ome rollout status` shows it:

| Phase | Meaning |
| --- | --- |
| `Pending` | The step waits for its new Instances to be ready. |
| `Canarying` | The step's new Instances are ready, and OME checks its gate. |
| `Paused` | The canary waits at a gate, including the last step, or holds after a [repin](../kubectl-ome/rollout.md#repin). |
| `Promoting` | The last step's gate has passed, and the canary waits for any remaining [`scaleDownDelaySeconds`](#fields). |
| `Stable` | The canary completed. |
| `Failed` | Capacity or analysis ran past its [ready timeout](#readytimeout), or a rollback couldn't find the stable revision. The canary records the cause in `canary.failed.reason`. |
| `RollingBack` | A rollback is moving Instances back to the stable revision. |
| `RolledBack` | Every Instance is back on the stable revision. |

## While a canary runs

| When | The canary |
| --- | --- |
| A change gives the group a newer revision | Starts again at step 1 with the newest revision, and keeps the same stable revision. This also clears a `Failed` hold. |
| You pause the rollout | Holds its step, but its clocks keep running, so it can advance or fail as soon as you resume. See [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md). |
| You run the alpha `kubectl ome rollout rollback`, or [its analysis](canary-analysis.md#how-a-sample-is-judged) rolls it back | Records 0% for the new revision at once, and moves every Instance back to the stable revision. The component stays on the stable revision until a change produces a revision other than those two. See [Roll back the canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#roll-back-the-canary). |
| A step runs past its [ready timeout](#readytimeout) | Goes to `Failed`. A stalled analysis with `onInconclusive: RollbackOnStall` rolls it back instead. |

## Traffic weight in status {#traffic-split}

When a step's new Instances are ready, OME records the step's `traffic` as the new revision's weight and the rest as the stable revision's. It writes both in `status.components.<component>.traffic` of the primary component:

| Field | Value |
| --- | --- |
| `revisionName` | The name of the revision's own Service, `<isvc>-<component>-rev-<hash>`. |
| `percent` | The revision's weight. A revision with a weight of 0 is left out. |
| `tag` | `canary` for the new revision, `stable` for the previous one. |
| `latestRevision` | `true` for the new revision. |
| `pairingProtocol` | The revision's `spec.rollout.pairingProtocol`, when it has one. |

`kubectl ome rollout status` shows the weight in TRAFFIC, as `target% -> observed%`, and `explain` shows the canary's revisions: see [`kubectl ome rollout`](../kubectl-ome/rollout.md#status-output-fields). At step 1 of the [example](#example), status records 10% for the new revision, but it has a quarter of the ready Instances and gets about a quarter of the requests.

## `readyTimeout`

The ready timeout bounds how long a step's capacity gate stays unmet, including a loss of capacity after the step starts serving, and how long an analysis step goes without a conclusive result. The first of these that's set applies:

| Order | Setting | Notes |
| --- | --- | --- |
| 1 | The `ome.io/rollout-ready-timeout` annotation on the InferenceService | A duration, such as `30m`. OME ignores zero or less, and admission warns (`RolloutTimeoutTooShort`) about a value under a minute. |
| 2 | `readyTimeout` in the group's `canary` | Above zero. |
| 3 | `defaultReadyTimeout` in the `rollout` block of the `inferenceservice-config` ConfigMap | `15m` with the ome-resources chart (`ome.controller.rollout.defaultReadyTimeout`). The kustomize manifests don't set it. |

With none set, capacity waits and inconclusive analysis have no timeout. The capacity timeout counts from `status.components.<component>.canary.capacityWaitSince`, set when the current capacity wait starts and cleared once capacity is met. A later dip starts a fresh capacity budget without restarting the step's warm-up or bake. For an analysis, the timeout counts from the last conclusive sample, or from when the step's new Instances were ready if there's none yet.

When the capacity timeout expires, the phase is `Failed`. The canary keeps its Instances and recorded traffic, and `promote` refuses. A stalled analysis with `onInconclusive: Hold` also parks in `Failed`; `RollbackOnStall` rolls back instead. Recover by applying a fix that produces a new target revision, [rolling back](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#roll-back-the-canary), or explicitly [retrying the same revision](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#retry-the-same-revision) after fixing the cause. Capacity or metrics recovery alone doesn't clear a failed hold.

The failure is recorded in `status.components.<component>.canary.failed`:

| Field | What it records |
| --- | --- |
| `reason` | `CapacityTimeout`, `AnalysisStalled`, or `StableRevisionMissing` when a rollback can't find its retained stable ControllerRevision. |
| `time` | When the canary entered the failed hold. It stays unchanged while the hold remains. |

## What admission rejects

The webhook rejects a canary with a message that names the field and gives one of these reasons in parentheses:

| Reason | Cause |
| --- | --- |
| `CanaryRequiresOMENative` | A component in the group doesn't declare OMENative. `deploymentMode=""` means it sets neither the field nor the annotation. |
| `CanaryInvalid` | A step or `readyTimeout` breaks a rule in [Fields](#fields), or a `capacity` isn't one of the forms in [New-revision capacity](#new-revision-capacity). |
| `CanaryInvalid` | The group names a component other than `router`, `engine` or `decoder`, or one the InferenceService doesn't declare, or sets `maintainRatio`. |
| `CanaryInvalid` | The InferenceService has an engine and a decoder, and the group names one: `a canary group naming either must name both`, or for the decoder, `must include its entrypoint Component "engine"`. |
| `MultipleCanaryGroups` | Two canary groups cover the same unit: the router, or the engine and decoder. |
| `GroupOrderingNotHonored` | A canary group sits beside other groups without `spec.rollout.groupOrdering: Concurrent`. |
| `SoakNotHonored` | The canary group sets `soak`. |
| `InvalidDuration` | The `ome.io/rollout-ready-timeout` annotation isn't a duration. |
| `AnalysisInvalid` | An analysis breaks a rule in [Canary metric analysis](canary-analysis.md#what-admission-checks). |

The API server itself rejects more than 20 steps, or a `traffic` outside 0 to 100. [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md#what-admission-rejects) lists the rules for every group.

## Related pages

- [Canary metric analysis](canary-analysis.md): the metric checks an analysis step runs.
- [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md): act on a canary at its gate.
- [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md): hold a rollout, canary included.
- [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md): run the engine and decoder that the prefill-decode example rolls out.
- [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md): how groups divide a rollout between components.
- [Deployment modes](../../concepts/architecture/deployment-modes.md): how to opt in to OMENative.
- [`kubectl ome rollout`](../kubectl-ome/rollout.md): the commands that show and act on a canary.
- [GroupCanary](../api/ome.v1beta1.md#ome-io-v1beta1-GroupCanary) in the API reference.
