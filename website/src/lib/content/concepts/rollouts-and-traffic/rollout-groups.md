---
title: Rollout groups
description: Rollout groups roll an InferenceService's OMENative components out as one, so prefill and decode change together, by canary, blue-green or rolling update, with metric-gated canary steps.
since: v1.3
---

A rollout group rolls a change out to components of an [InferenceService](../serving/inference-services.md) as a unit. Put the engine and the decoder in one group, and prefill and decode move to each new revision together, by blue-green, rolling update or canary. A canary moves capacity in steps, and a step can wait for a timer, for you, or for Prometheus analysis that advances the canary or rolls it back.

You declare up to three groups in `spec.rollout.groups`. A component can be in only one group, and a component in no group rolls out on its own.

Groups need [OMENative](../omenative/overview.md) declared on the InferenceService, with `spec.deploymentMode: OMENative` or the component's `ome.io/deploymentMode` annotation. A component that's OMENative only because of its `leader` and `worker`, or its runtime, doesn't count, as [Opt in to OMENative](../architecture/deployment-modes.md#opt-in-to-omenative) explains. A single-node component runs as RawDeployment by default, where a change is a plain Deployment update, with no group.

## Roll prefill and decode together

This InferenceService serves the pre-configured model `mistral-7b-instruct-v0-2`. Its prefill-decode runtime, `srt-mistral-7b-instruct-pd`, runs a router, an engine and a decoder, and needs GPU nodes with RDMA NICs, as [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md) shows. One group holds the engine and the decoder:

```yaml title="mistral-pd.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: mistral-pd
  namespace: mistral-demo
spec:
  deploymentMode: OMENative
  model:
    name: mistral-7b-instruct-v0-2
  runtime:
    name: srt-mistral-7b-instruct-pd
  engine:
    minReplicas: 1
    maxReplicas: 1
  decoder:
    minReplicas: 1
    maxReplicas: 1
  router:
    minReplicas: 1
    maxReplicas: 1
  rollout:
    groups:
      - components: [engine, decoder]
```

Apply the file:

```bash
kubectl apply -f mistral-pd.yaml
```

```output
inferenceservice.ome.io/mistral-pd created
```

- When a change gives both a new revision, they roll out together by [blue-green](#choose-a-progression), since the group sets no progression.
- The router is in no group, so it rolls out on its own.
- While a component's update waits, `status.components.<component>.lifecycle.rolloutHold` names the gate that holds it. [Lifecycle status](../architecture/deployment-modes.md#lifecycle-status) lists the gates.
- An edit to `spec.rollout` during a rollout [applies to the next one](rollout-policy.md#when-the-reference-is-resolved).

Each group can set:

| Field | Description |
| --- | --- |
| `components` | Required. One to three of `router`, `engine` and `decoder`. |
| `canary`, `blueGreen` or `rollingUpdate` | The progression. Set at most one. See [Choose a progression](#choose-a-progression). |
| `policyRef` | Alpha, and off by default. A [RolloutPolicy](rollout-policy.md) that supplies the progression when the group sets none. |
| `soak` | In a [sequence](#roll-components-one-at-a-time), how long to wait between components. |
| `maintainRatio` | For a blue-green or rolling-update group of two or more components. Keeps them in step, and their replica ratio within `tolerance` percent. See [Rolling update](#rolling-update). |

## Choose a progression

A group rolls out blue-green unless it sets another progression.

| Progression | How the group rolls out | Set |
| --- | --- | --- |
| Blue-green | Its components start together. Each updates at most 25% of its replicas at a time, rounded up, while both revisions serve requests. | Nothing, or `blueGreen: {}` |
| Rolling update | Each component moves within its own `maxSurge` or `maxUnavailable` budget, and `maintainRatio` can keep them in step. | `rollingUpdate` |
| Canary | Steps move a share of capacity to the new revision, and a step can wait for you or for metric analysis. | `canary` |

In a blue-green or rolling-update group, a failed Instance in any component marks the whole group failed. OME records a `CoordinationGroupFailed` Warning event on the InferenceService, but doesn't stop the other components.

### Rolling update

`maxSurge` and `maxUnavailable` set each component's own budget, as a count or a percentage, 25% by default. With the default `SurgeThenDrain` [update strategy](../architecture/omenative-update-strategies.md), a component starts a new Instance before it stops an old one, within `maxSurge`. With any other strategy, it moves within `maxUnavailable`. An edit that changes one component's pod settings, such as its runner image, must change the others' too, or the webhook rejects it (`RollingUpdateLockstepViolation`).

This group moves four engines and two decoders one Instance at a time, and keeps their 2:1 ratio:

```yaml
spec:
  engine:
    minReplicas: 4
    maxReplicas: 4
  decoder:
    minReplicas: 2
    maxReplicas: 2
  rollout:
    groups:
      - components: [engine, decoder]
        rollingUpdate:
          maxSurge: 1
          maxUnavailable: 0
        maintainRatio:
          tolerance: 25
```

- `maxSurge: 1` lets each component start one new Instance at a time. Under another strategy, `maxUnavailable: 0` would stall it.
- `maintainRatio` keeps the two in step. OME holds a component while it's more than 25% further through its rollout than the other. It also holds an update that would move their serving ratio more than 25% from 2:1. The held component's `rolloutHold` gate is `Ratio`.

`tolerance` runs from 0 to 100, and 0 allows no drift. When you leave it out, it's 5, the `ome-resources` value `ome.controller.coordination.defaultRatioTolerancePercent`. Above 50, the webhook warns that the check is effectively off (`RatioToleranceTooHigh`). A component whose update strategy is `InPlaceIfPossible` or `InPlaceOnly` skips the check.

### Canary

A canary group moves all its components through one canary's steps. Each step's `capacity` applies to each component: at `"25%"`, a quarter of the engine's replicas and a quarter of the decoder's, rounded up, run the new revision. The step waits until both are ready. When the InferenceService declares both, the engine and the decoder are one canary unit, so a canary group that names one must name both. The router is a unit of its own, and one canary group can hold both units.

```yaml
spec:
  rollout:
    groups:
      - components: [engine, decoder]
        canary:
          steps:
            - capacity: "25%"
              traffic: 10
              pause: {}
            - capacity: "100%"
              traffic: 100
```

The first step waits until you promote it. To promote a step, or to roll the canary back to its stable revision, see [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md). The router can run a canary of its own at the same time, under `groupOrdering: Concurrent`. The alpha `kubectl ome rollout promote` and `rollback` commands then refuse, since they act only on a rollout with one canary group. [Canary progression](../../reference/rollouts/canary-progression.md) describes the steps, and [Canary metric analysis](../../reference/rollouts/canary-analysis.md) describes their Prometheus checks.

!!! warning "Requests follow capacity, not the traffic weight"
    OME records each step's `traffic` weight in status, but its routes don't apply it. The new revision gets requests in proportion to its share of ready pods, which `capacity` sets.

## Change the engine-decoder wire protocol

`spec.rollout.pairingProtocol` is a token you choose, such as `kv-v1`, for the wire protocol between the engine and the decoder. Change it only when that protocol changes. A new token gives both components a new revision, and OME labels their pods with `ome.io/pairing-protocol`.

To move from one token to another, roll the engine and the decoder out in one blue-green group. If `mistral-pd` runs with `pairingProtocol: kv-v1`, this spec moves it to `kv-v2`:

```yaml
spec:
  engine:
    minReplicas: 2
    maxReplicas: 2
  decoder:
    minReplicas: 2
    maxReplicas: 2
  rollout:
    pairingProtocol: kv-v2
    groups:
      - components: [engine, decoder]
        blueGreen: {}
```

While the change rolls out, OME keeps at least one engine and decoder pair with the same token serving. An update that would take the last such pair out of service waits, with the `rolloutHold` gate `Pairing`. With one engine and one decoder, the change can briefly leave no pair, so this spec runs two of each.

The webhook rejects a change from one token to another when the engine and the decoder roll out separately or by rolling update (`PairingProtocolChangeUncoordinated`). It always accepts setting a token for the first time, or clearing it.

## Roll components one at a time

When the groups that aren't canaries are two or more blue-green groups of one component each, they form a sequence. OME rolls a sequence out one group at a time, in list order. With this `spec.rollout`, `mistral-pd` rolls out the decoder first, then the engine:

```yaml
spec:
  rollout:
    groups:
      - components: [decoder]
        soak: 10m
      - components: [engine]
```

When a change gives both a new revision, OME:

1. Rolls out the decoder, while the engine waits.
2. Waits out the 10-minute soak once the decoder finishes.
3. Rolls out the engine.

OME skips a component that the change leaves alone. Each wait lasts the longest `soak` of any group but the last, counted from when the component before it last finished a rollout.

While the decoder rolls out, the engine's `rolloutHold` shows why it waits:

```bash
kubectl get inferenceservice mistral-pd -n mistral-demo -o custom-columns='GATE:.status.components.engine.lifecycle.rolloutHold.gate,REASON:.status.components.engine.lifecycle.rolloutHold.reason'
```

```output
GATE         REASON
Sequential   Sequential waiting on decoder
```

During the soak, the reason is `Sequential.Soak: engine waiting out the 10m0s soak after decoder`.

If a component's rollout fails, the components after it wait, and OME records a `CoordinationGroupFailed` Warning event. The sequence goes on once another change, such as a fixed image or the previous one, makes the failed component healthy.

## How groups run together {#what-groupordering-promises}

Each canary group rolls out on its own, alongside the other groups. The other groups roll out at the same time, unless they form a [sequence](#roll-components-one-at-a-time).

`spec.rollout.groupOrdering` changes only what the webhook accepts, not how the groups roll out. `Sequential`, the default, accepts one group or a sequence alone. Any other list of two or more groups, such as a canary group beside another group, needs `groupOrdering: Concurrent`, or the webhook rejects it with `GroupOrderingNotHonored`.

## What admission rejects

When the webhook rejects a rollout, `kubectl` prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, then a message that ends in a reason in parentheses:

| Reason | Cause |
| --- | --- |
| `CoordinationRequiresOMENative` | A grouped component doesn't declare OMENative on the InferenceService. A canary group gets `CanaryRequiresOMENative`. |
| `InvalidComponentInCoordinationGroup`, `OrphanCoordinationGroup` | A group names a component other than `router`, `engine` or `decoder`, or one that the InferenceService doesn't declare. |
| `DuplicateComponentInCoordinationGroups` | A component is in two groups. |
| `GroupOrderingNotHonored` | Under `Sequential`, two or more groups other than a sequence alone. |
| `SoakNotHonored` | A `soak` outside a sequence. |
| `OrderNotHonored` | A group sets `order`, which no progression applies. |
| `InvalidRollingUpdateInteger`, `InvalidRollingUpdatePercent`, `ZeroBudgetPacingUnstartable` | A negative count, a string that isn't a percentage from `"0%"` to `"100%"`, or two budgets that resolve to zero. |
| `RollingUpdateLockstepViolation` | An edit changes the pod settings of only some of a rolling-update group's components. |
| `PairingProtocolChangeUncoordinated` | `pairingProtocol` changes tokens while the engine and the decoder roll out separately or by rolling update. |
| `RolloutPolicyRefUnsupported` | A group sets `policyRef` while RolloutPolicy is off. |

[Canary progression](../../reference/rollouts/canary-progression.md#what-admission-rejects) lists the rules for canary groups.

## Next steps

- [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md): move a canary past a step that waits for you, or return it to its stable revision.
- [`kubectl ome rollout`](../../reference/kubectl-ome/rollout.md): see where a rollout is, and the gate a canary step waits at.
- [Canary progression](../../reference/rollouts/canary-progression.md): every field of a canary step, and how a step advances.
- [Canary metric analysis](../../reference/rollouts/canary-analysis.md): gate a step on Prometheus metrics.
- [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md): hold a rollout in progress, and let it go on.
- [Rollout policy](rollout-policy.md): define a progression once, for groups to share. It's alpha, and off by default.
- [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md): run the engine and the decoder on OMENative.
