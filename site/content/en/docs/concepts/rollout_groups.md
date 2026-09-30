---
title: "Rollout Groups"
linkTitle: "Rollout Groups"
weight: 33
description: >
  Declare which InferenceService components roll out together, one at a time, or as independent concurrent rollouts with spec.rollout.groups and groupOrdering.
---

`spec.rollout.groups[]` is the single rollout surface for an InferenceService: a list of **rollout groups**, each naming the components that roll **together** and, optionally, the progression that drives them. There is no separate coordination-vs-canary API — canary is just one progression a group may choose, and whether the *list order* sequences the groups is the `groupOrdering` field.

This page covers the **structure**: group membership, what list order promises, and which shapes admission rejects. The semantics of each progression are documented elsewhere — canary steps and gates in [Promote or Roll Back a Canary](/ome/docs/tasks/promote-or-rollback-a-canary), reusable progression bodies in [Rollout Policy](/ome/docs/concepts/rollout_policy).

Rollout groups are only meaningful for components managed by the [OMENative deployment mode](/ome/docs/concepts/omenative); admission rejects a group naming a component in any other mode.

## Anatomy of a group

```yaml
spec:
  rollout:
    groups:
      - components: [engine, decoder]   # roll together, as one coupled unit
        blueGreen: {}                   # optional: omitted also means blueGreen
```

| Field | Meaning |
|---|---|
| `components` | 1–3 of `router`, `engine`, `decoder`. These components roll **together**. Required. |
| `canary` / `blueGreen` / `rollingUpdate` | The progression, a presence-based one-of: at most one may be set. **Omitted defaults to blueGreen** — `components: [engine]` alone is a valid group. |
| `policyRef` | Names a [RolloutPolicy](/ome/docs/concepts/rollout_policy) that supplies the progression. For every shape rule on this page, a ref-carrying group counts as its **declared** progression kind. |
| `soak` | Wait after this group completes before the next begins. Only honored in the one-at-a-time shape below. |
| `maintainRatio` | Bounds cross-component replica-ratio drift while the group rolls — see [maintainRatio](#maintainratio-bounding-replica-ratio-drift) below. Meaningful only on multi-component blueGreen/rollingUpdate groups; rejected on canary groups. |
| `order` | **Rejected when non-empty.** No progression applies a within-group sequence — the components in a group advance together. |

Three rules govern membership, enforced at admission:

- A component may appear in **at most one** group, canary groups included (`DuplicateComponentInCoordinationGroups`).
- Every member must be **declared** on the InferenceService — a group naming an absent component could never complete (`OrphanCoordinationGroup`).
- Every member must be **OMENative** (`CoordinationRequiresOMENative` / `CanaryRequiresOMENative`).

Components **not listed in any group roll independently** — that is the default; an InferenceService with no `spec.rollout` at all rolls every component on its own. The list holds at most 3 groups.

## Rolling components one at a time

To roll components one at a time, put each in its **own single-component group, in the desired order**, with blueGreen (or no) progression:

```yaml
spec:
  rollout:
    groups:
      - components: [decoder]
        soak: 10m               # wait after decoder completes, before the next group
      - components: [engine]
      - components: [router]
```

This is the one shape whose cross-group ordering the engine actually **enforces**: a run of two or more single-component blueGreen groups collapses into an internal sequential state machine that surges and flips one component fully before starting the next.

`soak` is the wait *after* a group completes, before the next group begins; a soak on the **last** group is ignored (nothing follows it). One subtlety: the sequential machine carries a **single** soak value applied between *every* consecutive component — the per-group soaks of the non-final groups collapse to their **maximum**, so declaring `10m` on one boundary and `2m` on another yields a 10-minute wait at both.

Because a soak anywhere else would be silently dropped, admission rejects it there instead (`SoakNotHonored`): on a canary group, or on any group when the rollout is not such a run of single-component blueGreen groups.

## groupOrdering: what list order promises

`spec.rollout.groupOrdering` declares what the order of `groups[]` means:

- **`Sequential`** (the default — an unset field, including on objects written before the field existed, means Sequential): list order is a promise. Group N reaches completion before group N+1 begins.
- **`Concurrent`**: list order promises nothing. The groups are independent rollouts running at the same time on their disjoint components, each with its own progression, gates, and rollback.

### Sequential admits only the shape it can enforce

The engine enforces cross-group order **only** for the one-at-a-time shape above. Under Sequential, admission therefore rejects any *other* multi-group list — a canary group, a rollingUpdate group, or a multi-component group anywhere in the list — rather than accepting an ordering that would be silently dropped (`GroupOrderingNotHonored`):

```
spec.rollout.groups: groups[] is ordered (group N completes before group N+1
begins), but that is enforced only for a run of single-Component blueGreen
groups; groups[1] is a canary group, so the groups would run concurrently and
the declared order would be dropped — set spec.rollout.groupOrdering:
Concurrent if the groups are independent and running them at the same time is
what you want (GroupOrderingNotHonored)
```

A **single** group makes no cross-group promise, so it is always admitted under Sequential regardless of its shape — one canary group, one engine+decoder pair group, one rollingUpdate group are all fine without declaring anything.

### Concurrent declares the groups independent

Setting `groupOrdering: Concurrent` admits every multi-group shape Sequential rejects. It is what lets one InferenceService run, say, a router canary and an engine canary as unrelated rollouts:

```yaml
spec:
  rollout:
    groupOrdering: Concurrent
    groups:
      - components: [router]
        canary:
          steps:
            - capacity: "50%"
              traffic: 20
              pause: {}
            - capacity: "100%"
              traffic: 100
      - components: [engine, decoder]
        canary:
          steps:
            - capacity: "25%"
              traffic: 10
            - capacity: "100%"
              traffic: 100
```

Each concurrent canary advances through its own steps independently — separate step counters, separate gates, separate rollback — because admission guarantees the groups drive disjoint components.

Concurrent relaxes **only** the cross-group ordering rule. Everything else on this page still applies: `order` stays rejected, duplicate membership stays rejected, and the canary unit rules below still hold.

### Declaration, not execution

`groupOrdering` changes what admission accepts, **not** how the engine executes. Execution is derived from the shape of the list:

- A run of two or more single-component blueGreen groups **always** collapses to the sequential machine — even under `Concurrent`.
- Any other list always runs its groups concurrently — under `Sequential` that shape simply never gets admitted.
- Canary groups never join the sequential run: under `Concurrent`, a list mixing canary groups with two or more single-component blueGreen groups runs each canary independently while the blueGreen groups still fold into one sequential run.

## Canary groups roll whole units

Canary traffic is driven through a unit's **entrypoint** component, which constrains which component sets a canary group may name:

- The **router** is its own unit.
- **Engine and decoder are one unit** whenever the InferenceService declares both: a canary group naming either must name both (`CanaryInvalid`), because splitting a prefill/decode pair across a canary boundary can leave a new-protocol prefill with no pairable decoder.
- A unit may be driven by **at most one** canary group — two ladders contending for one unit's revision and step counter are rejected (`MultipleCanaryGroups`). Two canary groups on *different* units (router + engine) are fine under `Concurrent`.

## maintainRatio: bounding replica-ratio drift

A multi-component group rolls its members **together**, but not in lockstep: each component's Instances are swapped on their own cadence, so a prefill/decode pair sized 4:2 can sit at 4:1 live capacity while a decoder swap is in flight. `maintainRatio` bounds that drift:

```yaml
spec:
  rollout:
    groups:
      - components: [engine, decoder]
        blueGreen: {}
        maintainRatio:
          tolerance: 25    # max % drift from the ratio at rollout start
```

When the run opens, the engine snapshots each member's desired replica count as the **ratio anchor** (visible at `status.rolloutCoordination.groups[].observedRatio.original`). While the group rolls, each step that would change live capacity — surging a new-revision pod in, draining an old one out — is checked pairwise: the projected **serving** capacity ratio (pods actually in the traffic rotation, not merely running) of every pair of members must stay within `anchor × (1 ± tolerance/100)`. With a 4:2 anchor (ratio 2.0) and `tolerance: 25`, the live engine:decoder ratio must stay within [1.5, 2.5].

A step that would leave the band is **paused, not failed**: the component holds, re-evaluates every reconcile, and proceeds once the pools rebalance — typically the lagging peer catching up. The refusal is reported under `status.components.<component>.lifecycle.rolloutHold` with gate `Ratio`:

```yaml
rolloutHold:
  gate: Ratio
  reason: 'surge would skew cross-Component serving ratio past tolerance'
  target: llama-chat-engine-7c9f21
  since: "2026-09-25T08:14:02Z"
```

The guard keys on the **field**, not the progression: it works the same on a blueGreen and a rollingUpdate group. It is ignored on single-component groups (no peer to skew against) and rejected on canary groups (`CanaryInvalid`) — the canary engine does not enforce it.

The band is enforced at whole-pod granularity, with two pragmatic escapes so a roll that can only proceed by a minimal step is not wedged: on a balanced pool, one surge is always admitted (the extra pod is transient and cannot starve a peer — this is also why an explicit `tolerance: 0` still lets a SurgeThenDrain roll advance one pod at a time), and one drain is admitted when its overshoot is rounding-error-sized (within twice the band). A skew beyond that — the 4:2 pair above dropping to 4:1 — holds until you widen the tolerance.

### Where the tolerance comes from

- **An explicit `tolerance`** (0–100) is used verbatim — including an explicit `0`, which pins the ratio exactly.
- **Omitted**, the operator-configured default fills it in: `coordination.defaultRatioTolerancePercent` in the `inferenceservice-config` ConfigMap.
- **Neither configured** — the binary has no built-in number — the group rolls with **no drift bound**: `maintainRatio: {}` under an unconfigured default enforces nothing, rather than silently inheriting a baked-in value.

The default fills in only for groups that set `maintainRatio` at all; a group without the field never gets a ratio guard, whatever the ConfigMap says.

The `ome-resources` chart ships the default as `5`:

```yaml
# charts/ome-resources/values.yaml
ome:
  controller:
    coordination:
      # Fills maintainRatio.tolerance when a group omits it. An explicit
      # per-group value (including 0) always wins. Remove the key to leave
      # the default unconfigured.
      defaultRatioTolerancePercent: 5
```

### In-place update strategies bypass the gate

Components whose [update strategy](/ome/docs/concepts/omenative-update-strategies) is `InPlaceIfPossible` or `InPlaceOnly` skip the ratio gate entirely. An in-place update drains a pod and returns the **same pod** (mark not-ready → patch → mark ready), so the net capacity change is effectively zero; running the gate would project that transient dip as a permanent loss and indefinitely over-block the smaller member of a pair — draining one decoder of the 4:2 pair projects 4:1 = 4.0, outside [1.5, 2.5], forever. The bypass is announced with a Normal `RatioGateBypassed` event (and a metric) so it is visible why the gate did not run; the strategy's `maxUnavailable` budget still bounds how many pods an in-place roll may pull from rotation at once.

## What admission rejects: summary

| Shape | Reason |
|---|---|
| Non-empty `groups[i].order`, under either ordering | `OrderNotHonored` |
| Multi-group list under Sequential that is not a pure run of single-component blueGreen groups | `GroupOrderingNotHonored` |
| A component in two groups | `DuplicateComponentInCoordinationGroups` |
| A group member not declared on the InferenceService | `OrphanCoordinationGroup` / `CanaryInvalid` |
| A group member that is not OMENative | `CoordinationRequiresOMENative` / `CanaryRequiresOMENative` |
| A `soak` the engine would drop (canary group, or a rollout that does not collapse to the one-at-a-time run) | `SoakNotHonored` |
| Two canary groups driving one unit, or a canary group splitting the engine+decoder unit | `MultipleCanaryGroups` / `CanaryInvalid` |
| More than one of `canary` / `blueGreen` / `rollingUpdate` on a group | CRD CEL rule |
| `maintainRatio` on a canary group | `CanaryInvalid` |

The ordering rules (`OrderNotHonored`, `GroupOrderingNotHonored`) apply on **create and on any update that changes `spec.rollout`** — a stored object carrying a shape that would be rejected today keeps reconciling, and keeps accepting unrelated spec updates, until its rollout is next edited. The ratchet covers `groupOrdering` itself: clearing `Concurrent` while keeping groups that Sequential cannot sequence is a rollout edit, and is rejected.

## What's next

- [Rollout Policy](/ome/docs/concepts/rollout_policy) — attach a reusable progression to a group with `policyRef`
- [Promote or Roll Back a Canary](/ome/docs/tasks/promote-or-rollback-a-canary) — step gates and the promote/rollback verbs
- [Repin a Drifted Rollout Plan](/ome/docs/tasks/repin-a-drifted-rollout-plan) — why mid-run edits to `spec.rollout` are inert until the next run
- [OMENative Deployment Mode](/ome/docs/concepts/omenative) — the pod-lifecycle manager rollout groups require
