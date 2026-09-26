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
| `maintainRatio` | Cross-component replica-ratio guard. Meaningful only on multi-component blueGreen/rollingUpdate groups; rejected on canary groups. |
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
