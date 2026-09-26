---
title: Canary Progression
linkTitle: Canary Progression
weight: 3
description: >
  Reference for spec.rollout.groups[].canary steps — what capacity and traffic
  each control, how a step's gate (immediate, timed, manual, analysis) is
  resolved, and what scaleDownDelaySeconds and readyTimeout bound.
---

A rollout group with a `canary` progression advances through an ordered list of
`steps`. Each step declares how much **capacity** (new-revision pods) to run
and how much **traffic** (percentage of requests) to send to the new revision,
plus an optional gate that decides when the step may advance. The two are
independent, which is what enables the capacity-ahead-of-traffic warm-up
pattern: run 50% of the pods on the new revision while sending it only 10% of
requests.

This page is the reference for the step fields and progression mechanics. For
making the promote-or-abort decision at a gate, see
[Promote or Roll Back a Canary](/ome/docs/tasks/promote-or-rollback-a-canary/).
For reusing one canary body across services, see
[Rollout Policy](/ome/docs/concepts/rollout_policy/). The evaluation semantics
of the analysis gate (sampling, failure budget, inconclusive handling) are
covered separately; this page only shows where analysis fits in the gate order.

## Example

```yaml
spec:
  rollout:
    groups:
      - components: [engine]
        canary:
          scaleDownDelaySeconds: 60   # drain window before old pods scale down
          readyTimeout: 20m           # bound on a stuck capacity gate
          steps:
            - capacity: "50%"         # warm up half the fleet...
              traffic: 10             # ...while sending it 10% of requests
              pause:
                duration: 10m         # timed gate: bake, then advance
            - capacity: "50%"
              traffic: 50
              pause: {}               # manual gate: hold for an explicit promote
            - capacity: "100%"
              traffic: 100            # final step: full capacity, full traffic
```

This rollout brings up half the engine replicas on the new revision, serves the
split 10%/90% for ten minutes, raises traffic to 50% and holds for an explicit
`kubectl ome rollout promote`, then rolls the remaining pods, shifts 100% of
traffic, waits out the 60-second drain, and scales the old revision down.

## Capacity: the new-revision pod count

`steps[].capacity` is the fraction of a Component's **desired replicas** to run
on the new revision at this step. It is a split of the existing replica set,
not a surge: with desired replicas `N` and a step capacity resolving to `k`,
the Component runs `k` pods on the new revision and `N - k` on the old one —
the total stays at `N` throughout the roll. Desired replicas is the Component's
`minReplicas` (falling back to `maxReplicas`).

Two forms are accepted, and the distinction is strict:

| Form | Example | Meaning |
| --- | --- | --- |
| Quoted percentage | `capacity: "25%"` | Fraction of desired replicas, integer 0–100. Rounds **up**, so any step with `traffic > 0` stages at least one new pod. |
| Unquoted integer | `capacity: 3` | Absolute pod count, clamped to desired replicas. |

A quoted plain number (`capacity: "3"`) is **rejected at admission** (reason
`CanaryInvalid`): the runtime resolver maps anything it cannot parse to zero
new capacity, so the webhook rejects every such form rather than let a step
silently stage nothing. In a [RolloutPolicy](/ome/docs/concepts/rollout_policy/)
body only the percentage form is admitted — an absolute count is
service-specific and defeats fleet portability.

Capacity is **per-Component**. In a multi-Component canary group (an
engine+decoder pair), the same step capacity is resolved against each
Component's own desired replicas, and the step does not proceed until **every**
Component's new-revision capacity is Ready — a Ready router cannot advance the
step while the engine or decoder canary pods behind it are still coming up.

### The capacity gate

No traffic shifts until the step's capacity is Ready. While the new-revision
pods come up, the Component reports rollout phase `Pending` and the controller
re-checks every 10 seconds. If the gate stays unsatisfied past the resolved
[ready timeout](#readytimeout-bounding-a-stuck-gate), the canary is marked
`Failed` and parks — the stable revision keeps serving, because no traffic has
shifted yet.

## Traffic: the service-level request split

`steps[].traffic` is the percentage (0–100) of requests routed to the new
revision at this step; the stable revision receives the remainder. Unlike
capacity it is **not** per-Component: one weight is programmed through the
group's primary Component (router, else engine, else decoder) as weighted
per-revision routing, which the generated HTTPRoute consumes.

Across steps, traffic must be **non-decreasing**, and the final step must reach
`100` — otherwise the rollout never fully cuts over. A step with `traffic > 0`
must also resolve to non-zero capacity (you cannot send traffic to zero pods);
a `traffic: 0` step is valid and serves as a pure capacity warm-up.

Exactness depends on whether the group's primary is the externally-routed
entrypoint:

- When the group includes the entrypoint (a router group, or an engine group on
  a service with no router), the split is exact — the weight is applied on the
  entrypoint's per-revision routes.
- For an engine (or engine+decoder) unit behind a router, the router discovers
  engine endpoints by label selector with no revision term, so a step that puts
  a fraction of engine replicas on the new revision moves roughly that fraction
  of requests. The split is capacity-driven and mediated by the router's load
  balancing; capacity, not traffic, is the effective contract there.

## How a step advances: gate resolution

A step's gate is resolved from field **presence**, in this order:

1. **`analysis` set** → metric-gated. The step's metric checks decide advance,
   hold, or rollback; `pause.duration`, if also set, is the minimum bake time
   before a passing sample may advance. The metrics source is the canary-level
   `prometheus` block, shared by every analysis step.
2. **else `pause` with `duration`** → timed. The step advances by itself once
   the duration elapses.
3. **else `pause: {}`** → manual. The step holds (phase `Paused`) until an
   explicit promote — `kubectl ome rollout promote`, which sets the
   `ome.io/rollout-promote` annotation to the current canary revision hash. One
   promote advances exactly one step; it is never replayed.
4. **else** → immediate. The step applies its capacity and traffic, then
   advances as soon as capacity is Ready.

| Gate | Step shape | How it advances |
| --- | --- | --- |
| Analysis | `analysis` set (optionally with `pause.duration` as bake) | metric checks pass, after warm-up and bake |
| Timed | `pause.duration` set, no `analysis` | by itself, once the duration elapses |
| Manual | `pause: {}`, no duration, no `analysis` | only on an explicit promote |
| Immediate | neither `pause` nor `analysis` | as soon as capacity is Ready and traffic is applied |

Gate timers are anchored to the moment the step **first serves its split** —
when capacity is met and the step's traffic weight is applied — not to when the
previous step advanced. On slow capacity (model load can take many minutes) a
timed pause therefore measures the bake from when the split is actually up,
rather than being consumed while pods are still starting.

### The final step

The final step (traffic `100`, capacity `100%`) honors the same gate as
intermediate steps, evaluated at full traffic: an analysis gate validates at
100% (a breach still rolls back), a bare `pause: {}` holds completion for an
explicit promote, and a timed pause holds for its duration. Once the gate
passes, completion additionally waits out the
[drain window](#scaledowndelayseconds-the-drain-window). The Component reports
phase `Promoting` from the moment 100% traffic shifts until completion, then
`Stable`.

## scaleDownDelaySeconds: the drain window

`canary.scaleDownDelaySeconds` is a canary-level setting (declared once, not
per step): the wait, in seconds, between shifting 100% of traffic to the new
revision and scaling the old revision's pods down, so in-flight requests on the
old revision can drain.

The window is anchored to the moment 100% traffic actually shifts (entering
`Promoting`), not to when the final step was entered — on slow capacity the
final step is entered well before traffic moves, and measuring from step entry
could consume the whole window before cutover. It runs alongside the final
step's gate: the rollout completes only when the gate has passed **and** the
window has elapsed. Unset, zero, or negative values complete immediately.

## readyTimeout: bounding a stuck gate

`canary.readyTimeout` bounds how long a step's **capacity gate** may stay
unsatisfied before the canary is marked `Failed`. The same resolved value also
serves as the stall timeout for an analysis gate that cannot read health
(inconclusive samples). In the capacity case, no traffic has shifted when the
timeout fires — the stable revision keeps serving.

The effective value is resolved in precedence order:

1. The `ome.io/rollout-ready-timeout` annotation on the InferenceService — a
   duration string (e.g. `"20m"`). Values under one minute draw an admission
   warning (`RolloutTimeoutTooShort`): most LLM runtimes need longer to reach
   Ready.
2. The plan's `canary.readyTimeout`. Must be greater than zero when set;
   admission rejects `0` or negative values.
3. The operator-configured default: the `rollout.defaultReadyTimeout` key of
   the `inferenceservice-config` ConfigMap. The Helm chart ships `"15m"`
   (`ome.controller.rollout.defaultReadyTimeout` in `ome-resources` values).

When none of the three yields a positive duration, the escalation is disabled:
the capacity gate waits indefinitely and never parks the canary `Failed` on
capacity wait alone.

The timeout clock is anchored to the start of the **current capacity wait**
(entering `Pending`), so a long bake on an earlier gate does not eat the
budget, and a capacity dip mid-step starts a fresh window.

`Failed` is a parked hold, not an automatic revert: the controller re-checks on
a slow five-minute heartbeat and stable keeps serving until you act. Recover by
pushing a fixed revision (a genuinely new target re-arms a fresh canary) or by
aborting with
[`kubectl ome rollout rollback`](/ome/docs/tasks/promote-or-rollback-a-canary/#roll-back-the-canary),
which is accepted in the `Failed` phase.

## What admission rejects

The InferenceService webhook validates every inline canary body (a
[RolloutPolicy](/ome/docs/concepts/rollout_policy/) body passes the same rules
at policy admission). Rejections carry reason `CanaryInvalid` unless noted:

- `steps` must be non-empty, with at most 20 entries.
- The final step's `traffic` must be `100`, and a percentage final `capacity`
  must be `"100%"` (an absolute final count is only checkable at runtime).
- `traffic` must be in `[0,100]` and non-decreasing across steps.
- A step with `traffic > 0` must have capacity resolving above zero.
- Every `capacity` must parse strictly: `"<N>%"` with integer N in `[0,100]`,
  or an unquoted non-negative integer.
- `readyTimeout`, when set, must be greater than zero.
- Every analysis step must be complete — positive `interval`,
  `failureLimit >= 1`, at least one metric with a numeric threshold and a valid
  operator (reason `AnalysisInvalid`).
- Every Component in a canary group must use the `OMENative` deployment mode
  (reason `CanaryRequiresOMENative`) — canary relies on per-revision pod
  selection and partition staging.
