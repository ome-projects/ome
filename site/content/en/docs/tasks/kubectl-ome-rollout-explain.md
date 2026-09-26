---
title: "Explain Rollout Intent, Plan, and Progress"
linkTitle: "kubectl-ome rollout explain"
weight: 21
date: 2026-09-26
description: >
  Read rollout intent, the effective (possibly pinned) plan, and observed progress side by side with kubectl ome rollout explain, including plan-drift reasons like SpecNewerThanRun.
---

`kubectl ome rollout explain INFERENCESERVICE` prints one read-only
**RolloutExplainReport** that answers *"what rollout plan is in effect, and
why?"*. It shows three renders of the plan — what the spec declares, what the
current source would render right now, and what the active run actually pinned
— alongside the controller's observed progress and the `RolloutPlanReady` /
`RolloutPlanDrift` conditions that explain any difference between them.

It complements the other rollout commands rather than repeating them:

| Command | Question it answers |
| --- | --- |
| `rollout status` | How far has the rollout progressed right now? |
| `rollout explain` | What plan is that progress following, and why — pinned or live, from spec or policy, drifted or in sync? |
| `rollout repin` | Should the run adopt the plan edit I just made? (a guarded action — see [Repin a Drifted Rollout Plan](/ome/docs/tasks/repin-a-drifted-rollout-plan/)) |

## Output formats

```bash
kubectl ome rollout explain chat -n prod            # compact plan outline (default)
kubectl ome rollout explain chat -n prod -o wide    # flat one-row-per-step matrix
kubectl ome rollout explain chat -n prod -o json    # full typed report
kubectl ome rollout explain chat -n prod -o yaml
```

Any other `-o` value is rejected with `unsupported output format` before a
single API request is made. The command issues exactly **one GET** of the
InferenceService in the target namespace and nothing else — no RolloutPolicy,
pod or child-resource reads — so the baseline read-only RBAC from the
[plugin page](/ome/docs/tasks/kubectl-ome/#required-rbac) (`get` on
`inferenceservices`) is all it needs. On a terminal the default table wraps
long detail cells to your terminal width; redirected output is written as the
full deterministic table. As everywhere in the plugin, the human-readable
tables are not a stable scripting interface — script against `-o json`.

## The three views

Every format organizes the plan into the same three views:

| View | What it renders | Evidence |
| --- | --- | --- |
| `Declared` | The groups in `spec.rollout` as written, with no controller input. | `Declared` |
| `Live` | The same spec render *plus* the controller's reported per-group resolution (resolved policy identity and observed render digests from `status.rollout.groups[]`). Shown only while a run is active; this is the render a [repin](/ome/docs/tasks/repin-a-drifted-rollout-plan/) would adopt and the next run would open with. | `Declared` groups, `Reported` resolution |
| `Effective` | The plan actually driving the rollout. With an active run this is the **pinned** plan frozen in `status.rollout.activeRun.plan` (`mode=Pinned; evidence=Reported`); with no run it is the live spec render (`mode=Live`). Only this view carries observed progress, holds, and the READY/DRIFT conditions. | `Reported` when pinned |

When the Declared and Effective views disagree while a run is active, the
DRIFT row tells you why: the pinned plan does not follow spec edits, so an
edit made mid-run shows up in `Declared` (and `Live`) but not in `Effective`
until the next run or an explicit repin.

## A worked example

A service whose active run pinned a canary from a RolloutPolicy, after which
someone replaced the group's `policyRef` with a different inline canary body:

```bash
kubectl ome rollout explain chat -n prod
```

```
VIEW        ITEM        DETAIL
Declared    PLAN        groups=1
            GROUP 0     Canary; components=engine; source=Declared/Inline
            STEP 1/2    capacity=25%; traffic=10%; gate=Manual
            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate
Live        PLAN        mode=Live; groups=1
            GROUP 0     Canary; components=engine; source=Declared/Inline
            STEP 1/2    capacity=25%; traffic=10%; gate=Manual
            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate
Effective   PLAN        mode=Pinned; evidence=Reported; groups=1
            READY       True/Pinned; evidence=Reported
            DRIFT       True/SpecNewerThanRun; evidence=Reported
            GROUP 0     Canary; components=engine; source=Reported/Policy
            CONFIG      policy=RolloutPolicy/guarded@4
                        digest=rp1:aaaaaaaaaaaa
            PHASE       Canarying
            REVISIONS   stable=aaaaaaaa,target=bbbbbbbb
            TRAFFIC     engine:aaaaaaaa=80%,engine:bbbbbbbb=20%
            STEP 1/2    capacity=50%; traffic=20%; gate=Manual
            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate
            ISSUES      EpochUnverifiable
```

Reading it top to bottom: the spec now declares an inline canary
(25% capacity / 10% traffic first step), but the run is executing the plan it
pinned from RolloutPolicy `guarded` at generation 4 (50% / 20% first step).
`READY True/Pinned` confirms an active run holds a pinned plan;
`DRIFT True/SpecNewerThanRun` says the spec has changed since the pin, so the
declared plan is inert until the next run or a repin. The observed rows show
the run mid-canary: phase `Canarying`, stable revision `aaaaaaaa` at 80%
traffic, target `bbbbbbbb` at 20%.

### Row glossary

| ITEM | Detail format |
| --- | --- |
| `PLAN` | `groups=<n>` on the Declared view; `mode=Live; groups=<n>` on the Live view; `mode=<Live\|Pinned>; evidence=...; groups=<n>` on the Effective view |
| `READY` / `DRIFT` | `<state>/<reason>; evidence=...` — the `RolloutPlanReady` and `RolloutPlanDrift` conditions (Effective view only, see below) |
| `GROUP n` | `<strategy>; components=...; source=<evidence>/<Inline\|Policy\|Defaulted>` — strategy is `Canary`, `BlueGreen`, `RollingUpdate` or `Unknown` |
| `CONFIG` | One line per setting: `policy=RolloutPolicy/<name>@<generation>`, `digest=rp1:...` render digests, `shadowed=RolloutPolicy/<name>` when an inline body shadows a `policyRef`, `strategy=defaulted` when the group omitted a progression and got the blue-green default, and `soak=` / `surge=` / `unavailable=` / `ratio=` values annotated as `value(Source;Effect)` — e.g. `soak=1m0s(Configured;IgnoredFinalGroup)` for a soak that cannot apply to the final group |
| `HOLD` | `<kind>; evidence=...` — something is holding progress (see [Holds](#holds)) |
| `PHASE` | The observed group phase (`Canarying`, `Promoting`, `Paused`, ...) |
| `SEQUENCE` | `current=<component>,previous=<component>` for controller-collapsed sequential groups |
| `REVISIONS` | Group level: `stable=`, `target=`, `rejected=` revision hashes |
| `TRAFFIC` | `<component>:<revisionHash>=<percent>%` per traffic target |
| `STEP i/n` | `capacity=...; traffic=...%; gate=<Manual\|Immediate\|Timed\|Analysis>` — the gate is `Analysis` when the step declares analysis, `Timed` for a pause with a duration, `Manual` for a pause without one, `Immediate` otherwise. Pause durations and analysis settings appear in the JSON/YAML report. |
| `ISSUES` | Comma-joined typed codes; `ISSUES <component>` scopes observed component issues |
| `COMPONENT <type>` | An observed component that belongs to no plan group: `<strategy>; phase=...; evidence=...`, followed by its own `REVISIONS` (`current=`, `ready=`, `previous=`) and `TRAFFIC` rows |

Observed progress is folded into the Effective group it belongs to, matching
by group index (or by component membership for a plan the controller
collapsed into one sequential group).

## Plan readiness and drift

The READY row projects the `RolloutPlanReady` condition:

| READY | Meaning |
| --- | --- |
| `True/Pinned` | An active run holds a pinned plan. |
| `True/NoActiveRun` | No run is open — nothing is pinned, nothing is in flight. |
| `False/PolicyNotFound` | A group's `policyRef` names a RolloutPolicy that does not exist in the namespace. |
| `False/PolicyNotReady` | The referenced RolloutPolicy exists but its body fails validation. |
| `False/ProgressionMismatch` | The `policyRef`'s declared progression does not match the policy's body. |
| `False/PlanInvalid` | The composed plan fails validation, or a `policyRef` is used while the rollout-policy feature is disabled on the cluster. |
| `False/ProviderUnbound` | A canary step's analysis names a metric provider that is not bound in the cluster's `metricProviders` configuration. |

Any `False` reason means the rollout is **parked**: the new revision is held
and the previous revision keeps serving. A parked plan also surfaces as a
`PlanParked` HOLD row.

The DRIFT row projects `RolloutPlanDrift`:

| DRIFT | Meaning |
| --- | --- |
| `False/InSync` | The live render equals the pinned plan. |
| `True/SpecNewerThanRun` | An inline group body in `spec.rollout` changed after the run pinned its plan. |
| `True/PolicyNewerThanRun` | A referenced RolloutPolicy changed after the run pinned its plan. |

Drift is information, not an error: the edit simply applies at the next run
unless you adopt it into the active run with
[`rollout repin`](/ome/docs/tasks/repin-a-drifted-rollout-plan/). Unlike the
condition message shown by `kubectl get isvc`, the explain report is
deliberately message-free — conclusions are typed codes, never copied
controller text.

The CLI also cross-checks the conditions against the run state it read in the
same snapshot. A condition that cannot be true — `Pinned` without an active
run, any drift other than `False/InSync` without a run, a duplicated or
unrecognized condition — is rendered as `Invalid/Unknown` with a
`PlanConditionMalformed` issue instead of being repeated at face value. A
condition that is absent entirely shows `Unobserved; evidence=Unavailable`.

## Holds

`HOLD` rows name what is stopping or would stop progress, scoped to the plan,
a group, or a step:

| Kind | Source |
| --- | --- |
| `GlobalPause` | The `ome.io/rollout-paused` annotation is set (see [Pause and Resume a Rollout](/ome/docs/tasks/pause-and-resume-a-rollout/)); evidence `Declared` because it is read from the object's annotations. |
| `PlanParked` | `RolloutPlanReady` is `False` — the plan could not be composed, so the rollout is parked. |
| `CanaryPreStep` | The controller reports a canary group holding *before* its step raises traffic — for example after a repin clamped the run into a new ladder. |
| `ObservedPaused` | An observed group reports phase `Paused`. |

## Policy-referenced groups

The command never reads a RolloutPolicy. What it can show about a
`policyRef` group depends on the view:

- **Declared** shows the reference (`source=Declared/Policy`) but not the
  policy's step ladder — the body lives in the policy object.
- **Live** and a non-pinned **Effective** view add the controller's reported
  resolution: the resolved policy identity and its observed render digest
  (`digest=rp1:...` in CONFIG). The step bodies are still unavailable, which
  the report states explicitly with a `PolicyBodyUnavailable` issue rather
  than leaving a gap. A policy group the controller has not resolved yet
  yields `ResolutionMissing`.
- A pinned **Effective** view shows the full materialized body — the pin froze
  the policy's steps into `status.rollout.activeRun.plan` — plus
  `policy=RolloutPolicy/<name>@<generation>` and the pinned render digest.

One pinned-plan quirk: a pinned inline blue-green group shows
`origin=Unknown` in CONFIG because the controller materializes the default
progression into the pinned plan, so status cannot distinguish an explicitly
authored empty `blueGreen` block from an omitted one.

## The wide table

`-o wide` renders the flat operator matrix instead of the outline: one row
per step per view, with fixed columns `VIEW`, `GROUP`, `EVIDENCE`,
`PLAN-MODE`, `SOURCE`, `STRATEGY`, `COMPONENTS`, `CONFIG`, `STEP`, `GATE`,
`PHASE`, `SEQUENCE`, `PLAN-READY`, `DRIFT`, `HOLD`, `REVISIONS`, `TRAFFIC`
and `ISSUES`. The same example, with the trailing columns elided to fit:

```
VIEW        GROUP   EVIDENCE   PLAN-MODE   SOURCE   STRATEGY   COMPONENTS   CONFIG                                                   STEP            GATE        PHASE       ...
Declared    0       Declared   -           Inline   Canary     engine       -                                                        1/2 25%/10%     Manual      -
Declared    0       Declared   -           Inline   Canary     engine       -                                                        2/2 100%/100%   Immediate   -
Live        0       Declared   Live        Inline   Canary     engine       -                                                        1/2 25%/10%     Manual      -
Live        0       Declared   Live        Inline   Canary     engine       -                                                        2/2 100%/100%   Immediate   -
Effective   0       Reported   Pinned      Policy   Canary     engine       policy=RolloutPolicy/guarded@4,digest=rp1:aaaaaaaaaaaa   1/2 50%/20%     Manual      Canarying
Effective   0       Reported   Pinned      Policy   Canary     engine       policy=RolloutPolicy/guarded@4,digest=rp1:aaaaaaaaaaaa   2/2 100%/100%   Immediate   Canarying
```

Every row repeats the group's context, so the wide table suits grep and
side-by-side diffing; the default outline suits reading in a terminal.

## The typed report

`-o json` and `-o yaml` emit the full `cli.ome.io/v1alpha1`
`RolloutExplainReport`: `metadata`, `collectedAt`, a `sources` list (the one
InferenceService read, with UID and generation), and `content` holding
`summary` (effective-plan mode plus the two conditions), `declaredGroups`,
`liveGroups`, `effectiveGroups`, `observed` (the same content as a
`kubectl ome rollout status` report — summary, groups, components, issues),
`holds` and `issues`.

The report is bounded and redacted by design:

- At most **3 groups** per view, **20 steps** per group, **3 observed
  components** and **32 issues** are rendered. Exceeding a bound truncates
  the excess and adds a `Truncated` warning; truncated plan views also record
  a `PlanTruncated` issue.
- Any issue at all adds a `PartialData` warning, so scripts can check
  `warnings` before trusting `content`.
- Revisions appear as safe hashes, digests must match the `rp1:` render-digest
  format, and free-text controller messages are never copied through.

Plan issues carry a typed code, the view they belong to, and optionally a
group index: `DeclaredPlanMalformed` / `LivePlanMalformed` /
`EffectivePlanMalformed` (the render fails the shared plan validators the
controller applies), `ActiveRunMalformed` (the pinned plan itself is
inconsistent), `PlanConditionMalformed`, `ResolutionMissing`,
`ResolutionMalformed`, `PolicyBodyUnavailable` and `PlanTruncated`. The
Effective view additionally folds in the observed rollout issue codes — the
`EpochUnverifiable` in the example is the standard reminder that reported
progress cannot be bound to the current object generation, exactly as in
`rollout status`.

## What's next

- [Repin a Drifted Rollout Plan](/ome/docs/tasks/repin-a-drifted-rollout-plan/) — act on a `True/SpecNewerThanRun` or `True/PolicyNewerThanRun` DRIFT row
- [Promote or Roll Back a Canary](/ome/docs/tasks/promote-or-rollback-a-canary/) — advance or abandon the run explain describes
- [Pause and Resume a Rollout](/ome/docs/tasks/pause-and-resume-a-rollout/) — the `GlobalPause` hold
- [Rollout Policy](/ome/docs/concepts/rollout_policy/) — the referenced-policy plans explain resolves
- [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome/) — installation, connection flags, RBAC and exit codes
