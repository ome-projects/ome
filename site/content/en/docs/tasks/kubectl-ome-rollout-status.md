---
title: "Read Rollout Progress with kubectl ome rollout status"
linkTitle: "kubectl-ome rollout status"
weight: 21
date: 2026-09-30
description: >
  How to read the kubectl ome rollout status report — the summary state and epoch, per-group phase and canary steps, target vs observed traffic, per-component revision hashes, and typed issue codes.
---

`kubectl ome rollout status INFERENCESERVICE` prints one read-only
**RolloutStatusReport** that answers *"how far has the rollout progressed
right now?"*: one bounded summary state, one entry per rollout group with its
strategy, phase and active canary step, one entry per component with its
revision hashes and traffic split, and a list of typed issue codes for
everything the projection refused to take at face value.

It is the progress view among the rollout commands; each sibling answers a
different question:

| Command | Question it answers |
| --- | --- |
| `rollout status` | How far has the rollout progressed right now? |
| [`rollout explain`](/ome/docs/tasks/kubectl-ome-rollout-explain/) | What plan is that progress following, and why — pinned or live, drifted or in sync? |
| [`rollout history`](/ome/docs/tasks/kubectl-ome-rollout-history/) | What evidence about past runs is still retained? |
| [`rollout validate`](/ome/docs/tasks/kubectl-ome-rollout-validate/) | Would the stored configuration hold up before I act on it? |

## Output formats

```bash
kubectl ome rollout status chat -n prod            # compact component matrix (default)
kubectl ome rollout status chat -n prod -o wide    # flat one-row-per-component matrix
kubectl ome rollout status chat -n prod -o json    # full typed report
kubectl ome rollout status chat -n prod -o yaml
```

Any other `-o` value is rejected with `unsupported output format` — and a
missing, extra or non-DNS-1123 name argument is rejected too — before a
single API request is made. The command then issues exactly **one GET** of
the InferenceService in the target namespace and nothing else: no pods, no
RolloutPolicies, no child resources. The baseline read-only RBAC from the
[plugin page](/ome/docs/tasks/kubectl-ome/#required-rbac) (`get` on
`inferenceservices`) is all it needs. Exit codes are 0 (report written) or 1
(the observation could not complete) — `rollout status` never exits 2. The
human-readable tables are not a stable scripting interface; script against
`-o json`.

## Which rollout the report describes

The report projects one rollout view: with an active run it validates and
reports progress against the run's **pinned plan** in
`status.rollout.activeRun.plan` (even when that pinned plan is intentionally
empty); with no active run it uses the live `spec.rollout`. That makes the
step ladder and traffic targets you see here the ones actually driving the
rollout — an edit made mid-run shows up in
[`rollout explain`](/ome/docs/tasks/kubectl-ome-rollout-explain/) as drift,
not here.

## The summary: state, reported state, evidence, epoch

Every format opens with the same five summary fields:

| Field | Values | Meaning |
| --- | --- | --- |
| `state` | `NotConfigured`, `Succeeded`, `InProgress`, `Paused`, `Staged`, `Failed`, `RollingBack`, `RolledBack`, `Unknown` | The aggregate the CLI itself asserts. |
| `reportedState` | same set | The aggregate computed from the controller-reported group and component phases. |
| `evidence` | `Declared`, `Reported` | How the conclusion was obtained: from the spec alone, or from status the controller wrote. |
| `epoch` | `NotApplicable`, `Unverifiable` | Whether the conclusion can be bound to the current object generation. |
| `coordinationReady` | `NotApplicable`, `Unobserved`, `True`, `False`, `Unknown`, `Invalid` | The `RolloutCoordinationReady` condition, cross-checked against the group phases. |

The split between `state` and `reportedState` is the report's central honesty
device. Rollout progress lives in the status subresource, which cannot be
bound to the generation you are looking at, so for any service with a
configured rollout the summary is `state: Unknown`, the computed aggregate
appears only as `reportedState`, qualified by `evidence: Reported` and
`epoch: Unverifiable`, and a standing `EpochUnverifiable` issue plus a
`PartialData` warning are recorded. Only a conclusion the spec alone proves —
in practice `NotConfigured`, when no component is OME-native-managed and no
rollout residue exists in status — is asserted in `state` itself, with
`evidence: Declared` and `epoch: NotApplicable`.

`reportedState` aggregates every group phase and contributing component phase
with a fixed precedence: `Failed`, then `RollingBack`, `RolledBack`,
`Paused`, `Unknown` (any phase the projection could not accept), then
`InProgress` (any actively progressing phase), `Staged`, and finally
`Succeeded` when everything is stable. This is the same value the
[`kubectl ome wait --for=rollout=...`](/ome/docs/tasks/kubectl-ome-wait/)
predicates match.

`coordinationReady` is `NotApplicable` unless the plan has a blue-green,
rolling-update or sequential group. When it applies, the condition is
repeated only if it agrees with what the group phases imply; a missing
condition shows `Unobserved`, and a duplicated, malformed or contradicting
one shows `Invalid` with a `StatusMalformed` issue.

## A worked example

An engine canary mid-run — second of three steps, 20% traffic shifted and
observed, waiting on a manual gate:

```bash
kubectl ome rollout status chat -n prod
```

```
FIELD          SERVICE             ENGINE       DECODER   ROUTER
STATE          Unknown             -            -         -
REPORTED       InProgress          -            -         -
EVIDENCE       Reported            -            -         -
EPOCH          Unverifiable        -            -         -
COORDINATION   NotApplicable       -            -         -
GROUP          -                   0            -         -
STRATEGY       -                   Canary       -         -
GROUP-PHASE    -                   Canarying    -         -
PHASE          -                   Canarying    -         -
STEP           -                   2/3          -         -
GATE           -                   Manual       -         -
CAPACITY       -                   50%          -         -
TRAFFIC        -                   20% -> 20%   -         -
ROLLED-OUT     -                   aaaaaaaa     -         -
READY          -                   bbbbbbbb     -         -
ISSUES         EpochUnverifiable   -            -         -
```

The default table is a component matrix sized for a terminal: the summary
occupies the `SERVICE` column, and each component's progress is read
vertically under `ENGINE`, `DECODER` and `ROUTER`.

### Row glossary

| Row | Meaning |
| --- | --- |
| `STATE` / `REPORTED` / `EVIDENCE` / `EPOCH` / `COORDINATION` | The summary fields above, always printed in the `SERVICE` column. |
| `GROUP` | The rollout-group index the component belongs to; `-` for an ungrouped component. |
| `STRATEGY` | `Canary`, `BlueGreen`, `RollingUpdate`, `Sequential`, `Independent` (an ungrouped OME-native component progressing on its own lifecycle) or `Unknown`. |
| `GROUP-PHASE` / `PHASE` | The group's phase and the component's own phase (`Stable`, `Canarying`, `Promoting`, `Surging`, `Waiting`, `Shifting`, `Updating`, `Draining`, `ScalingDown`, `Staged`, `AwaitingNextComponent`, `Paused`, `Failed`, `RollingBack`, `RolledBack`, ...). |
| `GROUP-CURRENT` / `GROUP-PREVIOUS` | The sequential cursor: which component a controller-collapsed sequential group is advancing now, and which one it finished last. Shown only when set. |
| `STEP` / `GATE` / `CAPACITY` | The active canary step as `i/n` (1-based here; the JSON `index` is 0-based), how it advances (`Immediate`, `Manual`, `Timed`, `Analysis`), and its declared capacity. |
| `TRAFFIC` | The step's `target% -> observed%` traffic weight. |
| `ROLLED-OUT` / `READY` / `PREVIOUS` | The component's latest rolled-out, latest ready, and previous revision, as safe 8-character hashes. |
| `ISSUES` | Last row, printed only when issues exist: service-scoped codes under `SERVICE`, component-scoped codes under their component, qualified as `Code(group=N)` where scoping isn't implied by the column. |

Rows where the service and every component would show `-` are omitted, which
is why the example has no `GROUP-CURRENT` or `PREVIOUS` row.

The gate names come from the step's declared shape: a step with `analysis`
is `Analysis`, a pause with a duration is `Timed`, a pause without one is
`Manual`, and anything else is `Immediate`. When a step declares analysis,
the JSON report adds an `analysis` field (`Unobserved`, `Passing`, `Failing`
or `Inconclusive`) — the CLI recomputes each metric result against the
declared thresholds rather than echoing a verdict, and an inconclusive
evaluation records an `AnalysisInconclusive` issue.

## The wide table

`-o wide` renders the flat operator matrix that machine diffs and grep
prefer: one row per component, with the summary columns repeated on every
row. The same example:

```
STATE     REPORTED-STATE   EVIDENCE   EPOCH          GROUP   STRATEGY   GROUP-PHASE   CURRENT-COMPONENT   PREVIOUS-COMPONENT   COMPONENT   COMPONENT-PHASE   STEP   GATE     CAPACITY   TARGET-TRAFFIC   OBSERVED-TRAFFIC   ROLLED-OUT   READY      PREVIOUS   ISSUES
Unknown   InProgress       Reported   Unverifiable   0       Canary     Canarying     -                   -                    engine      Canarying         2/3    Manual   50%        20%              20%                aaaaaaaa     bbbbbbbb   -          EpochUnverifiable
```

Issues that match no component row — for example a group-scoped code for a
group with no projected components — are appended on one extra summary-only
row so nothing is dropped.

## The typed report

`-o json` and `-o yaml` emit the full `cli.ome.io/v1alpha1`
`RolloutStatusReport`: `metadata`, `collectedAt`, a `sources` list (the one
InferenceService read, with UID and generation), `content` holding
`summary`, `groups`, `components` and `issues`, and code-only `warnings`.
The same example:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RolloutStatusReport",
  "metadata": {"namespace": "prod", "name": "chat"},
  "collectedAt": "2026-08-31T18:30:00Z",
  "sources": [
    {"kind": "InferenceService", "namespace": "prod", "name": "chat",
     "uid": "2c9d1a6e-6bfa-4f7f-9d6c-3d0f5b7a8e21", "generation": 7,
     "evidence": "Observed", "collectedAt": "2026-08-31T18:30:00Z"}
  ],
  "content": {
    "summary": {
      "state": "Unknown",
      "reportedState": "InProgress",
      "evidence": "Reported",
      "epoch": "Unverifiable",
      "coordinationReady": "NotApplicable"
    },
    "groups": [
      {"index": 0, "strategy": "Canary", "phase": "Canarying",
       "components": ["engine"],
       "stableRevisionHash": "aaaaaaaa", "targetRevisionHash": "bbbbbbbb",
       "step": {"index": 1, "total": 3, "capacity": "50%",
                "targetTraffic": 20, "observedTraffic": 20,
                "gate": "Manual", "enteredAt": "2026-08-31T18:10:00Z"}}
    ],
    "components": [
      {"type": "engine", "strategy": "Canary", "group": 0,
       "phase": "Canarying",
       "rolledOutRevisionHash": "aaaaaaaa", "readyRevisionHash": "bbbbbbbb",
       "traffic": [
         {"revisionHash": "aaaaaaaa", "percent": 80, "role": "Current"},
         {"revisionHash": "bbbbbbbb", "percent": 20, "role": "Target"}
       ]}
    ],
    "issues": [{"code": "EpochUnverifiable"}]
  },
  "warnings": [{"code": "PartialData"}]
}
```

Beyond what the tables show, the typed report carries:

- **Per-revision traffic targets.** Each component's `traffic` list splits
  observed traffic by revision hash — the per-unit canary split the tables
  summarize as one `target -> observed` pair. The `role` relates each hash to
  the component's own revisions: `Current` (latest rolled out), `Target`
  (latest ready — the incoming revision during a canary), `Previous`, or
  `Other`. A traffic list is shown only when it is internally valid: unique
  hashes, every percent within 0–100, summing to exactly 100; otherwise the
  whole list is dropped with a `TrafficInvalid` issue.
- **Group revision relations.** Canary groups carry `stableRevisionHash`,
  `targetRevisionHash` and, after a rollback, `rejectedRevisionHash`;
  coordination groups carry `observedSurge` and `transitionedAt`.
- **Sequential collapse.** Two or three singleton blue-green groups the
  controller coordinates as one sequence are reported as a single
  `Sequential` group with a `currentComponent` / `previousComponent` cursor;
  phase `AwaitingNextComponent` means one component finished and the next has
  not started.

The report is bounded and message-free by design: every field is a closed
enum or a safe value, unrecognized input is projected as `Unknown` rather
than copied through, revision names are reduced to their 8-character hash
(and withheld with `RevisionNameInvalid` when the recorded name does not
match the expected `<service>-<component>-<hash>` shape), and controller
message text never appears. Timestamps are normalized to UTC and the
document is canonically ordered — apart from `collectedAt`, it is
deterministic for a given InferenceService.

## Issue codes

Issues are the report's diagnostics: a stable code, optionally scoped to a
group index and/or component, with no free text. Any issue at all adds a
`PartialData` warning, so scripts can check `warnings` before trusting
`content`.

| Code | Meaning |
| --- | --- |
| `SpecMalformed` | The effective rollout plan fails the static bounds admission enforces (at most 3 groups of 1–3 components, one progression shape each, canaries within 20 steps and 10 analysis metrics per step, no component in two groups). |
| `StatusMalformed` | Reported status contradicts itself or the plan: wrong coordination policy or membership, phase residue that fits no declared rollout, traffic that does not match the pinned step, an invalid condition or timestamp. |
| `GroupStatusMissing` | A blue-green, rolling-update or sequential group has no matching coordination-group status. |
| `GroupStatusUnexpected` | Coordination status exists for a group the effective plan does not declare. |
| `ComponentStatusMissing` | A declared component reports no usable rollout status. |
| `CanaryStatusMissing` | A canary group's phase implies an active run but no canary status is recorded. |
| `CanaryStatusUnexpected` | Canary status is recorded where the plan declares no canary (or on a component that is not a canary primary). |
| `CanaryStepInvalid` | The recorded current step falls outside the pinned ladder, has an unsafe capacity, or a completed canary's record does not match its final step. |
| `RevisionNameInvalid` | A recorded revision or service name does not have the expected shape; its hash is withheld. |
| `TrafficInvalid` | A traffic list with duplicate hashes, out-of-range percentages, or a sum other than 100 was dropped. |
| `AnalysisInconclusive` | The latest analysis evaluation was inconclusive. |
| `EpochUnverifiable` | The standing reminder that reported progress cannot be bound to the current generation. |

The codes that mark evidence as malformed — every code above except
`GroupStatusMissing`, `ComponentStatusMissing`, `CanaryStatusMissing`,
`AnalysisInconclusive` and `EpochUnverifiable` — also clamp `reportedState`
to `Unknown`: the report never repeats a conclusion whose evidence
contradicts itself.

## What's next

- [Promote or Roll Back a Canary](/ome/docs/tasks/promote-or-rollback-a-canary/) — act on the step and gate this report shows
- [Pause and Resume a Rollout](/ome/docs/tasks/pause-and-resume-a-rollout/) — the `Paused` phases and how to clear them
- [Explain Rollout Intent, Plan, and Progress](/ome/docs/tasks/kubectl-ome-rollout-explain/) — the plan behind the progress, including drift
- [Wait for a Condition](/ome/docs/tasks/kubectl-ome-wait/) — block on the same `reportedState` reaching `Succeeded`, `Failed` or `RolledBack`
- [Read the kubectl ome status Report](/ome/docs/tasks/kubectl-ome-status/) — the one-view summary whose `Rollout` row points here
- [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome/) — installation, connection flags, RBAC and exit codes
