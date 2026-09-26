---
title: "Read Retained Rollout History"
linkTitle: "kubectl-ome rollout history"
weight: 21
date: 2026-09-26
description: >
  What rollout evidence kubectl ome rollout history shows, and why its bounded retention window is not a durable audit trail.
---

`kubectl ome rollout history INFERENCESERVICE` prints the rollout evidence
still retained on one InferenceService: the open run, the single record of the
most recently closed run, per-group plan provenance, and the current
status-reported revision relations. It is read-only — one exact, namespaced
`get` of the InferenceService and no other cluster reads. For installing the
plugin and the reader RBAC it needs, see the
[kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome/) page.

It answers a different question from its siblings:

| Command | Question it answers |
| --- | --- |
| `kubectl ome rollout status` | How far along is the rollout right now? |
| `kubectl ome rollout explain` | What does the pinned plan intend, and has the spec drifted from it? |
| `kubectl ome rollout history` | What evidence about past runs is still retained, and how trustworthy is it? |

Despite the name, it is unrelated to `kubectl ome runtime history`, which
lists pinned *runtime* snapshots — see
[Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision).

## The window, and why it is not an audit trail

The command invents no storage: everything it shows lives in
`status.rollout` on the InferenceService itself, and the API retains exactly
two run slots there:

- `status.rollout.activeRun` — the currently open run: its run ID, open and
  pin times, per-component target revision hashes, and the pinned plan with
  per-group provenance. Present only while a run is open.
- `status.rollout.lastRun` — a bounded record of the **most recently closed**
  run: its outcome (`Completed`, `RolledBack` or `Superseded`), open and close
  times, target revision hashes, and per-group provenance digests. Each run
  that closes overwrites this slot; there is no list.

So the report can never reach further back than one closed run. Close two
runs and the older one is gone from the cluster entirely — including runs
closed as `Superseded` by a quick retarget. The retained record also keeps
only digests, hashes, timestamps and outcome: never the plan body (post-close
plan history is your policy's git history, per the API contract) and not even
the closed run's ID. This is why every report says
`completeness: RetentionBounded`, and why the command's own help calls the
output "not a durable audit trail".

If you need an audit trail, build one outside the cluster: capture
`kubectl ome rollout history -o json` when runs close, archive the
`RolloutPlanRepinned`-style events before they expire, and keep plan bodies
in version control. This command can only show you what the controller still
retains at the moment you run it.

## Output formats

```bash
kubectl ome rollout history my-isvc              # compact table (default)
kubectl ome rollout history my-isvc -o wide      # one full row per record
kubectl ome rollout history my-isvc -o json      # typed report document
kubectl ome rollout history my-isvc -o yaml
```

Any other `-o` value, a missing or extra argument, or an invalid resource
name is rejected before a single API request is made. Exit codes are 0
(report written) or 1 (the observation could not complete) — `rollout
history` never exits 2. The table is not a stable scripting interface;
script against `-o json`.

## Reading the table

A service mid-run, with one completed run still retained:

```
TYPE     STATE       COMP     IDENT          TIME          DETAIL       ISS
WINDOW   Partial     -        -              -             bounded      1
CURR     Unknown     -        Reported       -             Unverified   1
ACTIVE   Active      -        0123456789ab   08-31T18:00   G:1 T:1      1
TARGET   Reported    engine   cccccccc       -             run-target   1
LAST     Completed   -        -              08-31T17:00   G:1          1
PROV-A   Inline      g0       cca45ec1fb0f   08-31T18:30   -            1
PROV-L   Inline      g0       cca45ec1fb0f   08-31T17:00   -            1
PROV-C   Inline      g0       cca45ec1fb0f   -             -            1
REV      Unknown     engine   aaaaaaaa       -             Current      1
REV      Unknown     engine   bbbbbbbb       -             Ready        1
REV      Unknown     engine   dddddddd       -             Previous     1
```

- **WINDOW** — the state of the retained window as a whole (see
  [Window states](#window-states-issues-and-warnings)); DETAIL is always
  `bounded`.
- **CURR** — the same current-rollout summary `rollout status` computes:
  state, evidence level (IDENT) and epoch (DETAIL — `N/A`, `Unverified` or
  `Unknown`). The epoch says whether the conclusion can be bound to the
  current generation; it usually cannot.
- **ACTIVE** — the open run. IDENT is the run ID's 12-hex suffix, TIME is
  when it opened (UTC, `MM-DDTHH:MM`), DETAIL is `G:<groups> T:<targets>`.
- **TARGET** — one row per component target pinned by the active run, with
  the target revision hash. Targets are shown only for the active run — the
  closed-run record's targets are not projected.
- **LAST** — the single retained closed run: its outcome, close time and
  group count. No run ID: the record does not keep one.
- **PROV-A / PROV-L / PROV-C** — per-group plan provenance from three views:
  the **A**ctive run's pinned plan, the **L**ast run's record, and the
  **C**urrent resolution (`status.rollout.groups[]` — what a run opened now
  would pin). STATE is the source (`Inline` or `Policy`), IDENT is the
  portable digest without its `rp1:` prefix, DETAIL names the policy when
  one is involved. Comparing PROV-A against PROV-C digests is the same drift
  check [`rollout repin`](/ome/docs/tasks/repin-a-drifted-rollout-plan/)
  acts on.
- **REV** — the current status-reported revision relations per component:
  role `Current` (latest rolled out), `Ready` (newest revision whose pods
  reached Ready — deliberately distinct from a run's pinned target) and
  `Previous`, with the component's rollout phase in STATE.
- **ISS** — the total issue count (window issues plus current-status issues,
  capped at `99+`). The same total is repeated on every row; it is not
  per-row.

Long values are truncated to fixed cell widths (`NotConf...`); `-o wide`
prints every safe field untruncated — full RFC3339 timestamps, full
`rp1:` digests, policy name, generation and evidence, shadowed-policy
previews, and one row per issue and warning code.

## Evidence, not testimony

Each digest row says how it was obtained, and the levels differ by view:

- **Active-run provenance is `Computed`.** The CLI re-derives each pinned
  group's progression digest from the pinned body in the same snapshot and
  refuses to display a digest that does not match — a mismatch makes the run
  `ActiveRunMalformed` instead.
- **Last-run provenance is `Reported`.** The plan body is gone, so the
  recorded digest can only be echoed, never re-verified.
- **Current-view provenance** is `Computed` for inline groups (re-derived
  from the spec) and `Reported` for policy-sourced groups (the controller's
  `observedDigest`; the policy itself is never read).

The revision rows are likewise the controller's reported status, qualified
by the CURR evidence level — not proof that those revisions exist or serve.

## Window states, issues and warnings

The WINDOW state (and `content.summary.state` in JSON) summarizes how much
of the retained evidence survived validation:

| State | Meaning |
| --- | --- |
| `Reported` | Every retained record projected cleanly. |
| `Partial` | Something was rejected or the current status carries issues; a `PartialData` warning accompanies it. |
| `Empty` | `status.rollout` exists but retains no runs, provenance or revisions. |
| `Unavailable` | `status.rollout` is absent and nothing else is retained (`RunStatusUnavailable` issue, `SourceUnavailable` warning). When revision evidence survives an absent `status.rollout`, the state is `Partial` instead, with the same issue and warning. |

Malformed evidence is dropped and replaced by a typed issue code — never
partially rendered. An active run with an invalid run ID, an inconsistent
open/pin chronology, zero or more than 3 groups, a digest that fails
re-computation, or duplicate components across groups is withheld as
`ActiveRunMalformed`; the closed-run record has the same guard
(`LastRunMalformed`). A `lastRun` whose close time postdates the active
run's open time is impossible history, so the last-run rows are dropped
with `RunChronologyMalformed`. The remaining codes —
`ActiveTargetUnavailable`, `ActiveTargetMalformed`,
`CurrentResolutionMissing`, `CurrentResolutionMalformed` — scope smaller
rejections to a view, group or component without failing the report.

## Scripting against JSON

`-o json` and `-o yaml` emit a typed `cli.ome.io/v1alpha1`
`RolloutHistoryReport`. The content is allowlisted: no object UID, no
condition messages, no annotations, and no pinned plan bodies ever appear.

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RolloutHistoryReport",
  "metadata": {"namespace": "prod", "name": "chat"},
  "collectedAt": "2026-08-31T18:30:00Z",
  "sources": [
    {"kind": "InferenceService", "namespace": "prod", "name": "chat",
     "generation": 7, "evidence": "Observed",
     "collectedAt": "2026-08-31T18:30:00Z"}
  ],
  "content": {
    "summary": {
      "state": "Partial",
      "completeness": "RetentionBounded",
      "currentState": "Unknown",
      "currentEvidence": "Reported",
      "currentEpoch": "Unverifiable",
      "activeRuns": 1,
      "retainedRuns": 1,
      "revisions": 3
    },
    "runs": [
      {"slot": "Active", "outcome": "Active", "runID": "chat-0123456789ab",
       "openedAt": "2026-08-31T18:00:00Z", "pinnedAt": "2026-08-31T18:30:00Z",
       "groupCount": 1,
       "targets": [{"component": "engine", "revisionHash": "cccccccc",
                    "evidence": "Reported"}]},
      {"slot": "Last", "outcome": "Completed",
       "openedAt": "2026-08-31T16:00:00Z", "closedAt": "2026-08-31T17:00:00Z",
       "groupCount": 1, "targets": []}
    ],
    "provenance": [
      {"view": "ActiveRun", "group": 0, "source": "Inline",
       "portableDigest": "rp1:cca45ec1fb0f", "digestEvidence": "Computed",
       "observedAt": "2026-08-31T18:30:00Z"},
      {"view": "LastRun", "group": 0, "source": "Inline",
       "portableDigest": "rp1:cca45ec1fb0f", "digestEvidence": "Reported",
       "observedAt": "2026-08-31T17:00:00Z"},
      {"view": "Current", "group": 0, "source": "Inline",
       "portableDigest": "rp1:cca45ec1fb0f", "digestEvidence": "Computed"}
    ],
    "revisions": [
      {"component": "engine", "role": "Current", "revisionHash": "aaaaaaaa",
       "phase": "Unknown"},
      {"component": "engine", "role": "Ready", "revisionHash": "bbbbbbbb",
       "phase": "Unknown"},
      {"component": "engine", "role": "Previous", "revisionHash": "dddddddd",
       "phase": "Unknown"}
    ],
    "statusIssues": [{"code": "EpochUnverifiable"}],
    "issues": []
  },
  "warnings": [{"code": "PartialData"}]
}
```

`summary.activeRuns` is 0 or 1 and `summary.retainedRuns` is 0 or 1 — by
construction, not by circumstance: those are all the slots the API has.

## What's next

- [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome/) — installation, exit
  codes and the read-only RBAC rule this command falls under
- [Rollout Policy](/ome/docs/concepts/rollout_policy/) — how plans are
  rendered, pinned into `status.rollout.activeRun`, and digested
- [Repin a Drifted Rollout Plan](/ome/docs/tasks/repin-a-drifted-rollout-plan/)
  — acting on the drift the provenance views expose
