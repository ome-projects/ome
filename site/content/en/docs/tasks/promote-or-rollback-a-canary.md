---
title: "Promote or Roll Back a Canary"
linkTitle: "Promote or Roll Back a Canary"
weight: 30
date: 2026-09-26
description: >
  Advance a gated canary step with kubectl ome rollout promote, or abort the canary back to the stable revisions with kubectl ome rollout rollback.
---

This page shows you how to make the promote-or-abort decision at a canary
gate: advancing a manually gated step with `kubectl ome rollout promote`,
bypassing an analysis gate with `--override-analysis --yes`, and aborting a
canary with `kubectl ome rollout rollback`. Both commands are **alpha**
guarded actions that operate on the current pinned rollout run and step of
one InferenceService.

They answer exactly one question — *does this canary step proceed, or is this
target rejected?* — and are distinct from `kubectl ome rollout pause` and
`resume` (the service-wide lifecycle hold) and `kubectl ome rollout repin`
(replacing the pinned plan).

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome/). Both commands
  patch the InferenceService, so you also need the action RBAC rule from
  [Required RBAC](/ome/docs/tasks/kubectl-ome/#required-rbac) in addition to read
  access on `inferenceservices` and `inferencereplicas`.
- The target must be an OMENative-managed InferenceService with an active
  pinned rollout run whose plan contains exactly one canary group.

## How a canary step gates

Each step in `spec.rollout.groups[].canary.steps[]` advances according to its
gate:

| Gate | Spec shape | How it advances |
| --- | --- | --- |
| Immediate | neither `pause` nor `analysis` | by itself, as soon as capacity and traffic are satisfied |
| Timed | `pause.duration` set | by itself, once the duration elapses |
| Manual | `pause: {}` with no duration | **only** on an explicit promote |
| Analysis | `analysis` set | when its metric checks pass (after warm-up and bake) |

A manual gate holds indefinitely:

```yaml
spec:
  rollout:
    groups:
      - components: [engine]
        canary:
          steps:
            - capacity: "50%"
              traffic: 50
              pause: {}        # manual gate: hold until an explicit promote
            - capacity: "100%"
              traffic: 100
```

`kubectl ome rollout status my-isvc` shows the active group, step, and gate.

## Promote a manually gated step

```bash
kubectl ome rollout promote my-isvc -n prod
```

The command reads the InferenceService and its InferenceReplicas, verifies
that the current step is actually holding at an **active indefinite manual
gate**, prints a preview on stderr, and asks for confirmation (pass `--yes`
to skip the prompt; noninteractive input requires it). It then submits a
JSON Patch that adds one "mailbox" annotation:

```
ome.io/rollout-promote: <canary revision hash>
```

The value is the current canary target's revision hash, so a promote can
never advance a different (newer or stale) target: the controller consumes
the annotation only while it still matches, advances **exactly one step**,
and then removes it. There is no automatic replay — promoting through two
manual gates takes two explicit promotes.

The patch itself is guarded by `test` operations on the object's UID and
`resourceVersion`, captured when the evidence was read. If anything changed
the InferenceService in between, the API server rejects the patch and the
command exits with code 3 (`guarded annotation patch rejected; refresh
rollout status and retry explicitly`). It never retries on its own.

Ordinary promote is deliberately narrow. It is refused (exit code 1, nothing
submitted) when:

- the step's gate is **Timed** — a timed gate advances by itself, and the
  controller ignores a promote on it;
- the step's gate is **Analysis** — analysis is never bypassed by ordinary
  promote (see the next section);
- observed traffic does not match the step's target traffic, the
  component is not holding in the `Paused` phase (or `Promoting` on the
  final 100% step), a pre-step exposure hold is in effect, or a previous
  promote is still being consumed.

## Override an analysis gate

When a step gates on analysis, ordinary promote refuses. To force the step
through anyway, you must state both the intent and the confirmation
explicitly:

```bash
kubectl ome rollout promote my-isvc -n prod --override-analysis --yes
```

`--override-analysis` exists only on `promote`, requires `--yes` (the
command fails with `analysis override requires --override-analysis --yes`
otherwise), and is accepted only while the active step's gate really is
Analysis — it is not a general-purpose force flag.

**What it bypasses:** the active step's health checks (its metric analysis),
warm-up (`analysis.initialDelay`), and bake (`pause.duration`) — for this
exact pinned step only. Later analysis steps still gate normally. The
preview carries an `ANALYSIS OVERRIDE` warning plus the step's analysis
evidence in normalized form (thresholds, sampled values, pass/fail per
metric — metric names and queries are never printed), and the result message
is prefixed `Analysis override:` so the bypass is visible in scripts and
logs.

## Roll back the canary

```bash
kubectl ome rollout rollback my-isvc -n prod
```

Rollback is the abort arm of the same decision. It submits the second
mailbox annotation, `ome.io/rollout-rollback: "true"`, guarded by the same
UID/`resourceVersion` tests. On consuming it, the controller:

- aborts **every canary member** — each component in the canary group — back
  to its **own reported stable revision** (the `stableRevision` the
  controller recorded per component in the pinned run; the preview lists
  each member's target and stable revision, with stable marked `(Reported)`);
- shifts traffic 100% to stable and drains the rejected revision's pods
  (phase `RollingBack` while they drain, `RolledBack` once gone);
- **holds the rejected target**: the rejected revision hash is recorded in
  status and is not retried. Removing the annotation does not re-run it;
  only a genuinely new target — different from both the stable and the
  rejected revision — re-arms a fresh canary.

There is no `--revision` flag: rollback never chooses a revision, it returns
to what the controller reported as stable. It is **not** a retry, not a
redeploy, and not a rollout-spec edit — `spec` is untouched, so to try again
you push a fixed revision rather than clearing anything by hand.

Rollback is accepted while the canary component is in the `Pending`,
`Canarying`, `Paused`, `Promoting`, or `Failed` phase — including before any
traffic has shifted, during a pre-step exposure hold, and after a run parked
`Failed` — and is refused once there is no active canary to abort (`Stable`,
or already `RollingBack`/`RolledBack`).

## Refusals both commands share

Both actions fail closed: when the CLI cannot prove the action applies to
exactly what you saw, it refuses with exit code 1 and submits nothing.

| Refusal | What to do |
| --- | --- |
| `globally paused canary cannot consume a request` | The service carries `ome.io/rollout-paused`. Resume first — a paused canary must not silently consume a queued decision. |
| `a rollout promote or rollback mailbox is present` | An earlier promote/rollback has not been consumed yet. Wait for the controller, or on a paused service discard it with `kubectl ome rollout resume --discard-pending-actions --yes`. |
| `no applicable active rollout or lifecycle work was observed` | No pinned run, no canary group, the run already completed, or (for rollback) the canary is already rolled back. |
| `controller safety evidence is stale or inconsistent` | Status the CLI read does not line up (revision hashes, target identity, timestamps, traffic). Re-check `kubectl ome rollout status` and retry once it settles. |
| `placement sources and derived services cannot be mutated` / target deleting | Placement-owned and terminating services are never valid targets. |
| `action not confirmed; noninteractive input requires --yes` | Add `--yes` when running from scripts or CI. |

## What acceptance means — and does not

API acceptance means the annotation request landed; it is **not** controller
convergence. The result message says so explicitly (`API accepted annotation
request; convergence not observed.`), and the command neither waits nor
replays. Follow up with the command echoed in the result:

```bash
kubectl ome rollout status my-isvc -n prod
```

If the command reports an unknown outcome (for example `API response is not
bound to the request; outcome unknown, check rollout status`), the request
may already have applied — check rollout status before running the action
again rather than resubmitting blindly.

## Dry-run, output, and timing

Both commands accept `--dry-run` and `-o`:

- `--dry-run=client` validates all evidence, prints the preview, and
  confirms, but sends no patch (`Validated locally; no patch sent.`).
- `--dry-run=server` sends the identical guarded JSON Patch with
  `dryRun=All` (`API dry-run accepted; no changes persisted.`).

Preview and the confirmation prompt go to **stderr**; **stdout** carries
exactly one `ActionResult` in the chosen format (`table`, `wide`, `json`, or
`yaml`), so `-o json` stays clean for scripts:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-26T10:12:00Z",
  "action": "rollout promote",
  "target": {
    "kind": "InferenceService",
    "namespace": "prod",
    "name": "my-isvc",
    "uid": "5f7e0c9a-…",
    "resourceVersion": "421"
  },
  "dryRun": "none",
  "revisionHash": "bbbbbbbb",
  "accepted": true,
  "applied": true,
  "message": "API accepted annotation request; convergence not observed.",
  "followUp": "kubectl ome rollout status my-isvc -n prod --context=prod-admin"
}
```

`revisionHash` identifies the exact canary target the action was bound to —
it is an identity, not a convergence claim.

The whole action runs under a 45-second context, with each API request
capped at 10 seconds (a shorter `--request-timeout` is preserved). Safety
inspection is bounded — 32 related InferenceReplicas over at most two pages,
2048 instances and 256 migrations per replica — and incomplete or malformed
evidence refuses instead of accepting a partial view.
