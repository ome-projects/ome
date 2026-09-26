---
title: "Repin a Drifted Rollout Plan"
linkTitle: "Repin a Drifted Rollout"
weight: 25
date: 2026-09-26
description: >
  Adopt a mid-run rollout plan edit into the active run with the guarded kubectl ome rollout repin command.
---

When a rollout run opens, the controller freezes ("pins") a render of the effective plan into `status.rollout.activeRun.plan`. Editing `spec.rollout` or a referenced RolloutPolicy while the run is active does not change the running rollout: the edit is inert, the `RolloutPlanDrift` condition turns `True`, and the new plan applies at the **next** run — unless you explicitly repin.

`kubectl ome rollout repin` (alpha) is the guarded drift-recovery verb. It asks the controller to replace the active run's pinned progression bodies with a fresh render of the current source, **preserving run identity and progress** — the run ID, its open time, and how far it has advanced all remain. This makes it distinct from the other rollout actions:

| Verb | Question it answers |
| --- | --- |
| `rollout pause` / `resume` | Should the pinned plan keep advancing right now? |
| `rollout promote` / `rollback` | Should this revision go forward or be abandoned? |
| `rollout repin` | Should the run adopt the plan edit I just made, without restarting? |

Rollout plan drift is also unrelated to *runtime* pin drift (`ome.io/runtime-sync`), which advances a pinned runtime snapshot — see [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision).

## Before you begin

- The [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) installed. All rollout action commands are alpha.
- RBAC to `get` and `patch` `inferenceservices` in the `ome.io` group — see the [action-command rule](/ome/docs/tasks/kubectl-ome/#required-rbac) on the plugin page.
- The service must use the OMENative deployment mode with an active rollout run.

## Step 1: Confirm the drift

The controller reports drift on the InferenceService:

```bash
kubectl get isvc my-service -n team-a \
  -o jsonpath='{.status.conditions[?(@.type=="RolloutPlanDrift")]}'
```

```json
{"type":"RolloutPlanDrift","status":"True","reason":"SpecNewerThanRun",
 "message":"groups[0]: live render rp1:9f2c41d0aa17 differs from pinned rp1:5b8e02c3fd64; the edit applies at the next run (or via ome.io/rollout-repin)"}
```

The reason is `SpecNewerThanRun` when the inline group body changed, or `PolicyNewerThanRun` when a referenced RolloutPolicy changed. `kubectl ome rollout explain my-service -n team-a` shows the same evidence alongside the run's observed progress.

## Step 2: Preview and confirm the repin

```bash
kubectl ome rollout repin my-service -n team-a
```

The command issues **one bounded, uncached GET** of the InferenceService — and zero related-resource reads — proves eligibility from that single snapshot, then prints a preview on stderr and asks `Confirm this exact action? [y/N]`:

```
ALPHA guarded rollout repin (not controller convergence)
FIELD               VALUE
Action              rollout repin
Context             prod-cluster
Workload NS         team-a
Target              InferenceService/my-service
UID                 1f0e9d8c-...
ResourceVersion     184467
Dry-run             none
Run                 my-service-0123456789ab
Pinned plan digest  rp1:83f0a1b2c9d5
Requested digest    rp1:1c7a90b2e3d4
Group count         1
Set annotation      ome.io/rollout-repin
Value               rp1:1c7a90b2e3d4
Run identity and progress remain; only pinned progression bodies change.
A clamped canary step may hold before raising traffic.
The controller CAS covers progression renders, not full plan topology.
The CLI separately refuses topology changes before sending a PATCH.
The exact InferenceService UID/resourceVersion is tested atomically.
```

On confirmation it sends exactly one JSON patch, guarded so it applies to the precise object version you just reviewed:

```json
[
  {"op": "test", "path": "/metadata/uid", "value": "1f0e9d8c-..."},
  {"op": "test", "path": "/metadata/resourceVersion", "value": "184467"},
  {"op": "add", "path": "/metadata/annotations/ome.io~1rollout-repin", "value": "rp1:1c7a90b2e3d4"}
]
```

Flags:

- `--yes` confirms the exact preview without the interactive prompt. Without a terminal on stdin the command fails unless `--yes` is set.
- `--dry-run=client` performs the read, eligibility proof, preview and confirmation but sends no PATCH. `--dry-run=server` sends the identical guarded PATCH with `dryRun=All`, so the API server evaluates it without persisting.
- `-o table|wide|json|yaml` formats the result. Preview and prompt go to stderr; stdout carries a single typed `ActionResult`, so `-o json` is safe to script against:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-26T09:00:00Z",
  "action": "rollout repin",
  "target": {"kind": "InferenceService", "namespace": "team-a", "name": "my-service", "uid": "1f0e9d8c-...", "resourceVersion": "184467"},
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted repin annotation; controller consumption and plan replacement were not observed.",
  "followUp": "kubectl ome rollout explain my-service -n team-a --context=prod-cluster",
  "rollout": {"runID": "my-service-0123456789ab", "pinnedPlanDigest": "rp1:83f0a1b2c9d5", "requestedPlanDigest": "rp1:1c7a90b2e3d4", "groupCount": 1}
}
```

`pinnedPlanDigest` and `requestedPlanDigest` are **combined plan digests** — an order-sensitive fold over every group's progression-render digest — so they never equal an individual per-group digest from the drift message, even for a one-group plan.

## What the command checks before patching

Every check runs against the single snapshot the command just read; any failure means no PATCH is sent. Each error below is reported with the prefix `rollout repin refused:`.

| Refusal | Meaning |
| --- | --- |
| `no active pinned run` | `status.rollout.activeRun` is absent — there is nothing to repin. |
| `a rollout action mailbox is already present` | A `rollout-repin`, `rollout-promote` or `rollout-rollback` annotation is already pending; the controller must consume it first. |
| `empty live plans are not supported by this command` | `spec.rollout` was removed or has no groups. A repin to an empty render closes the run — that is a deliberate abort, and this command refuses to do it implicitly. |
| `controller plan evidence is missing, stale or inconsistent` | The reported status is not a complete, current proof: the `RolloutPlanReady`/`RolloutPlanDrift` conditions are missing, duplicated, timestamped outside the run's open/pin window, or the drift message no longer matches the digests computed from this same snapshot. Re-read after the controller catches up. |
| `the current plan is already pinned` | The live render equals the pinned plan (drift is `InSync`) — there is nothing to adopt. |
| `live and pinned rollout topology differ` | Group count, `components`, `order`, `soak`, `maintainRatio`, or a group's progression kind (canary vs. blue-green vs. rolling) changed. A topology change is not a repin; it takes effect at the next run. |
| `this command supports at most one canary group` | More than one live group declares a canary progression. |

Because the command reads only the InferenceService, a policy-referenced group's current digest comes from the controller's reported resolution (`status.rollout.groups[].observedDigest`), never from reading the RolloutPolicy itself.

## Why the value is a digest, not "now"

The `ome.io/rollout-repin` annotation is a one-shot verb with a compare-and-swap contract: its value is the **expected combined render digest**. When the controller consumes the annotation it performs its own fresh render from the live source and compares:

- If the fresh render matches the requested digest, the pinned plan is replaced.
- If it does not match — a concurrent edit landed between your review and consumption — the repin is **rejected** with a `RolloutRepinRejected` warning event: `expected render digest rp1:... but the current render is rp1:... (a concurrent edit landed; re-issue with the current digest)`.

The controller also accepts the literal value `"now"`, which skips the digest check entirely and adopts *whatever renders at consumption time*. The guarded command never sends `"now"`: you confirmed a preview of one specific requested digest, and the CAS guarantees that exactly that reviewed render is pinned — or nothing is. Note the controller's CAS covers progression renders only, not plan topology; the CLI's separate topology refusal (above) is what keeps a repin from crossing a shape change.

The annotation is consumed in every branch, including rejection, so a failed repin never wedges the object — re-check the drift and re-issue the command.

## What the controller does after acceptance

API acceptance is **not** controller convergence — the result message says so explicitly. When the controller consumes the annotation and the CAS passes, it:

- Replaces `status.rollout.activeRun.plan` with the fresh render and re-stamps `pinnedAt`, preserving the run ID, open time and progress.
- Emits a Normal `RolloutPlanRepinned` event: `run my-service-0123456789ab repinned: rp1:... -> rp1:...`.
- Returns the `RolloutPlanDrift` condition to `False` / `InSync`.
- **Clamps a canary rather than raising exposure.** The current step index is clamped into the new ladder (a canary already finished under the old ladder stays finished), and if the clamped step's traffic exceeds the currently programmed weight, the run enters a pre-step hold at the current capacity and traffic until you explicitly `rollout promote`. A repin can only hold or tighten exposure, never silently increase it.

## Step 3: Verify

Use the follow-up command printed in the result:

```bash
kubectl ome rollout explain my-service -n team-a
```

and check the events for the outcome:

```bash
kubectl get events -n team-a --field-selector involvedObject.name=my-service \
  | grep -E 'RolloutPlanRepinned|RolloutRepinRejected'
```

## Failure modes and exit codes

The command uses a 45-second overall context, caps each API request at 10 seconds, bounds responses at 1 MiB, refuses redirects, and **never retries a mutation**:

- A guard conflict (the object changed between the read and the PATCH, so the `test` operations failed) exits with code **3** and `guarded annotation patch rejected; refresh rollout explain and retry explicitly`. Re-running the command re-reads and re-proves against the new object version.
- An ambiguous outcome — a server error, an oversized or malformed response — reports `outcome unknown; do not replay, check rollout explain`. The PATCH may or may not have applied; inspect before acting again.

## Setting the annotation by hand

You can trigger a repin without the plugin:

```bash
kubectl annotate isvc my-service -n team-a \
  ome.io/rollout-repin="rp1:1c7a90b2e3d4" --overwrite
```

This is a plain overwrite: nothing verifies an active drifted run, an unchanged topology, or the absence of a pending promote/rollback, and `--overwrite` can clobber a pending action. The controller's digest CAS still applies — but if you pass `"now"` even that last guard is skipped, and an edit that lands after you looked is adopted sight unseen. Prefer the guarded command; if you must annotate by hand, use the exact digest and watch for the `RolloutPlanRepinned` / `RolloutRepinRejected` event.

## What's next

- [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome) — installation, connection flags, and the RBAC rules for action commands
- [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision) — the *other* pin: runtime snapshots and `ome.io/runtime-sync`
