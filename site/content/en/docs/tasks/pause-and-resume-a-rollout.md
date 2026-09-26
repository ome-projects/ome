---
title: "Pause and Resume a Rollout"
linkTitle: "Pause and Resume a Rollout"
weight: 25
date: 2026-09-26
description: >
  Hold in-flight InferenceService rollout work with kubectl ome rollout pause and release it with resume
---

This page shows you how to pause an in-flight rollout of an OMENative
InferenceService with `kubectl ome rollout pause`, what a pause actually holds
(and what keeps moving), and how to release it with `kubectl ome rollout
resume` — including how to atomically discard pending promote/rollback
requests on resume.

Both commands are **alpha** guarded actions: they preview the exact change,
require confirmation, and submit a compare-and-swap patch that either applies
completely or not at all. The related `rollout promote`, `rollout rollback`
and `rollout repin` commands are separate actions and are not covered here.

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome/). Both
  commands accept the standard kubectl connection flags (`--kubeconfig`,
  `--context`, `-n`) plus `--ome-namespace` for the control-plane namespace
  used in runtime revision lookups.
- You need the read RBAC from the plugin page **and** the `patch` rule on
  `inferenceservices` shown in its [Required
  RBAC](/ome/docs/tasks/kubectl-ome/#required-rbac) section. `pause`
  additionally lists the service's InferenceReplicas to prove work is in
  flight, which the reader role already grants.
- The target service must run at least one component in `OMENative`
  deployment mode. Services owned by a placement control plane (or carrying
  placement annotations) are refused — a pause written locally would be
  fought or overwritten by the placement owner.

## What a pause holds — and what it does not

The commands manage one annotation on the InferenceService,
`ome.io/rollout-paused`, which the controller recognizes at exactly two
depths:

| Value    | Effect                                                                                                                                  |
|----------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `true`   | Pause: no Update, Create or Migration work starts or advances. Each component's RestartPolicy keeps repairing existing instances at their current revision. |
| `freeze` | Full stop: instance repair is suspended too. Only kubelet container restarts and deliberate scale-down remain.                            |

`kubectl ome rollout pause` always writes `"true"`. It never requests
`freeze`, and it never silently downgrades one: if the service already
carries either depth, the command refuses with `service is already paused;
freeze is preserved`. `kubectl ome rollout resume` removes the annotation
whichever recognized depth it holds. Any other value (`True`, `false`, an
empty string) is treated as *not paused* — the controller ignores it and
`resume` refuses to clear it.

Even at depth `true`, a pause is a hold on OMENative lifecycle work, not a
full workload freeze:

- **Timed canary gates keep aging.** A step whose timed gate expires during
  the pause may advance immediately on resume.
- **Deliberate scale-down and deletion teardown can still proceed.**
- **Status stays truthful** at either depth: an instance that loses every pod
  stops reporting Ready, even under a `freeze` where its repair stays parked.

## Pause a rollout

```bash
kubectl ome rollout pause chat -n prod
```

The command reads the live InferenceService, resolves its active runtime, and
collects fresh controller evidence that rollout or lifecycle work is actually
in flight. It then prints a preview to **stderr** — headed `ALPHA guarded
action preview (not controller convergence)` — showing the exact target
identity (UID and resourceVersion), the affected OMENative components, the
annotation about to be set, and the observed active operation/migration
counts, followed by the scope warnings above. Confirm at the
`Confirm this exact action? [y/N]` prompt.

The prompt requires a real terminal. In scripts and pipelines, pass `--yes`
to accept the exact previewed action non-interactively; without it,
non-interactive input fails with `action not confirmed; noninteractive input
requires --yes`.

On success, stdout carries exactly one `ActionResult`:

```bash
kubectl ome rollout pause chat -n prod --yes -o json
```

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-26T09:00:00Z",
  "action": "rollout pause",
  "target": {
    "kind": "InferenceService",
    "namespace": "prod",
    "name": "chat",
    "uid": "27b899a1-…",
    "resourceVersion": "84512"
  },
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted annotation request; convergence not observed.",
  "followUp": "kubectl ome rollout status chat -n prod --context=prod-cluster"
}
```

### When pause refuses

`pause` fails closed rather than writing an annotation it cannot prove safe:

- **Already paused** (either depth): `service is already paused; freeze is
  preserved`.
- **Nothing to hold**: `no applicable active rollout or lifecycle work was
  observed`. Pause holds in-flight work; it refuses on an idle service.
- **A promote or rollback mailbox is pending** (`ome.io/rollout-promote` or
  `ome.io/rollout-rollback` is set): pausing would strand a queued one-shot
  action, so the command refuses.
- **Stale or inconsistent controller evidence**, for example when the
  controller has not yet observed the current generation.
- **Inspection bounds exceeded**: safety evidence is capped at 32 related
  InferenceReplicas read over at most two pages, and 2048 instances and 256
  migrations per replica. Incomplete, stale or malformed evidence refuses
  instead of accepting a prefix.

## Verify the hold

Acceptance is **not** controller convergence: a successful result only means
the API server stored the annotation. Use the `followUp` command to watch the
coordination groups actually settle:

```bash
kubectl ome rollout status chat -n prod
```

## Resume a rollout

```bash
kubectl ome rollout resume chat -n prod
```

Resume removes `ome.io/rollout-paused` at either recognized depth — the same
command releases a CLI-written `true` and a manually applied `freeze`. It
refuses with `service has no recognized pause` when the annotation is absent
or holds an unrecognized value. Remember that timed canary gates kept aging
during the pause, so gated steps may advance immediately after the resume is
consumed.

Like pause, resume refuses when a promote or rollback mailbox is pending —
otherwise clearing the pause would release the rollout straight into a queued
action you may not know is there.

## Discard pending actions on resume

To clear the pause *and* the queued mailboxes in one step:

```bash
kubectl ome rollout resume chat -n prod --discard-pending-actions --yes
```

`--discard-pending-actions` removes any pending `ome.io/rollout-promote` and
`ome.io/rollout-rollback` annotations in the **same guarded patch** that
removes the pause. All removals share one set of UID/resourceVersion
preconditions, so either everything applies or nothing does — a queued action
can never survive while the pause is cleared, or vice versa. The preview
lists each discarded annotation with its value.

Because it deletes another operator's queued intent, the flag demands strong
confirmation: it requires `--yes` and fails before contacting the cluster
without it (`discarding pending actions requires --yes`). The flag exists
only on `resume`.

## How the guard works

Every write is a JSON Patch that begins with `test` operations on
`/metadata/uid` and `/metadata/resourceVersion` as read during the preview.
If anything else modified the InferenceService in between, the API server
rejects the whole patch and nothing is applied. The CLI reports
`guarded annotation patch rejected; refresh rollout status and retry
explicitly` and exits with code 3 (other failures exit 1). Re-running the
command re-reads the live object and previews a fresh patch; there is no
automatic retry.

Both commands accept `--dry-run`:

| Mode     | Behavior                                                                                             |
|----------|------------------------------------------------------------------------------------------------------|
| `none`   | Default. Sends the guarded patch after confirmation.                                                   |
| `client` | Validates, previews and confirms, but sends no patch. Result: `Validated locally; no patch sent.`      |
| `server` | Sends the identical guarded patch with `dryRun=All`. Result: `API dry-run accepted; no changes persisted.` |

Preview and prompt always go to stderr; stdout carries exactly one
`ActionResult` in the format chosen with `-o table|wide|json|yaml`. The whole
action runs under a 45-second context and each API request is capped at 10
seconds; a shorter `--request-timeout` is preserved. External credential
plugins and custom transports may not honor cancellation.
