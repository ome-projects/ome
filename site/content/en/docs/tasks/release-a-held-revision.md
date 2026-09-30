---
title: "Release a Held Revision"
linkTitle: "Release a Held Revision"
weight: 21
date: 2026-09-26
description: >
  How a failing target revision backs off and Holds under `lifecycle.updateRetry`, and how to release one exact Held retry block with the guarded `kubectl ome instance release-held` action
---

When update attempts for a component's target revision keep failing, the
controller records a **retry block** in `status.retryBlocks` on that
component's InferenceReplica — one block per failed target revision, shared by
every instance attempting it. A block in state `Backoff` still has attempts
left; a block in state **`Held`** means attempts are exhausted (or retry was
never configured), and the controller will not attempt that revision again on
its own. Only a new target revision — or an explicit operator release — ends
the hold. How many attempts a revision gets before that, and how they are
spaced, is the [`lifecycle.updateRetry`
backoff](#how-a-revision-becomes-held-the-lifecycleupdateretry-backoff).

`kubectl ome instance release-held` is the guarded way to request that
release. It writes a **release mailbox annotation**
(`ome.io/release-held-revision`) onto the exact InferenceReplica, naming the
exact Held revision; the controller answers the request on a later reconcile.
The command is **alpha**, ships with the [kubectl-ome
plugin](/ome/docs/tasks/kubectl-ome), and only works for InferenceServices
with `spec.deploymentMode: OMENative`.

> **Note:** This command submits a *request*. API acceptance is not the
> release itself — see [What acceptance means — and what it does
> not](#what-acceptance-means--and-what-it-does-not).

## How a revision becomes Held: the `lifecycle.updateRetry` backoff

Whether the controller retries a failed target revision at all — and how many
times, how fast — is set by one operator-level block, `lifecycle.updateRetry`,
in the `inferenceservice-config` ConfigMap. There is no per-service or
per-component override. The `ome-resources` chart renders it from
`ome.controller.lifecycle.updateRetry` with these defaults:

```yaml
ome:
  controller:
    lifecycle:
      updateRetry:
        maxAttempts: 3    # attempts at one target revision before Held
        initialDelay: 1m  # backoff after the first failed attempt
        maxDelay: 30m     # backoff ceiling
        multiplier: 2.0   # backoff growth per attempt
```

When an attempt toward the component's target revision fails **and the
failure is workload-caused** — evidence that deterministically travels with
the pod template, such as `ImagePullBackOff` or an invalid pod spec (see
[what happens when the deadline
expires](/ome/docs/administration/instance-readiness-deadlines/#what-happens-when-the-deadline-expires))
— the failure charges one attempt against the revision's retry block:

- **Attempts remain** (`attemptsStarted` < `maxAttempts`): the block enters
  `Backoff` and persists the resume time in `nextRetryAt`. The wait after
  attempt *n* is `initialDelay × multiplier^(n−1)`, capped at `maxDelay` —
  with the chart defaults, a bad revision is attempted three times in total,
  backing off 1m after the first failure and 2m after the second.
- **Attempts exhausted** (`attemptsStarted` ≥ `maxAttempts`): the block
  enters `Held` and `nextRetryAt` is cleared. The controller emits the
  one-shot [`RetryHeld` warning](#the-retryheld-event), reports the hold in
  the InferenceReplica's `status.rolloutHold` (mirrored onto the
  InferenceService's component status), and never attempts the revision
  again on its own.

While a block is in `Backoff`, no fresh attempt at that revision starts
before `nextRetryAt`. Once the backoff is due and an attempt is admitted, the
block shows `RetryInProgress` (`RUNNING` in the `retry-blocks` table) —
exactly one retry attempt runs at a time. An attempt that converges the
component on the revision removes the block. `attemptsStarted` counts these
lifecycle attempts, never kubelet container restarts.

A failure the revision cannot be blamed for — an elapsed readiness deadline,
an ambiguous kubelet reason — never charges the ladder: the block re-enters
`Backoff` at its current rung's delay with `attemptsStarted` unchanged, so no
number of such failures reaches `Held` by itself.

### When `updateRetry` is unset: the first failure Holds

The OME binary has **no built-in retry policy**. When the `updateRetry` block
is absent — or invalid: `maxAttempts` ≤ 0, `multiplier` < 1, a delay that
does not parse or is not positive, or `maxDelay` < `initialDelay` — the
controller treats retry as unconfigured and fails safe: the **first**
workload-caused failure toward a target revision Holds it immediately, with
the same `RetryHeld` event. The cause is named in a V(1) controller log;
there is never a silent fallback to baked-in numbers. The chart ships the
block by default, so this is the behavior when you remove or misedit it.

The policy is re-read from the ConfigMap on every reconcile (through a
short-TTL cache), so an edit applies to future failures without a controller
restart — but never retroactively: raising `maxAttempts` does **not** release
a block that is already `Held`. Only a new target revision or an explicit
release (the rest of this page) ends an existing hold; after a release, a
revision that fails again starts a fresh block at zero attempts.

### The `RetryHeld` event

At the transition into `Held` — once per block, not once per reconcile — the
controller emits a Warning event against the InferenceService:

```
Warning  RetryHeld  InferenceReplica prod/chat-engine component=engine update
to revision chat-engine-1f2a3b4c held after 3 failed attempt(s) (last
failure: ImagePullBackOff); publish a corrected revision or raise
lifecycle.updateRetry limits
```

```bash
kubectl get events -n prod --field-selector reason=RetryHeld
```

Publishing a corrected revision is the normal exit. Releasing the block — the
steps below — is for when the *same* revision is expected to succeed as-is: a
bad image tag that has since been pushed, a registry outage that has ended.

## Before you begin

- The [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) installed.
- RBAC: the plugin's baseline read rules, plus `patch` on
  `inferencereplicas` (`ome.io` group) — see [Required
  RBAC](/ome/docs/tasks/kubectl-ome/#required-rbac). The read pass also
  resolves the effective runtime, so it reads ServingRuntimes /
  ClusterServingRuntimes and, for pinned services, `ControllerRevision`
  snapshots in the OME control-plane namespace (`--ome-namespace`, default
  `ome`).
- An OMENative InferenceService whose component reports a Held retry block.

## Step 1: Find the Held revision

`kubectl ome instance retry-blocks` is the read-only discovery command — it
shows the controller-reported retry authority for one component and never
patches anything:

```bash
kubectl ome instance retry-blocks chat --component engine -n prod
```

Look for a row with state `HELD`. The `REL` column is `YES` only when the
collection evidence is complete and the Held block is current and valid —
exactly the eligibility that `release-held` re-verifies before submitting.

Target revisions are the component's workload revision names, formed as
`<service>-<component>-<hash>` where `<hash>` is eight lowercase hex
characters — for example `chat-engine-1f2a3b4c`. (These are pod-template
revisions of the component, not the runtime pin snapshots described in
[Runtime Revisions](/ome/docs/concepts/runtime-revision).)

## Step 2: Preview with a dry run

```bash
kubectl ome instance release-held chat \
  --component engine \
  --revision chat-engine-1f2a3b4c \
  --dry-run=client -n prod
```

`--revision` accepts two forms:

| Form | Example | Rules |
|------|---------|-------|
| Full scoped target name | `chat-engine-1f2a3b4c` | Must exactly equal one block's `targetRevision`. |
| Bare revision hash | `1f2a3b4c` | Exactly eight lowercase hex characters; must match exactly one block on the InferenceReplica. An ambiguous hash is refused. |

Whichever form you pass, the submitted annotation value is always the **full**
target revision name.

The preview and confirmation prompt go to **stderr**; stdout carries a single
typed `ActionResult`. The preview table shows the exact identities the request
is bound to (parent UID/resourceVersion/generation, InferenceReplica
UID/resourceVersion, the resolved revision and hash, attempts started, pause
depth) and restates the action's limits.

Dry-run modes:

- `--dry-run=client` runs the full read, eligibility check, preview,
  confirmation, and refresh, but sends no PATCH at all (`accepted: false`,
  message `Validated locally; no patch sent.`).
- `--dry-run=server` sends the same guarded JSON Patch with `dryRun=All`, so
  the API server (including admission) validates it without persisting
  anything.

## Step 3: Submit the release request

```bash
kubectl ome instance release-held chat \
  --component engine \
  --revision chat-engine-1f2a3b4c \
  -n prod
```

The command prints the preview and asks `Confirm this exact action? [y/N]`.
Pass `--yes` to skip the prompt — noninteractive input (a pipe or script)
*requires* `--yes`. `--yes` bypasses only the confirmation, never the
eligibility checks or the post-confirmation revalidation.

With `-o json` the result looks like:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-26T12:00:00Z",
  "action": "instance release-held",
  "target": {
    "kind": "InferenceReplica",
    "namespace": "prod",
    "name": "chat-engine",
    "uid": "6c9c2f6e-...",
    "resourceVersion": "812"
  },
  "dryRun": "none",
  "revisionHash": "1f2a3b4c",
  "accepted": true,
  "applied": true,
  "message": "API accepted release annotation request; controller release/convergence not observed.",
  "followUp": "kubectl ome instance retry-blocks chat --component=engine -n prod --context=my-context"
}
```

`-o table` (default) and `-o wide` render the same bounded fields as a table.
Keep the `target.name`, `target.uid`, and `revisionHash` values — the wait
predicate in [Step 4](#step-4-watch-the-outcome) needs them.

## Eligibility: what live evidence must exist

The command refuses to patch — with no request sent — unless all of the
following hold on the live objects, both when first read and again after you
confirm:

- The InferenceService exists, is `OMENative`, and its effective runtime
  resolves; `--component` (`engine`, `decoder`, or `router`) is one of the
  service's components.
- **Complete bounded source collection**: every InferenceReplica of the
  service (listed by the `ome.io/inferenceservice` label, at most 32 items in
  2 pages of 16) is admitted. A truncated listing, a duplicate component, or
  any sibling that fails validation refuses the whole request.
- The selected InferenceReplica is solely controller-owned by this exact
  InferenceService (owner reference UID match) and is not terminating. Its
  identity is re-read with a direct GET that must match the listing exactly.
- **Current native IR status**: the InferenceReplica's
  `status.observedGeneration` equals its `metadata.generation`.
- **Exact parent-generation stamp**: the IR's `ome.io/parent-generation`
  annotation equals the parent InferenceService's current
  `metadata.generation` — the controller's projection has caught up with your
  latest spec edit.
- **`ome.io/controller-write: "true"` is present** (the controller stamps it
  on every IR it writes). The patch preserves it; the InferenceReplica
  admission webhook rejects any write whose result lacks it.
- **The mailbox is absent**: no `ome.io/release-held-revision` annotation
  exists yet, whatever its value — one pending request at a time. A present
  mailbox refuses with `a release-held mailbox is already present`.
- Every retry block on the IR is internally valid (well-formed
  `<service>-<component>-<hash>` target revisions, consistent timestamps), and
  exactly one block matches `--revision` **in state `Held`**. `Backoff` and
  `RetryInProgress` blocks are not releasable.

## How the request is guarded

After the preview is confirmed, the command re-reads everything — parent,
effective runtime, the full InferenceReplica membership — and compares it to
the previewed snapshot. If anything changed (parent resourceVersion, UID, or
generation; the selected IR's resourceVersion; a block's state or attempt
count; sibling membership; the resolved runtime), it exits with code 3 and
patches nothing: `held-release preview became stale; inspect instance
retry-blocks and retry explicitly`.

The PATCH itself is a JSON Patch with `test` preconditions, so it can only
apply to the exact object version that was previewed:

```json
[
  { "op": "test", "path": "/metadata/uid", "value": "<ir-uid>" },
  { "op": "test", "path": "/metadata/resourceVersion", "value": "<ir-rv>" },
  { "op": "add",
    "path": "/metadata/annotations/ome.io~1release-held-revision",
    "value": "chat-engine-1f2a3b4c" }
]
```

If the API server rejects the guarded patch (conflict or precondition
failure), the command exits 3 without retrying. The whole action is capped at
45 seconds and each API call at 10 seconds (a shorter kubeconfig request
timeout is preserved). There is no implicit wait and no automatic replay.

## What acceptance means — and what it does not

A successful submission means exactly one thing: **the API server persisted
the mailbox annotation on the InferenceReplica**. The release itself happens
later, inside the controller:

1. On a later reconcile the controller reads the mailbox. If a retry block
   matching the named revision exists **and is Held**, it removes that block
   (and only that block) and emits a Normal `RetryBlockReleased` event on the
   InferenceService. If no block matches, or the matching block is no longer
   Held, the request is consumed as a no-op with a `RetryBlockReleaseSkipped`
   event.
2. The annotation is then deleted (consume = acknowledge) against a fresh
   read. The block removal commits before the annotation delete, so a crash
   between the two re-delivers harmlessly into the no-op branch.

Because of this design, acceptance does **not** prove any of the following:

- **Not release or retry.** Removing a Held block only removes the gate; the
  controller may attempt the revision again on its own schedule. Nothing
  forces a retry, and releasing a block is not garbage collection or restored
  availability.
- **Not resumption.** If the rollout is paused, the mailbox may still be
  consumed, but the release does not resume the paused workload.
- **No durable receipt.** Unlike `runtime sync`, there is no request ID: the
  annotation value is just the revision name, and the controller keeps no
  record correlating a removal to *your* request. Historical retry records may
  also be retention-pruned.
- **Later absence is not attribution.** Finding the annotation and the block
  both gone later is *consistent* with your release being handled, but not
  proof it caused the removal — a new target revision releases holds
  naturally, another actor may have raced you, and a fresh failure can
  re-create a Held block for the same revision. The controller also matches
  on its own snapshot and does not compare-and-delete the exact annotation
  value it handled, so a concurrently replaced request value can be consumed
  without a separate answer.

Verify the outcome by observation, not by the exit code of this command.

## Step 4: Watch the outcome

Re-run the read-only follow-up the result printed:

```bash
kubectl ome instance retry-blocks chat --component=engine -n prod
```

To block until the exact revision is no longer Held on the exact
InferenceReplica the action targeted, use the wait predicate with the values
from the `ActionResult`:

```bash
kubectl ome wait chat --for=held-revision=unheld \
  --component=engine \
  --revision=chat-engine-1f2a3b4c \
  --ir-name=chat-engine --ir-uid=6c9c2f6e-... \
  -n prod --timeout=2m
```

It polls every 5 seconds and exits `0` when the revision is not Held and the
mailbox is absent on the same-UID InferenceReplica; exit `2` means the state
was not observed within the timeout. The report labels attribution
`Unverifiable` — matching is state observation, not proof your request caused
it.

The controller's answer is also visible as events on the InferenceService:

```bash
kubectl get events -n prod \
  --field-selector reason=RetryBlockReleased
kubectl get events -n prod \
  --field-selector reason=RetryBlockReleaseSkipped
```

A fresh Warning with reason `RetryHeld` after your release means the revision
was attempted again and failed its way back to `Held` — see [the backoff
section](#how-a-revision-becomes-held-the-lifecycleupdateretry-backoff).

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | The request was accepted by the API server (or the dry run validated). Not proof of release. |
| `1` | Refused before sending (ineligible, mailbox present, not confirmed), or the request outcome is unknown (for example, an unreadable API response). When the message says the outcome is unknown, inspect `instance retry-blocks` before submitting another request. |
| `3` | Precondition failure: the preview became stale between confirmation and send, or the API server rejected the guarded patch. Nothing was applied; re-run explicitly if still needed. |

## Manual alternative: annotate directly

The controller accepts the mailbox annotation from any writer, with either
the full target revision name or the bare hash as the value:

```bash
kubectl annotate inferencereplica <ir-name> -n prod \
  ome.io/release-held-revision=chat-engine-1f2a3b4c
```

(Find the IR with
`kubectl get inferencereplicas -n prod -l ome.io/inferenceservice=chat`.)

This skips every safety property above: no eligibility check, no staleness
guard, no confirmation — an unknown revision or a non-Held block is silently
consumed as a no-op. The write passes the InferenceReplica admission webhook
only because the object keeps its existing `ome.io/controller-write: "true"`
annotation, and it still requires `patch` on `inferencereplicas`. Prefer the
guarded action.
