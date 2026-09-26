---
title: "kubectl-ome Guarded Actions"
linkTitle: "Guarded Actions"
weight: 3
description: >
  The shared safety contract behind every mutating kubectl-ome command: dry-run modes, confirmation, the guarded patch, the ActionResult output, and exit codes.
---

Every mutating command in the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) is a **guarded action**: it follows one shared safety contract, so dry-run, confirmation, output, and exit-code semantics are identical across commands. This page documents that contract once; individual command pages only describe what each action changes and when it is eligible.

All guarded actions are **alpha**. The commands and what they patch:

| Command | Patched object |
|---------|----------------|
| `kubectl ome rollout pause` / `resume` / `promote` / `rollback` / `repin` | InferenceService |
| `kubectl ome traffic drain` / `undrain` | InferenceService |
| `kubectl ome migration start` | InferenceService |
| `kubectl ome runtime sync` | InferenceService |
| `kubectl ome scale` | InferenceReplica (`scale` subresource) |
| `kubectl ome instance release-held` | InferenceReplica |

Read-only reports (`status`, `explain`, `history`, …) and the `wait` command are not guarded actions and are not covered here.

## The shared sequence

Every guarded action runs the same steps, in order:

1. **Local validation.** Flags and the target name are validated before any client is constructed. Invalid flags fail with a deliberately generic message (`invalid ... flags; use --help`) that never echoes the offending value.
2. **Live safety reads.** The action fetches the live target and whatever related evidence its eligibility rules need (for example, related InferenceReplicas for `rollout pause`, snapshot history for `runtime sync`). Reads are bounded and paged; incomplete, stale, or malformed evidence makes the action **refuse** rather than proceed on a partial view.
3. **Eligibility checks.** Each action verifies its own preconditions against that evidence (current controller evidence, exact target identity, no conflicting in-flight work, and so on).
4. **Preview on stderr.** The exact planned change — target identity, current state, and what the patch will set — is written to **stderr**, never stdout.
5. **Confirmation.** Interactive terminals are prompted; `--yes` replaces the prompt (see below).
6. **Guarded patch.** A JSON Patch is sent whose first operations are `test` preconditions on the target's `/metadata/uid` and `/metadata/resourceVersion` as captured at preview time (some actions also test the annotation value they are replacing). JSON Patch is atomic: if any `test` fails, the API server rejects the whole request and **nothing is modified**.
7. **One ActionResult on stdout.** Exactly one typed result document is written to stdout — and only after the outcome is known. On any failure, stdout stays empty.

## `--dry-run` modes

All actions accept `--dry-run=none|client|server` (default `none`):

| Mode | Live reads, eligibility, preview, confirmation | Patch sent | Persisted |
|------|-----------------------------------------------|------------|-----------|
| `none` | yes | yes | yes |
| `client` | yes | no | no |
| `server` | yes | yes, with `dryRun=All` | no |

`--dry-run=client` is **not offline**. It performs the same live safety reads, eligibility checks, preview, and confirmation as a real run — it only skips the patch. It therefore still needs read RBAC, a reachable cluster, and an eligible target; an ineligible target fails a client dry-run exactly as it would fail a real run.

`--dry-run=server` sends the **identical guarded JSON Patch** with the Kubernetes `dryRun=All` option: the API server evaluates the `test` preconditions and runs admission, but persists nothing. Because it is the same request, a stale target fails a server dry-run with exit code 3 just like a real run — and because Kubernetes authorizes dry-run writes as writes, it requires the same `patch` RBAC as a real run.

## What `--yes` does and does not do

Without `--yes`, an interactive terminal shows the preview and prompts:

```
Confirm this exact action? [y/N]
```

The prompt **defaults to no** — only `y` or `yes` proceeds. When stdin is not a terminal (a pipe or redirect), the action cannot be confirmed interactively at all and fails with `action not confirmed; noninteractive input requires --yes`; piping `yes` into the command does not work. Scripts must pass `--yes`.

`--yes` bypasses **only the prompt**. Every other safeguard still runs and can still refuse: the live safety reads, the eligibility checks, the UID/resourceVersion preconditions in the patch, and API server admission. There is no force flag; an ineligible target cannot be pushed through with `--yes`. Flags with destructive side effects (such as `rollout resume --discard-pending-actions`) additionally *require* `--yes` and refuse to even prompt without it.

## The ActionResult on stdout

Success writes exactly one `cli.ome.io/v1alpha1` `ActionResult` to stdout, in the format chosen with `-o table|wide|json|yaml`. A `rollout pause` example with `-o json`:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-15T21:00:00Z",
  "action": "rollout pause",
  "target": {
    "kind": "InferenceService",
    "namespace": "prod",
    "name": "chat",
    "uid": "uid-chat",
    "resourceVersion": "42"
  },
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted annotation request; convergence not observed.",
  "followUp": "kubectl ome rollout status chat -n prod --context=moirai"
}
```

Field semantics:

| Field | Meaning |
|-------|---------|
| `target` | The exact object identity the guard tested. `resourceVersion` is the **previewed** value the patch was conditioned on, not the version after the patch. |
| `dryRun` | The mode the action ran in: `none`, `client`, or `server`. |
| `accepted` | The API server accepted the request (real run or server dry-run). Always `false` for client dry-run, which sends nothing. |
| `applied` | The patch was persisted. `true` only with `dryRun: none`; both dry-run modes always report `false`. |
| `message` | Human-readable qualifier for the outcome. |
| `followUp` | The exact read-only command to observe what the controller actually does next. |
| `requestID`, `revisionHash`, `scale`, `traffic`, `rollout` | Optional action-specific details (for example, `runtime sync` emits the `requestID` token it wrote). |

**`accepted` and `applied` describe the API operation, not controller convergence.** Guarded actions write annotations or subresources that the OME controller consumes asynchronously; `applied: true` means the API server persisted the request, not that the rollout paused, the traffic drained, or the replicas scaled. Run the `followUp` command to observe convergence.

The invariants are enforced in the output itself: a result with `dryRun: client` can never claim `accepted`, and a result with any dry-run mode can never claim `applied`.

## Exit codes

| Code | Meaning for a guarded action |
|------|------------------------------|
| `0` | The action completed for its dry-run mode (validated locally, dry-run accepted, or patch applied). |
| `1` | Any other failure: invalid flags, refused or missing confirmation, ineligible target, RBAC denial, timeout, or an unverifiable response. Also used when the outcome is **unknown** (see below). |
| `3` | **Mutation conflict**: the guarded patch was rejected because the live object no longer matches the previewed identity. Nothing was modified. |

Exit code `2` is reserved for read-only assertion and `wait` commands and is never produced by a guarded action.

**Exit code 3** is the guard working as designed. It is returned when the API server answers the patch with a `409 Conflict`, or with its canonical generic `422` status for a failed JSON Patch `test` operation — meaning the target was modified, recreated, or raced by another actor between the preview and the patch. The failed request changed nothing; re-run the `followUp` status command to see the new state, then decide explicitly whether to re-run the action. The CLI recognizes only the API server's own generic patch-rejection status as a conflict: an admission-webhook rejection whose message merely quotes JSON Patch text still exits `1`.

Two failure shapes deserve care in scripts:

- **Outcome unknown (exit 1).** If the response is oversized, malformed, or not verifiably bound to the request — or writing the result to stdout fails after the patch was sent — the error says so (`outcome unknown` / `check ... status`) and the CLI refuses to claim success. The patch **may already have been accepted**; check with the follow-up status command instead of blindly retrying.
- **Generic error text.** Diagnostics are intentionally bounded and never relay raw API server messages or flag input, so different underlying causes can produce the same terse message. Script against the exit code and the follow-up command, not the error string.

## Timeouts and response bounds

Each action runs under a **45-second action context** covering everything including the confirmation wait; each individual API request is additionally capped at **10 seconds**. A shorter configured `--request-timeout` is preserved, never widened. External credential plugins or custom transports may not honor cancellation.

The guarded patch is sent over a bounded transport: its response body (including error responses) is capped at 1 MiB, and the response must prove it is bound to the request (correct kind, name, namespace, and UID) before the action reports acceptance. Safety reads are bounded and paged as well, with their own larger caps.

## RBAC

Both dry-run modes still perform the live safety reads, so a guarded action needs the plugin's read RBAC plus the `patch` rule for its target — including for `--dry-run=server`. See [kubectl-ome Plugin — Required RBAC](/ome/docs/tasks/kubectl-ome#required-rbac) for the exact rules, including the explicit `inferencereplicas/scale` subresource entry that `scale` needs.
