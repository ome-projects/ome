---
title: "Request an Instance Migration"
linkTitle: "Request an Instance Migration"
weight: 30
date: 2026-09-26
description: >
  Ask OME to move one OMENative instance off its current node with kubectl ome migration start.
---

This page shows you how to request the migration of a single OMENative
instance with `kubectl ome migration start`. The command is **alpha** and is
deliberately narrow: it requests **one** migration of **one** instance by
writing an annotation onto the InferenceService — it never edits spec, status
or scale, and it never forces the controller to act. "Accepted" means the API
server stored the request annotation; it does not mean the migration has been
scheduled, delivered to the controller, or completed.

## Before you begin

- The [kubectl-ome plugin](/docs/tasks/kubectl-ome/) is installed.
- The component you are migrating runs in `OMENative` deployment mode. The
  command refuses components served in any other deployment mode.
- You have the [action RBAC](/docs/tasks/kubectl-ome/#required-rbac): the
  read-only baseline role plus `patch` on `inferenceservices` and `get` on
  `configmaps` in the workload namespace (the command reads the
  `<service>-ome-migration-audit` ConfigMap to check for prior requests).

## How a migration request travels

`migration start` records the request as a **mailbox annotation** on the
InferenceService:

```
ome.io/migration-request-v1-<uuid>
```

The value is a small JSON document — the only thing the controller receives:

```json
{
  "schemaVersion": "v1",
  "component": "engine",
  "instance": 3,
  "from_node": "node-a",
  "hint_target_nodes": ["node-b", "node-c"],
  "reason": "kernel upgrade on node-a",
  "requested_at": "2026-09-26T08:00:00Z",
  "requested_by": "kubectl-ome"
}
```

The annotation is written with a guarded JSON Patch that first `test`s the
InferenceService's `uid` and `resourceVersion`. If anything else modified the
service between the preview and the patch, the API server rejects the request
and nothing is written. The controller later consumes the mailbox, so the
annotation is not a permanent record.

Before writing anything, the command verifies the source instance end to end:
the component must resolve to a native runtime, the InferenceReplica for the
component must be current (not paused, not stale, owned by this service), the
instance must exist in the desired plan and be `Ready` with no operation in
flight, and the complete set of current-revision Pods for the instance must be
found, owned and running on named nodes. Any gap refuses the request with exit
code 3 rather than submitting a migration the controller could not honor.

## Request a migration

Pass the InferenceService name, the component (`engine`, `decoder` or
`router`) and the canonical instance index (a plain non-negative integer — no
leading zeros):

```bash
kubectl ome migration start chat --component=engine --instance=3 \
  --reason="kernel upgrade on node-a" -n prod
```

The command prints a preview on **stderr** and asks for confirmation:

```
ALPHA guarded migration preview (not controller convergence)
FIELD              VALUE
Action             migration start
Context            prod-cluster
Workload NS        prod
OME NS             ome
Target             InferenceService/chat
UID                6f6d0a7e-2b1c-4f0e-8f5a-9c3d2e1f0a6b
ResourceVersion    421
Dry-run            none
Request UUID       0e0f9a34-58c2-4f7e-9a4d-2b7c1d7e2a10
Schema             v1
Component          engine
Instance           3
From node          node-a
Hint nodes         <absent>
Reason             "kernel upgrade on node-a"
Requested at       2026-09-26T08:00:12Z
Requested by       kubectl-ome
Lookup only        false
Source IR          chat-engine
IR UID             1c9dd0b2-7a4e-4c8f-b3d6-5e2f1a0c9d8e
IR version         381
Migration mode     Auto
Running revision   chat-engine-5f8c4d9b
Incarnation        1
Current nodes      node-a
Node hints are soft preferences; capacity, scheduling and controller
convergence are not guaranteed.
Parent UID/resourceVersion CAS is not an IR/Pod/runtime transaction.
Confirm this exact action? [y/N] y
```

The interactive prompt only works on a real terminal; in scripts and CI, pass
`--yes` to confirm the exact previewed action. After confirmation the command
re-reads the source evidence one more time (a changed InferenceReplica, Pod
set or revision refuses with exit code 3), then sends the guarded patch. Only
the final ActionResult goes to **stdout**. The default table view truncates
long values, so script against `-o json`:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-26T08:00:14Z",
  "action": "migration start",
  "target": {
    "kind": "InferenceService",
    "namespace": "prod",
    "name": "chat",
    "uid": "6f6d0a7e-2b1c-4f0e-8f5a-9c3d2e1f0a6b",
    "resourceVersion": "421"
  },
  "dryRun": "none",
  "requestID": "0e0f9a34-58c2-4f7e-9a4d-2b7c1d7e2a10",
  "accepted": true,
  "applied": true,
  "message": "API accepted annotation request; delivery and convergence not observed.",
  "followUp": "kubectl ome migration status chat --component=engine -n prod --context=prod-cluster"
}
```

Record the `requestID` — it is how you find this request later in
`kubectl ome migration status`.

The whole action runs under a 45-second budget and each individual API request
under a 10-second cap.

## When --from-node is required

The command derives the source node from the instance's current Pods:

- **Single-node instance** (one Pod, or a leader/worker gang scheduled onto
  one node): `--from-node` is optional. The node is inferred and recorded.
- **Gang spanning nodes** (multi-pod instance whose Pods run on more than one
  node): `--from-node` is **required**, because "move this instance" is
  ambiguous — you must name which node it should vacate.

In both cases, a `--from-node` value that is not one of the instance's current
nodes refuses with exit code 3. The recorded `from_node` always comes from the
verified Pod evidence, never from unverified input.

## What the hint, reason and requested-by flags record

```bash
kubectl ome migration start chat --component=engine --instance=3 \
  --from-node=node-a --hint-node=node-b --hint-node=node-c \
  --reason="draining node-a" --requested-by=ops-oncall -n prod
```

| Flag | Recorded as | Semantics |
| --- | --- | --- |
| `--hint-node` | `hint_target_nodes` | Ordered **soft** placement preferences. At most **eight**; no duplicates; none may equal the source node. |
| `--reason` | `reason` | Free-text note, at most 256 printable characters. Refused if it looks like a credential (`Bearer …`, `token=`, `://`, key-shaped strings). |
| `--requested-by` | `requested_by` | Tool/operator label, at most 128 characters. Defaults to `kubectl-ome`. |

Two of these deserve emphasis:

- **Node hints do not reserve anything.** They express preference order to the
  controller, but capacity, scheduling and convergence are not guaranteed; the
  instance may land on a node you did not hint at, or the migration may not be
  dispatched at all. If you need a hard placement constraint, that is node
  affinity on the workload, not a migration hint.
- **`--requested-by` is advisory, not identity.** It is a label stored
  verbatim in the request payload for humans reading migration history. It is
  not authenticated and proves nothing about who ran the command — the API
  server's audit log is the authority for that.

## Dry runs

```bash
kubectl ome migration start chat --component=engine --instance=3 \
  --dry-run=server --yes -n prod
```

| Mode | What happens |
| --- | --- |
| `none` (default) | Guarded patch is sent and persisted. `accepted` and `applied` are both true. |
| `client` | Full local validation, confirmation and source recheck — but **no patch is sent**. Message: `Validated locally; no patch sent.` |
| `server` | The **same** UID/resourceVersion-guarded patch is sent with `dryRun=All`. The API server (including admission) evaluates it but persists nothing. Message: `API dry-run accepted; no changes persisted.` |

Both dry-run modes still confirm and still recheck source evidence, so a
`--dry-run=client` run exercises everything except the API write.

## Looking up a prior request with --request-id

A fresh `migration start` generates a new v4 UUID exactly once, before the
confirmation prompt, and shows it in the preview. `--request-id` does **not**
resubmit that request — it is lookup-only:

```bash
kubectl ome migration start chat --component=engine --instance=3 \
  --request-id=0e0f9a34-58c2-4f7e-9a4d-2b7c1d7e2a10 -n prod
```

- If the identical mailbox annotation is still retained on the
  InferenceService — same component, instance, hints, reason and requested-by,
  and the same from-node if you pass one — the command exits 0 without sending
  anything: `Identical retained mailbox observed; no replay sent; acceptance
  and convergence not observed.`
- If the UUID was never seen, or is known only from status, history or the
  audit ledger (the mailbox was already consumed or pruned), or the retained
  payload differs from your flags, the command **refuses** with exit code 3.
- If the retained payload is malformed, or the audit ledger cannot be read
  during a lookup, the command refuses with exit code 1.

The reason it never replays: the mailbox is consumed by the controller and the
retained history is bounded and can be lossy, so the CLI cannot prove that
resending an old UUID would not execute the migration a second time. Retries
are therefore always explicit — run a fresh `migration start`, get a fresh
UUID, and review the fresh preview. To inspect an old request instead of
re-requesting, use `kubectl ome migration status` or
`kubectl ome migration history`.

## Why a request is refused

The command prefers refusing over submitting a request the controller could
not act on safely. Exit codes are stable:

| Exit code | Meaning |
| --- | --- |
| `0` | Request accepted (or identical retained request found via `--request-id`). |
| `1` | General error, or the outcome is unknown (for example an oversized or unrecognizable API response). Check `migration status` with the preview UUID before retrying. |
| `3` | A precondition conflicts or the guarded patch was rejected. Nothing was written. |

Common exit-3 refusals:

- The rollout is paused, or a promote/rollback request is pending on the
  service — migration would race the rollout.
- The InferenceReplica's migration policy is `Never` (or unrecognized).
- A pending request or a non-terminal migration already targets this
  component and instance — dispatch is serial; wait for it to finish.
- The instance is not `Ready`, has an operation in flight, is mid-revision, or
  its Pod set is incomplete or missing node names.
- The InferenceService changed between the preview and the patch (the
  `resourceVersion` guard fired). Re-run to review the current state.

A refusal is not something to work around with retries in a loop: re-run the
command once, read the fresh preview, and if it still refuses, inspect the
service with `kubectl ome migration status` and `kubectl ome status`.
