---
title: "Request a Transient Scale"
linkTitle: "Request a Transient Scale"
weight: 25
date: 2026-09-26
description: >
  Send one guarded, transient replica request to an OMENative component with `kubectl ome scale`
---

`kubectl ome scale` sends **one** UID/resourceVersion-guarded JSON Patch to
the `/scale` subresource of the InferenceReplica that backs one component
(`engine`, `decoder` or `router`) of an OMENative InferenceService, replacing
its `spec.replicas`. The command is **alpha** and ships with the [kubectl-ome
plugin](/ome/docs/tasks/kubectl-ome).

What it changes is deliberately narrow: the request touches only the selected
InferenceReplica's desired logical-Instance count. It never edits the parent
InferenceService's `minReplicas`/`maxReplicas`, autoscaling policies, scalers,
or any other component.

> **Note:** A successful exit means *transient API acceptance* — the API
> server persisted the `/scale` write on the exact object version that was
> previewed. It is **not** durable parent replica intent and **not**
> controller convergence — see [What acceptance means — and what it does
> not](#what-acceptance-means--and-what-it-does-not).

## Before you begin

- The [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) installed. The
  command accepts the standard kubectl connection flags (`--kubeconfig`,
  `--context`, `-n`) plus `--ome-namespace` for the control-plane namespace
  (default `ome`) used in runtime revision lookups.
- RBAC: the plugin's baseline read rules, plus `patch` on
  `inferencereplicas/scale` (`ome.io` group) — the subresource must be named
  explicitly in the rule; see [Required
  RBAC](/ome/docs/tasks/kubectl-ome/#required-rbac). The read pass resolves
  the effective runtime (ServingRuntimes / ClusterServingRuntimes and, for
  pinned services, `ControllerRevision` snapshots in the OME namespace) and
  also `get`s the InferenceReplica's `/scale` subresource.
- An InferenceService with `spec.deploymentMode: OMENative` whose selected
  component is currently stable (no rollout, lifecycle or migration work in
  flight — see [Eligibility](#eligibility-what-live-evidence-must-hold)).

## Why `--override-autoscaler --yes` is always required

Every invocation — including both dry-run modes — must carry **both**
`--override-autoscaler` and `--yes`. There is no ownership class that is
exempt and no interactive confirmation path: `--override-autoscaler` without
`--yes` is rejected as invalid arguments, and omitting either flag refuses
with:

```text
manual scale is transient and requires --override-autoscaler --yes for every ownership class
```

The reason is that on OMENative there is no unowned replica count. Whatever
the verified ownership class, something reconciles `spec.replicas` and can
overwrite your request:

| Class | Who overwrites the request |
|-------|----------------------------|
| `HPA` | OME's HorizontalPodAutoscaler can overwrite it immediately. |
| `KEDA` | KEDA or its generated HPA can overwrite it immediately. |
| `External` | The operator-owned scaler is not observed by this command and can overwrite it immediately. |
| `None` | The InferenceService projector restores the ISVC/runtime-derived desired replicas on reconciliation. |

So `--override-autoscaler` does not disable or pause anything — it is your
acknowledgment that the request is a *transient override* that reconciliation
is entitled to undo. `--yes` confirms this exact request; because it is
mandatory, the command never shows a `[y/N]` prompt.

## Step 1: Preview with a dry run

```bash
kubectl ome scale chat \
  --component engine \
  --replicas 3 \
  --override-autoscaler --yes \
  --dry-run=client -n prod
```

`--replicas` must be a positive base-10 integer (`1..2147483647`). Zero is
refused before anything is read: OMENative does not preserve zero replicas.

Both dry-run modes retain every safety read and the post-preview
revalidation; they differ only in what is sent:

- `--dry-run=client` sends no PATCH at all (`accepted: false`, message
  `Validated locally; no scale patch sent.`).
- `--dry-run=server` sends the same guarded JSON Patch with `dryRun=All`, so
  the API server (including admission) validates it without persisting
  anything (`accepted: true`, `applied: false`, message `API dry-run
  accepted; no changes persisted.`).

The preview goes to **stderr**, headed `ALPHA guarded scale preview (not
controller convergence)`; stdout carries a single typed `ActionResult`. The
preview table shows the exact identities the request is bound to (parent
UID/generation, InferenceReplica UID/resourceVersion/generation, the
parent-generation stamp), the `spec.replicas` transition, the verified bounds
and ownership (`class / managedBy`), the reported Instance counts, any pinned
sibling proofs, and how many InferenceReplica proofs will be revalidated. It
is followed by warning lines restating the action's limits — transient
request, ownership overwrite, scale-down drain, and that the guard is not a
multi-object transaction.

## Step 2: Submit the request

```bash
kubectl ome scale chat \
  --component engine \
  --replicas 3 \
  --override-autoscaler --yes \
  -n prod
```

With `-o json` the result looks like:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-26T12:00:00Z",
  "action": "scale",
  "target": {
    "kind": "InferenceReplica",
    "namespace": "prod",
    "name": "chat-engine",
    "uid": "6c9c2f6e-...",
    "resourceVersion": "81"
  },
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted transient scale request; convergence not observed.",
  "followUp": "kubectl ome autoscale status chat -n prod --context=my-context",
  "scale": {
    "component": "engine",
    "subresource": "/scale",
    "field": "spec.replicas",
    "priorReplicas": 1,
    "requestedReplicas": 3,
    "minReplicas": 1,
    "maxReplicas": 10,
    "class": "None",
    "managedBy": "none",
    "specSource": "isvc",
    "override": true,
    "transient": true,
    "parent": {
      "kind": "InferenceService",
      "namespace": "prod",
      "name": "chat",
      "uid": "0f6b1c2d-...",
      "generation": 7
    },
    "sources": [
      {
        "kind": "ServingRuntime",
        "namespace": "prod",
        "name": "simple",
        "uid": "9a8b7c6d-...",
        "generation": 1
      }
    ],
    "replicaGeneration": 2,
    "parentGenerationStamp": 7,
    "parentFreshness": "Unverifiable",
    "instances": { "replicas": 1, "ready": 1, "serving": 1, "available": 1 },
    "issues": [],
    "warnings": [
      "AlphaAction",
      "AutoscalerCanOverwrite",
      "NotMultiObjectTransaction",
      "ScaleDownCanDrain",
      "TransientReplicaRequest"
    ]
  }
}
```

`-o table` (default) and `-o wide` render the same bounded fields as a table.
`parentFreshness` is always `Unverifiable`: the parent-generation stamp must
match at read time (see below), but nothing proves the parent's status
projection is still current at send time.

## Eligibility: what live evidence must hold

The command refuses — with no request sent — unless all of the following hold
on the live objects:

- The InferenceService exists, is `OMENative`, is not terminating, and
  `--component` is one of its components.
- **The effective runtime resolves cleanly** through pin-aware resolution:
  either the live runtime under `autoSync` with no reported drift, or a
  consistent pinned `ControllerRevision` snapshot. An unacknowledged `runtime
  sync` token refuses.
- **Autoscaling is locally verifiable**: the resolved bounds are available
  with `minReplicas >= 1` and `maxReplicas >= minReplicas`, and the parent's
  reported per-component autoscaler status (class, managedBy, spec source)
  matches what the plugin resolved locally. A policy-sourced autoscaler that
  cannot be verified locally refuses.
- **The requested count is in bounds**: `--replicas` is within the verified
  `minReplicas..maxReplicas` and compatible with any configured pacing or
  rolling-update partition on the InferenceReplica.
- **The selected InferenceReplica is exact and current**: named
  `<service>-<component>`, solely controller-owned by this exact
  InferenceService (owner-reference UID match), not terminating,
  `status.observedGeneration` equal to `metadata.generation`, and its
  `ome.io/parent-generation` annotation equal to the parent's current
  `metadata.generation`.
- **The `/scale` subresource mirrors the InferenceReplica exactly**: same
  name, namespace, UID and resourceVersion, and its spec/status replica
  counts equal the InferenceReplica's.
- **No selected work is active**: the parent carries no pending promote or
  rollback mailbox annotation; the component and every rollout group
  containing it project as stable; the InferenceReplica has no in-flight
  update (`currentRevision` equals `updateRevision`), no instance in a
  deleting or unexplained transient phase, and no pending
  rollback-to-revision.
- **Pinned runs add sibling proofs**: if the parent has a valid pinned active
  rollout run, the command performs up to two additional exact reads of the
  status-selected sibling InferenceReplicas named by the run and requires
  their revisions to be consistent with the pinned plan. A contradictory or
  drifting sibling refuses. Without an active run, no other InferenceReplica
  is read.

## How the request is guarded

After the preview, the command re-reads its own evidence before sending
anything:

- Every source the eligibility proof was minted from — the parent
  InferenceService, the runtime declaration chain (or the pinned
  `ControllerRevision`) and, when the runtime was matched from a model, the
  base model — is re-read and must match the exact UID, resourceVersion and
  generation first observed.
- Every InferenceReplica actually used (the selected one, plus pinned
  siblings) is re-read and must match exactly: UID, resourceVersion,
  generation, spec, status, owner references and annotations.

Any change refuses with exit code 1 and patches nothing. These re-reads
narrow the race window; they are **not** a multi-object transaction — only
the selected InferenceReplica receives a compare-and-swap.

The PATCH itself is a JSON Patch with `test` preconditions, so it can only
apply to the exact object version that was previewed:

```json
[
  { "op": "test", "path": "/metadata/uid", "value": "<ir-uid>" },
  { "op": "test", "path": "/metadata/resourceVersion", "value": "<ir-rv>" },
  { "op": "replace", "path": "/spec/replicas", "value": 3 }
]
```

If the API server rejects the guarded patch as a precondition failure (a
conflict, or the generic JSON Patch `test` rejection), the command exits 3
without retrying. Any other rejection — admission, authorization, throttling,
an unreadable response — exits 1 and reports the outcome as **unknown**:
inspect `kubectl ome autoscale status` before preparing another request. The
whole action is capped at 45 seconds and each API request at 10 seconds (a
shorter kubeconfig request timeout is preserved). There is no implicit wait
and no automatic replay.

## What acceptance means — and what it does not

`accepted: true` (and, outside dry runs, `applied: true`) binds exactly one
fact: the API server persisted the new `spec.replicas` on the exact
InferenceReplica version the command previewed. It does **not** prove any of
the following:

- **Not durable intent.** The parent InferenceService's spec is unchanged.
  On reconciliation the owning scaler (`HPA`, `KEDA`, `External`) — or, for
  class `None`, the InferenceService projector itself — restores its own
  desired count. The request survives only until something reconciles it.
- **Not convergence.** The controller creates or removes logical Instances on
  its own schedule; the command never observes the result. The `followUp`
  command is how you watch it.
- **Scale-down is teardown.** Reducing the count may drain and delete
  Instances, and a rollout pause does not freeze that teardown. OMENative
  also does not preserve zero replicas — which is why `--replicas 0` is
  refused outright.

Verify the outcome by observation, not by this command's exit code:

```bash
kubectl ome autoscale status chat -n prod
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | The request was accepted by the API server (or the dry run validated). Not proof of durable intent or convergence. |
| `1` | Refused before sending (invalid arguments, missing `--override-autoscaler --yes`, out-of-bounds replicas, incomplete evidence, active work, or evidence that changed during revalidation), or the patch outcome is unknown. When the message says the outcome is unknown, inspect `kubectl ome autoscale status` before preparing another request. |
| `3` | The API server rejected the guarded patch as a precondition failure (conflict or JSON Patch `test` failure). Nothing was applied; re-inspect current state and prepare a new request rather than retrying the old one. |
