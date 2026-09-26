---
title: "Observing Migrations"
linkTitle: "kubectl-ome migration"
weight: 21
description: >
  Inspect live migration records and bounded historical evidence for an
  InferenceService with kubectl ome migration status and history
---

In the `OMENative` deployment mode the controller runs each engine, decoder,
and router instance of an InferenceService through an InferenceReplica
resource. A migration moves one instance off its node by allocating a surge
replacement, waiting for it to become ready, and draining the original.

The [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) offers two read-only
views of this process:

- `kubectl ome migration status` — the live, authoritative migration records
  reported by the service's InferenceReplicas: what is happening right now.
- `kubectl ome migration history` — three separately labeled evidence windows
  covering current and past migrations: what is or was happening.

Both commands only read. Requesting a migration (`kubectl ome migration
start`) is a mutating action with its own
[RBAC](/ome/docs/tasks/kubectl-ome#required-rbac).

## What the commands read

Both commands fetch exactly the InferenceService named on the command line in
the namespace given with `-n`, then list its related InferenceReplicas in the
same namespace. Replicas are matched through the
`ome.io/inferenceservice=<service>` relationship label when the service name
fits in a label value (63 characters); longer names fall back to a bounded
namespace scan that keeps only replicas whose `spec.parentRef` and controller
owner reference match the exact service.

Every read is bounded: at most two pages of 32 InferenceReplicas (64 total),
with a 10-second timeout per request. When a bound cuts the listing short,
the report says so — a `SourcesTruncated` issue in `status`, a truncated
source window in `history` — instead of silently claiming completeness.

## Live records: `migration status`

```bash
kubectl ome migration status chat -n prod
```

```
SUBJECT/COMP      STATUS                    DETAIL
12345678/engine   Accepted/Active/Current   MSG: waiting for target...
```

Each row is one entry of `status.migrations` on one InferenceReplica — the
single source of truth for migration work:

- **SUBJECT/COMP** — the first 8 characters of the migration request UUID,
  and the component (`engine`, `decoder`, or `router`).
- **STATUS** — `phase/classification/freshness`. A manual migration moves
  through `Accepted` (admitted, no surge instance yet), `SurgePending`
  (surge instance allocated, waiting to become ready), `SurgeReady`, and
  `Draining`; `Completed`, `Failed`, and `Relocated` are terminal.
  Auto-recovery records are born terminal as `Relocated`. Classification is
  `Active`, `Terminal`, or `Invalid`; freshness is `Current` when the
  replica's `status.observedGeneration` matches its generation, `Stale` when
  it is behind, and `Unobserved` or `Invalid` otherwise.
- **DETAIL** — the sanitized controller message (prefixed `MSG:`, the
  current blocker or terminal outcome), one row per issue code if the record
  has issues, or `-`. When there are no migrations at all, a single
  placeholder row carries the summary state (for example `-/-` /
  `Empty/Current`).

`--component engine|decoder|router` filters to one component. `-o json` and
`-o yaml` emit the full typed report (`apiVersion: cli.ome.io/v1alpha1`,
`kind: MigrationStatusReport`) with the fields the table clips: source and
surge instance indexes, trigger, from-node, target-node hints, `startedAt`,
`allocatedAt`, `deadline`, `completedAt`, per-record issue codes, and a
summary whose state is `Empty`, `Reported`, or `Partial`. The table keeps its
natural width within 80 columns; machine output caps sanitized messages at
256 display columns.

Two things `migration status` will never show, by design:

- It reads only the InferenceService and its InferenceReplicas — no
  migration audit ConfigMaps, pods, or Events. Both reads must succeed or
  the command fails.
- The API does not record a request timestamp or capacity/rate limits, so
  machine output marks that evidence `Unavailable` instead of inferring it.

Output is bounded to 200 records (from at most 800 scanned) and 8
target-node hints per record; truncation is reported as an issue.

## Current and past evidence: `migration history`

`migration history` joins three separately labeled windows into one report:

| Window | Source object | What it is |
| --- | --- | --- |
| `AUTH` (Authoritative) | `status.migrations` on each InferenceReplica | Live work state — the same records `migration status` shows |
| `PARENT` (ParentSummary) | the rolling `status.migrationHistory` window on the InferenceService | Controller-recorded summaries of past migrations |
| `AUDIT` (AuditHistory) | the optional `<service>-ome-migration-audit` ConfigMap (key `history.json`) | The controller's migration ledger, owned by the service |

All three live in the InferenceService's workload namespace; the command
never reads the OME control-plane namespace.

```bash
kubectl ome migration history chat -n prod
```

```
EVID     REQUEST/COMP   PHASE/STATE   WHEN           DETAIL
AUTH     1c2d3e4f/E     Draining/A    09-26T08:12Z   -
PARENT   9a8b7c6d/E     Completed/T   09-25T17:40Z   ReplacementReady
AUDIT    5e4f3a2b/R     Failed/T      09-19T18:41Z   NodeUnavailable
```

- **REQUEST/COMP** — the first 8 characters of the request ID and the
  component compacted to `E`, `D`, or `R`.
- **PHASE/STATE** — the phase plus `A` (active), `T` (terminal), `!`
  (invalid), or `?` (unknown). Parent entries may carry the legacy `Pending`
  and `InProgress` phases; audit entries use the ledger's `Started`,
  `Completed`, and `Failed`.
- **WHEN** — the newest known timestamp of the record (completed, allocated,
  started, or requested), in UTC.
- **DETAIL** — the first issue code on the record, or a clipped operational
  summary (the parent's `outcomeReason` or the audit outcome).

Use `-o wide` for every allowlisted field: per-source availability, the
observed window (`pages/items/bounded/truncated-or-complete`), full RFC 3339
timestamps, trigger, mode, freshness, from-node and target-node hints, event
counts, outcomes, and issue codes. `-o json` and `-o yaml` emit the same data
as a typed `MigrationHistoryReport`. The `--component` filter works as in
`status`.

Only the InferenceService read must succeed. The other windows degrade to
typed partial evidence instead of failing the command:

- InferenceReplica list forbidden or unreadable → `AuthoritativeUnavailable`
  issue, report state `Partial`.
- Audit ConfigMap absent → silently omitted; the ledger only exists once the
  controller has processed a migration request for the service.
- Audit ConfigMap forbidden → `AuditUnavailable`. Right name but not
  controller-owned by this exact service → `AuditIdentityInvalid`.
  Unparseable payload or one over 1 MiB → `AuditMalformed` /
  `AuditPayloadTooLarge`.

The report deliberately redacts raw ConfigMap payloads, annotations, caller
identity, request reasons, event messages (only a bounded event count
survives, at most 16 per record), UIDs, and resource versions.

Each window scans at most 800 entries, and at most 200 records are emitted
in total (`OutputTruncated` when clipped).

## Reading the two views together

Evidence, not phase, decides authority. Only `AUTH` records are current work
state; `PARENT` and `AUDIT` are bounded historical evidence and can never
replace or reactivate authoritative work — a terminal audit entry for a
request that is still active on the replica does not stop it, and a
historical entry never restarts anything. When windows disagree about one
request ID, the report says so with correlation issue codes:
`DuplicateWithinSource`, `CrossSourceIdentityConflict`,
`ChronologyConflict`, and `TerminalOutcomeConflict`.

As with the rest of the plugin, human-readable output is not a stable
scripting interface before GA — script against `-o json`.

## Required RBAC

The baseline reader role on the [plugin page](/ome/docs/tasks/kubectl-ome#required-rbac)
(`get`/`list` on `ome.io` resources) covers both commands' InferenceService
and InferenceReplica reads. `migration history` additionally reads the one
audit ConfigMap in the workload namespace, so it needs `get` on `configmaps`
there; without it the command still succeeds and reports the audit window as
`AuditUnavailable`. Keep that ConfigMap rule out of cluster-wide baseline
roles for the reasons described on the plugin page.
