---
title: "List Logical Instances with kubectl ome instance list"
linkTitle: "kubectl-ome instance list"
weight: 21
date: 2026-09-27
description: >
  Read the controller-reported logical instance inventory behind an OMENative InferenceService, and what the PODS and AOF columns mean
---

`kubectl ome instance list INFERENCESERVICE` prints one read-only
**InstanceListReport**: every logical instance the controller has recorded for
an OMENative InferenceService, grouped by component. The rows come from the
`status.instanceStatuses` that the controller persisted on the service's
InferenceReplicas — the command **does not read Pods and does not infer live
pod state**. Everything it shows is what the controller last wrote, qualified
by how fresh that evidence is.

It ships with the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) and needs
only the plugin's baseline read RBAC (`get` on `inferenceservices`, `list` on
`inferencereplicas` — see [Required
RBAC](/ome/docs/tasks/kubectl-ome/#required-rbac)). Use it when
[`kubectl ome status`](/ome/docs/tasks/kubectl-ome-status) shows a
component-level problem and you need to see *which* logical instance is behind
it. For one instance's full detail — condition records, operation contents,
last-failure diagnostics, bounded live pod evidence — switch to the
single-instance deep dive, `kubectl ome instance status INFERENCESERVICE INDEX
--component COMPONENT`.

## Output formats

```bash
kubectl ome instance list chat -n prod            # aligned table (default)
kubectl ome instance list chat -n prod -o json    # full typed report
kubectl ome instance list chat -n prod -o yaml
```

`table`, `json` and `yaml` are the only formats; anything else (including
`-o wide`) is rejected before a single API request is made. The command
accepts the standard kubectl connection flags (`--kubeconfig`, `--context`,
`-n`). As everywhere in the plugin, the table is not a stable scripting
interface — script against `-o json`.

## Reading the table

A service named `chat` mid-rollout of its engine revision:

```
COMP     IDX/INC   PHASE      PODS    REVS          AOF   EVIDENCE
engine   0/2       Ready      1/1/1   chat...7f6a   A--   OK
engine   1/1       Updating   0/0/2   chat...7f6a   AO-   OK
router   0/1       Ready      1/1/1   chat...7a8b   A--   OK
```

One row per logical instance, ordered engine → decoder → router, then by
index. A component whose rows had to be withheld (malformed or unavailable
evidence, see [EVIDENCE](#the-evidence-column)) still gets a single row of `-`
placeholders whose EVIDENCE cell says why. A service with no InferenceReplicas
at all — any deployment mode other than `OMENative` — prints one placeholder
row with `OK`: nothing reported and nothing wrong.

### COMP, IDX/INC, PHASE

`COMP` is the component (`engine`, `decoder`, `router`). `IDX/INC` is the
instance's stable ordinal and its incarnation: the index survives updates and
may become sparse after surge migration; the incarnation increments each time
the instance is *recreated* (full recreate update, restart-on-loss) — in-place
updates do not increment it. `PHASE` is the controller-recorded lifecycle
phase: `Pending`, `Creating`, `Ready`, `Updating`, `Restarting`, `Migrating`,
`Failed` or `Deleting`. Any other value is malformed evidence.

### PODS is serving/available/total

`PODS` shows the three pod counters the controller persisted for the instance,
in the order `serving/available/total`:

- **serving** (`servingPodCount`) — pods that are both `ContainersReady` *and*
  have `serving=True` on the controller-managed readiness gate, i.e. pods
  actually in the load-balancer rotation. The controller flips that gate
  during in-place updates and drain steps, so this is the count that reflects
  "missing from traffic".
- **available** (`availablePodCount`) — pods in rotation on the component's
  headless Service and, when `lifecycle.minReadySeconds` is set, Ready for at
  least that long.
- **total** (`podCount`) — all pods owned by the instance.

A well-formed row satisfies `available ≤ serving ≤ total`; a violation is
rendered as malformed evidence (`BAD:COUNTS`), not as numbers you might trust.

There is deliberately no "ready" pod count. The API's `readyPodCount` field is
retained for compatibility only — OMENative derives readiness from Pods and
does not persist the field — so this command intentionally ignores it, as its
`--help` text states. If you need live pod readiness, use
[`kubectl ome status`](/ome/docs/tasks/kubectl-ome-status) (observed labelled
Pods) or `kubectl ome instance status` (one instance with bounded live pod
evidence).

### REVS

The revision the instance's pods are running, as a workload revision name
(`<service>-<component>-<hash>`). While the instance converges toward a
different revision the cell reads `running>target`; `>target` means no running
revision was recorded yet. The table clips this cell in the middle to 11
columns (`chat...7f6a`), so distinct revisions can look identical — the full
`runningRevision` and `targetRevision` are always in `-o json` and `-o yaml`.

### AOF is admitted/operation/last-failure presence

Three fixed positions; a letter means the record is present, `-` that it is
absent:

| Flag | Source field | Meaning when set |
|------|--------------|------------------|
| `A` | `admitted` | The instance's pods have left the Kueue admission scheduling gate (quota granted): the instance has pods and none are admission-gated. `-` while gated or queued, or before pods exist. |
| `O` | `operation` | A durable record of an in-flight destructive action against this instance exists — set before the action starts, cleared after it completes. |
| `F` | `lastFailure` | The instance carries the preserved diagnostics of the pod whose failure (or stuck-pod escalation) last triggered a recreate. The record survives the failed pod's deletion, and a clean revision-roll recreate does not overwrite it. |

These are presence flags only: the inventory strips the operation and
last-failure payloads at its read boundary and exposes just
`operationPresent` / `lastFailurePresent` booleans in JSON. To see the
contents, use `kubectl ome instance status`.

### The EVIDENCE column

Every row states how trustworthy it is:

| Cell | Meaning |
|------|---------|
| `OK` | Reported by the controller, current, dense index set. |
| `EMPTY` | The component reports zero instances (scaled to zero). |
| `SPARSE` | Indices are not `0..n-1` — normal after surge migration. |
| `STALE:<observed>/<generation>` | The InferenceReplica's `status.observedGeneration` is behind its `metadata.generation`; rows still shown, marked `Stale`. |
| `STALE:PARENT` | The InferenceReplica's `ome.io/parent-generation` stamp is behind the InferenceService's current generation — the controller has not re-projected your latest spec edit; rows still shown. |
| `PARENT-UNAVL` | The parent-generation stamp is missing; rows withheld. |
| `NOT-OBSERVED` | The controller has not observed the InferenceReplica at all (`observedGeneration` is zero); rows withheld. |
| `NOT-REPORTED` | The aggregate says replicas exist but no per-instance rows were reported; rows withheld. |
| `BAD:...` | Malformed evidence: `BAD:DUP-COMP` (two InferenceReplicas claim one component), `BAD:DUP-IDX`, `BAD:GEN`, `BAD:PARENT`, `BAD:REVISION`, `BAD:COUNTS`, `BAD:PHASE`, `BAD:INSTANCE`; rows withheld. |
| `ROWS-LIMIT` | The instance-row budget was exhausted before this component; rows withheld. |

"Rows withheld" means the component renders as one `-` placeholder row — the
report refuses to print per-instance values it cannot vouch for, though the
component's aggregate replica counters remain visible in `-o json`. When
something applies to the whole listing, a trailing `summary` row carries it:
`TRUNCATED` (a collection or output bound was hit), `SOURCE-UNAVL` (the
InferenceReplica listing failed), or `REJECTED` (an object matched the label
but failed an identity proof).

## JSON and YAML

`-o json` and `-o yaml` carry the same bounded, sanitized values as the table
in a typed document: `apiVersion: cli.ome.io/v1alpha1`, `kind:
InstanceListReport`, plus `sources` (each object read, with UID and
generation), `content.summary` (`state` is `Reported`, `Partial` or
`Unavailable`, plus `components`, `instances` and `truncated`),
`content.components` (per-component aggregates: `replicas`, `readyReplicas`,
`servingReplicas`, `availableReplicas`, `updatedReplicas`,
`updatedReadyReplicas`, revisions, `indexSet`), `content.instances`,
`content.issues` (typed codes such as `CollectionUnavailable`,
`IdentityRejected`, `StatusRowsTruncated`, `ParentGenerationStale`) and
`warnings` (`PartialData`, `SourceUnavailable`, `StaleEvidence`,
`Truncated`). Each entry of `.content.instances[]` looks like:

```json
{
  "component": "engine",
  "inferenceReplica": "chat-engine",
  "index": 1,
  "incarnation": 1,
  "phase": "Updating",
  "runningRevision": "chat-engine-1f2a3b4c",
  "targetRevision": "chat-engine-9d8e7f6a",
  "pods": {
    "total": 2,
    "serving": 0,
    "available": 0
  },
  "admitted": true,
  "operationPresent": true,
  "lastFailurePresent": false,
  "evidence": "Reported"
}
```

## Which objects the rows come from

The command makes one exact `get` of the InferenceService (the returned object
must match the requested name and namespace and carry a usable UID), then a
bounded `list` of InferenceReplicas in the same namespace selected by
`ome.io/inferenceservice=<name>`. A matching label alone is never treated as
ownership: each listed object must also prove, independently, that it belongs
to this exact service —

- same namespace with a valid name and UID (`Metadata`),
- the `ome.io/inferenceservice` relationship label (`Label`),
- `spec.parentRef.name` naming the service (`ParentReference`),
- exactly one controller owner reference of kind `InferenceService` with the
  service's exact name **and UID** (`OwnerReference`),
- a known component (`engine`, `decoder`, `router`) matching its `component`
  label (`Component`).

An object that fails a check contributes no rows; it appears as an
`IdentityRejected` issue naming the failed proof, and the table trailer shows
`REJECTED`. A service whose name is too long to be a legal label value is
matched by the same proofs over the same bounded scan, just without the label
selector.

## Degraded and partial reads

If the InferenceService itself cannot be read, the command fails (exit 1).
Everything after that degrades instead of failing: when the InferenceReplica
listing errors, the command still exits 0 and writes a report whose summary is
`Partial` (pages fetched before the failure are kept) or `Unavailable`
(nothing was listed), with a `CollectionUnavailable` issue whose
`unavailableReason` is `Forbidden`, `UnsupportedAPI` (the CRD is not
installed) or `Unreadable`. Missing `list` permission on `inferencereplicas`
therefore looks like `SOURCE-UNAVL` in the trailer, not like a broken service.

## Observation bounds

| Read | Bound |
| --- | --- |
| InferenceService | one exact `get` |
| InferenceReplicas | pages of 50, at most 100 objects over at most 10 pages, 10 s per request |
| Instance rows | at most 1000 across all components |

Exceeding a bound never fails the command and never silently drops data: the
summary flips to `Partial` with `truncated: true`, a typed issue records what
was cut (`CollectionTruncated`, `StatusRowsTruncated`, `OutputTruncated`), and
the affected component shows `ROWS-LIMIT` or the trailer shows `TRUNCATED`.

## Related pages

- [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome) — install, exit codes,
  RBAC.
- [Read the kubectl ome status Report](/ome/docs/tasks/kubectl-ome-status) —
  service-level readiness with *observed* pod evidence.
- [Release a Held Revision](/ome/docs/tasks/release-a-held-revision) — the
  `instance retry-blocks` discovery command and the guarded
  `instance release-held` action.
