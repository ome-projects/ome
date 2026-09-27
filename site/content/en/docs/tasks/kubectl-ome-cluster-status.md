---
title: "Check WorkloadCluster Status with kubectl-ome"
linkTitle: "Cluster Status"
weight: 21
date: 2026-09-27
description: >
  List declared WorkloadClusters and the readiness their controller reported with kubectl ome cluster status — a bounded, current-context read that never probes remote clusters and never reads capacity.
---

`kubectl ome cluster status` shows the WorkloadCluster objects declared in
your **current Kubernetes context** and the `Ready` condition their
controller has reported, with an explicit freshness classification for every
claim. The command is **alpha**, and so is the API it
observes: the WorkloadCluster resource is merged, but the multi-cluster
reconciliation that consumes it is still under active development. Treat the
output as what it verifiably is — a read of local declarations and
controller-reported conditions, not a connectivity check.

The command is strictly observational. It issues exactly one cluster-scoped
GET (when you name a WorkloadCluster) or one bounded cluster-scoped LIST
(when you don't), and nothing else:

- **No reachability probe.** It never dials the remote cluster, so `Ready:
  True` in the output is a claim by whatever controller last wrote the
  object's status — never proof the cluster is reachable now.
- **No credential or profile read.** The declared kubeconfig Secret is never
  opened and a declared ClusterProfile is never resolved (`profileResolution`
  is always `NotAttempted`). A valid-looking declaration does not imply the
  referenced Secret exists or contains working credentials.
- **No capacity read.** No capacity or quota API is consulted, and the
  WorkloadCluster API itself defines its conditions as connection health
  only — never capacity. `Ready: True` says nothing about free GPUs or
  schedulable room.

See [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome) for installing the
plugin and the baseline read-only RBAC role.

## Basic usage

List every declared WorkloadCluster in the current context:

```bash
kubectl ome cluster status
```

```
CLUSTER          DECLARED           READY     FRESHNESS   CONDITIONS
@ observation    Complete           -         -           -
@ availability   -                  -         -           -
@ sources        returned=2         kept=2    max=64      cut=false
@ pages          seen=1/2           calls=1   cap=4       size=32
gpu-east         KubeConfigSecret   True      Current     Reported
gpu-west         ClusterProfile     False     Stale       Reported
```

Or read exactly one object (a single GET, no LIST):

```bash
kubectl ome cluster status gpu-east
```

The name must be a valid DNS-1123 subdomain; anything else is rejected before
any API call. `-n` has no effect — WorkloadClusters are cluster-scoped — but
the standard connection flags (`--context`, `--kubeconfig`) select which API
server is read, as for every kubectl-ome command.

How to read the table:

- Rows starting with `@` describe the **read itself**, not any cluster (the
  prefix cannot collide with a resource name). `@ observation` summarizes it
  (`Complete`, `Empty`, `Partial`, or `Unavailable`), `@ availability` names
  the failure cause if any, `@ sources` counts objects
  (`returned`/`kept`/`max`/`cut`), and `@ pages` counts pages and API calls
  against their limits.
- **DECLARED** is the connection *declaration* kind: `KubeConfigSecret`,
  `ClusterProfile`, or `Invalid` (neither, both, or malformed). It is never
  a statement that the declaration works.
- **READY** is the controller-reported `Ready` condition value: `True`,
  `False`, or `Unknown` — used both for a literal `Unknown` report and
  whenever no single trustworthy `Ready` condition exists.
- **FRESHNESS** classifies that report against the object's current spec
  (see below).
- **CONDITIONS** summarizes the condition group: `Reported`, `Missing`,
  `Malformed`, or `Truncated`. A trailing `*` marks bounded detail.

## Why Ready is not reachability — or capacity

Everything under READY and FRESHNESS is copied from the WorkloadCluster's own
`status.conditions`. The command adds validation and classification, but no
observation of its own, so interpret the values conservatively:

- **`Ready: True` is a report, not a probe.** It means a controller wrote
  `Ready=True` at some point. In an installation where multi-cluster
  reconciliation is not running, nothing writes status at all — objects show
  `CONDITIONS: Missing` and `FRESHNESS: Unobserved`, which is the expected
  shape, not an error.
- **`Current` freshness is about the spec, not the clock.** `Current` means
  the condition's `observedGeneration` equals the object's `generation` —
  the report covers the latest spec. A controller that stopped running
  yesterday still shows `Current` until someone edits the spec. No freshness
  value proves the report is recent.
- **Capacity is out of scope by API contract.** WorkloadCluster conditions
  report connection health only — never capacity or quota — and the command
  reads no other API. Do not use this output to decide whether a cluster has
  room for a workload.

## Freshness values

| Value | Meaning |
| --- | --- |
| `Current` | The `Ready` condition's `observedGeneration` equals the object's `generation` |
| `Stale` | The condition observed an older generation — the controller has not yet seen the latest spec |
| `Unobserved` | `observedGeneration` is `0` (never set), or no valid `Ready` condition exists |
| `Invalid` | The evidence cannot be trusted: negative or future `observedGeneration`, object generation `0`, or a malformed condition group |

## Bounded, validated condition evidence

The report is deliberately **message-free**: reasons come from a closed
allowlist (`Connected`, `Disconnected`, `ConnectionFailed`, `Missing`, and
`Other` for anything else) and condition messages never appear in any output
format. For human-readable messages, use
`kubectl describe workloadcluster <name>`.

Condition groups are validated as a whole before anything is trusted:

- A group with **more than 128 conditions** is not scanned at all —
  `CONDITIONS: Truncated`, `READY: Unknown`.
- A group containing **duplicate types, contradictory `Ready` entries, or
  invalid statuses or types** is `Malformed` — `READY: Unknown`,
  `FRESHNESS: Invalid`. A malformed entry anywhere in the group poisons it;
  the command never silently trusts a valid-looking prefix.
- After validation, at most **32** canonical condition details are retained
  in `wide`/`json`/`yaml` output (`Ready` sorts first, so it is never the
  one dropped); `conditionsTruncated: true` records the cut.

Objects that fail identity checks — a duplicate name in the listing, a
namespace on a cluster-scoped object, or an invalid name — stay in the
counts but are anonymized and reported as `Malformed`.

## Output formats

- `-o table` (default) — the compact view above; every line fits 80 columns.
- `-o wide` — not a wider table: narrow `FIELD`/`EVIDENCE` key/value rows
  carrying the full safe detail per cluster (generation, declared profile,
  connection state, every retained condition with RFC 3339 timestamps). Long
  valid names wrap instead of being clipped.
- `-o json` / `-o yaml` — the full typed report.

As with all kubectl-ome commands, the human tables are not a stable scripting
interface before GA — script against `-o json`.

A named read (`kubectl ome cluster status gpu -o json`) produces:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ClusterStatusReport",
  "collectedAt": "2026-01-01T00:00:00Z",
  "observation": "Complete",
  "requestedPages": 1,
  "observedPages": 1,
  "returnedSources": 1,
  "admittedSources": 1,
  "sourceLimit": 1,
  "pageLimit": 1,
  "requestLimit": 1,
  "pageSize": 1,
  "conditionScanLimit": 128,
  "conditionOutputLimit": 32,
  "sourcesTruncated": false,
  "clusters": [
    {
      "name": "gpu",
      "generation": 2,
      "declaredConnectionKind": "ClusterProfile",
      "sourceState": "Reported",
      "declaredProfile": "profile",
      "profileResolution": "NotAttempted",
      "evidence": "Reported",
      "reportedReady": "False",
      "conditionState": "Reported",
      "freshness": "Stale",
      "connectionState": "Unknown",
      "totalConditions": 1,
      "scannedConditions": 1,
      "retainedConditions": 1,
      "conditionsTruncated": false,
      "conditions": [
        {
          "type": "Ready",
          "status": "False",
          "observedGeneration": 1,
          "freshness": "Stale",
          "reason": "Other",
          "transitionTime": "2020-01-02T03:04:05Z",
          "transitionState": "Reported"
        }
      ]
    }
  ]
}
```

Note the shape of the evidence here: the controller reported `Ready: False`,
but for generation 1 of a generation-2 object, so `freshness` is `Stale` and
the roll-up `connectionState` refuses to conclude anything (`Unknown`). The
raw reason on the object was outside the allowlist, so it renders as `Other`.

`connectionState` (`wide`, `json`, `yaml` only) is the strictest roll-up:
`ReportedReady` or `ReportedNotReady` only when the declaration is valid
**and** the single `Ready` condition is `Current` **and** its transition time
is present and plausible (not missing, not in the future); anything less is
`Unknown`. The names keep the word "Reported" on purpose — even the best
value is controller evidence, never a successful probe. Secret coordinates,
messages, UIDs, and resource versions have no fields in this report at all.

## Read bounds and failure reporting

A no-argument invocation reads at most **2 pages of 32** and keeps at most
**64 objects** per consistent list attempt, with a hard cap of **4 API
requests** (the extra allowance covers one restart after an expired continue
token) and a **10-second** timeout per request. The `@ sources` and `@ pages`
rows — and the corresponding `sourceLimit`, `pageLimit`, `requestLimit`,
`pageSize`, and `sourcesTruncated` fields — always disclose how much of the
window was used, so a bounded read can never pass silently as a complete one.

`@ observation` distinguishes four outcomes:

- `Complete` — the read finished inside the window.
- `Empty` — the read finished and there are no WorkloadClusters.
- `Partial` — evidence was kept but is incomplete: the list was truncated by
  the window, or a later page failed after earlier pages succeeded (the
  failure cause still appears under `@ availability`).
- `Unavailable` — the read failed with nothing kept; `@ availability` (or
  `unavailableReason`) says why: `NotFound`, `Forbidden` (covers both RBAC
  denial and authentication failure), `MalformedPayload`, or `Unreadable`
  (timeouts and everything else).

A failed *read* does not fail the *command*: it still writes its report and
exits `0` — branch on `observation`, not the exit code. In particular,
missing RBAC surfaces as `Unavailable`/`Forbidden` in the report, which is
easy to mistake for a broken cluster — check permissions before debugging.
Non-zero exits follow the shared
[exit-code mapping](/ome/docs/tasks/kubectl-ome/#exit-codes) and mean the
command could not complete its observation at all: invalid invocation, no
usable kubeconfig/client, cancellation, or an output write failure.

## Required RBAC

Cluster-scoped `get` (named read) and `list` (no-argument read) on
`workloadclusters` in the `ome.io` group:

```yaml
  - apiGroups: ["ome.io"]
    resources: ["workloadclusters"]
    verbs: ["get", "list"]
```

The baseline reader role on the
[kubectl-ome page](/ome/docs/tasks/kubectl-ome/#required-rbac) already covers
this through its `ome.io` wildcard. No Secret, ClusterProfile, or remote
cluster permission is ever needed, because none of them is read.

## Next steps

- [Drain Traffic from a Workload Cluster](/ome/docs/tasks/drain-traffic-from-a-workload-cluster)
  — the guarded traffic action that targets a WorkloadCluster by name;
  `cluster status` is the observational complement for checking what is
  declared before you drain.
