---
title: "Inspecting Alfred's Advisory Recommendations"
linkTitle: "kubectl-ome recommendations"
weight: 21
description: >
  Read the configuration and latest advisory decision cycle Alfred persisted with kubectl ome admin recommendations
---

Alfred is OME's optional advisory caretaker, shipped as its own Helm chart
(`ome-alfred`). Each decision cycle it runs its placement policies and, when
its reporter is enabled, persists the cycle's recommendations to a ConfigMap.
`kubectl ome admin recommendations` inspects that persisted record: which
configuration is selected, what the latest cycle reported, and how fresh it
is. It is a bounded, read-only inspection — at most two named ConfigMap GETs,
never LISTs, never Secrets, and it never mutates anything.

Everything it shows is advisory evidence, never permission to execute.

## What the command reads

1. Alfred's configuration ConfigMap: `--alfred-config-name` (default
   `alfred-config`), key `--alfred-config-key` (default `config.yaml`),
   parsed with Alfred's own strict loader and defaults.
2. Only when that configuration is readable, valid, and has
   `recommendationsConfigMapEnabled: true` (the default): the
   recommendations ConfigMap the configuration itself names
   (`recommendationsConfigMapName`, default `alfred-recommendations`),
   reading only its `last-cycle.json` key.

Both GETs go to `--alfred-namespace`, which defaults to `--ome-namespace`
and then to `ome`. The kubectl workload namespace (`-n`) plays no part.
There is no flag that points directly at the record ConfigMap — its name is
always taken from the selected configuration. Each request is capped at 10
seconds.

On a fresh installation the record ConfigMap exists but is empty (the chart
pre-creates it because Alfred may update but never create ConfigMaps), so
the record reads `KeyAbsent` until Alfred persists its first cycle.

## Run it

```bash
# Inspect Alfred's record in namespace "ome"
kubectl ome admin recommendations

# Alfred installed in its own namespace; expand safe details
kubectl ome admin recommendations --alfred-namespace caretaker -o wide
```

The command accepts no positional arguments. Besides the standard kubectl
connection flags, it takes:

| Flag | Meaning |
| --- | --- |
| `--alfred-namespace` | Namespace holding both ConfigMaps (default: `--ome-namespace`, then `ome`) |
| `--alfred-config-name` | Name of Alfred's configuration ConfigMap (default `alfred-config`) |
| `--alfred-config-key` | Key inside the configuration ConfigMap (default `config.yaml`) |
| `--ome-namespace` | OME control-plane namespace, the fallback for `--alfred-namespace` (default `ome`) |
| `-o table\|wide\|json\|yaml` | Output format (default `table`) |

## Reading the report

A healthy run with one reported recommendation prints:

```
SUBJECT                  EVIDENCE            DETAIL
Alfred recommendations   Reported            Recent
Source namespace         ome                 Config and cycle sources
Config source            alfred-config       config.yaml
Record source            last-cycle.json     alfred-recommendations
ConfigMap config         Available           recommend-only
Latest cycle             Available           recommend-only
Rows                     1 shown/1 scanned   0 invalid; 0 omitted
prod/chat/engine#0       advisory            defragmentation/Fragmentation
Advisory evidence        Not authorization   Convergence unverified
Executability            Unverifiable        Not persisted in cycle
Scope                    Latest cycle only   Node/scheduling data omitted
```

The first row is the summary: the report state (`Reported`, `Empty`,
`Partial`, `Disabled`, or `Unavailable`) and the cycle's freshness.
`Disabled` means the configuration was read successfully but turns the
reporter off — the record ConfigMap is then never fetched. `Partial` means a
cycle was decoded but some rows were dropped or clipped; `Unavailable` means
no cycle could be decoded at all, and the config/record states below say
why.

### Config and record states

The `ConfigMap config` row reports what happened to the configuration read:

- `Available` — fetched, parsed by Alfred's strict loader, and valid.
- `NotFound`, `Forbidden`, `Unreadable` — the GET failed; `Unreadable`
  covers every failure the command does not classify further (the raw API
  error is never shown).
- `KeyAbsent` — the ConfigMap exists but has no `--alfred-config-key` entry.
- `Malformed` — the payload fails Alfred's strict schema: unknown fields,
  duplicate keys, invalid values, or a missing `schemaVersion`.
- `UnsupportedSchema` — `schemaVersion` is present but not `1`.
- `Oversized` — the payload exceeds 64 KiB.
- `IdentityMismatch` — the API returned an object other than the one
  requested.

The `Latest cycle` row reports the record read with the same GET states,
plus:

- `NotRead` — the configuration was unavailable, so no record was fetched.
- `Disabled` — `recommendationsConfigMapEnabled: false`.
- `KeyAbsent` — no `last-cycle.json` in the record ConfigMap.
- `Malformed` — the payload is not a single well-formed JSON object with a
  valid RFC 3339 timestamp and a recognized mode.
- `Oversized` — the payload exceeds 256 KiB.
- `ScanLimitExceeded` — the cycle carries more than 800 rows; nothing is
  shown rather than an arbitrary subset.

### Two modes, shown independently

The report carries two modes on purpose. The `ConfigMap config` row's detail
is the mode the selected ConfigMap declares; the `Latest cycle` row's detail
is the mode recorded inside the persisted cycle (`recommend-only` or
`execute`). When they differ the report raises a `ConfigRecordModeMismatch`
issue — typically the configuration was edited after the cycle ran. Neither
value proves what Alfred is executing right now: Alfred hot-reloads its
ConfigMap but keeps its last-known-good configuration when a reload fails
validation, and the report never claims to know which one is in force.

### Freshness

Freshness compares the cycle's own timestamp against the collection time,
with a window of twice the configuration's declared `decisionLoopInterval`
(default 5 minutes, so a 10-minute window):

- `Recent` — the cycle is no older than the window.
- `Stale` — older than the window; Alfred may be stopped, wedged, or simply
  not persisting.
- `Future` — the cycle claims a timestamp later than the collection time
  (clock skew or a hand-edited record).

Freshness is arithmetic on the persisted timestamp only. It does not verify
that Alfred's policy loop is running or healthy.

### Recommendation rows

Each remaining row is one recommendation from the cycle:
`namespace/name/component#instance`, its outcome, and `policy/reason` (for
example `defragmentation/Fragmentation` or `nodehealth/NodeUnhealthy`).
Outcomes are `advisory` (surfaced for a human, with a reason code such as
`RawDeploymentMigrationUnsupported`), `withheld`, `rejected`, or a reported
dispatch status (`submitted`, `acknowledged`, `completed`, `failed`,
`stalled`) — Alfred's own account of what it dispatched, nothing more. A
reserved `admitted` outcome is accepted but classified `Unverifiable`, with
no execution inferred.

The projection is deliberately conservative. At most 800 rows are validated
and at most 200 are emitted; the `Rows` line counts what was shown, scanned,
invalid, and omitted. Rows carrying any unrecognized value are dropped and
counted (`InvalidRows` issue), as are conflicting duplicate candidates
(`DuplicateCandidates`); either makes the state `Partial`. Free text, UUIDs,
node identities, and scheduling details are never emitted. `-o wide` expands
clipped identities and adds the per-row reason codes, the config and cycle
authority rows, the freshness window in seconds, and the cycle timestamp.

## What the report does not prove

The last three table rows state the non-claims, and they are the point:

- **Not authorization.** The record is advisory evidence. Nothing in it
  grants or implies permission to migrate anything.
- **No convergence.** A `completed` dispatch status is Alfred's reported
  history, not independent proof the workload moved or converged — verify
  with [`kubectl ome migration status` and
  `history`](/ome/docs/tasks/kubectl-ome-migration).
- **Executability is unverifiable.** The persisted cycle carries no
  executable flag, so every row reports executability `Unverifiable`.
- **Latest cycle only.** No complete history and no node remediation
  inventory is claimed; only `last-cycle.json` is read.
- **Not Alfred's running state.** The ConfigMap may differ from the
  last-known-good configuration Alfred is actually running, and a fresh
  record does not prove the policy loop is healthy.

## Scripting

`-o json` and `-o yaml` emit the typed report (`apiVersion:
cli.ome.io/v1alpha1`, `kind: AlfredRecommendationsReport`) with one source
reference per attempted GET and typed warnings (`SourceUnavailable`,
`PartialData`, `StaleEvidence`, `Truncated`). The human tables are not a
stable scripting interface before GA.

```bash
kubectl ome admin recommendations -o json \
  | jq '{state: .content.state, freshness: .content.freshness}'
```

The command follows the plugin's [shared exit
codes](/ome/docs/tasks/kubectl-ome#exit-codes) but asserts nothing, so it
never exits `2`: any produced report — including one whose every source is
unavailable — exits `0`. A missing or forbidden ConfigMap is a typed report
outcome, not an error; check `configState` and `recordState`. Exit `1` means
no report at all: invalid flags or names, an unusable kubeconfig,
cancellation, or a failed write.

## Required RBAC

Only `get` on `configmaps` in the Alfred namespace — the extra rule
described on the [kubectl-ome plugin
page](/ome/docs/tasks/kubectl-ome#required-rbac). Keep it out of cluster-wide
baseline roles; grant it through a namespace-scoped Role where the command
is used.
