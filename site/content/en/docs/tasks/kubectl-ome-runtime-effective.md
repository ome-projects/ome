---
title: "Inspect Effective Runtime Evidence"
linkTitle: "Runtime Effective Evidence"
weight: 21
date: 2026-09-26
description: >
  Use kubectl ome runtime effective to see which runtime, revision, and per-component deployment mode is driving an InferenceService, and whether it has drifted from the live runtime.
---

`kubectl ome runtime effective` answers one question about a single
InferenceService: **which runtime, revision, and per-component deployment mode
is actually driving it, and has it drifted from the live runtime?** It is the
read-only inspection companion to
[runtime revisions and pinning](/ome/docs/concepts/runtime-revision) — where
that page explains how pins work, this command shows you the current pin, sync,
status, and drift evidence for one service.

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome).
- The command is read-only and patches nothing. The baseline read-only
  ClusterRole on the plugin page covers everything it reads: the
  InferenceService, its (Cluster)ServingRuntime and, when the runtime is
  selected via the model, its (Cluster)BaseModel, plus `ControllerRevision`
  snapshots in the OME control-plane namespace.

## Run the command

```bash
kubectl ome runtime effective my-service -n team-a
```

For a service explicitly pinned to a snapshot of the ClusterServingRuntime
`cluster-runtime`, the output looks like this:

```
SCOPE     FIELD           VALUE
Live      STATE           Available
Live      RUNTIME         CSR/cluster-runtime
Live      HASH            e0e2b0d6
Live      ENGINE          RawDeployment (Default)
Active    STATE           Available
Active    RUNTIME         CSR/cluster-runtime
Active    REVISION        cr-cluster-runtime-e0e2b0d6
Active    HASH            e0e2b0d6
Active    ENGINE          RawDeployment (Default)
Service   PIN             ExplicitPin/Resolved
Service   SYNC            Acknowledged
Service   STATUS          Current
Service   DRIFT           ReportedTrue/RevisionMismatch
Service   LIVE-RELATION   Equal
```

Each row is one fact under one of three scopes:

- **Live** — the runtime object as it exists in the cluster at read time: the
  spec an `autoSync: true` service would render from. `HASH` is the 8-character
  content hash recomputed from the live spec.
- **Active** — the configuration the controller renders pods from,
  reconstructed from the same evidence the controller uses (the service's spec,
  `status.pinnedRevisionName`, and the fetched snapshot). For an auto-synced
  service (or a managed pin that has not pinned yet) this is the live runtime;
  for a pinned service it is the `ControllerRevision` snapshot, and a
  `REVISION` row names it.
- **Service** — pin, sync, status, drift, and issue facts about the
  InferenceService itself.

Component rows (`ENGINE`, `DECODER`, `ROUTER`) render as `MODE (SOURCE)`: the
effective deployment mode (`RawDeployment`, `MultiNode`, `VirtualDeployment`,
`OMENative`) and where it came from (`Default`, `ServiceSpec`,
`ServiceAnnotation`, `ComponentAnnotation`, `LeaderWorkerShape`). Comparing the
Live and Active component rows shows whether a runtime edit would change a
component's mode when you roll forward.

## Reading the Service rows

| Row | Meaning | Values |
| --- | --- | --- |
| `PIN` | Pin intent and its resolution, as `mode/state` | Modes: `AutoSync` (no pin), `ManagedPin` (`autoSync: false`), `ExplicitPin` (`spec.runtime.revision` set), `InvalidPin`. States include `NotApplicable`, `AwaitingPin`, `Resolved`, `DesiredReportedMismatch` (spec revision differs from `status.pinnedRevisionName`), `RevisionMissing`, `RevisionInvalid`, `RevisionDisabled`, `Unavailable`, `InvalidIntent` |
| `SYNC` | How the `ome.io/runtime-sync` annotation relates to `status.lastRuntimeSyncToken`, without printing either value | `Absent`, `Pending` (annotation not yet consumed), `Acknowledged` (controller consumed it), `StatusOnly` |
| `STATUS` | `status.observedGeneration` compared to `metadata.generation` in the fetched snapshot | `Current`, `Stale`, `Unobserved`, `Invalid` |
| `DRIFT` | The `RuntimeDrifted` condition as reported in status, as `state/cause` | `NotReported`, or `ReportedTrue`/`ReportedFalse`/`ReportedUnknown` with a cause such as `RevisionMismatch`, `RevisionMissing`, `SourceRuntimeMissing`, `PinAdvanced`; `Malformed` for contradictory conditions |
| `LIVE-RELATION` | The hash relation between the live runtime and the active configuration, recomputed by the CLI at read time | `Equal`, `Different`, `Ambiguous` (short hashes collide but full hashes differ), `Unknown` |
| `ISSUE` | One row per bounded evidence problem | For example `StatusStale`, `StatusUnobserved`, `InheritanceUnavailable`, `ActiveRevisionUnavailable`, or a revision-consistency code with the revision in parentheses, like `RevisionHashMismatch(cr-...)` |

## Has it drifted?

Two rows answer this, and they answer it differently:

- **`DRIFT`** is what the controller last **reported** — the `RuntimeDrifted`
  condition from the service's status. It can lag behind reality; check
  `STATUS` to see whether the status snapshot is even current.
- **`LIVE-RELATION`** is what the CLI **recomputed just now** by hashing the
  live runtime spec and the active configuration it fetched.

The example above shows them disagreeing: the status still carries
`ReportedTrue/RevisionMismatch`, but the freshly computed relation is `Equal`.
`Current` in `STATUS` only means `status.observedGeneration ==
metadata.generation` in the fetched snapshot — never wall-clock freshness or
rollout convergence. The command never inspects pods, so none of these rows
prove that pods have re-rendered or become ready.

When drift is real and you want the pinned service to adopt the live runtime,
use the guarded roll-forward described in
[Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision/#rolling-forward-to-the-latest-runtime).

## Output formats

`-o` accepts `table` (default), `wide`, `json`, and `yaml`.

The compact table abbreviates `ServingRuntime` as `SR` and
`ClusterServingRuntime` as `CSR` (a namespaced runtime renders as
`SR/<namespace>/<name>`), and bounds every value to 54 display columns. A name
too long for its cell is truncated and given a `#` plus 8 hex characters
derived from the full identity, so two long names that differ only in the
truncated middle still render distinctly.

`-o wide` prints the complete legacy table — one row per view and component,
full kind names, nothing truncated:

```
VIEW     STATE       REASON   RUNTIME                                 REVISION                      HASH       COMPONENT   MODE            MODE-SOURCE   PIN           PIN-STATE   SYNC           STATUS    DRIFT                           LIVE-RELATION   ISSUES
Live     Available   -        ClusterServingRuntime/cluster-runtime   -                             e0e2b0d6   engine      RawDeployment   Default       ExplicitPin   Resolved    Acknowledged   Current   ReportedTrue/RevisionMismatch   Equal           -
Active   Available   -        ClusterServingRuntime/cluster-runtime   cr-cluster-runtime-e0e2b0d6   e0e2b0d6   engine      RawDeployment   Default       ExplicitPin   Resolved    Acknowledged   Current   ReportedTrue/RevisionMismatch   Equal           -
```

`-o json` and `-o yaml` emit a typed `RuntimeEffectiveReport`
(`cli.ome.io/v1alpha1`) with everything the tables summarize plus fields the
tables omit: whether the runtime was named explicitly or selected from the
model (`content.selection.source`: `Explicit` or `Selected`), the runtime
inheritance chain, the requested and reported revision names, and the exact
objects the evidence was collected from (`sources`). Human-readable tables are
not a stable scripting interface — script against the structured formats:

```bash
# Which revision is driving the pods, and does it match the live runtime?
kubectl ome runtime effective my-service -n team-a -o json |
  jq -r '{revision: .content.active.revision.name, liveToActive: .content.liveToActive}'
```

## Scoping flags

- `-n` selects the InferenceService's namespace, as for any kubectl command.
- `--ome-namespace` (default `ome`) is the namespace where the
  `ControllerRevision` snapshots live. It must match the control plane's
  installation namespace: pointed at the wrong namespace, the command still
  succeeds but reports a pinned revision as missing or unavailable, which is
  easy to mistake for a broken pin.

## What is never printed

The report is allowlisted. Raw runtime specs, `ControllerRevision` payloads,
status messages, resource versions, and the values of synchronization tokens
are never printed in any output format — the structured formats add full names,
UIDs, and generations, but no spec content.
