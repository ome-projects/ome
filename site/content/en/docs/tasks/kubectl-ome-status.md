---
title: "Read the kubectl ome status Report"
linkTitle: "kubectl-ome status"
weight: 21
date: 2026-09-26
description: >
  What each section, bound and abbreviation in kubectl ome status means, and when to switch to a dedicated detail command.
---

`kubectl ome status INFERENCESERVICE` prints one read-only **StatusReport** for
an InferenceService: readiness, per-component pod health, recent warning
events, and one summary line each for rollout, autoscaling, placement, traffic,
runtime and accelerator state. This page explains how to read that report. For
installing the plugin and the RBAC it needs, see
[kubectl-ome Plugin](/docs/tasks/kubectl-ome/).

## Output formats

```bash
kubectl ome status my-isvc              # compact FIELD/VALUE table (default)
kubectl ome status my-isvc -o wide      # adds detail rows to the same table
kubectl ome status my-isvc -o json      # full report document
kubectl ome status my-isvc -o yaml
```

Any other `-o` value is rejected before a single API request is made. JSON and
YAML carry the same bounded, sanitized values as the table — long values are
truncated and anything containing `://` (such as URLs) is rendered as
`[OMITTED]`. The whole command runs under a 30-second budget. The
`--ome-namespace` flag (default `ome`) tells the command where the OME control
plane lives, which it needs to resolve runtime pin records.

## What the report claims — and what it doesn't

Every value in the report is *evidence*, qualified by how it was obtained:

- **Ready is reported condition evidence, not a convergence assertion.** A
  `True` means the controller recorded a Ready condition of `True`; it does not
  prove the service currently serves traffic.
- **Pod counts are observed labelled Pods**, not desired replicas or logical
  instances. Only pods in the service's namespace carrying both the
  `ome.io/inferenceservice` and `component` labels with an unambiguous
  name/UID are counted; anything else is rejected with a typed issue code.
- **Generation freshness is advisory.** The report cannot verify whether
  `observedGeneration` proves the controller has processed your latest change,
  so the generation row always says `advisory Unverifiable`.
- **Absence is typed, not blank.** A source that could not be read shows
  `Unavailable` with a reason (`Forbidden`, `Timeout`, `NotFound`, ...), which
  is different from a source that was read and observed empty (`Reported
  count=0`). Missing RBAC therefore degrades individual rows instead of
  failing the command.

## The sections, top to bottom

A typical default table for a healthy service using runtime auto-selection:

```
FIELD                VALUE
Name                 llama-70b
Namespace            team-a
Ready                True / Valid
Ready reason
Declared runtime
Model                llama-3-3-70b
Generation           4 observed=4; advisory Unverifiable
Pod observation      Reported count=2 truncated=false
Event observation    Reported count=1 truncated=false
engine               True / Valid; Ready pods=2/2 restarts=2
Rollout              NotConfigured reported=NotConfigured
Rollout evidence     Declared / NotApplicable
Autoscaling          Unavailable / Unavailable parent status
Placement            NotConfigured / NotApplicable / NotRecorded
Traffic              Unavailable / Unavailable parent status
Runtime active       Unavailable / Unavailable AutoSelectionNotProbed
Accelerator          Unavailable / Unavailable AutoSelectionNotProbed
Warning Pod          llama-70b-engine-7f8c9 BackOff
Full safe values     Use -o json or -o yaml
Rollout detail       kubectl ome rollout status NAME
Autoscale detail     kubectl ome autoscale status NAME
Placement detail     kubectl ome placement status NAME
Traffic detail       kubectl ome traffic status NAME
Runtime detail       kubectl ome runtime effective NAME
Accelerator detail   kubectl ome accelerator explain NAME
```

### Readiness

`Ready` shows `<status> / <validity>`. Status is `True`, `False`, `Unknown` or
`NotRecorded` — `NotRecorded` means no usable Ready condition exists (also
chosen when duplicate Ready conditions conflict). Validity is `Valid`,
`Invalid` (the recorded conditions were malformed or oversized) or
`Unavailable`. `Ready reason` repeats the condition's reason when the record
is valid.

Wide adds the condition message and a `Condition inspection` row of the form
`<state> <inspected>/<total>`: how many status conditions the evaluation
examined. At most 64 conditions are inspected; more than that yields
`LimitExceeded`.

### Declared spec and generation

`Declared runtime` and `Model` echo `spec.runtime.name` and `spec.model.name`.
An empty runtime row means the runtime is auto-selected by the operator — the
status command does not re-run runtime selection to find out which one (that
is what `Runtime active` reports as `AutoSelectionNotProbed`). `Generation`
shows `<metadata.generation> observed=<status.observedGeneration>`, always
marked advisory.

### Pod and event observation

These two rows describe *how well the command could see*, before you trust any
count below them. The form is `<state> count=<n> truncated=<bool> <reason>`:

- `Reported` — the collection completed inside its bounds.
- `Partial` — something was truncated, skipped or rejected; counts are lower
  bounds.
- `Unavailable` — the read failed entirely; the reason says why
  (`Forbidden`, `Unauthorized`, `NotFound`, `Timeout`, `Unavailable`,
  `Unreadable`, `MalformedPayload`, `UnsupportedAPI`).

Wide adds `Pod targets skipped` and `Event targets skip` — how many objects
were not queried for events because the target budget ran out.

### Component rows

Each declared or observed component (`engine`, `decoder`, `router`) gets one
row: `<ready> / <validity>; Ready pods=<ready>/<total> restarts=<n>`. The
ready state comes from the component's own condition (`EngineReady`,
`DecoderReady`, `RouterReady`); `Ready pods` counts pods whose `PodReady`
condition is `True`; `restarts` sums restart counts across all containers,
including init and ephemeral containers.

Wide adds a phases row per component:

```
engine phases   R=2 P=0 F=0 S=0 U=0 deleting=0
```

The abbreviations are pod phases: **R**=Running, **P**=Pending, **F**=Failed,
**S**=Succeeded, **U**=Unknown. `deleting` counts pods with a deletion
timestamp (terminating); such a pod still appears under its current phase, so
`R+P+F+S+U` always equals the pod total while `deleting` overlaps it. Wide
also shows each component's evidence level (`Observed` when pods were listed,
`Unavailable` when the pod read failed).

### Rollout

`Rollout <state> reported=<reported>` plus `Rollout evidence <evidence> /
<epoch>`. The first state is the CLI's bounded aggregate
(`NotConfigured`, `InProgress`, `Paused`, `Staged`, `Succeeded`, `Failed`,
`RollingBack`, `RolledBack`, `Unknown`); `reported=` is what the controller
recorded. The epoch (`NotApplicable` or `Unverifiable`) says whether that
conclusion can be bound to the current generation — it usually cannot, which
is why this row is a snapshot, not proof the rollout you just started is the
one being described. Wide adds coordination readiness and any rollout issue
and warning codes.

### Autoscaling

`Autoscaling <state> / <evidence> parent status`. This row uses **only** the
autoscaler status the controller wrote into the InferenceService — it performs
no live HPA, KEDA or InferenceReplica reads. A service with no autoscaler
status at all shows `Unavailable / Unavailable parent status`, which is normal
rather than a failure. The variant `Unavailable; parent read, no usable scaler
status` means the parent was read successfully but its reported scaler entries
did not add up to a usable summary. When per-component scaler status exists,
`Scale engine`-style rows show
`<state> <class>/<managedBy> <current->desired> (<replica evidence>)`.

### Placement

`Placement <state> / <mode> / <phase>` summarizes parent-reported multi-cluster
placement (`NotConfigured / NotApplicable / NotRecorded` for ordinary
single-cluster services). When placement is reported, extra rows appear:
`Placement homes` (validated candidate count), `Reported cluster`, and
`Placement endpoint <state> (reported; not probed)` — the endpoint is echoed
from status, never contacted. In `Split` mode a `Placement replicas` row shows
`admitted=` and `ready=` totals, aggregated only when every reported home
supplies a usable value. None of this proves capacity, routing or serving
health.

### Traffic

`Traffic <state> / <evidence> parent status` — the same parent-status-only
snapshot as autoscaling. Wide adds the policy Ready condition with its
freshness, the translator and the algorithm when traffic status is present.

### Runtime active

`Runtime active <state> / <active state> <reason>` reports the runtime
actually in effect for a *declared* runtime, including its pin state (wide
adds the active runtime's name, kind, origin, pin state and freshness). It
resolves only the exact runtime named in the spec — no candidate lists, no
ranking, no history. `NotConfigured` means neither runtime nor model is set;
`Unavailable AutoSelectionNotProbed` means the runtime is auto-selected and
this command deliberately does not probe which one won.

### Accelerator

`Accelerator <state> / <evidence> <reason>` summarizes GPU class selection.
Only current, valid class references already reported under the Engine and
Decoder component status trigger reads, each an exact `get` of that
AcceleratorClass — at most two, never a list. Per-component rows show the
selection state and class name. As with the runtime row, auto-selected
runtimes yield `AutoSelectionNotProbed`.

### Warning events

`Warning Pod` / `Warning InferenceService` rows list recent Warning events as
`<object name> <reason>`, newest first. Only events that verifiably belong to
this InferenceService or one of its accepted pods (matching name **and** UID)
are shown; everything else is rejected with an `EventIdentityRejected` or
`EventMalformed` issue. Wide adds each event's message, count and last-seen
timestamp.

### Issues and the trailer

`Issue` rows carry typed codes describing evidence that was rejected or
bounded: `CollectionLimitExceeded`, `PodIdentityRejected`, `PodMalformed`,
`EventIdentityRejected`, `EventMalformed`, `UnsupportedComponent`,
`RolloutUnavailable`, `AutoscaleUnavailable`, `PlacementUnavailable`,
`InvalidGeneration` and friends. The closing rows (`Full safe values`,
`... detail`) are always printed and point at the richer commands. Wide
appends `Collected at` plus the source generation and evidence level.

## Observation bounds

All windows apply after decoding; whatever exceeds a bound is dropped and
marked, never silently truncated mid-value.

| Read | Bound |
| --- | --- |
| InferenceService | one exact `get`, 10 s |
| Pods | up to 1000 pods over at most two 500-item pages, 10 s per request, selected by `ome.io/inferenceservice=<name>` |
| Warning Events | at most 16 targets — the InferenceService plus up to 15 pods — 25 events per target, 8 concurrent requests, 5 s each, 100 events kept |
| Conditions | at most 64 inspected per Ready evaluation |
| Component rows | at most 3 (engine, decoder, router) |
| Whole command | 30 s |

When there are more pods than event targets, pods that are not Ready or are
terminating are queried first, so the events you see skew toward the pods most
likely to explain a problem. Oversized rollout or autoscaler status payloads
are not partially rendered: the affected summary is dropped and
`CollectionLimitExceeded` plus `RolloutUnavailable` or `AutoscaleUnavailable`
issues are recorded instead.

## When to switch to a detail command

The rollout, autoscaling, placement, traffic, runtime and accelerator rows are
deliberately bounded snapshots. Switch to the dedicated command whenever a
summary row shows `Partial` or `Unavailable` with a reason you need to chase,
an issue code appears, or you need fields the summary omits:

| Summary row | Detail command | What it adds |
| --- | --- | --- |
| `Rollout` | `kubectl ome rollout status` | per-group and per-component rollout state, steps and revisions |
| `Autoscaling`, `Scale ...` | `kubectl ome autoscale status` | live HPA and KEDA scaler evidence (needs extra RBAC — see the [plugin page](/docs/tasks/kubectl-ome/#required-rbac)) |
| `Placement` | `kubectl ome placement status` | the full placement report behind the parent snapshot |
| `Traffic` | `kubectl ome traffic status` | the full traffic policy report |
| `Declared runtime`, `Runtime active` | `kubectl ome runtime effective` | how the effective runtime was resolved, including pins |
| `Accelerator` | `kubectl ome accelerator explain` | full accelerator class reasoning per component |

For scripting, always use `-o json` or `-o yaml`; the human-readable table is
not a stable interface.
