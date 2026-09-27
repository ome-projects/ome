---
title: "Inspect One Logical Instance with kubectl-ome"
linkTitle: "Instance Status"
weight: 21
date: 2026-09-27
description: >
  Show one OMENative logical instance — identity, phase, incarnation, the operation in progress and what it is waiting on, migrations, and durable announcement markers — joined with bounded live Pod and Warning Event evidence.
---

When an InferenceService component runs in `OMENative` deployment mode, the
controller manages it as a set of indexed **logical instances** and persists
one authoritative status row per instance on the component's InferenceReplica.
`kubectl ome instance status` reads exactly one of those rows and joins it
with bounded live evidence — the instance's own Pods and their Warning Events
— into a single diagnostic report:

```bash
kubectl ome instance status INFERENCESERVICE INDEX --component COMPONENT
```

The command is read-only and ships with the [kubectl-ome
plugin](/ome/docs/tasks/kubectl-ome). It is the single-instance drill-down:
`kubectl ome instance list` shows the whole inventory of a service's logical
instances, and retry blocks and held revisions have their own commands (see
[Release a Held Revision](/ome/docs/tasks/release-a-held-revision)).

## Before you begin

- The [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) installed.
- RBAC: the plugin's [baseline read-only
  role](/ome/docs/tasks/kubectl-ome/#required-rbac) covers everything this
  command reads — the InferenceService and its InferenceReplicas (`ome.io`
  group), Pods and Events (core group), ServingRuntimes /
  ClusterServingRuntimes for effective-mode resolution, and, for pinned
  services, `ControllerRevision` snapshots in the OME control-plane namespace
  (`--ome-namespace`, default `ome`). A denied Pod or Event read does not fail
  the command — the report says so; a denied InferenceReplica list yields
  state `Unavailable`.
- An InferenceService whose selected component runs in `OMENative` mode.

## Basic usage

```bash
kubectl ome instance status chat 0 --component engine -n prod
```

- `INFERENCESERVICE` — the InferenceService name.
- `INDEX` — the logical instance index, an integer from 0 through 2147483647.
- `--component` — `engine`, `decoder`, or `router` (required).
- `-o` — `table` (default), `wide`, `json`, or `yaml`.

Example output for an engine instance mid-update, with a surge pod that
cannot schedule:

```
FIELD        VALUE
state        Reported engine[0] evidence=Reported
deployment   mode=OMENative source=ServiceSpec origin=LiveRuntime evidence...
encoding     name=DenseV1 evidence=Reported reason=-
instance     chat-engine inc=3 phase=Updating admitted=true
revisions    running=chat-engine-1f2a3b4c target=chat-engine-9d8c7b6a
persisted    pods=2 serving=1 available=1
lifecycle    activeOrdinal=0 readySince=2026-09-20T10:12:00Z
condition    Ready=True gen=7 evidence=Reported reason=-
announced    GangSplitRisk@#3
operation    Update id=update-0-1758960240 step=WaitReady retry=1
op target    revision=chat-engine-9d8c7b6a reason=-
op timing    start=2026-09-27T08:44:00Z progress=2026-09-27T08:59:10Z dead...
op hold      waiting=Unschedulable refused=-
op strategy  SurgeThenDrain
op nodes     from=- surge=- hints=-
pod          chat-engine-0-default-0 default/Running ready=True serving=Tr...
pod          chat-engine-0-default-1 default/Pending ready=False serving=F...
event        Pod/chat-engine-0-default-1 reason=FailedScheduling count=4
```

How to read it:

- **state** is the summary: the report state (vocabulary
  [below](#report-states)), the selected component and index, and the quality
  of the authoritative evidence (`Reported`, `Stale`, `Malformed`, or
  `Unavailable`).
- **deployment** is the component's effective deployment mode with its
  provenance: `source` names the field that decided it
  (`ComponentAnnotation`, `ServiceAnnotation`, `ServiceSpec`,
  `LeaderWorkerShape`, or `Default`) and `origin` names where that
  configuration was read (`InferenceService`, `LiveRuntime`, or
  `ControllerRevision`).
- The compact table is a vertical FIELD/VALUE view whose lines stay within 80
  display columns; values longer than 64 columns are clipped with an ASCII
  `...`. Use `-o wide` for complete values.
- The **pod** rows are live corroboration, not the authority: `ready` is the
  Kubernetes `Ready` condition, `serving` is the controller-owned
  `ome.io/serving` readiness gate, and restarts total the init and regular
  container counters. The **persisted** row above them is what the controller
  recorded on the InferenceReplica — comparing the two is the point.
- **event** rows are Warning Events for this instance's pods only, and they
  are deliberately message-free (target, reason, count, and times only).

## Where each value comes from

**InferenceReplica status is authoritative.** Identity, phase, incarnation,
revisions, admission, the persisted pod counts, conditions, the operation in
progress, the last failure, migrations, and announcement markers all come
from the controller-persisted instance row. Live Pods and Events only
corroborate: a Pod can never create a logical instance in the report or
upgrade stale, malformed, or duplicate InferenceReplica evidence.

**Effective deployment mode follows the controller.** A service-level
`ome.io/deploymentMode: VirtualDeployment` annotation, or a component-level
`ome.io/deploymentMode` annotation on the InferenceService spec, decides the
mode directly. Otherwise the command resolves the effective runtime the same
way the controller does — the live ServingRuntime / ClusterServingRuntime
(including runtime inheritance), or, for pinned services, the pinned
`ControllerRevision` snapshot, which wins over the live runtime.
`--ome-namespace` selects the namespace those snapshots live in (default
`ome`).

A component whose effective mode is anything other than `OMENative` (for
example `RawDeployment` or `MultiNode`) is reported as **`NotOMENative`**,
and nothing else is read — logical instances only exist under OMENative. If
the mode cannot be resolved at all (runtime missing, disabled, forbidden, an
inheritance cycle, a malformed pinned revision), the deployment block shows
`evidence=Unavailable` with a reason, but the instance evidence is still
collected — mode-resolution failure never blocks the authoritative read.

## The instance block

### Identity, phase, and lifecycle

- **inferenceReplica / index** — the owning InferenceReplica and the selected
  instance index.
- **incarnation** — the instance's rebuild counter. Restarts and
  recreate-style updates advance it; every pod is labeled with the
  incarnation it belongs to, so a pod on an older incarnation is visibly
  left over.
- **phase** — `Pending`, `Creating`, `Ready`, `Updating`, `Restarting`,
  `Migrating`, `Failed`, or `Deleting`.
- **runningRevision / targetRevision** — the component workload revisions
  (`<service>-<component>-<hash>`) the instance is on and converging toward.
  They differ while an update is in flight.
- **pods** (`persisted` in the table) — the controller-persisted
  total/serving/available pod counts for this instance.
- **admitted** — whether the instance is admitted to serve.
- **readySince** — when the instance last entered `Ready`. It anchors
  post-Ready failure detection: container restarts that finished before this
  time belong to boot.
- **activeOrdinal** — which of the two pod-naming slots (0 or 1) holds the
  canonical pod. The `SurgeThenDrain` update strategy alternates slots (the
  surge pod is created at `1 - activeOrdinal`); in-place and recreate
  strategies keep it at 0. Any other persisted value is rejected as
  malformed.
- **conditions** — each with its own `observedGeneration` and per-condition
  evidence: a condition observed at an older generation than the
  InferenceReplica's current one renders as `Stale`, and contradictory
  same-type conditions are dropped as malformed.

### The operation in progress

`operation` is present only while the controller has work in flight against
this instance; it is written before the action starts and cleared when it
completes.

- **type** — `Create`, `Update`, `Restart`, `Migrate`, or `Delete`; **id** is
  the operation's idempotency key; **step** is the fine-grained resume point
  within it (for example `Drain`, `DeletePods`, `WaitZero`, `Recreate`,
  `WaitReady`).
- **reason** — why the operation exists (for example a revision-roll cause).
  Set once when the operation starts, and distinct from `waiting`.
- **waiting** — what *external* condition is currently holding the operation
  back: `QuotaExceeded`, `NodeUnknown`, `SourceUnrouted`, `Unschedulable`,
  `PodGroupTerminating`, or `Paused`. Empty means nothing external blocks it.
  A token this CLI build does not know renders as `Unknown` with an
  `OperationWaitingUnknown` issue rather than echoing the raw value.
- **startedAt / lastProgressAt / deadline** — when the operation began, when
  it last advanced (stall detection), and the hard timeout for the current
  step. An absent deadline means the step clock is parked — for example while
  the instance's pod creates are quota-gated.
- **retryCount** — the per-step escalation counter.
- **capacityRefusedAt** — when admission last refused one of the operation's
  pod creates for lack of quota (shown as `refused=` in the `op hold` row).
- **strategy** — the update mechanism the operation was pinned to when it
  opened: `SurgeThenDrain`, `RecreatePod`, `InPlaceIfPossible`, or
  `InPlaceOnly`. Only `Update` operations carry it; unknown values render as
  `Unknown` with an `OperationStrategyUnknown` issue.
- **surgeIndex / fromNode / targetNodeHints / requestUUID** — set only for
  `Migrate`: the instance index allocated for the surge replacement, the node
  being vacated, the caller's preferred nodes, and the migration request
  UUID.

### Last failure

`lastFailure` preserves the most recent genuine pod failure — pod and
container name, the kubelet reason (`OOMKilled`, `CrashLoopBackOff`,
`ImagePullBackOff`, …), the exit code when a process actually ran, and when
the controller recorded it. Clean revision-roll recreates do not overwrite
it, so it stays useful across rollouts.

### Migrations

The report includes every migration record on the InferenceReplica that
involves the selected instance, labeled with its **role**: `Source` (this
instance is being moved) or `Surge` (this instance is the replacement). Each
carries the request UUID, trigger (`Manual` or `Auto`), phase (`Accepted`,
`SurgePending`, `SurgeReady`, `Draining`, `Completed`, `Failed`, or
`Relocated`), attempt counter, timestamps, source/surge indices, and node
information. Internally inconsistent records are dropped with a
`MigrationInvalid` issue rather than partially shown. To start or track a
migration, see [Request an Instance
Migration](/ome/docs/tasks/request-an-instance-migration) and
[kubectl-ome Migration](/ome/docs/tasks/kubectl-ome-migration).

### Durable announcement markers

`announcements` are the instance's **once-only message markers** — the
controller's durable record that a message an operator needs to read once
per episode (not once per reconcile) was already emitted as an event. Each
marker is `<Reason>@<episode>`, split in the report into `reason` and
`episode`:

- An episode of `#<incarnation>` (for example `GangSplitRisk@#3`) marks a
  *standing* message about the instance as built.
- Any other episode is an operation ID (for example
  `RepairHeld@update-0-1758960240`) and marks a message about that attempt.

A marker's presence means "this was already announced for this episode and
will not be repeated" — it does not mean the condition still holds. Markers
of ended episodes are pruned lazily, by the next announcement write, so an
old operation's marker lingering next to a newer operation is normal. The
volatile event message itself is deliberately excluded; only the bounded
reason and episode tokens are retained (at most 16 markers per row).

## Encoding provenance

`encoding` names the wire representation the controller used for the
per-instance status rows: `DenseV1` (the dense `instanceStatuses` list) or
`ColumnarV2` (the grouped `instanceStatusColumns` payload). Both decode to
exactly the same rows — this field is provenance for debugging mixed-version
clusters, not a behavior switch. It is `Reported` only when a complete
related InferenceReplica was collected and validated; whenever that evidence
cannot be established the encoding is explicitly `Unavailable` with a reason
— the command never guesses.

## Live Pods and Warning Events

Pods are listed with the exact instance selector
(`ome.io/inferenceservice=<service>`, `component=<component>`,
`ome.io/managed-by=OMENative`, `ome.io/instance-index=<index>`), and every
returned pod is identity-checked before it may appear: it must carry a sole
controller owner reference to the selected InferenceReplica's exact UID and
the canonical pod name `<service>-<component>-<index>-<runner>-<ordinal>`.
Pods that fail verification are dropped and flagged (`PodIdentityRejected`),
never merged.

At most 16 pods appear in the report. When more match, the cap keeps problem
pods first: deleting pods, then pods that are not Running/Ready/Serving or
sit on the wrong incarnation, then healthy ones — so truncation drops healthy
duplicates, not the pod you are debugging.

Warning Events are collected per accepted pod only (at most 16 targets, 50
events). Events on the InferenceService or InferenceReplica are deliberately
excluded: the controller emits those for multiple logical instances against
the same target, so they cannot be attributed to this one. Event messages
are never shown in any output format — only the target, reason, count, and
first/last-seen times. For the message text, fall back to `kubectl get
events` or `kubectl describe pod`.

All free-form text that does appear (operation and condition reasons,
migration messages) is sanitized, credential-shaped text is redacted, and
lengths are capped.

## Report states

| `state` | Meaning |
|---------|---------|
| `Reported` | Exactly one authoritative row was found and the evidence is complete. |
| `Partial` | A report was produced, but some evidence is degraded, truncated, or unavailable — the `issues` list says exactly what. |
| `Unavailable` | The InferenceReplica listing could not be read (or was truncated), so no authoritative row could be established. |
| `NotProjected` | The service has no accepted InferenceReplica for the selected component. |
| `Missing` | The component exists but reports no row at the selected `INDEX`. |
| `NotOMENative` | The component's effective deployment mode is not `OMENative`; no instance, pod, or event evidence is read. |

The command exits `0` whenever it wrote its report — **including** `Missing`
and `NotOMENative`; absence is stated in the report, not in the exit code.
Exit `1` means the command could not complete at all (invalid arguments, the
InferenceService could not be read, or output failed) — see the shared
[exit codes](/ome/docs/tasks/kubectl-ome/#exit-codes).

## Issues, warnings, and bounded reads

Every degradation is stated, never silently dropped. `issues` carries closed
vocabulary codes, each optionally with an `unavailableReason` (`NotFound`,
`Forbidden`, `Disabled`, `Cycle`, `MaxDepthExceeded`, `MalformedPayload`,
`UnsupportedAPI`, `Unreadable`, `NotConfigured`, `StaleGeneration`).
Frequently seen codes:

| Issue code | Meaning |
|------------|---------|
| `CollectionUnavailable` / `CollectionTruncated` | The InferenceReplica listing failed or exceeded its paging budget. |
| `InstanceMissing` | No row at the selected index. |
| `AuthoritativeInvalid` | The authoritative row (or a nested record) failed validation. |
| `ParentGenerationStale` / `StaleGeneration` | The controller's status has not caught up with the latest spec edit — retry after reconciliation. |
| `ConditionGenerationStale` | A condition was observed at an older generation. |
| `PodsUnavailable` / `EventsUnavailable` | The live read was denied or failed; the authoritative block is unaffected. |
| `PodIdentityRejected` / `EventIdentityRejected` | A live object failed exact identity verification and was excluded. |
| `OperationWaitingUnknown` / `OperationStrategyUnknown` | The controller wrote a token this CLI build does not know. |
| `MigrationInvalid` / `AnnouncementInvalid` | An inconsistent nested record was dropped. |
| `EncodingUnsupported` | Status-encoding provenance could not be established. |

Report-level `warnings` summarize: `PartialData`, `SourceUnavailable`,
`StaleEvidence`, `Truncated`.

All reads are bounded: InferenceReplicas at most 60 items over 3 pages, Pods
at most 64 over 2 pages, Warning Events at most 50 over 2 pages, and every
API request is capped at 10 seconds. Per instance row, at most 16
conditions, migrations, node hints, and announcement markers are copied;
anything beyond a budget flags `Truncated` in the report.

## Output formats

- `-o table` (default) — the compact FIELD/VALUE view above.
- `-o wide` — the same report without elision: every source with UID and
  generation, complete revisions and node names, full operation and
  migration detail, and RFC 3339 timestamps, with values bounded at 256
  columns.
- `-o json` / `-o yaml` — the typed `InstanceStatusReport`
  (`cli.ome.io/v1alpha1`), carrying the same safe, sanitized content with
  deterministic ordering. Resource versions are excluded. As with all
  kubectl-ome commands, the human tables are not a stable scripting
  interface before GA — script against `-o json`.

The same mid-update scenario as JSON:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "InstanceStatusReport",
  "metadata": { "namespace": "prod", "name": "chat" },
  "collectedAt": "2026-09-27T09:00:00Z",
  "sources": [
    { "kind": "EventList", "namespace": "prod", "name": "chat/engine/0",
      "evidence": "Observed", "collectedAt": "2026-09-27T09:00:00Z" },
    { "kind": "InferenceReplica", "namespace": "prod", "name": "chat-engine",
      "uid": "6c9c2f6e-...", "generation": 7,
      "evidence": "Reported", "collectedAt": "2026-09-27T09:00:00Z" },
    { "kind": "InferenceService", "namespace": "prod", "name": "chat",
      "uid": "0b8d4c21-...", "generation": 12,
      "evidence": "Reported", "collectedAt": "2026-09-27T09:00:00Z" },
    { "kind": "PodList", "namespace": "prod", "name": "chat/engine/0",
      "evidence": "Observed", "collectedAt": "2026-09-27T09:00:00Z" }
  ],
  "content": {
    "summary": {
      "state": "Reported",
      "component": "engine",
      "index": 0,
      "evidence": "Reported",
      "truncated": false
    },
    "deployment": {
      "mode": "OMENative",
      "source": "ServiceSpec",
      "origin": "LiveRuntime",
      "evidence": "Reported"
    },
    "encoding": { "name": "DenseV1", "evidence": "Reported" },
    "instance": {
      "inferenceReplica": "chat-engine",
      "index": 0,
      "incarnation": 3,
      "phase": "Updating",
      "runningRevision": "chat-engine-1f2a3b4c",
      "targetRevision": "chat-engine-9d8c7b6a",
      "pods": { "total": 2, "serving": 1, "available": 1 },
      "admitted": true,
      "readySince": "2026-09-20T10:12:00Z",
      "activeOrdinal": 0,
      "conditions": [
        {
          "type": "Ready",
          "status": "True",
          "observedGeneration": 7,
          "evidence": "Reported",
          "lastTransitionTime": "2026-09-20T10:12:00Z"
        }
      ],
      "migrations": [],
      "announcements": [
        { "reason": "GangSplitRisk", "episode": "#3" }
      ],
      "operation": {
        "id": "update-0-1758960240",
        "type": "Update",
        "step": "WaitReady",
        "startedAt": "2026-09-27T08:44:00Z",
        "lastProgressAt": "2026-09-27T08:59:10Z",
        "deadline": "2026-09-27T09:14:00Z",
        "retryCount": 1,
        "targetRevision": "chat-engine-9d8c7b6a",
        "waiting": "Unschedulable",
        "strategy": "SurgeThenDrain",
        "targetNodeHints": []
      }
    },
    "pods": [
      {
        "name": "chat-engine-0-default-0",
        "runner": "default",
        "revision": "1f2a3b4c",
        "incarnation": 3,
        "phase": "Running",
        "ready": "True",
        "servingReady": "True",
        "node": "gpu-node-3",
        "restartCount": 0,
        "deleting": false
      },
      {
        "name": "chat-engine-0-default-1",
        "runner": "default",
        "revision": "9d8c7b6a",
        "incarnation": 3,
        "phase": "Pending",
        "ready": "False",
        "servingReady": "False",
        "restartCount": 0,
        "deleting": false
      }
    ],
    "events": [
      {
        "targetKind": "Pod",
        "targetName": "chat-engine-0-default-1",
        "reason": "FailedScheduling",
        "count": 4,
        "firstSeen": "2026-09-27T08:45:02Z",
        "lastSeen": "2026-09-27T08:58:40Z"
      }
    ],
    "issues": []
  },
  "warnings": []
}
```
