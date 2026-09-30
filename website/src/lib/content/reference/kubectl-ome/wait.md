---
title: kubectl ome wait
description: Block until an InferenceService reaches the state you name, and exit with a code that scripts can branch on.
since: v1.3
---

`kubectl ome wait` blocks until an [InferenceService](../../concepts/serving/inference-services.md) reports the state you name with `--for`, then prints one report. It exits `0` when it saw the state, `2` when the wait ended without it, and `1` on an error, so a script can branch on the result. It only reads, and gives up after `--timeout`: 1 minute by default, at most 24 hours.

That default suits a state that should already hold. Model loads, rollouts and migrations take longer, so set `--timeout` for them, such as `--timeout=30m`.

```text
kubectl ome wait INFERENCESERVICE --for=PREDICATE [flags]
```

## Predicates

`--for` takes one of these predicates:

| Predicate | Also needs | Waits until |
| --- | --- | --- |
| [`condition=Ready`](#condition-ready) or `condition=Ready=True` | None | The Ready condition is `True`. |
| [`condition=Ready=False`](#condition-ready) | None | The Ready condition is `False`. |
| [`condition=Ready=Unknown`](#condition-ready) | None | The Ready condition is `Unknown`. |
| [`rollout=stable`](#rollout) | None | The rollout reports `Succeeded`. |
| [`rollout=failed`](#rollout) | None | The rollout reports `Failed`. |
| [`rollout=rolled-back`](#rollout) | None | The rollout reports `RolledBack`. |
| [`migration=terminal`](#migration-terminal) | `--request-id` | The migration with that request ID ends: it completed, failed or relocated. |
| [`replicas=ready`](#replicas-ready) | `--component`, `--replicas` | The component's InferenceReplica reports exactly that many ready replicas. |
| [`replicas=current`](#replicas-current) | `--component`, `--replicas`, and optionally `--ir-name` with `--ir-uid` | The component's InferenceReplica asks for and reports exactly that many Instances. |
| [`runtime-sync=acknowledged`](#runtime-sync-acknowledged) | `--request-id` | OME acknowledged the runtime sync with that request ID. |
| [`held-revision=unheld`](#held-revision-unheld) | `--component`, `--revision`, `--ir-name`, `--ir-uid` | The revision is no longer held on that InferenceReplica. |

The migration, replica and held-revision predicates read a component's InferenceReplica (IR), which OME writes only for components that run on [OMENative](../../concepts/omenative/overview.md).

Each predicate takes only the flags in its row, and refuses others with an [error](#error-messages). The first check runs at once, so a state that already holds matches immediately. After that, `condition=Ready` and `rollout` watch the InferenceService, and the other predicates check every 5 seconds. Every wait ends with one of these outcomes:

| Outcome | Exit code | When |
| --- | --- | --- |
| `Matched` | `0` | A check saw the state you asked for. |
| `TimedOut` | `2` | `--timeout` passed first. The report shows the last check. |
| `NotFound` | `2` | The InferenceService didn't exist at the first read. |
| `Deleted` | `2` | The InferenceService was being deleted, or disappeared after the first read. |
| `Replaced` | `2` | The InferenceService was deleted and created again under the same name, with a new UID. |

`NotFound`, `Deleted` and `Replaced` end the wait right away, so create the InferenceService before you wait on it.

Status that's missing or malformed never matches: the report's validity row shows `Unavailable` or `Invalid`, and the wait goes on. The migration, replica and held-revision predicates also wait while an InferenceReplica lags its own spec, and the last two while it lags the InferenceService's. The replica and held-revision predicates follow one InferenceReplica, the one you name or the one they find at the first check. If it's replaced or deleted, the wait can't match and runs until the timeout. [`kubectl ome instance list`](instance.md) shows what's wrong with an InferenceReplica.

The CLI treats a status timestamp later than your machine's clock as invalid, so when that clock runs behind the cluster's, a match waits until it catches up.

### `condition=Ready` {#condition-ready}

```text
kubectl ome wait INFERENCESERVICE --for=condition=Ready[=True|False|Unknown] [--timeout=DURATION]
```

Waits for the status of the InferenceService's `Ready` condition: `True` when you give none. `condition=Ready=Unknown` needs a Ready condition with that status. While there's none, the report shows `NotRecorded`, and the wait goes on. Ready conditions that disagree never match.

### `rollout` {#rollout}

```text
kubectl ome wait INFERENCESERVICE --for=rollout=stable|failed|rolled-back [--timeout=DURATION]
```

Waits for the rollout state that [`kubectl ome rollout status`](rollout.md#status-output-fields) shows as REPORTED: `stable` waits for `Succeeded`, `failed` for `Failed` and `rolled-back` for `RolledBack`. Any other state keeps the wait going. So `rollout=stable` runs until its timeout after a rollout fails, and a canary waiting at a gate keeps it waiting. A service without rollouts reports `NotConfigured`, which never matches: wait for `condition=Ready` instead.

`EpochUnverifiable` and `AnalysisInconclusive` show as Rollout issue rows but don't block a match. `EpochUnverifiable` shows for any service with rollout groups.

### `migration=terminal` {#migration-terminal}

```text
kubectl ome wait INFERENCESERVICE --for=migration=terminal --request-id=UUID [--timeout=DURATION]
```

Waits for the end of one Instance migration, named by the request ID that the alpha [`kubectl ome migration start`](migration.md) prints. It matches when the request reaches `Completed`, `Failed` or `Relocated`, and all three exit `0`. To tell them apart, read the Migration outcome row, or `content.migration.migrationOutcome` in JSON.

A request queues behind earlier migrations of the same component, including those from [Alfred](../../concepts/scheduling/alfred.md), which is alpha, and behind an update's surge Instance in flight on that component. It fails at its [deadline](../../concepts/omenative/migration-and-transient-scale.md#deadline), by default 30 minutes after OME accepts it. So give `--timeout` at least that, such as `--timeout=30m`, and more when a rollout may pause the migration.

### `replicas=ready` {#replicas-ready}

```text
kubectl ome wait INFERENCESERVICE --for=replicas=ready --component=engine|decoder|router --replicas=N [--timeout=DURATION]
```

Waits until the component's InferenceReplica reports exactly `N` ready replicas in `status.readyReplicas`. `N` can be `0`.

!!! warning "Index gaps leave the count invalid"
    After a [surge migration](../../concepts/omenative/migration-and-transient-scale.md#surge-migration), yours or Alfred's, or a [`SurgeThenDrain`](../../concepts/architecture/omenative-update-strategies.md#surgethendrain) update of multi-pod Instances, `replicas=ready` shows Count validity `Invalid` for every component of the service, and times out. Until the Instance indices have no gaps again, as after a scale-up, `kubectl ome instance list` shows `SPARSE`; meanwhile, wait with `replicas=current`, which counts Instances in any phase. This is a known bug.

### `replicas=current` {#replicas-current}

```text
kubectl ome wait INFERENCESERVICE --for=replicas=current --component=engine|decoder|router --replicas=N [--ir-name=NAME --ir-uid=UID] [--timeout=DURATION]
```

Waits until the component's InferenceReplica asks for and reports exactly `N` Instances, in `spec.replicas` and `status.replicas`. `N` must be at least `1`.

To wait on the InferenceReplica that a scale request acted on, pass `--ir-name` and `--ir-uid`. Take them from the `target.name` and `target.uid` that the alpha [`kubectl ome scale`](scale.md) prints with `-o json`.

### `runtime-sync=acknowledged` {#runtime-sync-acknowledged}

```text
kubectl ome wait INFERENCESERVICE --for=runtime-sync=acknowledged --request-id=UUID [--timeout=DURATION]
```

Waits until OME acknowledges a sync that the alpha [`kubectl ome runtime sync`](runtime.md) requested. Pass the request ID that command prints. OME acknowledges the sync when it repins the service to the runtime's current spec: it copies the request's token from the `ome.io/runtime-sync` annotation to `status.lastRuntimeSyncToken`.

The wait matches once the token is acknowledged, the service still has a managed pin, with `autoSync: false` and no `revision`, and there's no `RuntimeDrifted` condition. A service placed by multi-cluster placement, which is alpha, never matches.

If the pin already matches the runtime's current spec, OME doesn't repin, and Token state stays `Pending` until the timeout. A newer sync request replaces your token: Token state shows `Superseded`, and the wait can't match.

### `held-revision=unheld` {#held-revision-unheld}

```text
kubectl ome wait INFERENCESERVICE --for=held-revision=unheld --component=engine|decoder|router --revision=REVISION --ir-name=NAME --ir-uid=UID [--timeout=DURATION]
```

Waits until a revision that OME held after repeated failures is no longer held on an InferenceReplica. Take the flag values from the result of the alpha [`kubectl ome instance release-held`](instance.md). Pass its `target.name` and `target.uid` as `--ir-name` and `--ir-uid`. For `--revision`, pass the full revision name, `<service>-<component>-<hash>`: add the prefix to the result's `revisionHash`, or copy the name from `kubectl ome instance retry-blocks`.

The wait matches once the revision's retry block is no longer `Held` and the `ome.io/release-held-revision` annotation is gone.

## What a match means {#what-a-match-does-not-mean}

Exit code `0` means a check saw the requested state on the InferenceService the wait is bound to. It isn't a receipt for a command you ran. To wait for one request, use a predicate that takes its ID: `migration=terminal` or `runtime-sync=acknowledged`.

| Predicate | A match shows |
| --- | --- |
| `condition=Ready`, `rollout` | The state the controller last wrote. A `True` or `Succeeded` from before your change matches immediately. |
| `migration=terminal` | Your migration ended, successfully or not. |
| `replicas=ready` | The InferenceReplica's ready count. [`kubectl ome traffic status`](traffic.md) shows which revisions receive traffic. |
| `replicas=current` | The desired and current Instance counts, in any phase. |
| `runtime-sync=acknowledged` | OME recorded your token and repinned the service. Pods may still be moving to the new revision: follow them with [`kubectl ome rollout status`](rollout.md#status). |
| `held-revision=unheld` | The revision is no longer held, whether your release or OME's own pruning cleared it. A later failure can hold it again. |

After a spec change, the replica counts can include Instances of the old revision.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | None | IR component for replica or held-revision waits: engine, decoder, router |
| `--for` | None | Required: condition=Ready[=True\|False\|Unknown], rollout=stable\|failed\|rolled-back, migration=terminal, replicas=ready\|current, runtime-sync=acknowledged, or held-revision=unheld |
| `--ir-name` | None | Exact ActionResult target.name; required for held-revision, optional paired with --ir-uid for replicas=current |
| `--ir-uid` | None | Exact ActionResult target.uid; required for held-revision, optional paired with --ir-name for replicas=current |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |
| `--replicas` | `0` | Exact count: nonnegative for replicas=ready; positive for replicas=current |
| `--request-id` | None | Canonical UUID for migration=terminal; canonical v4 UUID for runtime-sync=acknowledged |
| `--revision` | None | Full ISVC-COMPONENT-REVISIONHASH, required only for held-revision=unheld |
| `--timeout` | `1m0s` | Positive wait timeout, at most 24h |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

## Output fields

The command prints one report when the wait ends, as a table with the columns FIELD and VALUE. The table cuts long labels and values with `...`, and `-o wide` adds rows. [Output formats](overview.md#output-formats) describes the formats.

`-o json` and `-o yaml` print every value in full, in a report with `apiVersion: cli.ome.io/v1alpha1` and `kind: WaitReport`. Its `content` holds:

- `requested`, `outcome`, `reason` and `evidence`;
- `observed`, the Ready condition, with `status`, `validity`, `generationFreshness` and `inspection`;
- one object for the other predicates: `rollout`, `migration`, `readyReplicas`, `scale`, `runtimeSync` or `heldRevision`;
- `elapsedMilliseconds`, `counts`, `sourceMethod` and `pollingFallback`.

These rows appear in every report, unless the row says otherwise:

| Row | What it shows |
| --- | --- |
| Service | `NAMESPACE/NAME` of the InferenceService. |
| Requested | The predicate in the report's spelling, for example `Ready=True` or `HeldRevision=Unheld`. |
| Outcome | `Matched`, `TimedOut`, `NotFound`, `Deleted` or `Replaced`, as [Predicates](#predicates) describes. |
| Reason | Why the last check matched or didn't. `PredicateUnmet` means the wait ended before any check finished. |
| Evidence | `Reported` when the value comes from valid status the controller wrote, or `Unavailable`. For `condition=Ready` and `rollout`, invalid status shows `Reported` too. |
| Source method | How the CLI read the last snapshot: `InitialGET`, `Watch`, `Poll`, or `RefreshGET` after a watch expired. |
| Polling fallback | `true` when a `condition=Ready` or `rollout` wait polled instead of watching. `replicas=current` and `held-revision=unheld` leave it out. |
| Elapsed milliseconds | How long the wait ran. |
| GET / WATCH / polls | `READS / WATCHES / POLLS`: the reads the CLI made, the watches it opened, and how many of the reads were polls. |
| Events / observations | `condition=Ready` and `rollout` only: the watch events the CLI took, and the snapshots it checked. |

The Request ID, Target ready, Target replicas and Revision rows show your flag values, as does Component outside `migration=terminal`. The caveat rows, Expansion, Count authority, Read scope, Cancellation, and Attribution outside `migration=terminal`, print fixed notes. [What a match means](#what-a-match-does-not-mean) explains the caveats.

### `condition=Ready` fields {#condition-ready-fields}

| Row | What it shows |
| --- | --- |
| Observed Ready | The Ready condition's status at the last check: `True`, `False` or `Unknown`, or `NotRecorded` when there's none to show. |
| Condition validity | `Valid`, `Unavailable` or `Invalid`. The Reason and Inspection warning rows say why. |
| Generation freshness | Always `Unverifiable`: the CLI can't tell which spec the condition describes. |
| Condition inspection | `STATE (INSPECTED/TOTAL)`, counting the InferenceService's conditions. STATE is `Complete`, `Partial` when there's a warning, or `NotInspected`. |
| Inspection warning | One row per warning about the conditions: `ConflictingReadyConditions`, `DuplicateReadyConditions`, `FutureConditionTimestamp`, `InvalidConditionRecord` or `OversizedConditionRecord`. |

| Reason | Meaning |
| --- | --- |
| `ConditionMatched` | The Ready condition is valid and has the requested status. |
| `ConditionNotMatched` | The Ready condition is valid, with another status. |
| `ConditionNotRecorded` | The InferenceService has no Ready condition. |
| `InvalidCondition` | The Ready condition is invalid. |

### `rollout` fields {#rollout-fields}

Reported rollout, Rollout state, Rollout epoch and Coordination Ready show REPORTED, STATE, EPOCH and COORDINATION from [`rollout status`](rollout.md#status-output-fields).

| Row | What it shows |
| --- | --- |
| Rollout validity | `Valid`, `Unavailable` or `Invalid`. The Reason row says why. |
| Rollout inspection | `Complete`, or `NotInspected` when no check finished. |
| Rollout issue | One row per issue, as `CODE[ group=N][ component=NAME]`. |
| Inspection warning | One row per warning: `ConflictingReadyConditions`, `FutureConditionTimestamp`, `InvalidConditionRecord`, `InvalidPinnedRun`, `InvalidRolloutRecord`, `InvalidRolloutTimestamp`, `MissingRolloutEvidence`, `OversizedConditionRecord` or `StaleReportedCondition`. |

| Reason | Meaning |
| --- | --- |
| `RolloutMatched` | The rollout is valid and reports the requested state. |
| `RolloutNotMatched` | The rollout is valid and reports another state. |
| `RolloutNotRecorded` | Status the rollout needs is missing. |
| `InvalidRollout` | The rollout record is invalid. |

`-o wide` adds counts of what the CLI inspected, then Cancellation.

### `migration=terminal` fields {#migration-terminal-fields}

| Row | What it shows |
| --- | --- |
| Migration phase | The phase in the request's record: `Accepted`, `SurgePending`, `SurgeReady`, `Draining`, `Completed`, `Failed` or `Relocated`. `Unknown` when the CLI found no record. |
| Migration outcome | `InProgress` until the migration ends. Then `Completed`, `Failed`, or, for a relocation, `Relocated` or `RelocationConfirmed`. `Unknown` when the CLI found no record. |
| Component | The component whose InferenceReplica holds the record: `engine`, `decoder` or `router`. |
| Migration validity | `Valid`, `Unavailable` or `Invalid`. The Reason row says why. |
| Attribution | `Exact request ID in live IR status` for a valid match. Other values say what the CLI couldn't verify. |

| Reason | Meaning |
| --- | --- |
| `MigrationMatched` | The record is valid and terminal. |
| `MigrationInProgress` | The record is valid and still in progress. |
| `MigrationNotRecorded` | The CLI found no record for the request ID, or the InferenceService was missing, deleted or replaced. |
| `InvalidMigration` | The record or the snapshot is incomplete, stale or malformed. |

`-o wide` adds Inspected IR sources and Inspected records, counts of what the CLI read, then Cancellation.

### `replicas=ready` fields {#replicas-ready-fields}

| Row | What it shows |
| --- | --- |
| Observed ready | The InferenceReplica's `status.readyReplicas` at the last check, or `<unavailable>` when the count isn't valid. |
| Count validity | `Valid`, `Unavailable` or `Invalid`. The Reason row says why. |
| Interpretation | `Exact at observed IR snapshot` for a valid match, `Last observation; currentness unverified` when valid at the timeout, otherwise `No verified exact count`. |

| Reason | Meaning |
| --- | --- |
| `ReplicaReadyMatched` | The count is valid and equals `--replicas`. |
| `ReplicaReadyNotMatched` | The count is valid and differs. |
| `ReplicaReadyNotRecorded` | The service has no InferenceReplica for the component, or the InferenceService was missing, deleted or replaced. |
| `InvalidReplicaReadyEvidence` | An InferenceReplica of the service is stale, malformed or replaced, or has a gap in its Instance indices. |

### `replicas=current` fields {#replicas-current-fields}

| Row | What it shows |
| --- | --- |
| Spec replicas | The InferenceReplica's `spec.replicas`. |
| Current replicas | Its `status.replicas`: Instances in any phase. |
| Ready replicas | Its `status.readyReplicas`. |
| Status encoding | `DenseV1` or `ColumnarV2`, the [status encoding](../../guides/omenative/change-the-status-encoding.md) of its Instance rows. |
| Validity | `Valid`, `Unavailable` when there's nothing to check yet, or `Invalid`. |
| Freshness | `Current`, `Stale` when the InferenceReplica hasn't observed its latest generation, or `Unavailable`. |
| Interpretation | `Exact desired and current count at IR snapshot` for a valid match, `Last observation; currentness unverified` when valid at the timeout, otherwise `No verified exact scale count`. |

Spec replicas, Current replicas and Ready replicas show `<unavailable>`, and Status encoding is empty, when the snapshot isn't valid.

| Reason | Meaning |
| --- | --- |
| `ReplicaScaleMatched` | The snapshot is valid, and both counts equal `--replicas`. |
| `ReplicaScaleNotMatched` | The snapshot is valid, and a count differs. |
| `ReplicaScaleNotRecorded` | Nothing to check yet: the component has no scale target or InferenceReplica, its InferenceReplica is still catching up, or the InferenceService is gone, replaced or being deleted. |
| `InvalidReplicaScaleEvidence` | The InferenceReplica isn't the one the wait is bound to, is being deleted, belongs to another InferenceService or generation, or has counts or status the CLI can't use. |
| `UnsupportedReplicaScaleTarget` | The scale target isn't an `ome.io/v1beta1` InferenceReplica. |

### `runtime-sync=acknowledged` fields {#runtime-sync-acknowledged-fields}

| Row | What it shows |
| --- | --- |
| Token state | `Acknowledged` when the annotation and the status hold your token, `Pending` when only the annotation does, or `StatusOnly`, `Superseded`, `Absent`, `Invalid` or `Unavailable`. |
| Drift state | `Clear` when there's no `RuntimeDrifted` condition, `ReportedTrue`, `ReportedFalse` or `ReportedUnknown` for its status, or `Invalid` or `Unavailable`. |
| Managed pin | `Managed` when `spec.runtime` sets `autoSync: false` with no `revision`, and the status records the pinned revision. Otherwise `NotApplicable`, `Unavailable` until the pinned revision is recorded, or `Invalid`. |
| Placement | `Direct`, or `UnsupportedPlacement` for a service that multi-cluster placement owns. |
| Validity | `Valid`, `Unavailable` or `Invalid`. |
| Interpretation | `Token acknowledged in bound parent snapshot` for a valid match, `Last observation; currentness unverified` when valid at the timeout, otherwise `No verified exact acknowledgment`. |

| Reason | Meaning |
| --- | --- |
| `RuntimeSyncObserved` | The token is acknowledged, with a managed pin and no `RuntimeDrifted` condition. |
| `RuntimeSyncNotAcknowledged` | The token isn't acknowledged, a `RuntimeDrifted` condition is present, or the pin isn't managed. |
| `RuntimeSyncNotRecorded` | The InferenceService was missing, deleted or replaced. |
| `UnsupportedPlacement` | Multi-cluster placement owns the service. |
| `InvalidCondition` | A token, the pin or a condition is invalid. |

`-o wide` adds Generation freshness, Inspected conditions, Read scope and Cancellation.

### `held-revision=unheld` fields {#held-revision-unheld-fields}

| Row | What it shows |
| --- | --- |
| InferenceReplica | The InferenceReplica's name, once the CLI has matched it to your flags and the InferenceService. Empty until then. |
| Target state | The revision's retry block: `Held`, `Backoff`, `RetryInProgress`, `Absent` when the InferenceReplica has none for it, or `Unknown` when the CLI couldn't read the blocks. |
| Mailbox | The `ome.io/release-held-revision` annotation: `Pending` when it names your revision, `Superseded` when it names another, `Absent` when it isn't set, or `Unknown`. |
| Validity | `Valid`, `Partial`, `Unavailable` or `Invalid`. The Reason row says why. |
| Interpretation | `Exact revision no longer Held on original IR` for a match, `Last observation; currentness unverified` when valid at the timeout, otherwise `No verified current unheld state`. |

| Reason | Meaning |
| --- | --- |
| `Unheld` | The revision isn't held, and the mailbox is absent. |
| `Held` | The revision is still held. |
| `MailboxPending` | The release request for your revision is still in the annotation. |
| `MailboxSuperseded` | The annotation names another revision. |
| `ReplicaMissing` | The InferenceReplica doesn't exist. |
| `ReplicaReplaced` | The InferenceReplica has another UID than `--ir-uid`. |
| `Deleting` | The InferenceReplica is being deleted. |
| `UnsupportedPlacement` | Multi-cluster placement owns the service or the InferenceReplica. |
| `SourceStale` | The InferenceReplica hasn't caught up with the latest change. |
| `SourceIncomplete` | The InferenceService changed during the check, or is missing, deleted or replaced. |
| `SourceInvalid` | The InferenceService or the InferenceReplica doesn't match the target. |
| `InvalidRetryBlocks` | The InferenceReplica's retry blocks are malformed, or too many to inspect. |
| `InvalidStatus` | The CLI can't decode the Instance status, or it's too large. |

`-o wide` adds InferenceReplica UID, Status encoding, Inspected sources / blocks, Read scope and Cancellation.

## Examples

Wait for `chat` in `prod` to be ready, allowing 30 minutes for the model to load:

```bash
kubectl ome wait chat --for=condition=Ready --timeout=30m -n prod
```

```output
FIELD                   VALUE
Service                 prod/chat
Requested               Ready=True
Outcome                 Matched
Observed Ready          True
Condition validity      Valid
Reason                  ConditionMatched
Evidence                Reported
Generation freshness    Unverifiable
Freshness caveat        Reported condition; not current-spec convergence
Condition inspection    Complete (1/1)
Source method           InitialGET
Polling fallback        false
Elapsed milliseconds    0
GET / WATCH / polls     1 / 0 / 0
Events / observations   0 / 1
```

The same wait as JSON:

```bash
kubectl ome wait chat --for=condition=Ready --timeout=30m -n prod -o json
```

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "WaitReport",
  "metadata": {
    "namespace": "prod",
    "name": "chat"
  },
  "collectedAt": "2026-09-28T14:05:12Z",
  "sources": [],
  "content": {
    "requested": "Ready=True",
    "outcome": "Matched",
    "reason": "ConditionMatched",
    "observed": {
      "status": "True",
      "validity": "Valid",
      "generationFreshness": "Unverifiable",
      "inspection": {
        "state": "Complete",
        "total": 1,
        "inspected": 1,
        "warnings": []
      }
    },
    "evidence": "Reported",
    "elapsedMilliseconds": 0,
    "counts": {
      "gets": 1,
      "watches": 0,
      "polls": 0,
      "events": 0,
      "observations": 1
    },
    "sourceMethod": "InitialGET",
    "pollingFallback": false
  },
  "warnings": []
}
```

In a script, keep the report and branch on the exit code. Here the wait timed out after 2 minutes:

```bash
kubectl ome wait chat --for=condition=Ready --timeout=2m -n prod -o json > wait.json
echo "exit code $?"
jq -r '.content.outcome + " " + .content.reason' wait.json
```

```output
error: RequestedConditionUnmet
exit code 2
TimedOut ConditionNotMatched
```

To see why, run [`kubectl ome status`](status.md) or follow [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md).

Wait for the rollout of `chat` to succeed:

```bash
kubectl ome wait chat --for=rollout=stable --timeout=2m -n prod
```

```output
FIELD                   VALUE
Service                 prod/chat
Requested               Rollout=Stable
Outcome                 Matched
Reported rollout        Succeeded
Rollout state           Unknown
Rollout validity        Valid
Reason                  RolloutMatched
Evidence                Reported
Rollout epoch           Unverifiable
Coordination Ready      NotApplicable
Freshness caveat        Reported rollout; not current-spec convergence
Attribution caveat      Same-object state; not action attribution
Rollout inspection      Complete
Expansion               JSON/YAML retain complete safe report values
Source method           InitialGET
Polling fallback        false
Elapsed milliseconds    0
GET / WATCH / polls     1 / 0 / 0
Events / observations   0 / 1
Rollout issue           EpochUnverifiable
```

Rollout state shows `Unknown`, with the issue `EpochUnverifiable`, because the CLI can't tie the reported state to the current spec.

Wait for a migration to end, with the request ID that `kubectl ome migration start` printed:

```bash
kubectl ome wait chat --for=migration=terminal --request-id=12345678-1234-4234-8234-123456789abc --timeout=30m -n prod
```

```output
FIELD                  VALUE
Service                prod/chat
Requested              Migration=Terminal
Request ID             12345678-1234-4234-8234-123456789abc
Outcome                Matched
Migration phase        Completed
Migration outcome      Completed
Component              engine
Migration validity     Valid
Reason                 MigrationMatched
Evidence               Reported
Attribution            Exact request ID in live IR status
Source method          InitialGET
Polling fallback       false
Elapsed milliseconds   0
GET / WATCH / polls    1 / 0 / 0
```

Wait for the engine of `chat` to have exactly two ready replicas, with the rows `-o wide` adds:

```bash
kubectl ome wait chat --for=replicas=ready --component=engine --replicas=2 -n prod -o wide
```

```output
FIELD                  VALUE
Service                prod/chat
Requested              Replicas=Ready
Component              engine
Target ready           2
Outcome                Matched
Observed ready         2
Count validity         Valid
Reason                 ReplicaReadyMatched
Evidence               Reported
Interpretation         Exact at observed IR snapshot
Source method          InitialGET
Polling fallback       false
Elapsed milliseconds   0
GET / WATCH / polls    1 / 0 / 0
Count authority        IR status; not /scale or an action receipt
Readiness caveat       Ready count; not serving or availability
Cancellation           Cooperative requests; plugins may ignore it
```

Wait for the engine of `chat` to ask for and report four Instances:

```bash
kubectl ome wait chat --for=replicas=current --component=engine --replicas=4 -n prod
```

```output
FIELD                  VALUE
Service                prod/chat
Requested              Replicas=Current
Component              engine
Target replicas        4
Outcome                Matched
Spec replicas          4
Current replicas       4
Ready replicas         3
Status encoding        DenseV1
Validity               Valid
Freshness              Current
Reason                 ReplicaScaleMatched
Evidence               Reported
Interpretation         Exact desired and current count at IR snapshot
Count caveat           Current = logical Instances any phase; Ready separate
Attribution            Exact IR snapshot; not action attribution
Source method          InitialGET
GET / WATCH / polls    1 / 0 / 0
Elapsed milliseconds   0
```

Wait for OME to acknowledge a runtime sync, with the request ID that `kubectl ome runtime sync` printed:

```bash
kubectl ome wait chat --for=runtime-sync=acknowledged --request-id=123e4567-e89b-42d3-a456-426614174000 -n prod
```

```output
FIELD                  VALUE
Service                prod/chat
Requested              RuntimeSync=Acknowledged
Request ID             123e4567-e89b-42d3-a456-426614174000
Outcome                Matched
Token state            Acknowledged
Drift state            Clear
Managed pin            Managed
Placement              Direct
Validity               Valid
Reason                 RuntimeSyncObserved
Evidence               Reported
Interpretation         Token acknowledged in bound parent snapshot
Sync caveat            Reported token; not live-runtime convergence
Attribution            Same-object state; not action attribution
Source method          InitialGET
Polling fallback       false
Elapsed milliseconds   0
GET / WATCH / polls    1 / 0 / 0
```

Wait for a held revision to be released, with the values from the result of `kubectl ome instance release-held`:

```bash
kubectl ome wait chat --for=held-revision=unheld \
  --component=engine --revision=chat-engine-7f9c4d2b \
  --ir-name=chat-engine --ir-uid=3f6d2c9e-8b41-4d7a-9c0e-5a1b7e2f4d68 \
  --timeout=2m -n prod
```

```output
FIELD                  VALUE
Service                prod/chat
Requested              HeldRevision=Unheld
Component              engine
Revision               chat-engine-7f9c4d2b
InferenceReplica       chat-engine
Outcome                Matched
Target state           Absent
Mailbox                Absent
Validity               Valid
Reason                 Unheld
Evidence               Reported
Interpretation         Exact revision no longer Held on original IR
Attribution            Unverifiable; not action attribution
State caveat           Pruning/re-Held races; no request receipt
Source method          InitialGET
GET / WATCH / polls    1 / 0 / 0
Elapsed milliseconds   0
```

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | A wait whose outcome is `Matched`. |
| `1` | General error | An error from [Error messages](#error-messages), such as a bad flag, a failed API request or Ctrl-C. The command prints no report. |
| `2` | Assertion unmet | A wait whose outcome is `TimedOut`, `NotFound`, `Deleted` or `Replaced`, after it prints the report. |

Errors print to stderr as `error: <message>`. After exit code `2`, the message is `error: RequestedConditionUnmet`. A second Ctrl-C stops the command at once.

### Error messages {#error-messages}

| Message | Cause |
| --- | --- |
| `InvalidWaitFlags` | An unknown flag, or a malformed value, such as `--timeout=30` with no unit. |
| `accepts 1 arg(s), received N` | No InferenceService name, or more than one. |
| `InvalidInferenceServiceName` | An invalid InferenceService name. |
| `InvalidWaitPredicate: require condition=Ready[=True\|False\|Unknown], rollout=stable\|failed\|rolled-back, migration=terminal, replicas=ready\|current, runtime-sync=acknowledged, or held-revision=unheld` | A missing or unknown `--for` predicate. |
| `InvalidMigrationRequestID: require canonical UUID only with migration=terminal` | A missing or malformed `--request-id` for `migration=terminal`, or one on a predicate that takes none. |
| `InvalidRuntimeSyncRequestID: require canonical v4 UUID with runtime-sync=acknowledged` | `runtime-sync=acknowledged` without a canonical version 4 `--request-id`. |
| `InvalidReadyReplicaFlags: require --component=engine\|decoder\|router and --replicas=N only with replicas=ready; N must be nonnegative` | A missing or invalid `--component` or `--replicas` for `replicas=ready`, or either on a predicate that takes neither. |
| `InvalidCurrentReplicaFlags: require --component=engine\|decoder\|router and positive --replicas=N; --ir-name and --ir-uid must be paired` | A missing or invalid flag for `replicas=current`, or an unpaired `--ir-name` or `--ir-uid`. |
| `InvalidHeldRevisionTarget: require --component, full --revision, --ir-name and --ir-uid only with held-revision=unheld` | A missing or invalid flag for `held-revision=unheld`, `--replicas` with it, or `--revision`, `--ir-name` or `--ir-uid` on a predicate that doesn't take it. |
| `InvalidWaitTimeout: require positive duration no greater than 24h` | `--timeout` is zero, negative or over 24 hours. |
| `InvalidOutputFormat: supported table, wide, json, yaml` | An unsupported `-o` format. |
| `WaitConfigurationUnavailable` | The CLI can't load the kubeconfig or build a client. |
| `InvalidNamespace` | An invalid namespace name. |
| `InvalidIdentity` | The InferenceService the CLI read lacks a valid name, namespace, UID or resourceVersion. |
| `AcquisitionFailed` | An API request failed, or an InferenceReplica's Instance status didn't decode. |
| `AcquisitionBudgetExceeded` | More reads than the timeout allows. |
| `EventBudgetExceeded` | Too many watch events. |
| `ConditionInspectionLimit` | Too many conditions to inspect, or, for `runtime-sync=acknowledged`, too many finalizers. |
| `RolloutInspectionLimit` | The rollout status is too large to inspect. |
| `WaitOutputFailed` | The CLI couldn't write the report. |
| `Canceled` | Ctrl-C or SIGTERM. |

## Related guides

- [Release a held revision](../../guides/roll-out-changes/release-a-held-revision.md)
- [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md)
- [Request an instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md)
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
