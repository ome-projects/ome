---
title: kubectl ome migration
description: See an InferenceService's running and past OMENative migrations, and request one with the alpha start action.
since: v1.3
---

`kubectl ome migration` shows the migrations of an [InferenceService](../../concepts/serving/inference-services.md) whose components run on [OMENative](../../concepts/omenative/overview.md), and its alpha `start` action asks OME to move an [Instance](../../concepts/omenative/instances.md) to another node. OME starts a replacement Instance elsewhere, waits until it's ready and serving, then drains and removes the old one, so the component keeps its serving capacity. A multi-node Instance, a leader and its workers, moves as one unit. [Migration and transient scale](../../concepts/omenative/migration-and-transient-scale.md) explains how migrations work, and [Deployment modes](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative) shows how to opt in to OMENative.

```text
kubectl ome migration SUBCOMMAND INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`status`](#status) | Shows the migrations that are running or recently ended. |
| [`history`](#history) | Adds older migrations from the service's audit ConfigMap. |
| [`start`](#start) | Alpha. Asks OME to move one Instance to another node. |

Each subcommand takes the name of one InferenceService. `status` and `history` print a table, or the whole report with `-o json` or `-o yaml`, as [Output formats](overview.md#output-formats) describes. [Required RBAC](overview.md#required-rbac) lists the permissions each command needs, and `start` also needs the patch rule in [Guarded actions](guarded-actions.md#required-rbac).

[Alfred](../../concepts/scheduling/alfred.md), OME's alpha GPU cluster caretaker, recommends Instance moves when free GPUs fragment or a node turns unhealthy and, when you [let it](../../guides/scheduling/let-alfred-migrate-instances.md), asks OMENative to carry them out. It sends the same request as `start`, so `status` and `history` show its migrations too. While any migration request waits or runs in the cluster, yours included, Alfred sends none of its own. [Global limits](../../concepts/scheduling/alfred-policies.md#global-limits) lists its other limits.

## `status`

```text
kubectl ome migration status INFERENCESERVICE [flags]
```

`status` shows the migrations recorded in the InferenceReplicas' `status.migrations`: those running, and those that ended within the audit window. The ome-resources chart sets that window to one hour, in `ome.controller.lifecycle.audit.window`. A request shows up once OME accepts it. For older migrations, use [`history`](#history).

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | None | Filter by component: engine, decoder, or router |
| `-o`, `--output` | `table` | Output format: table, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#status-output-fields}

The table has a row for each record's message and one for each issue, with the issue's code in DETAIL:

| Column | What it shows |
| --- | --- |
| SUBJECT/COMP | The request ID's first eight characters, then the component. An issue row can show `REPORT` or an InferenceReplica. A name over eight characters prints as `~` and a hash. |
| STATUS | The phase, the classification and the freshness, such as `SurgePending/Active/Current`. An issue row of the report shows the report's state and freshness instead. |
| DETAIL | `MSG:` and the start of OME's message, or an issue code. `-o json` and `-o yaml` print the message in full. |

The report's state is `Partial` when there's any issue, otherwise `Empty` when there are no records, and `Reported` when there are.

The phase says where the migration is. [How a migration works](../../concepts/omenative/migration-and-transient-scale.md#surge-migration) describes each step.

| Phase | Meaning |
| --- | --- |
| `Accepted` | OME has taken the request, which waits to start. |
| `SurgePending` | The replacement Instance has its index, and OME waits for its pods. |
| `SurgeReady` | The replacement is ready and serving. |
| `Draining` | OME drains the old Instance. |
| `Completed` | The replacement serves, and the old Instance is gone. |
| `Failed` | The migration ended without completing. The message says why. |
| `Relocated` | OME rebuilt a failed Instance on another node on its own. A request never ends here. |

The classification is `Active` while the migration runs, `Terminal` once it has ended, and `Invalid` for a malformed record, whose issue codes say what's wrong.

A request that stays in `Accepted` is waiting: [The request stays Accepted](../../guides/scale-and-migrate/request-an-instance-migration.md#the-request-stays-accepted) explains why, and what to do.

The freshness says whether the InferenceReplica's status has caught up with its spec:

| Freshness | Meaning |
| --- | --- |
| `Current` | The status is up to date. |
| `Stale` | OME hasn't caught up with a recent change. The records show, each with the issue `SourceGenerationStale`. |
| `Unobserved` | OME hasn't reported on the InferenceReplica yet, so its records stay hidden. |
| `Invalid` | The InferenceReplica is malformed or doesn't belong to the service. Its issue codes say how. |

`-o json` and `-o yaml` print a `MigrationStatusReport`:

| Field | What it holds |
| --- | --- |
| `sources` | The InferenceService and each InferenceReplica the CLI read, with its generation and freshness. |
| `content.summary` | The report's state, the number of records, and how many are active, terminal and invalid. |
| `content.migrations` | Each record in full: the request ID, the component, the source and replacement Instances, the trigger, the phase, the nodes, the times, the message and the outcome. |
| `content.issues` | Each issue, with the InferenceReplica, request and component it concerns. |
| `warnings` | `PartialData` for a `Partial` report, and `Truncated` when the CLI stopped at its limits. |

In a record, the trigger is `Manual` for a request and `Auto` for a relocation. Its `startedAt` is when OME accepted the request, `allocatedAt` when the replacement got its index, `deadline` when the migration fails if it hasn't completed, and `completedAt` when it ended. [Deadline and caps](../../concepts/omenative/migration-and-transient-scale.md#deadline) explains the deadline. The outcome is `InProgress` while the migration runs, then `Completed`, `Failed`, `Relocated`, or `RelocationConfirmed` for a relocation OME marked as succeeded, and `Unknown` for an invalid record. The report doesn't print the request's reason.

The issue codes are:

| Code | Meaning |
| --- | --- |
| `SourceGenerationStale`, `SourceGenerationUnobserved`, `SourceGenerationInvalid` | An InferenceReplica's freshness is `Stale`, `Unobserved` or `Invalid`. |
| `SourceIdentityInvalid`, `SourceLabelMismatch`, `SourceParentMismatch`, `SourceOwnerMismatch`, `SourceComponentInvalid`, `SourceComponentDuplicate` | An InferenceReplica is malformed or doesn't belong to the service, or two claim the same component. |
| `RequestIDInvalid`, `RequestIDDuplicate`, `TriggerInvalid`, `PhaseInvalid`, `TriggerPhaseConflict`, `SourceIndexInvalid`, `SurgeIndexInvalid`, `AllocationInvalid`, `AttemptInvalid`, `TimestampInvalid`, `TerminalShapeInvalid`, `SucceededInvalid`, `NodeHintInvalid` | A record has a missing or invalid field, a field that doesn't fit its trigger or phase, or another record's request ID. |
| `NodeHintsTruncated`, `RecordsTruncated`, `SourcesTruncated` | A record or the report holds more than the CLI shows. |

### Examples {#status-examples}

Show the migrations of `chat` in `prod`, while OME waits for a replacement's pods:

```bash
kubectl ome migration status chat -n prod
```

```output
SUBJECT/COMP      STATUS                        DETAIL
7c9e6679/engine   SurgePending/Active/Current   MSG: surge allocated; w...
```

The engine's request `7c9e6679` has its replacement, and OME's message is `surge allocated; waiting for surge pods`. Once the migration completes, the same command shows:

```output
SUBJECT/COMP      STATUS                       DETAIL
7c9e6679/engine   Completed/Terminal/Current   MSG: migrated to instan...
```

The full message is `migrated to instance=4`: Instance 4 serves in place of the Instance that moved. For a service with no migration records, the command shows:

```output
SUBJECT/COMP   STATUS          DETAIL
-/-            Empty/Current   -
```

## `history`

```text
kubectl ome migration history INFERENCESERVICE [flags]
```

`history` shows the migration records an InferenceService keeps, from three sources, and marks each record with its source:

| Evidence | EVID | Source | What it holds |
| --- | --- | --- | --- |
| `Authoritative` | `AUTH` | The InferenceReplicas' `status.migrations` | The records [`status`](#status) shows. |
| `ParentSummary` | `PARENT` | The InferenceService's `status.migrationHistory` | Empty in this release. |
| `AuditHistory` | `AUDIT` | The key `history.json` of the ConfigMap `<name>-ome-migration-audit` | An entry for each request OME accepted or rejected, and for each relocation, until the rebuilt Instance is `Ready`. It keeps the last 200 migrations that ended. |

Only `AUTH` records are authoritative. An `AUDIT` entry that's still `Started` doesn't show on its own that the migration runs.

### Flags {#history-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | None | Filter by component: engine, decoder, or router |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#history-output-fields}

The table has a row for each record, and a row for each issue:

| Column | What it shows |
| --- | --- |
| EVID | `AUTH`, `PARENT` or `AUDIT`. An issue row shows the source it concerns, or `REPORT`. |
| REQUEST/COMP | The first eight characters of the request ID, then the component's initial: `E`, `D` or `R`. |
| PHASE/STATE | The phase, then `A` for active, `T` for terminal or `!` for an invalid record. An issue row shows the report's state. |
| WHEN | The record's latest time, in UTC, as `MM-DDTHH:MMZ`. |
| DETAIL | The record's first issue code, or the start of its message. An issue row shows its code. |

The phase is the one the source records: a [`status`](#status) phase for `AUTH`, and `Started`, `Completed` or `Failed` for `AUDIT`. An `AUDIT` record's message is its outcome, such as `migrated`. The report's state is `Partial` when there's any issue or invalid record, otherwise `Reported` when there are records, and `Empty` when there are none.

`-o wide` prints one row per record, with every field in full. It also adds a row for each issue, and one for each source without records. When that source is unavailable, the row's ISSUES cell gives the reason, such as `NotFound` or `Forbidden`. A missing audit ConfigMap, as on a service that never had a migration, shows `NotFound`. It isn't an issue, and the report can still be `Reported`.

`-o json` and `-o yaml` print a `MigrationHistoryReport` with the three sources, a summary, the records and the issues.

The issue codes are:

| Code | Meaning |
| --- | --- |
| `AuthoritativeUnavailable`, `AuditUnavailable` | The CLI couldn't read the InferenceReplicas or the audit ConfigMap, often for lack of permission. |
| `AuditIdentityInvalid`, `AuditMalformed`, `AuditPayloadTooLarge` | The audit ConfigMap isn't owned by the service, fails to parse, or is too large to read. |
| `RecordInvalid`, `RequestIDInvalid`, `ComponentUnavailable`, `ComponentInvalid`, `InstanceInvalid`, `ReplacementInvalid`, `ModeInvalid`, `PhaseInvalid`, `TimestampInvalid`, `EventInvalid`, `NodeInvalid`, `DuplicateWithinSource` | A record lacks a field, has one the CLI can't use, or appears twice in one source. |
| `CrossSourceIdentityConflict`, `ChronologyConflict`, `TerminalOutcomeConflict` | Two sources disagree about the same request. |
| `AuthoritativeTruncated`, `ParentHistoryTruncated`, `AuditHistoryTruncated`, `OutputTruncated`, `EventsTruncated`, `NodeHintsTruncated` | A source, a record or the report holds more than the CLI shows. |

### Examples {#history-examples}

Show the migration history of `chat` in `prod`:

```bash
kubectl ome migration history chat -n prod
```

```output
EVID    REQUEST/COMP   PHASE/STATE   WHEN           DETAIL
AUTH    7c9e6679/E     Completed/T   09-28T10:31Z   migrated to insta...
AUDIT   7c9e6679/E     Completed/T   09-28T10:31Z   migrated
AUDIT   3f2a9c1e/E     Failed/T      09-28T09:20Z   deadline exceeded...
```

The engine's request `7c9e6679` completed at 10:31 UTC. The InferenceReplica still holds its record, and the audit ConfigMap has its entry. The earlier request `3f2a9c1e` failed at 09:20, when its deadline passed before the replacement's pods became ready. Its record has left the InferenceReplica's status, so only the audit ConfigMap still has it.

## `start`

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

```text
kubectl ome migration start INFERENCESERVICE --component COMPONENT --instance INDEX [flags]
```

`start` asks OME to migrate one Instance of an OMENative component. It adds the request to the InferenceService as an annotation, `ome.io/migration-request-v1-` followed by a new request ID, and changes nothing else. OME records the request in the InferenceReplica's status, removes the annotation and runs the migration. [Migration requests](../../concepts/omenative/migration-and-transient-scale.md#migration-requests) describes the annotation, and [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md) follows a request from start to end.

The Instance must be `Ready` on the component's current revision, with no operation in progress. To find its index, run [`kubectl ome instance list`](instance.md#list), or list the node's pods as [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md) shows. The CLI takes the node the Instance's pods run on as the source. When they span several nodes, as a leader and its workers can, name one with `--from-node`. The replacement prefers the `--hint-node` nodes, all equally. It can still land elsewhere, but never on the source node. Since it starts before the old Instance drains, the cluster needs room for one more whole Instance off the source node, or the migration fails at its [deadline](../../concepts/omenative/migration-and-transient-scale.md#deadline).

`start` is one of the [guarded actions](guarded-actions.md#guarded-commands), and follows their contract. To follow the request, run [`status`](#status), or block until it ends with [`kubectl ome wait`](wait.md) and the request ID.

### Flags {#start-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | None | Component: engine, decoder or router (required) |
| `--dry-run` | `none` | Dry-run: none, client or server |
| `--from-node` | None | Source node hosting a current Pod; required for a multi-node gang |
| `--hint-node` | None | Ordered soft target-node preferences (maximum eight) |
| `--instance` | None | Canonical instance index (required) |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output: table, wide (bounded), json or yaml |
| `--reason` | None | Bounded non-secret advisory reason |
| `--request-id` | None | Lookup-only canonical UUID; never regenerated or replayed |
| `--requested-by` | `kubectl-ome` | Bounded advisory tool/operator label, not authenticated identity |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

The values have these limits:

- `--instance` is a whole number without a sign or leading zeros.
- `--from-node` is a node name that hosts one of the Instance's pods.
- `--hint-node` takes up to eight node names, comma-separated or in repeated flags, each once, and not the source node.
- `--reason` is printable text up to 256 bytes, without `://` or anything that looks like a credential or token.
- `--requested-by` is up to 128 letters, digits and `_.:/@+-` characters. It's a label, not your identity.
- `--request-id` is a UUID in canonical form. It looks up a request you already sent, and never sets the ID of a new one.

### How `start` runs {#how-start-runs}

`start` runs the [shared steps](guarded-actions.md#the-shared-sequence) of a guarded action:

1. The CLI checks the flags, reads what the migration needs, and refuses if the migration isn't safe now, as [Refusals](#start-refusals) lists.
2. It generates the request ID and prints a preview of the request to stderr.
3. It asks `Confirm this exact action? [y/N]` on stderr, and goes ahead only on `y` or `yes`. `--yes` confirms without the prompt, and scripts need it.
4. It reads the InferenceReplica, the Instance's pods and its ControllerRevision again, and exits `3` if any of them changed.
5. It sends a JSON Patch that tests the InferenceService's UID and resourceVersion from the preview, then adds the annotation. If the service changed since the read, the patch fails and the CLI exits `3`.
6. It prints the result, an ActionResult, to stdout.

With `--dry-run client`, the CLI runs every step but the patch. With `--dry-run server`, it sends the patch with `dryRun=All`, so the API server and OME's webhook check the request and store nothing. Each run generates a new request ID, so a dry run's ID never becomes the real request's. Guarded actions describes [the prompt](guarded-actions.md#the-confirmation-prompt), [the dry-run modes](guarded-actions.md#dry-run-modes) and [the time limit](guarded-actions.md#timeouts-and-response-bounds).

When the patch fails with anything but a conflict, the command exits `1`. The error says `outcome unknown`, as in `migration request failed or outcome unknown; inspect migration status using the preview UUID:` followed by the cause. After a server error, a timeout or a network error, the API server may have stored the request anyway, as [When the outcome is unknown](guarded-actions.md#when-the-outcome-is-unknown) explains. The CLI never retries.

Before you send a new request, look the first one up. Run `start` again with the same flags, the hints in the same order, and `--request-id` set to the `Request UUID` from its preview. If the request is still on the service with the same values, the lookup sends nothing and exits `0`. Otherwise it refuses with the precondition conflict and exits `3`: OME has taken the request, it never arrived, or your flags differ. [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md#the-outcome-is-unknown) shows how to tell which.

The preview shows the target and the request, then the component's InferenceReplica and migration mode, and the Instance's running revision and current nodes, as in the first [example](#start-examples). Check `From node` and `Current nodes` before you confirm.

The preview ends with warnings: node hints are only preferences, and the patch guards only the InferenceService. These can join them:

| Warning | When |
| --- | --- |
| `Other migration requests are queued; dispatch is serial and capacity is not guaranteed.` | Another request waits on the service. |
| `Other migration work is queued or executing; dispatch is serial and capacity is not guaranteed.` | Another migration of the component hasn't ended. |
| `Audit lookup unavailable; retained delivery history is incomplete.` | The CLI can't read the audit ConfigMap. The command goes on. |

The result on stdout is an [ActionResult](guarded-actions.md#the-actionresult), whose `request-id` is the ID that [`kubectl ome wait`](wait.md) takes. `applied` means the API server stored the request, not that OME took it or moved anything. The `follow-up` command shows what OME did. The table cuts long values, so use `-o json` or `-o yaml` for the full message and follow-up command.

### Refusals {#start-refusals}

A refusal changes nothing. These come before the command reads the cluster, and exit `1`:

| Message | Cause |
| --- | --- |
| `instance must be a canonical integer from 0 through 2147483647` | `--instance` is missing, negative, or has a sign or leading zeros. |
| `invalid migration request values; use --help` | The component isn't `engine`, `decoder` or `router`, or another value breaks its limits in [Flags](#start-flags). |
| `invalid migration node hints` | A hint isn't a valid node name, repeats, or names the `--from-node` node. |
| `invalid inference service name`, `action namespaces are invalid` | The InferenceService name, `-n` or `--ome-namespace` isn't a valid name. |
| `resolve workload namespace failed`, `selected context is unavailable or unsafe` | The CLI couldn't read the namespace or context from your kubeconfig, or the context's name is unsafe. |

These come after it reads the cluster, and exit `1`:

| Message | Cause |
| --- | --- |
| `required Kubernetes API request failed; check access and connectivity` | A read failed, often for lack of permission. Without `--request-id`, a failed read of the audit ConfigMap only adds a warning. |
| `action refused: active runtime is unavailable, inconsistent or unbound` | No component uses OMENative, the CLI failed to resolve the active serving runtime, or the runtime differs from the one `spec.runtime.name` names. |
| `retained migration mailbox identity is invalid`, `invalid retained migration payload`, `source revision payload is invalid or exceeds bounds`, and similar | A request annotation on the service, or the ControllerRevision the Instance runs, is malformed or too large to check. |
| An error that starts with `audit lookup` | With `--request-id`, the audit ConfigMap is malformed, too large to check, or not owned by the service. |

`start` also refuses a target that's being deleted, has an unsafe identity or is too large to check safely. It refuses one that takes part in multi-cluster placement too, which is alpha, still in development, and off by default. [Target checks](guarded-actions.md#target-checks) lists these messages.

`migration precondition conflicts or is stale; inspect migration status and retry explicitly` means the state of the service doesn't allow the migration now, and the command exits `3`. The causes, the most likely first:

- The service is paused with `ome.io/rollout-paused`, or its InferenceReplica is paused. See [`kubectl ome rollout`](rollout.md#pause-and-resume).
- The service has the `ome.io/rollout-promote` or `ome.io/rollout-rollback` annotation. The rollback annotation stays after a rollback until a different target revision appears, as [`promote` and `rollback`](rollout.md#promote-and-rollback) describes.
- The Instance doesn't exist, isn't `Ready`, has an operation in progress, or a rollout is moving it to a new revision. [`kubectl ome instance`](instance.md) shows its state.
- The Instance already has a request waiting, or is the source or the replacement in a migration that hasn't ended.
- The component's `lifecycle.migrationPolicy.mode` is `Never`.
- The Instance's pods run on more than one node and you didn't pass `--from-node`, or the `--from-node` node hosts none of them.
- The InferenceReplicas' status lags a recent change.
- The component lacks an InferenceReplica, or another component uses OMENative and this one doesn't.
- A pod of the Instance is missing, unscheduled or not on the running revision.
- The source's node affinity requires the source node, so no replacement could run anywhere else.
- A hint names the node the CLI took as the source.
- With `--request-id`, the request's annotation isn't on the service, or its values differ from your flags.
- The InferenceReplica, the Instance's pods or its ControllerRevision changed after the CLI first read them.

`guarded migration rejected; inspect migration status using preview UUID` means the InferenceService changed after the CLI read it, often through a status update. Nothing changed, and the command exits `3`. Run it again: it reads the service again, and generates a new request ID.

### Examples {#start-examples}

Run every check and see the request, without sending it:

```bash
kubectl ome migration start chat -n prod --component engine --instance 3 --reason "node-a maintenance" --dry-run client --yes
```

The preview, on stderr:

```output
ALPHA guarded migration preview (not controller convergence)
FIELD              VALUE
Action             migration start
Context            prod-us-east
Workload NS        prod
OME NS             ome
Target             InferenceService/chat
UID                5f0c8a52-3b1e-4c8d-9f27-6a1d0e4b7c93
ResourceVersion    48213
Dry-run            client
Request UUID       6ff01fa3-2a47-4725-9c85-786ed59ac166
Schema             v1
Component          engine
Instance           3
From node          node-a
Hint nodes         <absent>
Reason             "node-a maintenance"
Requested at       2026-09-28T10:12:04Z
Requested by       kubectl-ome
Lookup only        false
Source IR          chat-engine
IR UID             0b6e2f41-9d3c-4a57-8e12-c4f5a6b7d890
IR version         48190
Migration mode     Auto
Running revision   chat-engine-5d8f7c9b
Incarnation        1
Current nodes      node-a
Node hints are soft preferences; capacity, scheduling and controller convergence
are not guaranteed.
Parent UID/resourceVersion CAS is not an IR/Pod/runtime transaction.
```

The result, on stdout:

```output
FIELD           VALUE
action          migration start
target          InferenceService/prod/chat
dry-run         client
accepted        No
applied         No
request-id      6ff01fa3-2a47-4725-9c85-786ed59ac166
revision-hash   -
message         Validated locally; no patch sent.
follow-up       kubectl ome migration status chat --component=engine ...
hint            Use -o json or -o yaml for full values.
```

Instance 3's pods run only on `node-a`, so without `--from-node` the CLI takes `node-a` as the source. Nothing changed.

Move Instance 3 off `node-a`, preferring two nodes for the replacement, and confirm at the prompt:

```bash
kubectl ome migration start chat -n prod --component engine --instance 3 --hint-node node-c,node-d --reason "node-a maintenance"
```

The preview shows `Hint nodes` as `node-c, node-d`. After you answer `y`, the result, on stdout:

```output
FIELD           VALUE
action          migration start
target          InferenceService/prod/chat
dry-run         none
accepted        Yes
applied         Yes
request-id      7c9e6679-7425-40de-944b-e07fc1f90ae7
revision-hash   -
message         API accepted annotation request; delivery and converg...
follow-up       kubectl ome migration status chat --component=engine ...
hint            Use -o json or -o yaml for full values.
```

Keep the `request-id`: OME tracks the request by it.

Look up a request whose outcome is unknown, with the flags you sent it with and the `Request UUID` from its preview:

```bash
kubectl ome migration start chat -n prod --component engine --instance 3 --hint-node node-c,node-d --reason "node-a maintenance" --request-id 7c9e6679-7425-40de-944b-e07fc1f90ae7
```

The preview shows `Lookup only` as `true`, and ends at `IR version`. The result, on stdout:

```output
FIELD           VALUE
action          migration start
target          InferenceService/prod/chat
dry-run         none
accepted        No
applied         No
request-id      7c9e6679-7425-40de-944b-e07fc1f90ae7
revision-hash   -
message         Identical retained mailbox observed; no replay sent; ...
follow-up       kubectl ome migration status chat --component=engine ...
hint            Use -o json or -o yaml for full values.
```

The request is still on the service, waiting for OME, so the command sent nothing and exited `0`. Don't send it again: run [`status`](#status) until the request shows up.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every subcommand that finishes, including a `start` lookup that finds the request on the service. |
| `1` | General error | Every subcommand, for an invalid name or flag or a failed read. For `start`, also most refusals, a declined prompt, Ctrl-C, a timeout and an unknown outcome. |
| `3` | Mutation conflict | `start`, when the state of the service doesn't allow the migration, a lookup doesn't find the request, or the service, its InferenceReplica or the Instance changed during the command. |

Errors print to stderr as `error: <message>`. [Exit codes](guarded-actions.md#exit-codes) in Guarded actions says what to do after each.

## Related guides

- [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md)
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
