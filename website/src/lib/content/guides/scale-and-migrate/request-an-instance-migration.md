---
title: Request an Instance migration
description: Move one OMENative Instance to another node, with its replacement serving before the old one drains, using the alpha kubectl ome migration start command.
since: v1.3
---

A migration moves one [Instance](../../concepts/omenative/instances.md) of an [OMENative](../../concepts/omenative/overview.md) component to another node, without a dip in serving capacity. OME starts a replacement Instance elsewhere, waits until it serves, then drains the old one, so you can empty a node for maintenance. A multi-node Instance, a leader and its workers, moves as one unit. [How a migration works](../../concepts/omenative/migration-and-transient-scale.md#surge-migration) explains each step.

!!! note "Alpha"
    `kubectl ome migration start` is alpha. Its flags and behavior can change between releases.

<div class="prerequisites" markdown>

- An [InferenceService](../../concepts/serving/inference-services.md) with a component [on OMENative](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative), and an Instance of it that's `Ready` on the current revision, with no rollout or other operation in progress.
- A migration mode of `Auto`, the default, or `Surge`, as [Keep a component in place](#keep-a-component-in-place) explains.
- Room for one more whole Instance outside the node you empty, since the replacement starts before the old one drains.
- The kubectl-ome plugin, from [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md).
- Permission to read the InferenceService and what it uses, and to patch it. [Required RBAC](../../reference/kubectl-ome/guarded-actions.md#required-rbac) lists the rules.
- Migration caps in the controller's settings, which the ome-resources chart sets. On a kustomize install, add them first, as [The request stays Accepted](#the-request-stays-accepted) shows.

</div>

The examples move Instance 3 of `chat`'s engine off the node `node-a`, in the namespace `prod`. [Keep a component in place](#keep-a-component-in-place) shows `chat`, whose engine runs Instances 0 to 3. To list the Instances on a node, run `kubectl get pods -n prod -l ome.io/inferenceservice=chat --field-selector spec.nodeName=node-a -L component,ome.io/instance-index`.

## Step 1: Preview the request

A client dry run runs every check and shows the request, without sending it:

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

Check `From node`, `Migration mode` and `Current nodes`. Without `--from-node`, the CLI takes the one node that the Instance's pods run on. The result on stdout shows the message `Validated locally; no patch sent.` Each run generates a new `Request UUID`, so the real request gets a different one.

With `--dry-run server`, the API server and OME's webhook check the request too. When they accept it, the message is `API dry-run accepted; no changes persisted.`

## Step 2: Request the migration

Choose the flags for the request:

| Flag | What it does |
| --- | --- |
| `--from-node` | The node to leave. Needed when the Instance's pods span several nodes, as a leader and its workers can. |
| `--hint-node` | Up to eight nodes that the replacement should prefer, comma-separated. The scheduler can still pick any node but the source. |
| `--reason` | Why you're moving the Instance, kept in the migration record. Plain text, without URLs or credentials. |
| `--yes` | Skips the confirmation prompt. Needed when the command runs without a terminal. |

[Flags](../../reference/kubectl-ome/migration.md#start-flags) lists the rest, with their limits.

Send the request, and answer `y` at the prompt `Confirm this exact action? [y/N]`:

```bash
kubectl ome migration start chat -n prod --component engine --instance 3 --reason "node-a maintenance"
```

The command prints the preview to stderr, then the result to stdout:

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

`applied` means that the API server stored the request on the InferenceService. Nothing has moved yet. Keep the `request-id`, which you need to wait for the migration.

## Step 3: Follow the request

Check the request with `migration status`:

```bash
kubectl ome migration status chat -n prod --component engine
```

Once OME has started the replacement, which it calls the surge, the output is:

```output
SUBJECT/COMP      STATUS                        DETAIL
7c9e6679/engine   SurgePending/Active/Current   MSG: surge allocated; w...
```

SUBJECT/COMP shows the start of the request ID, and the component. STATUS shows the phase, whether the migration is `Active` or `Terminal`, and `Current` when the InferenceReplica's status has caught up with its spec. DETAIL shows the start of OME's message, here `surge allocated; waiting for surge pods`, which `-o yaml` prints in full.

The request appears once OME accepts it, in the phase `Accepted`. It then moves through `SurgePending`, `SurgeReady` and `Draining` to `Completed`, or ends in `Failed`.

To block until the migration ends, run `kubectl ome wait chat -n prod --for=migration=terminal --request-id=7c9e6679-7425-40de-944b-e07fc1f90ae7 --timeout=30m`. Its `--timeout` should cover the migration's [deadline](../../concepts/omenative/migration-and-transient-scale.md#deadline), 30 minutes by default: without it, the command stops after 60 seconds. It exits `0` when the migration ends, even when it fails, so check the outcome:

```bash
kubectl ome migration status chat -n prod --component engine
```

```output
SUBJECT/COMP      STATUS                       DETAIL
7c9e6679/engine   Completed/Terminal/Current   MSG: migrated to instan...
```

The full message is `migrated to instance=4`: Instance 4, the replacement, serves on another node, and Instance 3 is gone. The record stays in `status` for the caps' `window`, one hour with the chart. For older migrations, use `history`:

```bash
kubectl ome migration history chat -n prod
```

```output
EVID    REQUEST/COMP   PHASE/STATE   WHEN           DETAIL
AUTH    7c9e6679/E     Completed/T   09-28T10:31Z   migrated to insta...
AUDIT   7c9e6679/E     Completed/T   09-28T10:31Z   migrated
AUDIT   3f2a9c1e/E     Failed/T      09-28T09:20Z   deadline exceeded...
```

EVID says where each row comes from: `AUTH` for the InferenceReplica's status, and `AUDIT` for the audit ConfigMap `chat-ome-migration-audit`, which keeps the last 200 migrations that ended. The earlier request `3f2a9c1e` failed when its deadline passed. [Output fields](../../reference/kubectl-ome/migration.md#history-output-fields) explains the other columns.

## Keep a component in place

Each component has its own migration mode, `lifecycle.migrationPolicy.mode`: `Auto`, the default, `Surge`, which acts the same, or `Never`. `chat` serves a Llama model in prefill-decode mode. Its engine runs prefill in four Instances that can move, and its decoder stays where it is:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  runtime:
    name: srt-llama-3-2-1b-instruct-pd
  engine:
    minReplicas: 4
    maxReplicas: 4
    lifecycle:
      migrationPolicy:
        mode: Auto
  decoder:
    minReplicas: 1
    maxReplicas: 1
    lifecycle:
      migrationPolicy:
        mode: Never
  router:
    minReplicas: 1
    maxReplicas: 1
```

The runtime needs GPU nodes with RDMA networking, as [PD mode](../../reference/operate-ome/ome-serving-values.md#pd-mode) explains. With `Never`, `migration start` refuses a request for the decoder, and OME fails one written another way. `Never` also turns off [automatic relocation](../../concepts/omenative/migration-and-transient-scale.md#automatic-relocation). You can change the mode on a running component, and OME applies it without a rollout.

## Troubleshooting

### `migration start` refuses the request

A refusal prints `error:` and a message, changes nothing, and exits `1`, or `3` for a conflict. The messages you're most likely to see:

| Message | What to do |
| --- | --- |
| `required Kubernetes API request failed; check access and connectivity` | A read failed, often for lack of a permission in [Before you begin](#before-you-begin). |
| `action refused: active runtime is unavailable, inconsistent or unbound` | The service needs a component [on OMENative](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative) and a serving runtime that the CLI can resolve. |
| `migration precondition conflicts or is stale; inspect migration status and retry explicitly` | The state of the service blocks the move now: see the causes below. |
| `action not confirmed; noninteractive input requires --yes` | Answer `y` at the prompt, or pass `--yes` when the command runs without a terminal. |
| `context deadline exceeded` | The command has 45 seconds in all, prompt included. Answer sooner, or pass `--yes`. |
| `guarded migration rejected; inspect migration status using preview UUID` | The InferenceService changed after the CLI read it, often through a status update. Run the command again. |

The usual causes of the precondition conflict:

- The InferenceReplica's status lags a recent change. Try again in a moment.
- The service is paused. [Pause and resume a rollout](../roll-out-changes/pause-and-resume-a-rollout.md) shows how to resume.
- The service has the `ome.io/rollout-promote` or `ome.io/rollout-rollback` annotation. The rollback annotation stays until a different target revision appears, as [`promote` and `rollback`](../../reference/kubectl-ome/rollout.md#promote-and-rollback) describes.
- The Instance isn't `Ready`, has an operation in progress, or already has a request or a migration. [`kubectl ome instance list`](../../reference/kubectl-ome/instance.md) shows its state.
- The component's migration mode is `Never`.
- The Instance's pods run on more than one node, and you didn't pass `--from-node`.

[Refusals](../../reference/kubectl-ome/migration.md#start-refusals) lists the messages and their causes.

### The outcome is unknown

`migration request failed or outcome unknown; inspect migration status using the preview UUID:`, followed by a cause, means that the patch failed. The API server may have refused it, for example for lack of permission, or stored it despite a server error, a timeout or a network error. Before you send a new request, look up the first one with the same flags and the `Request UUID` from its preview:

```bash
kubectl ome migration start chat -n prod --component engine --instance 3 --reason "node-a maintenance" --request-id 7c9e6679-7425-40de-944b-e07fc1f90ae7
```

When the request is still on the InferenceService, waiting for OME, the command sends nothing, exits `0` and prints:

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

Don't send it again: follow it with `migration status`. When the lookup exits `3` instead, OME has taken the request, or it never arrived. A row in `migration status` that starts with `7c9e6679` means that OME took it. Otherwise, send a new request without `--request-id`.

### The request stays Accepted

When `status` keeps showing the request as `Accepted` with this message:

```bash
kubectl ome migration status chat -n prod --component engine
```

```output
SUBJECT/COMP      STATUS                    DETAIL
7c9e6679/engine   Accepted/Active/Current   MSG: waiting for migrat...
```

The full message is `waiting for migration capacity policy (no lifecycle.audit configured)`. The controller's migration caps are missing or invalid, so it holds every request and records a `MigrationPolicyUnconfigured` warning event.

The ome-resources chart sets the caps, and the kustomize manifests in `config/` leave them out. With the chart, set them in your values, then upgrade the release:

```yaml
ome:
  controller:
    lifecycle:
      audit:
        maxInFlightMigrations: 3
        maxMigrationsPerWindow: 10
        window: 1h
```

All three fields are required and must be greater than zero. Without the chart, add the same `audit` block to the JSON under the `lifecycle` key of the `inferenceservice-config` ConfigMap in the OME namespace, beside `instanceReadyTimeout`. Add the chart's `requeue` block there too, `"requeue": {"operation": "5s", "gate": "3s"}`. Without it, OME can take many minutes to retry a held request. OME picks up the change without a restart, and a request still held at its deadline fails with `deadline exceeded in phase Accepted: surge never allocated`.

Without that message, the request is waiting its turn, for one of the causes in [When a request waits or fails](../../concepts/omenative/migration-and-transient-scale.md#what-refuses-a-migration-request). Pausing the service holds its migrations, but their deadlines keep running, so one whose deadline passes fails when you resume.

### The migration fails

When `status` shows `Failed`, run it with `-o yaml` to read the full message. OME also records a warning event on the InferenceService, which `kubectl get events -n prod --field-selector involvedObject.name=chat` lists. The usual messages are:

| Message | Event | What happened |
| --- | --- | --- |
| `migration rate cap reached (10/10 in the last 1h0m0s)` | `RateLimited` | `maxMigrationsPerWindow` migrations started within `window`. Wait for the oldest to age out of the window, then send the request again. |
| `deadline exceeded in phase SurgePending: surge pods never became ready` | `MigrationExpired` | The migration missed its deadline while waiting for what the message names, here the replacement's pods. Check them with `kubectl describe pod`. |
| `deadline exceeded in phase Accepted: surge never allocated`, right after the request | `MigrationExpired` | No Instance readiness timeout is set, or the controller's `lifecycle` settings are invalid. Set a timeout, as [Set Instance readiness deadlines](../omenative/set-instance-readiness-deadlines.md) shows. This is a known bug. |
| `surge pod <pod> wedged in <reason> past the stuck-pod grace` | `MigrationSurgeWedged` | A replacement pod is stuck in `ImagePullBackOff`, `CrashLoopBackOff` or a similar state. |
| `request.FromNode=node-a does not match observed source node=<node>` | `MigrationFromNodeMismatch` | The source Instance moved to another node before the migration started. |
| `migrations disabled by MigrationPolicy Mode=Never` | `MigrationRequestRejected` | The component's migration mode is `Never`. |

When a migration fails after its replacement started, OME removes the replacement and keeps the source Instance. [Watch migrations](../../concepts/omenative/migration-and-transient-scale.md#watch-migrations) lists every migration event.

## Next steps

- [kubectl ome migration](../../reference/kubectl-ome/migration.md): the flags, refusals and output fields.
- [kubectl ome wait](../../reference/kubectl-ome/wait.md): what `wait` can block on, and its exit codes.
- [Migration and transient scale](../../concepts/omenative/migration-and-transient-scale.md): deadlines, caps and automatic relocation.
- [Set Instance readiness deadlines](../omenative/set-instance-readiness-deadlines.md): the timeout behind each migration's deadline.
- [Let Alfred migrate Instances](../scheduling/let-alfred-migrate-instances.md): Alfred, OME's alpha GPU cluster caretaker, recommends Instance moves when free GPUs fragment or a node turns unhealthy and, when you let it, asks OMENative to carry them out.
- [Request a transient scale](request-a-transient-scale.md): send a transient replica request to an OMENative component.
