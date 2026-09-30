---
title: kubectl ome instance
description: List the Instances of an InferenceService's OMENative components, inspect one Instance, show a component's retry blocks, and release a held revision (alpha).
since: v1.3
---

`kubectl ome instance` shows the [Instances](../../concepts/omenative/instances.md) of an [InferenceService](../../concepts/serving/inference-services.md)'s [OMENative](../../concepts/architecture/deployment-modes.md#omenative) components. An Instance is one replica of a component: one pod, or a leader pod and its workers, which OME updates, repairs and reports as one unit. Use `list` to find an Instance that isn't `Ready`, `status` to see its pods and their warnings, and `retry-blocks` to see which revisions OME is retrying or holding, and why. Once you've fixed the cause, the alpha `release-held` lets OME try a held revision again.

OME reports the Instances in the status of each component's InferenceReplica, named `<InferenceService>-<component>`, such as `chat-engine`. Only components with an InferenceReplica appear: [Opt in to OMENative](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative) shows how a component gets the mode.

```text
kubectl ome instance SUBCOMMAND INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`list`](#list) | Lists the Instances of every OMENative component. |
| [`status`](#status) | Shows one Instance, with its pods and their Warning events. |
| [`retry-blocks`](#retry-blocks) | Shows the retry blocks of one component. |
| [`release-held`](#release-held) | Alpha. Asks OME to release one held revision. |

`status`, `retry-blocks` and `release-held` act on one component, which `--component` names: `engine`, `decoder` or `router`. `-o json` and `-o yaml` print the whole report, as an `InstanceListReport`, `InstanceStatusReport`, `InstanceRetryBlocksReport` or `ActionResult`. Only `status` has a wide view: `release-held` treats `-o wide` as `table`. The overview covers the other [output formats](overview.md#output-formats) and the [permissions](overview.md#required-rbac) each command needs.

`list` and `status` say how current the status they show is, with an evidence level:

| Evidence | Meaning |
| --- | --- |
| `Reported` | The status is current. |
| `Stale` | OME hasn't caught up with the latest change, so the rows can be out of date. |
| `Malformed` | The status contradicts itself or holds invalid values. |
| `Unavailable` | The CLI has no status it can use, as when OME hasn't written it yet. |

## `list`

```text
kubectl ome instance list INFERENCESERVICE [flags]
```

`list` shows one row for each Instance of the InferenceService's OMENative components, engine first, then decoder and router, in index order. The rows come from the InferenceReplicas' status, so they show what OME last recorded. [`status`](#status) shows the live pods.

### Flags {#list-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#list-output-fields}

| Column | What it shows |
| --- | --- |
| COMP | The component. |
| IDX/INC | The Instance's [index](../../concepts/omenative/instances.md#index) and [incarnation](../../concepts/omenative/instances.md#incarnation), as `index/incarnation`. |
| PHASE | The [Instance phase](../../concepts/omenative/instances.md#instance-phases): `Pending`, `Creating`, `Ready`, `Updating`, `Restarting`, `Migrating`, `Failed` or `Deleting`. |
| PODS | The Instance's pods as OME last counted them: `serving/available/total`, as [Readiness and availability](../../concepts/omenative/instances.md#readiness-and-availability) defines them. |
| REVS | The revision the Instance runs and the one it's moving to: `running>target`, `>target` before any revision runs, or the running revision alone. A long value keeps its first and last 4 characters, as in `chat...4d2b`. To spot an update, check PHASE and the `O` flag, or run `status`. |
| AOF | Three flags. `A`: the Instance is admitted, with pods and no scheduling gate. `O`: an [operation](../../concepts/omenative/instances.md#operations) is under way. `F`: OME recorded a failure. `-` when a flag is off. |
| EVIDENCE | How current the component's status is. |

EVIDENCE shows one value for each component:

| Value | Meaning |
| --- | --- |
| `OK` | The status is current. |
| `SPARSE` | The status is current, and the indexes have gaps, as after a migration. |
| `EMPTY` | The status is current, with no rows, as for a component scaled to zero. |
| `STALE:<observed>/<generation>` | OME last observed an older generation of the InferenceReplica. The rows still show. |
| `STALE:PARENT` | The InferenceReplica reflects an older generation of the InferenceService. The rows still show. |
| `NOT-OBSERVED` | OME hasn't written status for the InferenceReplica yet. |
| `NOT-REPORTED` | The status counts replicas but lists no Instances. |
| `PARENT-UNAVL` | The InferenceReplica doesn't record which InferenceService generation it reflects. |
| `ROWS-LIMIT` | The component's rows didn't fit in the table, so none of them show. |
| `BAD:<problem>` | The status is invalid. `DUP-COMP`: a second InferenceReplica claims the component. `DUP-IDX`: two rows share an index. Any other problem names the invalid value: `GEN` (generation), `PARENT` (InferenceService generation), `REVISION`, `COUNTS` (pod or replica counts), `PHASE` or `INSTANCE` (index or incarnation). `COUNTS` can appear briefly while a pod stops serving, as during an update. |
| `BAD`, `UNAVAILABLE` | The status is malformed or unavailable, with no more specific value. |

A component with no rows to show gets one row of dashes with its EVIDENCE, such as `NOT-OBSERVED`. A problem with the whole collection adds a last row, `summary`. It shows `SOURCE-UNAVL` (the CLI couldn't list or decode an InferenceReplica), `REJECTED` (it left out an InferenceReplica that failed the identity checks) or `TRUNCATED` (there's more than the table shows). With no rows at all, the table shows one row of dashes with a `summary` value or the overall state: `OK`, `PARTIAL` or `UNAVAILABLE`.

`-o json` and `-o yaml` print every value in full, and every issue behind the EVIDENCE values.

### Examples {#list-examples}

List the Instances of `chat` in `prod`:

```bash
kubectl ome instance list chat -n prod
```

```output
COMP     IDX/INC   PHASE   PODS    REVS          AOF   EVIDENCE
engine   0/1       Ready   1/1/1   chat...ne-a   A--   OK
```

The engine has one Instance, index 0 in its first incarnation. It's `Ready` on the revision `chat-engine-a`, cut to 11 characters, with its one pod serving and available.

When the InferenceService also has a decoder that's updating, and OME hasn't caught up with its latest change, the table looks like this:

```output
COMP      IDX/INC   PHASE      PODS    REVS          AOF   EVIDENCE
engine    0/1       Ready      1/1/1   chat...ne-a   A--   OK
decoder   3/8       Updating   1/1/4   chat...-new   AO-   STALE:3/4
```

The decoder's Instance 3 is in its eighth incarnation and moving from `chat-decoder-old` to `chat-decoder-new`, with an operation under way. One of its four pods is serving and available. `STALE:3/4` means OME last observed generation 3 of the decoder's InferenceReplica, which is now at generation 4.

## `status`

```text
kubectl ome instance status INFERENCESERVICE INDEX --component COMPONENT [flags]
```

`status` shows one Instance: the row OME reports for it, the migrations it takes part in, and its pods with their Warning events. INDEX is the Instance's [index](../../concepts/omenative/instances.md#index), the first number under IDX/INC in `list`. The pod and event rows are live. The phase, revisions, pod counts, conditions, operation and last failure are what OME last recorded.

`status` first works out the component's deployment mode, from the `ome.io/deploymentMode` annotation or else, as OME does, from the InferenceService and its runtime. A component in another mode gets the state `NotOMENative`.

`status` finds the Instance's pods by their `ome.io/instance-index` label. When an Instance has more than 16 pods, it shows the 16 most likely to explain a problem, such as pods being deleted or not Ready. It shows the reason and count of each pod's Warning events, but not their messages: `kubectl describe pod` shows those.

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | None | Component: engine, decoder or router (required) |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#status-output-fields}

The first row, `state`, gives the report's state, the Instance and the component's evidence level, as in `Reported engine[0] evidence=Reported`. The state is one of:

| State | Meaning |
| --- | --- |
| `Reported` | OME reports the Instance on current evidence, and the CLI read everything it looked for. |
| `Partial` | Part of the report is stale, malformed or missing, as when the CLI couldn't read the pods. The `issue` rows say what. |
| `Missing` | The component's InferenceReplica has no row with that index. |
| `NotProjected` | The component has no InferenceReplica. |
| `NotOMENative` | The component doesn't run in OMENative. |
| `Unavailable` | The CLI couldn't read the InferenceReplicas, or has nothing it can use for the component, as when OME hasn't written its status yet. |

The `deployment` and `encoding` rows always show. The others appear when they have a value:

| Field | What it shows |
| --- | --- |
| `deployment` | The deployment mode, how it was set (`source=`), where the CLI read it (`origin=`), and its evidence level. |
| `encoding` | The status encoding, `DenseV1` or `ColumnarV2`, its evidence level, and why it's unavailable, if it is. |
| `instance` | The InferenceReplica, the incarnation, the phase, and whether the Instance is admitted. |
| `revisions` | The revision the Instance runs, and the one it's moving to. |
| `persisted` | The pod counts OME wrote: total, serving and available. |
| `lifecycle` | The Instance's active pod-name slot, 0 or 1 (`activeOrdinal=`), and when it last became Ready (`readySince=`). |
| `condition` | One row for each condition: its type, status, observed generation, evidence level and reason. |
| `announced` | One row for each warning OME has raised once for the Instance, as `reason@episode`, such as `GangSplitRisk@#1`. |
| `operation` | The operation under way: its type, ID, step and retry count. |
| `op target` | The revision the operation moves to, and why. |
| `op timing` | When the operation started, when it last made progress, and its deadline. |
| `op hold` | What holds the operation back, such as `QuotaExceeded`, and when quota admission last refused one of its pods. |
| `op strategy` | The update strategy the operation runs under, such as `SurgeThenDrain`. |
| `op nodes` | The node the operation moves the Instance from, the surge Instance's index, and the target node hints. |
| `failure`, `fail time` | The last failure: the pod, the container, the reason and the exit code, then when OME recorded it. |
| `migration` | One row for each migration of the Instance: its request UUID, its role, the phase, and the source and surge indexes. The role is `Source` for the Instance being moved, `Surge` for its replacement. |
| `pod` | One row for each pod: its name, its runner from the `ome.io/runner` label, its phase, whether it's Ready and serving, restarts, node, and whether it's being deleted. |
| `event` | One row for each Warning event: the pod, the reason and the count. |
| `issue` | One row for each issue code, with a reason when there is one. |

The default table cuts long values, so the `op timing`, `failure`, `migration` and `pod` rows often lose their last fields. `-o wide` prints every field in full, one to a row. In a `pod` row, `ready` is the pod's Kubernetes Ready condition, and `serving` is the `ome.io/serving` readiness gate that OME sets. `deleting` is `true` when the pod has a deletion timestamp: [Recover stuck deletions](../../guides/omenative/recover-stuck-deletions.md) covers pods that stay that way. Text that looks like a credential shows as `[REDACTED]`.

A migration request can wait in the `Accepted` phase, and its [deadline](../../concepts/omenative/migration-and-transient-scale.md#deadline) keeps running while it waits, even while the rollout is paused. [`kubectl ome migration`](migration.md) inspects and requests migrations.

### Examples {#status-examples}

Show Instance 0 of the engine of `chat`:

```bash
kubectl ome instance status chat 0 --component engine -n prod
```

```output
FIELD        VALUE
state        Reported engine[0] evidence=Reported
deployment   mode=OMENative source=ServiceSpec origin=LiveRuntime evidence...
encoding     name=DenseV1 evidence=Reported reason=-
instance     chat-engine inc=1 phase=Failed admitted=true
revisions    running=- target=chat-engine-6c4f8b1d
persisted    pods=1 serving=0 available=0
lifecycle    activeOrdinal=0 readySince=-
failure      pod=chat-engine-0-default-0 container=ome-container reason=Er...
fail time    2026-09-14T20:52:31Z
pod          chat-engine-0-default-0 default/Running ready=False serving=F...
event        Pod/chat-engine-0-default-0 reason=BackOff count=21
```

Instance 0 is `Failed` and has never been Ready, so `revisions` names only its target. OME recorded its last failure at `2026-09-14T20:52:31Z`, and its pod has a `BackOff` event with a count of 21. Add `-o wide` to see the cut `failure` and `pod` fields, such as `failure exit code` and `pod[0] restarts`.

## `retry-blocks`

```text
kubectl ome instance retry-blocks INFERENCESERVICE --component COMPONENT [flags]
```

`retry-blocks` shows the revisions that OME is retrying or holding for one component, from its InferenceReplica's `status.retryBlocks`. A revision here is a version of the component's pod template, named `<InferenceService>-<component>-<hash>`, such as `chat-engine-7f9c4d2b`, not a [runtime revision](../../concepts/runtimes/runtime-revisions.md). When updates to a revision fail, OME retries it until its attempts run out, then holds it. Only failures that the revision causes, such as an image that can't be pulled, count as attempts. A held revision stays held until you release it with [`release-held`](#release-held) or change the component's pod template. [How a revision gets held](../../guides/roll-out-changes/release-a-held-revision.md#how-holds-and-releases-work) lists the failures that count and the retry defaults.

### Flags {#retry-blocks-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | None | Component: engine, decoder or router (required) |
| `-o`, `--output` | `table` | Output format: table, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#retry-blocks-output-fields}

| Column | What it shows |
| --- | --- |
| COMP | The component. |
| STATE | `BACKOFF` while attempts remain, `RUNNING` (for `RetryInProgress`) while one runs, `HELD` once none remain, or `INVALID` for a state the CLI doesn't know. `ISSUE` marks an issue row. |
| TARGET | The target revision. A name over 14 characters shows as its first 5 characters, `#` and a digest, as in `chat-#4c6a152e`. |
| ATT | The number of attempts started, up to `99+`. |
| NEXT | When the next attempt can start, as `YY-MM-DDThh:mmZ` in UTC, or `-`. |
| REL | `YES` when the block qualifies for `release-held`, `NO` otherwise. |
| REASON | The last failure OME recorded, cut short. On an issue row, the issue's alias. |

The digest isn't the revision's hash, so don't pass it to `release-held`. Take the full name from `-o json`, or use its hash, the last 8 characters of the name.

REL is `YES` only for a valid `Held` block on current status, while nothing is being deleted. `release-held` checks more, so it can still refuse a block marked `YES`, as [Refusals](#release-held-refusals) lists.

Each issue adds a row with `ISSUE` in STATE and an alias in REASON:

| Alias | Meaning |
| --- | --- |
| `STATUS_UNOBS` | OME hasn't written status for the InferenceReplica yet. |
| `STATUS_STALE`, `PARENT_STALE` | OME hasn't caught up with the latest change. Wait, then run the command again. |
| `NO_COMPONENT` | The component has no InferenceReplica. |
| `DUP_COMP` | More than one InferenceReplica claims the component. No blocks show. |
| `COLL_UNAV` | The CLI couldn't list the InferenceReplicas, or couldn't decode one. |
| `ID_REJECT` | The CLI left out an InferenceReplica that doesn't belong to the InferenceService. |
| `COLL_TRUNC`, `BLOCK_TRUNC` | There are more InferenceReplicas or retry blocks than the CLI reads. |
| `PARENT_MISS`, `PARENT_BAD`, `PARENT_AHEAD`, `OBSGEN_BAD` | The InferenceReplica's `ome.io/parent-generation` annotation or observed generation is missing or invalid. |
| `TARGET_BAD`, `STATE_BAD`, `ATT_BAD`, `TIME_BAD`, `DUP_TARGET` | A block has an invalid or repeated target, an unknown state, no attempts, or times that don't fit together. |

A component with no blocks and no issues shows one row with `EMPTY` in STATE.

`-o json` and `-o yaml` print each block in full, with `releaseEligible` for REL, and a `summary` with the block counts.

### Examples {#retry-blocks-examples}

Show the retry blocks of the engine of `chat`:

```bash
kubectl ome instance retry-blocks chat --component engine -n prod
```

```output
COMP     STATE   TARGET           ATT   NEXT   REL   REASON
engine   HELD    chat-#4c6a152e   3     -      YES   ImagePullB...
```

The engine holds a revision after 3 attempts. `-o json` shows its full name, `chat-engine-7f9c4d2b`, and the full reason, `ImagePullBackOff`. REL shows that the block qualifies for `release-held`, either by that name or by the hash `7f9c4d2b`.

## `release-held`

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

```text
kubectl ome instance release-held INFERENCESERVICE [flags]
```

`release-held` asks OME to release one `Held` retry block of a component, so that OME tries the revision again. It sets the `ome.io/release-held-revision` annotation on the component's InferenceReplica to the block's full target revision. OME then removes the Held block, deletes the annotation and goes back to updating the component's Instances to the revision, so fix what made the revision fail first. A paused InferenceService stays paused, and the update waits until you resume it. The command doesn't wait for OME: check with `retry-blocks`, or in a script with [`kubectl ome wait --for=held-revision=unheld`](wait.md#held-revision-unheld). [Release a held revision](../../guides/roll-out-changes/release-a-held-revision.md) walks through a release.

`--revision` takes the block's full target revision, such as `chat-engine-7f9c4d2b`, or its hash, the last 8 characters, such as `7f9c4d2b`. Either way, the annotation gets the full name. Besides the read permissions, `release-held` needs `patch` on `inferencereplicas`.

### Flags {#release-held-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | None | Component: engine, decoder or router (required) |
| `--dry-run` | `none` | Dry-run mode: none, client or server |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output: table, wide (bounded), json or yaml |
| `--revision` | None | Exact full target revision or eight-lowercase-hex hash (required) |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### How release-held runs {#how-release-held-runs}

`release-held` follows the [guarded flow](rollout.md#how-the-actions-run) of the rollout actions, with the component's InferenceReplica as its target. Its preview, a `HELD RELEASE PREVIEW` table on stderr, shows the revision with its state and attempts, and the annotation it sets. After you confirm, it reads everything again, even for a client dry run. It has 45 seconds in all, including the time the prompt waits for you. These errors come after you confirm:

| Message | Exit | Meaning |
| --- | --- | --- |
| `held-release preview became stale; inspect instance retry-blocks and retry explicitly` | `3` | Something changed after the preview, or couldn't be read again. Nothing was stored. |
| `guarded held-release rejected; inspect instance retry-blocks` | `3` | The InferenceReplica changed after the CLI read it. Nothing was stored. |
| `held-release request outcome unknown; inspect instance retry-blocks before another explicit request`, or another message with `outcome unknown` | `1` | Any other patch failure, even a permission error. The API server may have stored the annotation. |

After any of these errors, run `retry-blocks` before you try again.

The result depends on the dry-run mode:

| `--dry-run` | `accepted` | `applied` | Message |
| --- | --- | --- | --- |
| `client` | `No` | `No` | `Validated locally; no patch sent.` |
| `server` | `Yes` | `No` | `API dry-run accepted; no changes persisted.` |
| `none` | `Yes` | `Yes` | `API accepted release annotation request; controller release/convergence not observed.` |

`applied` means that the API server stored the annotation, not that OME released the block. `--yes` skips only the prompt, never a check. [Guarded actions](guarded-actions.md) describes the preview, the prompt, the dry-run modes and the ActionResult.

### Refusals {#release-held-refusals}

Like every guarded action, `release-held` refuses an InferenceService that's being deleted, has an unsafe identity, is too large to check safely, or uses multi-cluster placement (alpha). [Target checks](guarded-actions.md#target-checks) lists those messages. It also refuses, changes nothing and exits `1` in these cases:

| Message | Cause |
| --- | --- |
| `action refused: no exact current valid Held retry block is releasable` | One of the causes below the table. |
| `action refused: a release-held mailbox is already present; inspect instance retry-blocks` | An earlier release still waits in the `ome.io/release-held-revision` annotation. See [A release request is already waiting](../../guides/roll-out-changes/release-a-held-revision.md#a-release-request-is-already-waiting). |
| `action refused: active runtime is unavailable, inconsistent or unbound` | The CLI couldn't resolve the runtime that the InferenceService runs, or no component runs in OMENative. Check that `--ome-namespace` names OME's namespace. |
| `action refused: controller safety evidence is stale or inconsistent` | The component's InferenceReplica changed while the CLI read it, or an InferenceReplica's status doesn't decode. Run the command again. |

The first refusal has these causes, likeliest first:

- No block matches `--revision`, or the block that matches isn't `Held`.
- An InferenceReplica of the InferenceService, even another component's, has stale status. Wait for OME to catch up.
- A block on the component's InferenceReplica is malformed, as when its failure time is in the future because your machine's clock runs behind the cluster's.
- An InferenceReplica of the InferenceService fails the identity checks or is being deleted, or the component's InferenceReplica lacks `ome.io/controller-write: "true"`.
- The component doesn't run in OMENative, has no InferenceReplica, or is claimed by two InferenceReplicas.

[The command refuses the release](../../guides/roll-out-changes/release-a-held-revision.md#the-command-refuses-the-release) shows how to find out which applies.

### Examples {#release-held-examples}

Run every check and see the preview, without sending the patch:

```bash
kubectl ome instance release-held chat --component engine --revision chat-engine-7f9c4d2b -n prod --dry-run client
```

The preview goes to stderr. After you confirm, the result goes to stdout:

```output
FIELD           VALUE
action          instance release-held
target          InferenceReplica/prod/chat-engine
dry-run         client
accepted        No
applied         No
request-id      -
revision-hash   7f9c4d2b
message         Validated locally; no patch sent.
follow-up       kubectl ome instance retry-blocks chat --component=en...
hint            Use -o json or -o yaml for full values.
```

Release the revision by its hash, without a prompt, as a script would:

```bash
kubectl ome instance release-held chat --component engine --revision 7f9c4d2b -n prod --yes
```

```output
FIELD           VALUE
action          instance release-held
target          InferenceReplica/prod/chat-engine
dry-run         none
accepted        Yes
applied         Yes
request-id      -
revision-hash   7f9c4d2b
message         API accepted release annotation request; controller r...
follow-up       kubectl ome instance retry-blocks chat --component=en...
hint            Use -o json or -o yaml for full values.
```

[Step 2](../../guides/roll-out-changes/release-a-held-revision.md#step-3-submit-the-release) of the guide shows the result as JSON, with every value in full.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every subcommand that finishes, even when a read command prints a partial report. |
| `1` | General error | Every subcommand, for an invalid argument or flag, a failed read, or Ctrl-C. `release-held` also returns it for a refusal, an action you didn't confirm, its 45 seconds running out, or an unknown outcome. |
| `3` | Mutation conflict | `release-held`, when what the preview showed changed before the patch, or couldn't be read again after you confirmed. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Release a held revision](../../guides/roll-out-changes/release-a-held-revision.md)
- [Reset failed Instances](../../guides/omenative/reset-failed-instances.md)
- [Set Instance readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md)
- [Change the Instance status encoding](../../guides/omenative/change-the-status-encoding.md)
- [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md)
- [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md)
- [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md)
- [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
