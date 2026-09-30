---
title: Guarded actions
description: "Every mutating kubectl-ome command follows one safety contract: dry runs, confirmation, a guarded patch, the ActionResult, exit codes and RBAC."
since: v1.3
---

Every kubectl-ome command that changes a resource is a guarded action. It checks that the change is safe, shows you the exact change and asks you to confirm it. Then it sends one JSON Patch, which the API server applies only if the target hasn't changed since the CLI read it. Each command's page lists its own flags, refusals and preview.

In the result, `applied` means that the API server stored the change, not that the controller acted on it. Every result names a read-only follow-up command, and [When the outcome is unknown](#when-the-outcome-is-unknown) lists where to check each action.

## Guarded commands {#guarded-commands}

!!! note "Alpha"
    These commands are alpha. Their flags and behavior can change between releases.

These are the only kubectl-ome commands that write. Every other command, `admin` and `wait` included, only reads. They need `patch` on top of the read rules: see [Required RBAC](#required-rbac).

| Command | Patches | Change |
| --- | --- | --- |
| [`rollout pause`](rollout.md#pause-and-resume) | The InferenceService | Sets the `ome.io/rollout-paused` annotation to `true`. |
| [`rollout resume`](rollout.md#pause-and-resume) | The InferenceService | Removes `ome.io/rollout-paused`. With `--discard-pending-actions`, it also removes `ome.io/rollout-promote` and `ome.io/rollout-rollback`. |
| [`rollout promote`](rollout.md#promote-and-rollback) | The InferenceService | Sets `ome.io/rollout-promote` to the canary revision hash. |
| [`rollout rollback`](rollout.md#promote-and-rollback) | The InferenceService | Sets `ome.io/rollout-rollback` to `true`. |
| [`rollout repin`](rollout.md#repin) | The InferenceService | Sets `ome.io/rollout-repin` to the digest of the current plan. |
| [`traffic drain`](traffic.md#drain-and-undrain) | The control-plane InferenceService | Adds a drain entry, keyed by its drain ID, to `ome.io/traffic-drain`. |
| [`traffic undrain`](traffic.md#drain-and-undrain) | The control-plane InferenceService | Removes a drain entry from `ome.io/traffic-drain`. |
| [`migration start`](migration.md#start) | The InferenceService | Adds an `ome.io/migration-request-v1-<request-id>` annotation that holds the migration request as JSON. |
| [`runtime sync`](runtime.md#sync) | The InferenceService | Sets `ome.io/runtime-sync` to a new `cli-runtime-sync-<UUID>` token, which asks OME to roll a [pinned runtime](../../concepts/runtimes/runtime-revisions.md) forward. |
| [`scale`](scale.md) | The `/scale` subresource of an InferenceReplica | Replaces `spec.replicas`. The owner of the count, such as an autoscaler, can overwrite it. |
| [`instance release-held`](instance.md#release-held) | The InferenceReplica | Sets `ome.io/release-held-revision` to the name of the held revision. |

`traffic drain` and `traffic undrain` act on multi-cluster routing, which is alpha and still in development.

Every guarded command takes these flags:

| Flag | Default | Description |
| --- | --- | --- |
| `--dry-run` | `none` | Dry-run mode: none, client or server |
| `-o`, `--output` | `table` | Output: table, wide (bounded), json or yaml |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

## How an action runs {#the-shared-sequence}

Every guarded command runs these steps in order, and stops at the first one that fails:

1. It checks your flags, reads the target and whatever the change depends on, and refuses if the change isn't safe now. A refusal changes nothing.
2. It prints a preview of the exact change to stderr: the kubeconfig context, the namespaces, the target with its UID and resourceVersion, the dry-run mode, the change and warnings. The preview shows every value in full.
3. It asks you to confirm, unless you pass `--yes`. See [The confirmation prompt](#the-confirmation-prompt).
4. `runtime sync`, `instance release-held`, `migration start` and `scale` read their inputs again, and stop if anything changed.
5. It sends one JSON Patch that tests the UID and resourceVersion it read. If the target changed since then, the API server applies none of it, and the command exits `3`. The patch goes through admission like any other write. A client dry run stops before this step.
6. It prints the result, an [ActionResult](#the-actionresult), to stdout. After an error, stdout stays empty and stderr shows `error: <message>`.

### Target checks {#target-checks}

Every guarded command refuses when the InferenceService fails one of these checks. For `scale` and `instance release-held`, that's the InferenceService that owns the InferenceReplica.

| Message | Cause |
| --- | --- |
| `action refused: target identity is missing or unsafe` | The InferenceService's name, namespace, UID, resourceVersion or generation is missing or invalid. |
| `action refused: target is being deleted` | The InferenceService is being deleted. |
| `action refused: safety inputs exceed inspection bounds` | The InferenceService is too large for the command to check safely. |
| `action refused: placement sources and derived services cannot be mutated` | The InferenceService uses multi-cluster placement, which is alpha, or placement created it. |

`traffic drain` and `traffic undrain` act on the control-plane InferenceService and have refusals of their own, which [`kubectl ome traffic`](traffic.md#drain-and-undrain-refusals) lists.

## The confirmation prompt {#the-confirmation-prompt}

After the preview, the command asks on stderr:

```text
Confirm this exact action? [y/N]
```

Type `y` or `yes`, in any case, and press Enter to go ahead. Any other answer stops the command with `action not confirmed; noninteractive input requires --yes`, and nothing is sent.

The prompt needs a terminal on stdin. In a script, a pipe or CI, pass `--yes`. Without it, the command prints the preview and fails with the same error. Piping `yes` into the command doesn't work. `--yes` skips only the prompt.

Three requests need `--yes` and never prompt: `rollout resume --discard-pending-actions`, `rollout promote --override-analysis`, and every `scale` request, which also needs `--override-autoscaler`.

## Dry-run modes {#dry-run-modes}

`--dry-run` takes one of three modes. Every mode runs the reads, the checks, the preview and the prompt.

| Mode | Patch | `accepted` | `applied` |
| --- | --- | --- | --- |
| `none` | Sent, and stored when the API server accepts it. | `Yes` | `Yes` |
| `client` | Not sent. | `No` | `No` |
| `server` | Sent with `dryRun=All`. The API server runs the `test` operations and admission, and stores nothing. | `Yes` | `No` |

A client dry run still reads the cluster, so it needs the read permissions and refuses whatever the real run would refuse. It can't catch what only the API server checks: your permission to patch, admission, and a change to the target just before the patch. A server dry run checks all three.

A client dry run reports `Validated locally; no patch sent.`, and a server dry run reports `API dry-run accepted; no changes persisted.` Some commands word them a little differently.

## The ActionResult {#the-actionresult}

A command that succeeds prints one ActionResult to stdout, in the format that `-o` names. The preview and the prompt go to stderr, so you can redirect the result on its own. This server dry run of `rollout resume` prints the result as JSON and discards the preview:

```bash
kubectl ome rollout resume chat -n prod --context=prod-cluster --dry-run server --yes -o json 2>/dev/null
```

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-28T14:05:00Z",
  "action": "rollout resume",
  "target": {
    "kind": "InferenceService",
    "namespace": "prod",
    "name": "chat",
    "uid": "5d9f3c2a-8b41-4f7e-a0c6-1e2b3c4d5e6f",
    "resourceVersion": "184467"
  },
  "dryRun": "server",
  "accepted": true,
  "applied": false,
  "message": "API dry-run accepted; no changes persisted.",
  "followUp": "kubectl ome rollout status chat -n prod --context=prod-cluster"
}
```

Without `-o`, the result prints as a table after the preview. [`kubectl ome rollout`](rollout.md#pause-and-resume-examples) shows a preview and its result.

The ActionResult has these fields:

| Field | Table row | What it holds |
| --- | --- | --- |
| `apiVersion`, `kind` | None | `cli.ome.io/v1alpha1` and `ActionResult`. |
| `collectedAt` | None | When the CLI built the result, in UTC. |
| `action` | `action` | The command, such as `rollout resume`. |
| `target` | `target` | The object the patch targets: its `kind`, `namespace`, `name`, `uid`, and the `resourceVersion` that the patch tested, from before the change. The table shows `Kind/namespace/name`. |
| `dryRun` | `dry-run` | `none`, `client` or `server`. |
| `requestID` | `request-id` | For `migration start`, the ID of the migration request. For `runtime sync`, the UUID in the value it set. |
| `revisionHash` | `revision-hash` | For `promote` and `rollback`, the canary revision hash. For `runtime sync`, the target hash from the preview. For `instance release-held`, the held revision's hash. |
| `accepted` | `accepted` | Whether the API server accepted the patch. |
| `applied` | `applied` | Whether the change was stored. |
| `message` | `message` | What happened, and what the CLI didn't observe. |
| `followUp` | `follow-up` | A read-only command to run next, and [When the outcome is unknown](#when-the-outcome-is-unknown) lists where to check each action. |
| `rollout` | `run-id`, `pinned-plan-digest`, `requested-plan-digest`, `group-count` | `rollout repin` only: the run, the pinned and requested plan digests, and the number of groups. |
| `traffic` | `override-id`, `cluster`, `overrides` | `traffic drain` and `traffic undrain` only: the drain ID, the WorkloadCluster, and the number of drain entries before and after. |
| `scale` | Rows of their own | `scale` only: the component, the replica counts and the scaling owner. See [`kubectl ome scale`](scale.md#output-fields). |

The table shows `-` for an empty field, which `-o json` and `-o yaml` leave out. The table also cuts long values, so use `-o json` or `-o yaml` for the full message and follow-up. With `-o wide`, `rollout repin`, `traffic drain` and `traffic undrain` add the target's `uid` and `resource-version`, and `scale` adds more rows. Other commands print the default table.

You can pass a result's fields to [`kubectl ome wait`](wait.md). Pass the `requestID` of `migration start` or `runtime sync` to `--request-id`. Pass the `target.name` and `target.uid` of `scale` or `instance release-held` to `--ir-name` and `--ir-uid`.

## Exit codes {#exit-codes}

In scripts, branch on the exit code and read the result with `-o json`. Error messages are short and fixed, so different causes can print the same message.

| Code | Meaning | What to do |
| --- | --- | --- |
| `0` | The command finished, a dry run included. | After a real run, check what the controller did, as the action's page shows. |
| `1` | An invalid flag, a failed read, a refusal, a declined prompt, Ctrl-C, a timeout, a patch rejected for any reason but a conflict, or an unknown outcome. A failed second read in `runtime sync` or `instance release-held` exits `1` only when the 45-second limit or Ctrl-C ended it. | If it failed after you confirmed, check the target before you run it again. See [When the outcome is unknown](#when-the-outcome-is-unknown). |
| `3` | The target or another input changed after the CLI read it, or the second read in `runtime sync` or `instance release-held` failed, so the action didn't apply. `migration start` also exits `3` for [refusals](migration.md#start-refusals) caused by the state of the InferenceService or its Instance. | Run the command that the message names, check that the action still makes sense, and run it again. |

When the second read finds a change, `runtime sync`, `instance release-held` and `migration start` exit `3`, and `scale` exits `1`.

### When the outcome is unknown {#when-the-outcome-is-unknown}

An error after you confirm doesn't always mean that nothing changed. After a server error, a timeout, a lost connection or a response that the CLI can't match to its request, the API server may have stored the patch. Some commands then say `outcome unknown`, and others print a plain error such as `context deadline exceeded`. The CLI never retries on its own, so check before you run the action again:

| Command | Check with |
| --- | --- |
| `rollout pause`, `resume`, `promote` and `rollback` | `kubectl ome rollout status` |
| `rollout repin` | `kubectl ome rollout explain` |
| `traffic drain` and `undrain` | The `ome.io/traffic-drain` annotation, as [`kubectl ome traffic`](traffic.md#drain-and-undrain) shows |
| `migration start` | `kubectl ome migration status`, or a [lookup](migration.md#how-start-runs) with the request UUID from the preview |
| `runtime sync` | `kubectl ome runtime effective` |
| `scale` | `kubectl ome autoscale status` |
| `instance release-held` | `kubectl ome instance retry-blocks` |

## Timeouts {#timeouts-and-response-bounds}

A guarded command has 45 seconds in all, including the time the prompt waits for you. Each API request has at most 10 seconds, or less with a shorter `--request-timeout`. When time runs out, the command fails with `context deadline exceeded`.

## Required RBAC {#required-rbac}

The guarded commands need the read rules on [kubectl-ome overview and install](overview.md#required-rbac), and this rule to send their patches:

```yaml title="kubectl-ome-actions.yaml"
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kubectl-ome-actions
rules:
  - apiGroups: ["ome.io"]
    resources: ["inferenceservices", "inferencereplicas", "inferencereplicas/scale"]
    verbs: ["patch"]
```

| Resource | Commands that patch it |
| --- | --- |
| `inferenceservices` | `rollout pause`, `resume`, `promote`, `rollback` and `repin`, `traffic drain` and `undrain`, `migration start` and `runtime sync` |
| `inferencereplicas` | `instance release-held` |
| `inferencereplicas/scale` | `scale`. A rule that names only `inferencereplicas` doesn't cover the subresource. |

A server dry run needs `patch` too, since Kubernetes authorizes it as a write. A client dry run sends no patch, so it needs only the read rules. `migration start` also reads ConfigMaps, one of the overview's extra rules.

Every patch targets an object in the namespace of the InferenceService, so bind the ClusterRole with a RoleBinding in each namespace where people run the actions:

```bash
kubectl apply -f kubectl-ome-actions.yaml
kubectl create rolebinding kubectl-ome-actions --clusterrole=kubectl-ome-actions --group=ome-operators -n prod
```

```output
clusterrole.rbac.authorization.k8s.io/kubectl-ome-actions created
rolebinding.rbac.authorization.k8s.io/kubectl-ome-actions created
```

Replace `ome-operators` with your group, or bind a user with `--user` or a service account with `--serviceaccount=NAMESPACE:NAME`. The built-in `edit` and `admin` roles don't include this rule, so grant it yourself.

To check a permission, ask the API server. The subresource needs `--subresource`:

```bash
kubectl auth can-i patch inferencereplicas.ome.io --subresource=scale -n prod
```

```output
yes
```

Without `patch`, the command exits `1`. Most commands print `required Kubernetes API request failed; check access and connectivity`, and `scale` and `instance release-held` call the outcome unknown.

## Related pages {#related-pages}

- [kubectl-ome overview and install](overview.md)
- [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md)
- [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md)
- [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md)
- [Release a held revision](../../guides/roll-out-changes/release-a-held-revision.md)
- [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md)
- [Request an instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md)
- [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md), on alpha multi-cluster routing
