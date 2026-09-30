---
title: Release a held revision
description: Get OME to retry a revision it stopped retrying, once you've fixed what made it fail, with the alpha kubectl ome instance release-held command.
since: v1.3
---

OME holds a revision of an [OMENative](../../concepts/omenative/overview.md) component when updates to it keep failing for a reason of its own, such as an image that can't be pulled. With the default [SurgeThenDrain](../../concepts/architecture/omenative-update-strategies.md#surgethendrain) update strategy, the old Instances keep serving meanwhile. Once you've fixed the cause, release the revision with `kubectl ome instance release-held`, and OME tries it again.

You need a release only when the fix is outside the [InferenceService](../../concepts/serving/inference-services.md): you pushed the image, fixed the registry credentials or created a missing Secret. When the fix is a change to the InferenceService, such as a new image tag, apply it instead: the change makes a new revision, which needs no release. The examples release the held revision of the `engine` component of `chat`, in the namespace `prod`.

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

<div class="prerequisites" markdown>

- An InferenceService with an OMENative component that has a held revision.
- The kubectl ome plugin, a kubeconfig context for the cluster, and the plugin's [read permissions](../../reference/kubectl-ome/overview.md#required-rbac). See [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md).
- Permission to `patch` `inferencereplicas.ome.io` in `prod`. Check with `kubectl auth can-i patch inferencereplicas.ome.io -n prod`, which prints `yes` when you have it.

</div>

## How a revision gets held {#how-holds-and-releases-work}

A new image, or any other change to a component's pod template, gives the component a new target revision, named `<InferenceService>-<component>-<hash>`, such as `chat-engine-7f9c4d2b`. It isn't a [runtime revision](../../concepts/runtimes/runtime-revisions.md). When an update to it fails, OME records a retry block for the revision in the status of the component's [InferenceReplica](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica), `chat-engine`:

| State | `retry-blocks` shows | Meaning |
| --- | --- | --- |
| `Backoff` | `BACKOFF` | Attempts remain. OME can try again at `nextRetryAt`. |
| `RetryInProgress` | `RUNNING` | An attempt is running. |
| `Held` | `HELD` | No attempts remain. OME stops trying the revision until you release it or replace it. |

Only failures that the revision causes count as attempts: `ImagePullBackOff`, `ErrImagePull`, `InvalidImageName`, `CreateContainerConfigError`, and `InvalidPodSpec`, which means that the API server rejected the pod as invalid. Failures that a node or its GPUs can also cause, such as `CrashLoopBackOff` or an Instance that isn't ready in time, never hold a revision.

With the ome-resources chart, `ome.controller.lifecycle.updateRetry` gives a revision 3 attempts: the second 1 minute after the first failure, and the third 2 minutes after the second. A manifest install sets no retry policy, so the first failure that counts holds the revision. Raising these limits doesn't release a revision that's already held.

When OME holds a revision, it records a `RetryHeld` warning event on the InferenceService, and the component's rollout hold shows the gate `Held`, as [When a rollout doesn't move](../../concepts/architecture/omenative-update-strategies.md#when-a-rollout-doesnt-move) describes.

??? note "How `chat` got a held revision"
    The `chat` in these examples got its held revision when someone set its engine image to a tag that doesn't exist:

    ```yaml
    apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata:
      name: chat
      namespace: prod
    spec:
      deploymentMode: OMENative
      model:
        name: qwen3-0-6b
      runtime:
        name: srt-qwen3-0-6b
      engine:
        minReplicas: 3
        maxReplicas: 3
        runner:
          name: ome-container
          image: docker.io/lmsysorg/sglang:v0.0.0-missing
        lifecycle:
          updateStrategy:
            type: SurgeThenDrain
            rollingUpdate:
              maxSurge: 1
    ```

    With SurgeThenDrain and `maxSurge: 1`, OME gave one Instance at a time a new pod next to its old one, and the cluster had room for it. The new pod couldn't pull the image, which counts as an attempt. After the third attempt, OME held the revision, and the three old Instances kept serving. It recorded a `RetryHeld` warning event:

    ```bash
    kubectl get events -n prod --field-selector reason=RetryHeld \
      -o custom-columns=TYPE:.type,OBJECT:.involvedObject.name,MESSAGE:.message
    ```

    ```output
    TYPE      OBJECT   MESSAGE
    Warning   chat     InferenceReplica prod/chat-engine component=engine update to revision chat-engine-7f9c4d2b held after 3 failed attempt(s) (last failure: ImagePullBackOff); publish a corrected revision or raise lifecycle.updateRetry limits
    ```

    The fix here is an image that exists, set in the InferenceService, and the new revision needs no release.

## Step 1: Find the held revision

Show the component's retry blocks:

```bash
kubectl ome instance retry-blocks chat --component engine -n prod
```

```output
COMP     STATE   TARGET           ATT   NEXT   REL   REASON
engine   HELD    chat-#4c6a152e   3     -      YES   ImagePullB...
```

The block is `HELD` after 3 attempts, so OME has stopped trying the revision. `YES` under `REL` means that the block passed the first checks for a release. `TARGET` shortens long names to an alias whose digest isn't the revision's hash, so get the full name at the end of this step. [Output fields](../../reference/kubectl-ome/instance.md#retry-blocks-output-fields) explains every column, including the `ISSUE` rows that flag a malformed block.

`REASON` is the last failure, cut short. Fix its cause before you release the revision, or the revision fails again:

| Last failure | Cause and fix | Then |
| --- | --- | --- |
| `ImagePullBackOff` or `ErrImagePull` | The image can't be pulled. Push it, or fix the [registry credentials](../../getting-started/private-registries.md). If the image name is wrong, fix it in the InferenceService. | Release the revision, unless you changed the InferenceService. |
| `CreateContainerConfigError` | A Secret, ConfigMap or key that the pod refers to is missing. Create it. | Release the revision. |
| `InvalidImageName` or `InvalidPodSpec` | The image name or the pod spec is invalid. Fix the InferenceService. | Apply the fix. The new revision needs no release. |

Get the full name of the held revision from the InferenceReplica:

```bash
kubectl get inferencereplica chat-engine -n prod \
  -o jsonpath='{.status.retryBlocks[?(@.state=="Held")].targetRevision}'
```

```output
chat-engine-7f9c4d2b
```

## Step 2: Release the revision {#step-3-submit-the-release}

Run the release. `--revision` takes the full name, or its last 8 characters, `7f9c4d2b`, and `-o json` prints the result in full:

```bash
kubectl ome instance release-held chat -n prod --component engine \
  --revision chat-engine-7f9c4d2b -o json
```

If OME runs in a namespace other than `ome`, add `--ome-namespace <namespace>`.

The command checks that the release is safe, prints a preview to stderr, and asks `Confirm this exact action? [y/N]`. Answer `y`: the command has 45 seconds in all, including the prompt. It then checks again, sets the `ome.io/release-held-revision` annotation on the InferenceReplica and prints the result:

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-28T14:03:12.418207153Z",
  "action": "instance release-held",
  "target": {
    "kind": "InferenceReplica",
    "namespace": "prod",
    "name": "chat-engine",
    "uid": "3f6d2c9e-8b41-4d7a-9c0e-5a1b7e2f4d68",
    "resourceVersion": "184213"
  },
  "dryRun": "none",
  "revisionHash": "7f9c4d2b",
  "accepted": true,
  "applied": true,
  "message": "API accepted release annotation request; controller release/convergence not observed.",
  "followUp": "kubectl ome instance retry-blocks chat --component=engine -n prod --context=prod-cluster"
}
```

`applied` means that the API server stored the request. OME then removes the Held block, deletes the annotation and goes back to updating the engine's Instances to the revision. If the InferenceService is paused, OME still releases the block, and the update waits until you [resume it](pause-and-resume-a-rollout.md).

For a trial run, add `--dry-run server`: the API server validates the request without storing it. In a script, or anywhere without a terminal, add `--yes`, which skips only the prompt, never a check. [Guarded actions](../../reference/kubectl-ome/guarded-actions.md) describes the preview, the prompt and the dry-run modes.

## Step 3: Check the release {#step-4-watch-the-outcome}

Show the retry blocks again. Once OME has answered the request, the Held block is gone:

```bash
kubectl ome instance retry-blocks chat --component engine -n prod
```

```output
COMP   STATE   TARGET   ATT   NEXT   REL   REASON
-      EMPTY   -        -     -      NO    -
```

The `RetryBlockReleased` event records the release:

```bash
kubectl get events -n prod --field-selector reason=RetryBlockReleased \
  -o custom-columns=TYPE:.type,OBJECT:.involvedObject.name,MESSAGE:.message
```

```output
TYPE     OBJECT   MESSAGE
Normal   chat     InferenceReplica prod/chat-engine component=engine: Held retryBlock for revision chat-engine-7f9c4d2b removed at operator request (ome.io/release-held-revision annotation)
```

If the revision fails again for a reason that counts, OME starts a new block, with a fresh set of attempts. In a script, [`kubectl ome wait --for=held-revision=unheld`](../../reference/kubectl-ome/wait.md#held-revision-unheld) waits for the release, with `--component`, the full name as `--revision`, and the result's `target.name` and `target.uid` as `--ir-name` and `--ir-uid`.

## Release by annotation

Without the plugin, set the annotation yourself:

```bash
kubectl annotate inferencereplica chat-engine -n prod \
  ome.io/release-held-revision=chat-engine-7f9c4d2b
```

```output
inferencereplica.ome.io/chat-engine annotated
```

The value can be the full name or the hash alone, `7f9c4d2b`. Check the release as in [Step 3](#step-4-watch-the-outcome). The annotation needs the same `patch` permission, but skips the command's safety checks and its prompt. If the annotation is already there, kubectl changes it only with `--overwrite`, which replaces a request that OME hasn't answered yet: see [A release request is already waiting](#a-release-request-is-already-waiting).

## Troubleshooting

Errors print to stderr as `error: <message>`. [Exit codes](../../reference/kubectl-ome/instance.md#exit-codes) lists what `release-held` returns.

### The command refuses the release

A refusal sends nothing. These are the refusals you're most likely to see:

| Message | What to do |
| --- | --- |
| `action refused: no exact current valid Held retry block is releasable` | Pass the full name from [Step 1](#step-1-find-the-held-revision), or its hash, and check that `retry-blocks` shows the block as `HELD`. On a `STATUS_STALE` or `PARENT_STALE` row, wait for OME to catch up, then try again. |
| `action refused: a release-held mailbox is already present; inspect instance retry-blocks` | An earlier request is waiting. See [A release request is already waiting](#a-release-request-is-already-waiting). |
| `action refused: active runtime is unavailable, inconsistent or unbound` | The command can't confirm the runtime that the InferenceService runs. Check that the runtime exists and is enabled, that you can read it, and that `--ome-namespace` names OME's namespace. |
| `action refused: controller safety evidence is stale or inconsistent` | The InferenceReplica changed while the command read it. Run the command again. |

If your machine's clock runs behind the cluster's, recent failures look like they're in the future, and the command refuses the release. Fix the clock, then run the command again. [Refusals](../../reference/kubectl-ome/instance.md#release-held-refusals) lists every refusal and its causes.

### A release request is already waiting

An earlier request is still in the InferenceReplica's `ome.io/release-held-revision` annotation. Wait for OME to answer it, then check the blocks with `kubectl ome instance retry-blocks`. If the annotation stays, check that the OME controller is running. To withdraw the request, remove the annotation:

```bash
kubectl annotate inferencereplica chat-engine -n prod ome.io/release-held-revision-
```

```output
inferencereplica.ome.io/chat-engine annotated
```

### Other errors

| Message | What to do |
| --- | --- |
| `held-release preview became stale; inspect instance retry-blocks and retry explicitly` or `guarded held-release rejected; inspect instance retry-blocks` | Something changed after the preview, or couldn't be read again, and nothing was stored. Check `retry-blocks`, and run the command again if the revision is still held. |
| `action not confirmed; noninteractive input requires --yes` | Answer `y`, or pass `--yes` when there's no terminal. |
| `required Kubernetes API request failed; check access and connectivity` | A read failed before anything was sent. Check the name, the namespace, the context and your permissions. |
| `held-release request outcome unknown; inspect instance retry-blocks before another explicit request`, or another error that contains `outcome unknown` or `outcome may be unknown` | The API server may have stored the annotation. It can follow a timeout, a network error, a webhook denial or a missing `patch` permission. Before you run the command again, check `retry-blocks` and whether the InferenceReplica has the annotation. |

### OME skips the release

A `RetryBlockReleaseSkipped` event means that OME answered a request without removing a block, and deleted the annotation. When no block matches the value, its message ends with `release requested for revision "<value>" but no retryBlock exists for it; nothing to release`. When the block is `Backoff` or `RetryInProgress`, so OME is still retrying the revision, it ends with `retryBlock for revision <name> is <state>, not Held; nothing to release`. Send a new request if `retry-blocks` still shows the revision as held.

### The revision is held again

A new `RetryHeld` event after the release means that the revision failed again and used up its new attempts. Read the reason with `retry-blocks`, fix the cause, and release the revision again, or change the InferenceService.

## Next steps

- [kubectl ome instance](../../reference/kubectl-ome/instance.md): the flags, output and refusals of `retry-blocks` and `release-held`.
- [kubectl ome wait](../../reference/kubectl-ome/wait.md#held-revision-unheld): wait for a release in a script.
- [Reset failed Instances](../omenative/reset-failed-instances.md): start `Failed` Instances over once you've fixed the cause.
- [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md): how OME rolls a new revision out to a component's Instances.
- [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md): find out why an InferenceService isn't serving.
