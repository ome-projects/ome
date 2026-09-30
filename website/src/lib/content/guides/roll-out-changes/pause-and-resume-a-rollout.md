---
title: Pause and resume a rollout
description: Pause in-flight rollout work on an OMENative InferenceService with the alpha kubectl ome rollout pause action, and continue it with resume.
since: v1.3
---

A pause holds the rollout of an [InferenceService](../../concepts/serving/inference-services.md) while you look into a problem: no update, migration or new [Instance](../../concepts/omenative/instances.md) starts, and a canary stays on its step. A pause lasts until you resume it, and then OME carries on where it stopped. You pause with `kubectl ome rollout pause` and resume with `kubectl ome rollout resume`. Both act on the components that use the OMENative [deployment mode](../../concepts/architecture/deployment-modes.md).

!!! note "Alpha"
    `kubectl ome rollout pause` and `resume` are alpha. Their flags and behavior can change between releases.

<div class="prerequisites" markdown>

- OME v1.3 or later, and an InferenceService `chat` in the namespace `prod` with at least one OMENative component. Multi-node engines and decoders use OMENative by default, and any component can [opt in](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative).
- Rollout or lifecycle work in progress on `chat`, such as the update that a change to `spec.engine` starts. To pause before you change anything, see [Pause with kubectl annotate](#pause-with-kubectl-annotate).
- The kubectl-ome plugin, and the permissions in [Required RBAC](../../reference/kubectl-ome/guarded-actions.md#required-rbac). See [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md).
- If OME isn't installed in the namespace `ome`, add `--ome-namespace <namespace>` to each command.

</div>

## What a pause holds

`pause` sets the `ome.io/rollout-paused` annotation on the InferenceService to `true`. The deeper value, `freeze`, holds repairs too. Only the exact values `true` and `freeze` count, so `True` isn't a pause. A pause holds only the components that use OMENative:

| Work | While paused |
| --- | --- |
| Updates | No Instance starts an update. A [`SurgeThenDrain`](../../concepts/architecture/omenative-update-strategies.md#surgethendrain) update under way stops before its old pods start draining, so old and new pods run side by side until you resume. On a multi-pod Instance it stops on its current step, even mid-drain with old pods out of service. Other updates under way finish. |
| New Instances | None start, whether you raise the replicas or an autoscaler does. One already being created can finish. |
| Migrations | None start or advance. |
| Canaries | Stay on their step, and a revision that appears during the pause starts no canary. A promote or rollback request waits until the pause ends. |
| [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md) | Blue-green, rolling-update and sequential groups stop in the phase `Paused`. |
| Spec changes | Roll out when you resume. Setting or removing the annotation starts no rollout of its own. |
| Repairs | At `true`, repairs go on as the [restart policy](../../concepts/omenative/instance-restart-policy.md) sets them. At `freeze`, no repair starts, and one under way finishes. The kubelet still restarts containers at either depth. |
| Scale-down and deletion | Go ahead. |

An Instance's [readiness deadline](../omenative/set-instance-readiness-deadlines.md) stops during the pause, and restarts in full when you resume. Other clocks keep running and act as soon as you resume. A canary whose timed gate ran out advances, and a canary step still waiting for its new Instances fails if its ready timeout passed. A migration past its deadline fails, and so does an attempt whose pod stayed unschedulable or stuck past its grace period.

!!! warning "Deleted pods wait for the resume"
    Under the restart policy `None`, the default for single-pod Instances, a pod that's deleted during a pause, for example by a node drain, isn't replaced until you resume. Until then, a single-pod Instance that lost its pod shows `Pending`.

While the service is paused, `kubectl ome rollout promote` and `rollback` refuse, and `kubectl ome migration start` refuses a new migration.

## Step 1: Pause the rollout

```bash
kubectl ome rollout pause chat -n prod
```

The command checks that `chat` has work to hold, prints a preview of the change to stderr, and asks `Confirm this exact action? [y/N]`. The preview's `Affected` row lists the components that the pause holds. After you answer `y`, it prints the result:

```output
FIELD           VALUE
action          rollout pause
target          InferenceService/prod/chat
dry-run         none
accepted        Yes
applied         Yes
request-id      -
revision-hash   -
message         API accepted annotation request; convergence not obse...
follow-up       kubectl ome rollout status chat -n prod --context=pro...
hint            Use -o json or -o yaml for full values.
```

`applied` means that the API server stored the annotation, not that OME has acted on it yet. In a script, pass `--yes` instead of answering the prompt. To check the change without storing it, add `--dry-run server`. [Guarded actions](../../reference/kubectl-ome/guarded-actions.md) explains the preview and the result.

Check the annotation:

```bash
kubectl get inferenceservice chat -n prod \
  -o custom-columns='NAME:.metadata.name,PAUSED:.metadata.annotations.ome\.io/rollout-paused'
```

```output
NAME   PAUSED
chat   true
```

## Step 2: Check the hold

OME writes an InferenceReplica named `<InferenceService>-<component>` for each OMENative component, and passes the pause on to it. Check that each one is paused:

```bash
kubectl get inferencereplica -n prod -l ome.io/inferenceservice=chat \
  -o custom-columns='NAME:.metadata.name,PAUSED:.spec.paused,MODE:.spec.pauseMode'
```

```output
NAME          PAUSED   MODE
chat-engine   true     <none>
```

An OMENative decoder or router adds a row of its own. At the `freeze` depth, MODE shows `Freeze`. `kubectl ome rollout explain chat -n prod` also shows the pause, as the hold `GlobalPause` in its `Effective` view.

## Step 3: Resume the rollout

When you're ready, resume the rollout:

```bash
kubectl ome rollout resume chat -n prod
```

The preview shows the annotation that `resume` removes, `ome.io/rollout-paused`, and its value. `resume` ends either depth, `true` or `freeze`. After you answer `y`, it prints the result:

```output
FIELD           VALUE
action          rollout resume
target          InferenceService/prod/chat
dry-run         none
accepted        Yes
applied         Yes
request-id      -
revision-hash   -
message         API accepted annotation request; convergence not obse...
follow-up       kubectl ome rollout status chat -n prod --context=pro...
hint            Use -o json or -o yaml for full values.
```

OME then carries on with the work it held, including any change you made to the spec during the pause. Check that the annotation is gone:

```bash
kubectl get inferenceservice chat -n prod \
  -o custom-columns='NAME:.metadata.name,PAUSED:.metadata.annotations.ome\.io/rollout-paused'
```

```output
NAME   PAUSED
chat   <none>
```

## Pause with kubectl annotate

`pause` needs work in progress, and always sets `true`. Set the annotation yourself to hold a change before you make it, or to hold repairs too. To hold the next change, pause first, then change the spec:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-paused=true
```

```output
inferenceservice.ome.io/chat annotated
```

The change waits until you resume. To hold repairs as well, set `freeze`:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-paused=freeze --overwrite
```

```output
inferenceservice.ome.io/chat annotated
```

To go back, set `true` with `--overwrite`. However you set the pause, `kubectl ome rollout resume` ends it.

## Troubleshooting

A refusal changes nothing. These errors exit `1`, except a patch conflict, which exits `3`. They're the ones you're most likely to see. [Refusals](../../reference/kubectl-ome/rollout.md#pause-and-resume-refusals) and [Target checks](../../reference/kubectl-ome/guarded-actions.md#target-checks) list the rest.

### No active work was observed

```text
error: action refused: no applicable active rollout or lifecycle work was observed
```

`pause` holds only work in progress, and found none. [`kubectl ome rollout status`](../../reference/kubectl-ome/rollout.md#status) shows where the rollout is. To hold a change before you make it, [set the annotation yourself](#pause-with-kubectl-annotate).

### OME's status is stale

```text
error: action refused: controller safety evidence is stale or inconsistent
```

OME's status doesn't match the InferenceService yet, or contradicts itself. This happens most often right after a change. Wait a moment and run the command again. If it keeps failing, check the ISSUES row of `kubectl ome rollout status chat -n prod`, which the `status` [output fields](../../reference/kubectl-ome/rollout.md#status-output-fields) explain.

### The patch conflicts with a newer change

```text
error: guarded annotation patch rejected; refresh rollout status and retry explicitly
```

The InferenceService changed between the preview and the patch, so nothing was applied. Any write changes it, OME's own status updates included, so a long wait at the prompt makes this more likely. Run `kubectl ome rollout status chat -n prod`, then run the command again.

### The service is already paused

```text
error: action refused: service is already paused; freeze is preserved
```

`pause` found the annotation already set to `true` or `freeze`, so there's nothing to do. To change the depth, see [Pause with kubectl annotate](#pause-with-kubectl-annotate).

### The service has no recognized pause

```text
error: action refused: service has no recognized pause
```

`resume` found no pause to end: the annotation is missing, or holds a value other than exactly `true` or `freeze`, such as `True`. Nothing is held. To clear such a value, remove the annotation:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-paused-
```

```output
inferenceservice.ome.io/chat annotated
```

### A promote or rollback request is present {#discard-pending-actions-on-resume}

```text
error: action refused: a rollout promote or rollback mailbox is present
```

The InferenceService has an `ome.io/rollout-promote` or `ome.io/rollout-rollback` annotation. After a canary rollback, the rollback annotation stays until a different target revision appears, as [Roll back the canary](promote-or-roll-back-a-canary.md#roll-back-the-canary) explains.

For `pause`, wait until OME acts on a promote request, or cancel it with `kubectl annotate inferenceservice chat -n prod ome.io/rollout-promote-`. Remove a rollback annotation the same way, with `ome.io/rollout-rollback-`. That doesn't retry the rejected revision.

For `resume`, remove the request in the same patch that ends the pause. `--discard-pending-actions` needs `--yes`:

```bash
kubectl ome rollout resume chat -n prod --discard-pending-actions --yes
```

The result shows `applied` `Yes`. Discarding a request that OME hadn't acted on cancels it, so send it again after the resume if you still want it. To keep the request instead, remove only the pause annotation, and OME acts on it once the pause is gone:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-paused-
```

```output
inferenceservice.ome.io/chat annotated
```

### The runtime is unavailable or inconsistent

```text
error: action refused: active runtime is unavailable, inconsistent or unbound
```

Either no component of `chat` uses OMENative, or the runtime that OME reports differs from the spec, as right after a runtime change. A single-node component uses RawDeployment unless it [opts in](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative), and [`kubectl ome runtime effective`](../../reference/kubectl-ome/runtime.md#effective) shows each component's mode. After a runtime change, wait a moment and run the command again.

### The API request fails

```text
error: required Kubernetes API request failed; check access and connectivity
```

The InferenceService doesn't exist, RBAC denied a request, the admission webhook rejected the patch, or the server or the network failed. The command doesn't show the API server's message. The patch may have been stored anyway, so check the annotation, as at the end of [Step 1](#step-1-pause-the-rollout), before you run the command again. Do the same after `context deadline exceeded` or `API response is not bound to the request; outcome unknown, check rollout status`.

To see a webhook's message, make the same change with `kubectl annotate` and `--dry-run=server`, which stores nothing:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-paused=true --dry-run=server
```

If the webhook admits the change, kubectl prints:

```output
inferenceservice.ome.io/chat annotated (server dry run)
```

## Next steps

- [Promote or roll back a canary](promote-or-roll-back-a-canary.md): advance a gated canary step, or abort the canary.
- [kubectl ome rollout](../../reference/kubectl-ome/rollout.md#pause-and-resume): every flag and refusal of `pause` and `resume`.
- [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md): how OMENative replaces an Instance's pods.
