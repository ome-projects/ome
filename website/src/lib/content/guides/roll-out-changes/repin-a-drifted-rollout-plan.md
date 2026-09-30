---
title: Repin a drifted rollout plan
description: Apply an edit to spec.rollout or a RolloutPolicy to the active rollout run, keeping its progress, with the alpha kubectl ome rollout repin action.
since: v1.3
---

When you edit a rollout's steps or analysis mid-run, `kubectl ome rollout repin` moves the run in progress onto the edited plan, and the run keeps its progress. Without a repin, a run keeps the plan it pinned when it started, and an edit to `spec.rollout` or a RolloutPolicy waits for the next run. The command works only on components that use OMENative: [Deployment modes and OMENative](../../concepts/architecture/deployment-modes.md) shows which components use it by default and how to opt others in.

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

<div class="prerequisites" markdown>

- An InferenceService whose components use OMENative, with a rollout run in progress, and an edit to its `spec.rollout` or to a RolloutPolicy it uses. The examples use `chat` in the namespace `prod`.
- If a group uses a rollout policy, the RolloutPolicy API. It's alpha and off by default: turn it on with `ome.rolloutPolicy.enabled` on both the ome-crd and ome-resources charts. See [Rollout policy](../../concepts/rollouts-and-traffic/rollout-policy.md).
- The [kubectl-ome plugin](../../reference/kubectl-ome/overview.md).
- Permission to `get` and `patch` `inferenceservices` in the `ome.io` group in the namespace, as [Required RBAC](../../reference/kubectl-ome/guarded-actions.md#required-rbac) shows.

</div>

## What a repin can change

A repin applies edits to a group's progression, whether the group sets it inline or takes it from a rollout policy. Edits to the groups' shape wait for the next run, and `repin` refuses them.

| Edit | Applies |
| --- | --- |
| A group's `canary` block, including its steps, `prometheus`, `scaleDownDelaySeconds` and `readyTimeout`, or its `blueGreen` or `rollingUpdate` block | Now, with a repin |
| The number of groups, or a group's progression kind, components, order, soak or `maintainRatio` | At the next run |

DRIFT tracks progressions and the number of groups, so an edit to a group's components, order, soak or `maintainRatio` alone leaves it `False`.

The run keeps its ID, start time and target revisions. The canary keeps its step, or takes the last step if the new plan has fewer steps, and a canary that finished stays finished. When that step's traffic weight is higher than the weight OME recorded for the canary, the canary holds before the step until you [release it](#what-the-controller-does-next).

!!! warning "Capacity sets the canary's share of requests"
    A canary's share of requests follows its share of ready Instances, which each step's `capacity` sets. The step's `traffic` weight is only recorded in status. A repin moves the canary to the new step's capacity, even while it holds before the step.

## Step 1: Confirm the drift

Check the rollout plan's conditions:

```bash
kubectl get inferenceservice chat -n prod -o custom-columns='PLAN-READY:.status.conditions[?(@.type=="RolloutPlanReady")].reason,DRIFT:.status.conditions[?(@.type=="RolloutPlanDrift")].reason'
```

```output
PLAN-READY   DRIFT
Pinned       SpecNewerThanRun
```

`Pinned` means a run is in progress. DRIFT is `PolicyNewerThanRun` if the group whose plan changed takes its progression from a rollout policy, and `SpecNewerThanRun` otherwise.

To compare the plans, run `kubectl ome rollout explain chat -n prod`. `Effective` is the plan the run follows, and `Live` is the plan that a repin pins. [Output fields](../../reference/kubectl-ome/rollout.md#explain-output-fields) describes each row, including ISSUES `EpochUnverifiable`, which all services with rollout groups show.

## Step 2: Repin the run

Run `repin`. It checks that a repin is safe, previews the `ome.io/rollout-repin` annotation it sets, and asks `Confirm this exact action? [y/N]`:

```bash
kubectl ome rollout repin chat -n prod
```

Answer `y`. The command prints the result:

```output
FIELD                   VALUE
action                  rollout repin
target                  InferenceService/prod/chat
dry-run                 none
accepted                Yes
applied                 Yes
run-id                  chat-0123456789ab
pinned-plan-digest      rp1:56fe0ddd6edd
requested-plan-digest   rp1:1d153db2a5df
group-count             1
message                 API accepted repin annotation; controller consumption...
follow-up               kubectl ome rollout explain chat -n prod --context=pr...
hint                    Use -o json or -o yaml for full values.
```

`applied` means the API server stored the annotation, not that the controller acted on it. The annotation carries the digest of the plan you previewed, and the controller rejects the repin if the plan changed after the preview. The result's digests cover the whole plan, so they differ from each group's digest.

Add `--dry-run server` to have the API server check the request without storing it. Scripts pass `--yes` to skip the prompt. [Guarded actions](../../reference/kubectl-ome/guarded-actions.md) covers the prompt, dry runs and errors.

## Step 3: Check the result

The controller removes the annotation when it acts on it, whatever the outcome. This prints nothing once it has:

```bash
kubectl get inferenceservice chat -n prod \
  -o jsonpath='{.metadata.annotations.ome\.io/rollout-repin}'
```

The controller also records the outcome as an event on the InferenceService. A repin that applied gives a Normal `RolloutPlanRepinned` event, with a message like `run chat-0123456789ab repinned: rp1:56fe0ddd6edd -> rp1:1d153db2a5df`. A Warning `RolloutRepinRejected` event means the controller [rejected the repin](#the-controller-rejects-the-repin).

Check that DRIFT is back to `InSync`:

```bash
kubectl get inferenceservice chat -n prod -o custom-columns='PLAN-READY:.status.conditions[?(@.type=="RolloutPlanReady")].reason,DRIFT:.status.conditions[?(@.type=="RolloutPlanDrift")].reason'
```

```output
PLAN-READY   DRIFT
Pinned       InSync
```

If DRIFT is still `True`, see [DRIFT stays True after the repin](#drift-stays-true-after-the-repin). Then explain the rollout again: the `Effective` view shows the new plan. The controller counts outcomes in the metric `ome_rollout_repin_total`, by `namespace` and `outcome` (`applied` or `rejected`).

## Repin by annotation

Without the plugin, set the `ome.io/rollout-repin` annotation yourself, to the digest of the current plan. Only the controller's digest check guards this path. First check that DRIFT is `True`, the groups kept [their shape](#what-a-repin-can-change), and no promote or rollback waits. With more than one canary group, the controller fits only the first to its new steps, so another group's traffic weight can rise without a hold.

Get the digest the controller reports for each group, after the group's index:

```bash
kubectl get inferenceservice chat -n prod -o jsonpath='{range .status.rollout.groups[*]}{.index}{" "}{.observedDigest}{"\n"}{end}'
```

```output
0 rp1:bbbbbbbbbbbb
```

A blank digest means one of two things. For a group that rolls out blue-green by default, because it sets no progression or policy, use `rp1:cca45ec1fb0f`. For a group whose rollout policy is missing, fix the policy first, or the controller rejects the repin.

Join the digests with `|`, in group order, and hash them. `sha256sum` works the same as `shasum -a 256`. For one group:

```bash
printf '%s' 'rp1:bbbbbbbbbbbb' | shasum -a 256 | cut -c1-12
```

```output
1d153db2a5df
```

For two groups, the input is like `'rp1:cca45ec1fb0f|rp1:bbbbbbbbbbbb'`. Put `rp1:` in front of the result, and set the annotation:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-repin=rp1:1d153db2a5df
```

```output
inferenceservice.ome.io/chat annotated
```

To replace a repin that still waits, add `--overwrite`. The value `now` in place of a digest skips the digest check, and the controller pins whatever the plan is when it acts. Then check the result as in [Step 3](#step-3-check-the-result).

## Troubleshooting

A refused `repin` changes nothing and exits `1`, and a conflict exits `3`. [Refusals](../../reference/kubectl-ome/rollout.md#repin-refusals) lists them all, and [Guarded actions](../../reference/kubectl-ome/guarded-actions.md) covers the errors that all actions share.

| Message | Cause | Fix |
| --- | --- | --- |
| `rollout repin refused: controller plan evidence is missing, stale or inconsistent` | The controller hasn't reported your edit yet. | Wait a few seconds and retry. If the refusal stays, check the group's rollout policy, the spec, and your computer's clock against the cluster's. |
| `rollout repin refused: live and pinned rollout topology differ` | The edit changed [the groups' shape](#what-a-repin-can-change). | Undo those changes to repin the rest now, or let the edit wait for the next run. |
| `rollout repin refused: a rollout action mailbox is already present` | A repin, promote or rollback annotation is on the InferenceService. | Wait for the controller to act on a repin or promote, and retry. Remove a rollback annotation yourself before you apply a fix, as [Roll back the canary](promote-or-roll-back-a-canary.md#roll-back-the-canary) shows. |
| `rollout repin refused: the current plan is already pinned` | The run already follows the current plan. | If DRIFT is `True`, see [DRIFT stays True after the repin](#drift-stays-true-after-the-repin). |
| `rollout repin refused: no active pinned run` | No rollout run is in progress. | None needed: the next run pins the current plan. |
| `rollout repin refused: this command supports at most one canary group` | The spec has more than one canary group. | Let the edit wait for the next run. |
| `guarded annotation patch rejected; refresh rollout explain and retry explicitly` | The InferenceService changed after the command read it, for example in a status update. | Explain the rollout again, then run `repin` again. |
| `rollout repin request outcome unknown; do not replay, check rollout explain` | The API server may have stored the annotation. | Check the result as in [Step 3](#step-3-check-the-result) before you run `repin` again. |
| `required Kubernetes API request failed; check access and connectivity` | You lack `get` or `patch`, the InferenceService doesn't exist, or the API server is unreachable. | Check [Required RBAC](../../reference/kubectl-ome/guarded-actions.md#required-rbac), the name and your connection. |

### The controller rejects the repin

A Warning `RolloutRepinRejected` event means the run kept its pinned plan. `expected render digest ... (a concurrent edit landed; re-issue with the current digest)` means the plan changed after your preview. Explain the rollout, and repin again if you still want the current plan. `the current source does not render` names the problem, for example `PolicyNotFound`. Fix it, then repin again.

### The canary holds after the repin {#what-the-controller-does-next}

Explain shows HOLD `CanaryPreStep`, and `kubectl ome rollout promote` refuses: the new step's traffic weight is higher than the weight OME recorded for the canary. PHASE is `Pending` until the canary reaches the step's capacity, then `Paused`. Release the hold with the promote annotation, set to the canary revision that explain shows as `target=` in REVISIONS:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-promote=bbbbbbbb
```

```output
inferenceservice.ome.io/chat annotated
```

The controller ends the hold and records the step's weight, and the step's gate then applies as usual. To back out instead, [roll the canary back](promote-or-roll-back-a-canary.md#roll-back-the-canary).

### DRIFT stays True after the repin

DRIFT stays `True`, or turns `True` again right after a repin, and the `RolloutPlanDrift` condition's message contains `live render (unresolvable) differs from pinned rp1:cca45ec1fb0f`. Set `blueGreen: {}` on the group that sets no progression and names no rollout policy: the plan stays the same, and DRIFT returns to `False/InSync`. This is a known bug.

## Next steps

- [kubectl ome rollout](../../reference/kubectl-ome/rollout.md#repin): the `repin` command in full.
- [Guarded actions](../../reference/kubectl-ome/guarded-actions.md): the contract every mutating kubectl-ome command follows.
- [Promote or roll back a canary](promote-or-roll-back-a-canary.md): move a canary through its steps.
- [Rollout policy](../../concepts/rollouts-and-traffic/rollout-policy.md): share progressions between InferenceServices.
- [Runtime revisions and pinning](../../concepts/runtimes/runtime-revisions.md): the other pin, for runtime snapshots and `ome.io/runtime-sync`.
