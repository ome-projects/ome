---
title: Promote or roll back a canary
description: Advance a gated canary step with kubectl ome rollout promote, or abort the canary back to the stable revisions with rollback; both are alpha.
since: v1.3
---

`kubectl ome rollout promote` moves a canary past a step that waits for you, and `kubectl ome rollout rollback` returns the canary to its stable revision at any step. You'll also recover a step that never becomes ready. Both commands are alpha, so their flags and behavior can change between releases. They work on components that use the OMENative deployment mode, and [Deployment modes](../../concepts/architecture/deployment-modes.md) shows how to opt in.

<div class="prerequisites" markdown>

- An [InferenceService](../../concepts/serving/inference-services.md) with one [canary group](../../concepts/rollouts-and-traffic/rollout-groups.md) and a canary in progress. A canary starts when a change gives the group's components a new revision while an earlier one serves. The examples use the InferenceService `chat` in the namespace `prod`.
- The kubectl-ome plugin. See [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md).
- The permissions that [Required RBAC](../../reference/kubectl-ome/guarded-actions.md#required-rbac) lists: the plugin's read rules, and `patch` on InferenceServices.

</div>

## How a canary step gates

Each step moves `capacity`, a share of the component's Instances, to the new revision, and records `traffic`, a weight for it. The canary waits in the phase `Pending` until the step's new Instances are ready. Then the step's gate decides when it moves on:

| Gate | The step sets | The step advances when | To move it on yourself |
| --- | --- | --- | --- |
| Immediate | Only `capacity` and `traffic` | Its traffic weight is recorded. | Nothing: it moves on at once. |
| Timed | `pause` with a `duration` | The duration has passed since the weight was recorded. | Wait for the duration. `promote` refuses. |
| Manual | `pause: {}` | You run `promote`. | Run `kubectl ome rollout promote`. |
| Analysis | `analysis` | Its metric checks pass. | Run `kubectl ome rollout promote --override-analysis --yes`. |

While the canary waits at a gate, its phase is `Paused`, or `Promoting` at the last step. An analysis gate can also roll the canary back on its own when too many checks fail.

!!! warning "Capacity sets the new revision's share of requests"
    The step's `traffic` weight is only recorded in status. Requests go to the ready Instances of both revisions, so `capacity` sets the new revision's share.

The engine of `chat` rolls out in two steps:

```yaml
spec:
  rollout:
    groups:
      - components:
          - engine
        canary:
          steps:
            - capacity: "25%"
              traffic: 10
              pause: {}
            - capacity: "100%"
              traffic: 100
```

The first step moves a quarter of the engine's Instances to the new revision, records a 10% weight and waits for a `promote`. The last step's gate is immediate, so the canary completes once every Instance runs the new revision. A canary keeps the steps it started with, so an edit to the `canary` block while it runs waits for the next rollout, unless you [repin](repin-a-drifted-rollout-plan.md) it.

## Step 1: Check the canary's state

Check which step the canary is at and which gate holds it:

```bash
kubectl ome rollout status chat -n prod
```

```output
FIELD          SERVICE             ENGINE       DECODER   ROUTER
STATE          Unknown             -            -         -
REPORTED       Paused              -            -         -
EVIDENCE       Reported            -            -         -
EPOCH          Unverifiable        -            -         -
COORDINATION   NotApplicable       -            -         -
GROUP          -                   0            -         -
STRATEGY       -                   Canary       -         -
GROUP-PHASE    -                   Paused       -         -
PHASE          -                   Paused       -         -
STEP           -                   1/2          -         -
GATE           -                   Manual       -         -
CAPACITY       -                   25%          -         -
TRAFFIC        -                   10% -> 10%   -         -
ISSUES         EpochUnverifiable   -            -         -
```

The engine's canary is at step 1 of 2, with 25% of its Instances on the new revision. PHASE is `Paused` and GATE is `Manual`, so it waits for a `promote`. TRAFFIC shows the step's weight, then the weight OME recorded. Any service with rollout groups shows STATE `Unknown` and the issue `EpochUnverifiable`, so read REPORTED and the ENGINE column instead. [Output fields](../../reference/kubectl-ome/rollout.md#status-output-fields) describes each row.

For the canary's revisions, run `kubectl ome rollout explain chat -n prod`: REVISIONS shows them as `stable=` and `target=`.

## Step 2: Pass the gate

### At a manual gate {#step-2-promote-a-manually-gated-step}

When GATE shows `Manual`, promote the canary. `promote` sets the `ome.io/rollout-promote` annotation to the canary's revision, and OME moves the canary one step:

```bash
kubectl ome rollout promote chat -n prod
```

The command previews the step, its gate and the revisions, and asks you to confirm. The preview counts steps from 0, so the first step shows as `0 of 2 (zero-based)`. Answer `y` when it shows the step you expect. In a kubeconfig context named `prod-us-east`, the result is:

```output
FIELD           VALUE
action          rollout promote
target          InferenceService/prod/chat
dry-run         none
accepted        Yes
applied         Yes
request-id      -
revision-hash   bbbbbbbb
message         API accepted annotation request; convergence not obse...
follow-up       kubectl ome rollout status chat -n prod --context=pro...
hint            Use -o json or -o yaml for full values.
```

`applied` means that the API server stored the request, not that the canary moved. [Step 3](#step-3-check-that-the-canary-moved-on) shows how to check. `-o yaml` prints the values that the table cuts short.

### At an analysis gate {#step-3-override-an-analysis-gate}

When GATE shows `Analysis`, the canary advances on its own once its metric checks pass, and plain `promote` refuses. To pass the gate without waiting for the checks, add `--override-analysis`, which requires `--yes`. The override passes only the current step's gate, whether the analysis is warming up, sampling or baking.

Because `--yes` skips the prompt, run the command with `--dry-run client` first. It prints the preview, with the analysis state and each metric's results, and sends nothing. Then run:

```bash
kubectl ome rollout promote chat -n prod --override-analysis --yes
```

The preview's warning starts with `ANALYSIS OVERRIDE: bypasses health checks, warm-up and bake`, and the result's message starts with `Analysis override:`:

```output
FIELD           VALUE
action          rollout promote
target          InferenceService/prod/chat
dry-run         none
accepted        Yes
applied         Yes
request-id      -
revision-hash   bbbbbbbb
message         Analysis override: API accepted annotation request; c...
follow-up       kubectl ome rollout status chat -n prod --context=pro...
hint            Use -o json or -o yaml for full values.
```

## Step 3: Check that the canary moved on

OME acts on the request after the command returns, so run the `status` command from Step 1 again. STEP shows the next step, and PHASE shows `Pending` or `Canarying` while the canary moves to it, then `Paused` at its next gate. For `chat`, STEP shows `2/2` while the rest of the Instances move to the new revision. The last step's gate is immediate, so PHASE then shows `Stable` and STEP shows `-`. When the last step pauses or runs analysis, or the group sets `scaleDownDelaySeconds`, the canary waits in `Promoting` first. OME removes the `ome.io/rollout-promote` annotation once it records the advance, and another `promote` refuses until then.

## Roll back the canary

`rollback` returns the canary to its stable revision. It works at any step and gate, even on a `Failed` canary, until the canary completes or rolls back:

```bash
kubectl ome rollout rollback chat -n prod
```

Answer `y` when the preview shows the canary you mean. The result is:

```output
FIELD           VALUE
action          rollout rollback
target          InferenceService/prod/chat
dry-run         none
accepted        Yes
applied         Yes
request-id      -
revision-hash   bbbbbbbb
message         API accepted annotation request; convergence not obse...
follow-up       kubectl ome rollout status chat -n prod --context=pro...
hint            Use -o json or -o yaml for full values.
```

`revision-hash` shows the rejected canary revision. OME records a 0% weight for it at once, but its Instances keep getting requests until OME replaces them with Instances on the stable revision. In a group of several components, each component goes back to its own stable revision. PHASE shows `RollingBack` until only stable Instances remain, then `RolledBack`.

A rollback holds the rejected revision. The spec still asks for it, and `explain` shows it in REVISIONS as `rejected=`. The component stays on its stable revision until a change to the spec produces a revision other than the rejected and stable ones.

The `ome.io/rollout-rollback` annotation stays on the InferenceService after the rollback. While it's there, `pause`, `promote`, `rollback` and `repin` refuse, and so do `kubectl ome scale`, `kubectl ome runtime sync` and `kubectl ome migration start`. Remove it before you apply a fix, or to use those commands:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-rollback-
```

```output
inferenceservice.ome.io/chat annotated
```

Removing the annotation doesn't retry the rejected revision. A canary that its analysis rolled back holds the revision the same way, but without the annotation.

## When a step never becomes ready

When a step's new Instances aren't ready within the ready timeout, as with a missing image tag, the canary stops at `Failed`. This change points the engine of `chat` at a missing tag, with 10 minutes for each step to become ready:

```yaml title="chat.yaml"
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
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 4
    maxReplicas: 4
    runner:
      name: ome-container
      image: docker.io/lmsysorg/sglang:v0.0.0-missing
  rollout:
    groups:
      - components:
          - engine
        canary:
          readyTimeout: 10m
          steps:
            - capacity: "25%"
              traffic: 10
              pause: {}
            - capacity: "100%"
              traffic: 100
```

```bash
kubectl apply -f chat.yaml
```

```output
inferenceservice.ome.io/chat configured
```

Step 1's new Instance never becomes ready, so `status` shows PHASE `Pending`. After 10 minutes, PHASE shows `Failed`, and the canary stays there while the Instances on the stable revision keep serving. The `ome.io/rollout-ready-timeout` annotation overrides `readyTimeout`, and the ome-resources chart sets a default of `15m` for groups that set neither. [`readyTimeout`](../../reference/rollouts/canary-progression.md#readytimeout) has the details.

`promote` refuses on a failed canary. To recover:

1. [Roll back the canary](#roll-back-the-canary), and wait until PHASE shows `RolledBack`.
2. Remove the `ome.io/rollout-rollback` annotation, as that section shows.
3. Fix the image in `chat.yaml`, and apply it again. The new revision starts a fresh canary at step 1.

## Troubleshooting

When `promote` or `rollback` refuses, it prints `error:` and the reason, changes nothing, and exits `1`. [Refusals](../../reference/kubectl-ome/rollout.md#promote-and-rollback-refusals) lists the messages and their causes.

### `promote` refuses at the gate {#promote-refuses-at-the-gate}

`error: action refused: promote requires an active indefinite manual gate; analysis requires explicit override` means the canary isn't waiting at a gate that this `promote` can pass. Run `status` and check:

- GATE: plain `promote` needs `Manual`, and `--override-analysis --yes` needs `Analysis`.
- PHASE: the canary must be `Paused`, or `Promoting` at the last step. `Pending` means the step's new Instances are still starting. `Failed` means the ready timeout passed before they were ready, or while the step's analysis got only inconclusive results: see [When a step never becomes ready](#when-a-step-never-becomes-ready).
- An earlier `promote` that OME is still finishing: wait, and run `status` again.
- HOLD `CanaryPreStep` in `explain`, after a repin: `promote` can't release this hold. Set the promote annotation by hand, as [Repin a drifted rollout plan](repin-a-drifted-rollout-plan.md#what-the-controller-does-next) shows.

### A promote or rollback request is present

`error: action refused: a rollout promote or rollback mailbox is present` means the InferenceService has the `ome.io/rollout-promote` or `ome.io/rollout-rollback` annotation, whatever its value, even `false`. A promote request clears once OME advances the step. The rollback annotation stays after a rollback, as [Roll back the canary](#roll-back-the-canary) describes. A promote request that names an earlier canary revision, as when the target changed first, never advances the canary. Remove it:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-promote-
```

```output
inferenceservice.ome.io/chat annotated
```

### Other refusals and errors

| Message | Meaning |
| --- | --- |
| `action refused: no applicable active rollout or lifecycle work was observed` | The canary already completed or rolled back, or hasn't started. |
| `action refused: globally paused canary cannot consume a request` | The rollout is paused. [Resume it](pause-and-resume-a-rollout.md) first. |
| `action refused: controller safety evidence is stale or inconsistent` | OME is still catching up with a recent change: wait, run `status`, and retry. More than one canary group always causes it. |
| `guarded annotation patch rejected; refresh rollout status and retry explicitly` | The InferenceService changed after the command read it. Check `status`, and run the command again. It exits `3`. |

If the command fails after it sent the patch, as with a timeout, the request may have been stored. Run `status` before you run the command again, as [When the outcome is unknown](../../reference/kubectl-ome/guarded-actions.md#when-the-outcome-is-unknown) explains.

## Next steps

- [Canary progression](../../reference/rollouts/canary-progression.md): how canary steps set capacity and traffic, and how each gate advances.
- [Canary metric analysis](../../reference/rollouts/canary-analysis.md): the checks an analysis gate runs, and when they roll a canary back.
- [Pause and resume a rollout](pause-and-resume-a-rollout.md): hold a rollout where it is, then continue it.
- [Repin a drifted rollout plan](repin-a-drifted-rollout-plan.md): move a running canary to edited steps.
