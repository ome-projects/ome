---
title: Request a transient scale
description: Change how many Instances one OMENative component runs with the alpha kubectl ome scale command, until its autoscaler or OME sets the count again.
since: v1.3
---

`kubectl ome scale` changes how many [Instances](../../concepts/omenative/instances.md) one [OMENative](../../concepts/omenative/overview.md) component runs, and leaves the [InferenceService](../../concepts/serving/inference-services.md) and its other components alone. It checks the component first, and refuses while other work is in progress on it or when the count is outside its bounds. The change is transient: the component's autoscaler, or OME, can set the count again at any time. For a lasting change, edit the component's `minReplicas` and `maxReplicas` instead. The command is alpha, so its flags and behavior can change between releases.

<div class="prerequisites" markdown>

- An InferenceService with a component on OMENative and no rollout, migration or other Instance change in progress on it. [Deployment modes](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative) shows how to opt in.
- The kubectl-ome plugin. See [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md). If OME runs in a namespace other than `ome`, pass it with `--ome-namespace`.
- Permission to patch `inferencereplicas/scale` in the `ome.io` API group, and the read access that [Required RBAC](../../reference/kubectl-ome/guarded-actions.md#required-rbac) lists. A rule that names only `inferencereplicas` doesn't include the subresource.

</div>

## The example service

The steps add a third engine Instance to `chat`, a [prefill-decode](../../concepts/serving/inference-services.md#components) InferenceService in `prod`, for a burst of long prompts. The engine runs prefill and the decoder runs decode, each with its own [InferenceReplica](../../concepts/architecture/deployment-modes.md#omenative), so the request leaves the decoder and the router alone. The engine's `autoscaler` block, which is alpha, sets the class `External`, so OME runs no autoscaler for the engine and keeps the count you set:

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
    minReplicas: 2
    maxReplicas: 4
    autoscaler:
      class: External
  decoder:
    minReplicas: 1
    maxReplicas: 1
  router:
    minReplicas: 1
    maxReplicas: 1
```

`scale` accepts 2 to 4 engine Instances, from `minReplicas` to `maxReplicas`. `--replicas` counts Instances: here one pod each, and on a multi-node component a leader and its workers, which OME adds or removes as a unit. To deploy the service yourself, follow [Serve a prefill-decode model](../omenative/serve-a-prefill-decode-model.md), which lists what this model and runtime need. The third engine Instance needs a fourth GPU node.

## How long a request lasts {#scaling-alongside-an-autoscaler}

Your count lasts until the owner of the count sets its own. The dry run in Step 1 shows the owner's class in its `verified ownership` row:

| Class | Who sets the count again | How long your request lasts |
| --- | --- | --- |
| `HPA`, the default | The HorizontalPodAutoscaler that OME creates. | The HPA can replace it at once with the count its metrics call for. |
| `KEDA` | The ScaledObject that OME creates, through KEDA's HPA. | The same as `HPA`. |
| `External` | Your own autoscaler, if you run one. See [Bring your own autoscaler](bring-your-own-autoscaler.md). | Until your autoscaler, or you, change it. OME keeps any count of 1 or more. |
| `None` | OME, which sets it back to `minReplicas`. | A few moments: your change prompts OME to set the count back. |

## Step 1: Preview with a dry run

Every request needs `--override-autoscaler` and `--yes`, whatever the class. `--override-autoscaler` accepts that the owner can overwrite your count, and `--yes` confirms the request, since `scale` never prompts. So preview each request with a client dry run, which runs every check and sends nothing:

```bash
kubectl ome scale chat -n prod --component engine --replicas 3 --override-autoscaler --yes --dry-run client
```

The preview on stderr shows `/scale spec.replicas` as `2 -> 3`, `bounds` as `2..4` and `verified ownership` as `External / external`. The result on stdout shows `accepted` and `applied` as `No`. [Preview](../../reference/kubectl-ome/scale.md#preview) describes every row. `--dry-run server` also has the API server check the request, including your permission to patch `inferencereplicas/scale`, and stores nothing.

## Step 2: Submit the request

!!! warning "A lower count deletes Instances"
    When you ask for fewer Instances than the component has, OME drains and deletes the extra ones, even while the rollout is paused.

Run the same command without `--dry-run`:

```bash
kubectl ome scale chat -n prod --component engine --replicas 3 --override-autoscaler --yes
```

The command repeats its checks just before it sends the request, and refuses if anything changed. The result shows `target` as `InferenceReplica/prod/chat-engine`, `replicas` as `2 -> 3`, and `accepted` and `applied` as `Yes`. That means the API server stored the count, not that the new Instance is up.

## Step 3: Check the outcome

Run the follow-up command from the result, with `--live-scale` to read the engine's InferenceReplica too:

```bash
kubectl ome autoscale status chat -n prod --live-scale
```

The engine's LIVE-SPEC shows 3, the count you asked for. Right after the request, LIVE-EVIDENCE can show `Stale`; if it does, run the command again. If LIVE-SPEC later shows another count, the owner has set its own.

Then list the Instances:

```bash
kubectl ome instance list chat -n prod
```

A third engine Instance appears. Wait until its [PHASE](../../concepts/omenative/instances.md#instance-phases) is `Ready`. The decoder and the router keep one Instance each.

In a script, [`kubectl ome wait --for=replicas=current`](../../reference/kubectl-ome/wait.md#replicas-current) with `--component=engine --replicas=3` waits until the engine asks for and reports 3 Instances. It exits `0` on a match, even before the new Instance is ready, and `2` when it times out, after 60 seconds or the `--timeout` you set.

## Troubleshooting

When `scale` refuses, it prints `error:` and the reason, changes nothing, and exits `1`. These are the errors you're most likely to see. [Refusals](../../reference/kubectl-ome/scale.md#refusals) lists every refusal.

### Work is active on the component

`error: manual scale refused: selected lifecycle, migration or rollout work is active` means the component is busy. One of its Instances is being created, updated, restarted, migrated or deleted, or a migration, rollout or rollback is in progress. Instances that an autoscaler or your earlier request added or removed count too. Check PHASE in `kubectl ome instance list chat -n prod`, and run `kubectl ome migration status chat -n prod`. `scale` needs the engine's PHASE in `kubectl ome rollout status chat -n prod` to be `Stable` or `Unknown`. If the engine is in a rollout group, its GROUP-PHASE must be `Stable` too.

### The count is out of bounds

`error: manual scale replicas must be positive and within verified bounds and partitions` means `--replicas` is outside the component's `minReplicas` to `maxReplicas`, or below the `partition` of its [rolling update](../../concepts/architecture/omenative-update-strategies.md#pacing-with-rollingupdate). `scale` reads the maximum from `maxReplicas` in the InferenceService or its runtime. Without one, it accepts only the minimum, even when the controller's [replica defaults](../operate-ome/set-replica-defaults.md) give the autoscaler a wider range. To move the bounds, change `minReplicas` and `maxReplicas`.

### The command line is rejected

`error: invalid scale arguments; require one service, component engine|decoder|router, positive base-10 --replicas, supported output/dry-run and valid namespace/context/timeout` means the command line is incomplete or malformed, as when you pass `--override-autoscaler` without `--yes`. `error: manual scale is transient and requires --override-autoscaler --yes for every ownership class` means `--override-autoscaler` is missing. Every class needs it.

### A read fails

`error: required Kubernetes API request failed; check access and connectivity` means a read failed. Check the name, the namespace and your permissions. Without `-n`, `scale` uses your kubeconfig context's namespace.

### The service doesn't run on OMENative

`error: action refused: active runtime is unavailable, inconsistent or unbound` means no component of the service runs on OMENative, or the CLI couldn't resolve the service's serving runtime. [Deployment modes](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative) shows how to opt in.

### The component can't be verified

These errors mean the CLI couldn't confirm who owns the count, or that something changed between its reads:

- `error: manual scale source evidence is unavailable or inconsistent`
- `error: manual scale requires complete current selected replica and Scale evidence`
- `error: action refused: controller safety evidence is stale or inconsistent`
- `error: manual scale source changed; inspect current status and prepare a new request`

Usually the controller is still catching up with a recent change: wait, check LIVE-EVIDENCE in `kubectl ome autoscale status chat -n prod --live-scale`, and run the dry run again. Also check that the component exists and runs on OMENative.

### A promote or rollback request is present

`error: action refused: a rollout promote or rollback mailbox is present` means the InferenceService has an `ome.io/rollout-promote` or `ome.io/rollout-rollback` annotation. A promote annotation left from an earlier rollout can stay. Remove it with `kubectl annotate inferenceservice chat -n prod ome.io/rollout-promote-`. A rollback annotation stays until a different target revision appears, as [Roll back the canary](../roll-out-changes/promote-or-roll-back-a-canary.md#roll-back-the-canary) explains. To remove it yourself, run:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-rollback-
```

```output
inferenceservice.ome.io/chat annotated
```

Removing it doesn't retry the rejected revision.

### The patch is rejected as a conflict

`error: guarded scale precondition rejected; inspect current status and prepare a new request` means the InferenceReplica changed after the CLI read it, often because an autoscaler or OME updated it. Nothing changed, and the command exits `3`. Run a new dry run before you try again.

### The outcome is unknown

`error: scale request outcome unknown; inspect kubectl ome autoscale status before preparing another request` means the request failed without showing whether the API server stored it, as after a timeout or a network error. Check LIVE-SPEC in `kubectl ome autoscale status chat -n prod --live-scale` before you run `scale` again: the CLI never retries. You also see this error when you lack permission to patch `inferencereplicas/scale`, and then nothing changed. A server dry run checks that permission.

## Clean up

When the burst ends, and no engine Instance is being created, scale the engine back to 2 the same way:

```bash
kubectl ome scale chat -n prod --component engine --replicas 2 --override-autoscaler --yes
```

The result shows `replicas` as `3 -> 2`. OME drains and deletes one engine Instance, keeping `Ready` Instances first and lower indices next, as [When the count changes](../../concepts/omenative/migration-and-transient-scale.md#when-the-count-changes) describes.

## Next steps

- [kubectl ome scale](../../reference/kubectl-ome/scale.md): every flag, output row and refusal.
- [Component autoscaling](../../concepts/serving/component-autoscaling.md): set `minReplicas`, `maxReplicas` and the autoscaler for a lasting change.
- [Bring your own autoscaler](bring-your-own-autoscaler.md): hand a component's count to an autoscaler that you run.
- [Migration and transient scale](../../concepts/omenative/migration-and-transient-scale.md): which Instances OME keeps when the count changes.
