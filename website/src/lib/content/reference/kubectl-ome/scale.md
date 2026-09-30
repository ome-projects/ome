---
title: kubectl ome scale
description: Request a temporary Instance count for one component of an OMENative InferenceService (alpha).
since: v1.3
---

`kubectl ome scale` changes how many [Instances](../../concepts/omenative/instances.md) one component of an [InferenceService](../../concepts/serving/inference-services.md) runs on OMENative, without editing the InferenceService. Use it for a short burst or a test. The count is transient: the component's autoscaler, or OME, can set it again at any time. For a lasting change, set `minReplicas` and `maxReplicas` instead. [Migration and transient scale](../../concepts/omenative/migration-and-transient-scale.md) explains what OME does with the count, and [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md) walks through a request.

```text
kubectl ome scale INFERENCESERVICE [flags]
```

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

Every request needs `--component`, `--replicas`, `--override-autoscaler` and `--yes`, whatever the component's autoscaler. `--override-autoscaler` acknowledges that the owner of the count can overwrite the request, and `--yes` confirms it. The command never prompts, so preview a request with `--dry-run client` or `--dry-run server` first. [Guarded actions](guarded-actions.md) describes the contract that every kubectl-ome action follows.

## What the request changes

The command sets `spec.replicas` through the `/scale` subresource of the component's InferenceReplica, which is named `<service>-<component>`, such as `chat-engine`. It leaves the InferenceService, its runtime, its autoscaler and the other components as they are. The component's autoscaler class decides who sets the count next, as [Component autoscaling](../../concepts/serving/component-autoscaling.md) describes:

| Class | What happens to the request |
| --- | --- |
| `HPA`, the default | The HorizontalPodAutoscaler that OME creates can overwrite it at once, with the count its metrics call for. |
| `KEDA` | The KEDA ScaledObject that OME creates, or the HorizontalPodAutoscaler that KEDA generates from it, can overwrite it at once. |
| `External` | Your own autoscaler can overwrite it at once. See [Bring your own autoscaler](../../guides/scale-and-migrate/bring-your-own-autoscaler.md). |
| `None` | OME sets the count back to the component's minimum within moments: `minReplicas`, a replica default, or 1. |

!!! warning "A lower count deletes Instances"
    When the count goes down, OME drains and deletes Instances, even on a stable, paused InferenceService. [When the count changes](../../concepts/omenative/migration-and-transient-scale.md#when-the-count-changes) says which Instances it keeps.

A result with `applied` as `Yes` means the API server stored the count, not that the Instances have changed. To check, run the `follow-up` command with `--live-scale`, which shows the InferenceReplica's count as LIVE-SPEC, and list the Instances with [`kubectl ome instance list`](instance.md#list). In a script, pass `target.name` and `target.uid` from `-o json` to [`kubectl ome wait --for=replicas=current`](wait.md#replicas-current) as `--ir-name` and `--ir-uid`.

### Bounds {#bounds}

`--replicas` counts Instances: one pod each, or a leader and its workers on a multi-node component. It must be within the component's bounds, `minReplicas` to `maxReplicas`, as the InferenceService or its runtime sets them. When neither sets `maxReplicas`, `scale` accepts only `minReplicas`, or 1 when that's unset or 0. This holds even when OME's [replica defaults](../../guides/operate-ome/set-replica-defaults.md) give the autoscaler a wider range, so set `maxReplicas` first if you need more room.

`--replicas` must be at least 1. To scale an idle component to zero, run it as a RawDeployment with a KEDA autoscaler: see [Scale to zero with KEDA](../../guides/scale-and-migrate/scale-to-zero-with-keda.md).

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--component` | No default | Required selected component: engine, decoder or router |
| `--dry-run` | `none` | Dry run: none, client or server (all retain safety reads) |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output: table, wide, json or yaml |
| `--override-autoscaler` | `false` | Authorize a transient reconciliation-overwritten request; requires --yes |
| `--replicas` | No default | Required positive base-10 logical Instance count (1..2147483647) |
| `--yes` | `false` | Confirm this exact transient request without a terminal prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

Besides the read rules in the [overview](overview.md#required-rbac), `scale` needs `patch` on `inferencereplicas/scale`: see [Required RBAC](guarded-actions.md#required-rbac). A rule that names only `inferencereplicas` doesn't cover the subresource.

## How the action runs

`scale` follows the [guarded action sequence](guarded-actions.md#the-shared-sequence). It reads the InferenceService, its runtime and the component's InferenceReplica, and [refuses](#refusals) when the request isn't safe now. Otherwise it prints a [preview](#preview) to stderr, reads everything again, sends the patch and prints the result to stdout. Every dry-run mode makes the same reads and checks as a real request. A client dry run sends nothing. A server dry run has the API server check the patch, including your permission to patch `inferencereplicas/scale`, and store nothing.

After it sends the patch, the command can fail with these messages:

| Message | Exit | What happened, and what to do |
| --- | --- | --- |
| `guarded scale precondition rejected; inspect current status and prepare a new request` | `3` | The InferenceReplica changed after the command read it, so nothing changed. Run a new dry run. |
| `scale request outcome unknown; inspect kubectl ome autoscale status before preparing another request` | `1` | The patch failed, as on a timeout, Ctrl-C or a server error, so the count may be stored. Check [`kubectl ome autoscale status`](autoscale.md) with `--live-scale` before you run `scale` again: the command never retries. A denied patch prints this too, and changes nothing. |
| `write scale result failed; API acceptance may already have occurred; inspect current status` | `1` | The command couldn't print its result, and the count may be stored. Check `kubectl ome autoscale status` with `--live-scale`. |

## Refusals

A refusal changes nothing and exits `1`. [Troubleshooting](../../guides/scale-and-migrate/request-a-transient-scale.md#troubleshooting) in Request a transient scale says what to do about the common ones.

| Message | Cause |
| --- | --- |
| `invalid scale arguments; require one service, component engine\|decoder\|router, positive base-10 --replicas, supported output/dry-run and valid namespace/context/timeout` | A missing or malformed argument: not exactly one InferenceService name, a `--component` other than `engine`, `decoder` or `router`, a `--replicas` that isn't a plain number of 1 or more, `--override-autoscaler` without `--yes`, or an invalid flag value. |
| `required Kubernetes API request failed; check access and connectivity` | The kubeconfig couldn't be loaded, or a read failed, as when the InferenceService doesn't exist or you can't read it. |
| `scale namespace is unavailable or invalid` | Without `-n`, the kubeconfig context gives an empty or invalid namespace. |
| `scale context is unavailable or unsafe` | The kubeconfig context name can't be read or shown safely. |
| `action refused: target identity is missing or unsafe` | The InferenceService's identity, such as its UID or generation, is missing, invalid or unsafe to show. |
| `action refused: target is being deleted` | The InferenceService is being deleted. |
| `action refused: safety inputs exceed inspection bounds` | The InferenceService or its InferenceReplica is too large for the command to check safely. |
| `action refused: placement sources and derived services cannot be mutated` | The InferenceService takes part in alpha multi-cluster placement, as a source or as a service that placement created. |
| `action refused: active runtime is unavailable, inconsistent or unbound` | The command can't resolve the service's runtime or match it to the one OME reports, or no component runs on OMENative. |
| `manual scale source evidence is unavailable or inconsistent` | The command couldn't verify where the component's count and bounds come from. The usual causes: a runtime sync is pending, the live runtime drifted, the alpha `spec.scalingPolicy.mode` isn't `Independent`, or the component doesn't exist or doesn't run on OMENative. |
| `manual scale requires locally verifiable autoscaling; winning policy resolution is unavailable` | The component gets its autoscaling from an alpha AutoscalerPolicy, through `autoscalerPolicyRef`, with no inline `autoscaler`. |
| `manual scale requires complete current selected replica and Scale evidence` | The InferenceReplica's count is unset or 0, it doesn't match the component, or it changed between the command's reads. |
| `action refused: controller safety evidence is stale or inconsistent` | OME hasn't caught up with a recent change, something changed after the preview, or [`kubectl ome rollout status`](rollout.md#status) shows an issue other than `EpochUnverifiable` or `AnalysisInconclusive`. |
| `manual scale refused: selected lifecycle, migration or rollout work is active` | A rollout, rollback, migration or Instance operation is in progress on the component, or its rollout group isn't `Stable`. |
| `manual scale is transient and requires --override-autoscaler --yes for every ownership class` | `--override-autoscaler` is missing. |
| `manual scale replicas must be positive and within verified bounds and partitions` | `--replicas` is outside the component's [bounds](#bounds), or below the partition of its rolling update. |
| `action refused: a rollout promote or rollback mailbox is present` | The InferenceService has the `ome.io/rollout-promote` or `ome.io/rollout-rollback` annotation. A rollback annotation stays until a different target revision appears: see [`promote` and `rollback`](rollout.md#promote-and-rollback). |
| `action refused: a logical annotation value is unsafe to preview` | A runtime, model or runtime revision that the request depends on has a name or UID that can't be shown safely. |
| `write scale preview failed; no request submitted` | The command couldn't write the preview to stderr. |
| `manual scale source changed; inspect current status and prepare a new request` | The InferenceService or its runtime changed after the preview, or couldn't be read again. |

A runtime sync is pending while the InferenceService's `ome.io/runtime-sync` annotation differs from `status.lastRuntimeSyncToken`. It also stays pending if you remove the annotation after OME acted on it. To clear it without a sync, set the annotation to the value of `status.lastRuntimeSyncToken`, or remove the annotation when that field is empty. [Roll forward to the latest runtime](../../concepts/runtimes/runtime-revisions.md#roll-forward-to-the-latest-runtime) explains when a value waits.

## Output fields

### Preview

Before the patch, in every dry-run mode, the command prints this table to stderr under the title `ALPHA guarded scale preview (not controller convergence)`:

| Row | What it shows |
| --- | --- |
| `context`, `workload namespace`, `OME namespace` | The kubeconfig context, the InferenceService's namespace, and the namespace from `--ome-namespace`. |
| `parent`, `parent UID`, `parent generation` | The InferenceService's name, UID and `metadata.generation`. |
| `IR`, `IR UID`, `IR RV`, `IR generation` | The InferenceReplica that the patch targets. The patch tests its UID and resourceVersion. |
| `parent stamp` | The InferenceReplica's `ome.io/parent-generation` annotation, which must equal `parent generation`. |
| `component` | The component, from `--component`. |
| `/scale spec.replicas` | `<current> -> <requested>`. |
| `bounds` | `<min>..<max>`: the counts that `scale` accepts. See [Bounds](#bounds). |
| `verified ownership` | `<class> / <owner>`: `HPA / ome`, `KEDA / ome`, `External / external` or `None / none`. |
| `verified source` | Where the autoscaling settings come from: `isvc`, `runtime`, or `default` when neither sets them. |
| `dry-run` | `none`, `client` or `server`. |
| `reported Instances` | `<replicas> total; <ready> ready; <serving> serving; <available> available`, from the InferenceReplica's status. |
| `override / transient`, `lifecycle / canary`, `parent freshness` | Always `true / true`, `No observed selected active work` and `Unverifiable (advisory)`. |
| `pinned sibling proof`, `IR proofs revalidated` | The other InferenceReplicas that the command read during a rollout run, if any, and how many InferenceReplicas it reads again before the patch. |

A long value continues on the next row. Warnings follow the table: the request is transient, the owner of the count can overwrite it, a scale-down can drain Instances even on a paused InferenceService, and the patch guards only the InferenceReplica.

### ActionResult

The result on stdout is an [ActionResult](guarded-actions.md#the-actionresult). Its table shows:

| Field | What it shows |
| --- | --- |
| `action` | `scale`. |
| `target` | The InferenceReplica, as `InferenceReplica/<namespace>/<name>`. |
| `component`, `replicas`, `bounds`, `class / owner`, `source`, `dry-run`, `instances` | As in the matching preview rows. |
| `field`, `override`, `transient`, `parent freshness` | Always `/scale spec.replicas`, `Yes`, `Yes` and `Unverifiable`. |
| `accepted` | Whether the API server accepted the patch. Always `No` for a client dry run. |
| `applied` | Whether the count was stored. Always `No` for a dry run. |
| `message` | `Validated locally; no scale patch sent.` for a client dry run, `API dry-run accepted; no changes persisted.` for a server dry run, and otherwise `API accepted transient scale request; convergence not observed.` |
| `follow-up` | `kubectl ome autoscale status <service> -n <namespace> --context=<context>`. |
| `hint` | `Use -o json or -o yaml for full values.`, since the table cuts values at 56 characters. |

`-o wide` adds the identities of the InferenceReplica, the InferenceService and each object the request depends on, a `warning` row for each warning, and the `issue` row `ReportedDesiredCountDiscrepancy` when the InferenceReplica's `status.replicas` differs from its `spec.replicas`.

## Examples

In these examples, the engine of `chat` in `prod` runs one Instance, with bounds of 1 to 10. Its class is `None`, so OME sets the count back to the component's minimum within moments.

Preview a request for 3 engine Instances with a client dry run:

```bash
kubectl ome scale chat -n prod --component engine --replicas 3 --override-autoscaler --yes --dry-run client
```

It prints this preview to stderr:

```output
ALPHA guarded scale preview (not controller convergence)
FIELD                   VALUE
context                 prod-cluster
workload namespace      prod
OME namespace           ome
parent                  chat
parent UID              uid-chat
parent generation       7
IR                      chat-engine
IR UID                  uid-ir
IR RV                   81
IR generation           2
parent stamp            7
component               engine
/scale spec.replicas    1 -> 3
bounds                  1..10
verified ownership      None / none
verified source         isvc
override / transient    true / true
dry-run                 client
reported Instances      1 total; 1 ready; 1 serving; 1 available
lifecycle / canary      No observed selected active work
parent freshness        Unverifiable (advisory)
IR proofs revalidated   1
This is a transient /scale request, not a change to parent replica intent.
The ISVC projector restores ISVC/runtime-derived desired replicas on
reconciliation.
Scale-down may drain/delete logical Instances; pause does not freeze teardown.
IR UID/resourceVersion CAS is not a multi-object transaction.
```

Then it prints the result to stdout:

```output
FIELD              VALUE
action             scale
target             InferenceReplica/prod/chat-engine
component          engine
field              /scale spec.replicas
replicas           1 -> 3
bounds             1..10
class / owner      None / none
source             isvc
override           Yes
transient          Yes
dry-run            client
accepted           No
applied            No
instances          1 total; 1 ready; 1 serving; 1 available
parent freshness   Unverifiable
message            Validated locally; no scale patch sent.
follow-up          kubectl ome autoscale status chat -n prod --context=p...
hint               Use -o json or -o yaml for full values.
```

`--dry-run server` prints the same, with `server` in the `dry-run` rows, `accepted` as `Yes` and the message `API dry-run accepted; no changes persisted.`

Send the request, and print the result as JSON:

```bash
kubectl ome scale chat -n prod --component engine --replicas 3 --override-autoscaler --yes -o json
```

The preview is the same, with `none` in the `dry-run` row. The result on stdout is:

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-15T21:00:00Z",
  "action": "scale",
  "target": {
    "kind": "InferenceReplica",
    "namespace": "prod",
    "name": "chat-engine",
    "uid": "uid-ir",
    "resourceVersion": "81"
  },
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted transient scale request; convergence not observed.",
  "followUp": "kubectl ome autoscale status chat -n prod --context=prod-cluster",
  "scale": {
    "component": "engine",
    "subresource": "/scale",
    "field": "spec.replicas",
    "priorReplicas": 1,
    "requestedReplicas": 3,
    "minReplicas": 1,
    "maxReplicas": 10,
    "class": "None",
    "managedBy": "none",
    "specSource": "isvc",
    "override": true,
    "transient": true,
    "parent": {
      "kind": "InferenceService",
      "namespace": "prod",
      "name": "chat",
      "uid": "uid-chat",
      "generation": 7
    },
    "sources": [
      {
        "kind": "ServingRuntime",
        "namespace": "prod",
        "name": "simple",
        "uid": "uid-runtime",
        "generation": 1
      }
    ],
    "replicaGeneration": 2,
    "parentGenerationStamp": 7,
    "parentFreshness": "Unverifiable",
    "instances": {
      "replicas": 1,
      "ready": 1,
      "serving": 1,
      "available": 1
    },
    "issues": null,
    "warnings": [
      "AlphaAction",
      "AutoscalerCanOverwrite",
      "NotMultiObjectTransaction",
      "ScaleDownCanDrain",
      "TransientReplicaRequest"
    ]
  }
}
```

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | A request or dry run that printed its result. |
| `1` | General error | Any failure but a conflict: invalid arguments, a failed read, a refusal, or a patch whose outcome is unknown. |
| `3` | Mutation conflict | A patch the API server rejected because the InferenceReplica changed after the command read it. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
