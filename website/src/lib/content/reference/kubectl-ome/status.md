---
title: kubectl ome status
description: "Show why an InferenceService is or isn't ready, in one report: conditions, pods, model, runtime, rollout, autoscaling, traffic and warning events."
since: v1.3
---

`kubectl ome status` shows in one report why an [InferenceService](../../concepts/serving/inference-services.md) is or isn't ready. It covers the Ready condition, each component's pods, the rollout, autoscaling, placement and traffic, the runtime the service runs with and the accelerator it asks for, and recent Warning events. It only reads. Start with it when a service misbehaves, then run the detail command for the part that looks wrong.

```text
kubectl ome status INFERENCESERVICE [flags]
```

It reads the InferenceService, its pods, and the Warning events of both. When the service names a runtime and isn't a [VirtualDeployment](../../concepts/architecture/deployment-modes.md), it also reads the model, the [serving runtime](../../concepts/runtimes/serving-runtimes.md) and the runtimes it inherits from, and the [runtime revisions](../../concepts/runtimes/runtime-revisions.md) the service names. [Required RBAC](overview.md#required-rbac) lists the permissions it needs.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output format: table, wide, json, or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

Set `--ome-namespace` when OME runs in another namespace: the CLI reads runtime revisions there.

## Output fields

The table has two columns, FIELD and VALUE, with one row per fact, in the order of the sections below. It cuts long values and ends them with `...`. `-o wide` adds the rows marked Wide. `-o json` and `-o yaml` print the whole report, a `StatusReport` with the sections below under `content`, and keep longer values. [Output formats](overview.md#output-formats) covers what every command shares.

`parent status` in a value means the CLI read it from the InferenceService's own status.

The CLI masks the reason and message of the Ready condition and of events. A value that contains `://` shows as `[OMITTED]`, and text that looks like a credential shows as `[REDACTED]`. To read the full text, use `kubectl describe` or `kubectl get events`.

The report's sections come in this order. When one looks wrong, run the command in the last column, with the service's name and the same `-n`:

| Section | What it shows | Look closer with |
| --- | --- | --- |
| [The service](#the-service) | The Ready condition, the model and runtime the spec names, and the generation. | |
| [Pods and components](#pods-and-components) | Ready and total pods, and restarts, for each component. | [`kubectl ome logs`](logs.md); for an OMENative component, [`kubectl ome instance list`](instance.md#list) |
| [Rollout](#rollout) | The rollout state and its issues. | [`kubectl ome rollout status`](rollout.md#status), then [`rollout explain`](rollout.md#explain) |
| [Autoscaling](#autoscaling) | The autoscaler status the controller recorded. | [`kubectl ome autoscale status`](autoscale.md#status) |
| [Placement](#placement) | Alpha. The multi-cluster placement the controller recorded. | [`kubectl ome placement status`](placement.md#status) |
| [Traffic](#traffic) | The traffic policy's readiness, translator and algorithm. | [`kubectl ome traffic status`](traffic.md#status) |
| [Runtime and accelerator](#runtime-and-accelerator) | The runtime the service runs with, its pin, and its accelerator settings. | [`kubectl ome runtime effective`](runtime.md#effective), [`kubectl ome accelerator explain`](accelerator.md#explain) |
| [Warning events and issues](#warning-events-and-issues) | Recent Warning events, and anything the CLI couldn't read or trust. | |

In a healthy report, Ready and each component row start with `True / Valid`, every pod is Ready, and no `Issue` row appears. Some rows read `Unknown` or `Unavailable` on a healthy service too:

- Rollout shows `Unknown` on most services, with the rollout state in `reported=`.
- Traffic shows `Unavailable` when the service declares no traffic behavior.
- Runtime active and Accelerator show `AutoSelectionNotProbed` when the service names a model but no runtime.

The report marks how the CLI got each value with an evidence level:

| Evidence | Meaning |
| --- | --- |
| `Declared` | Read from the spec or annotations of the InferenceService. |
| `Reported` | Read from the status the controller wrote. |
| `Observed` | Read from the API server by this command. |
| `Computed` | Worked out by the CLI from other values. |
| `Unavailable` | The CLI couldn't get the value. |

### The service

| Row | What it shows |
| --- | --- |
| Name, Namespace | The InferenceService. |
| Ready | The Ready condition of the service, as `<status> / <validity>`. |
| Ready reason | The reason of the Ready condition, when its validity is `Valid`. |
| Ready message | Wide. The message of the Ready condition, when its validity is `Valid`. |
| Condition inspection | Wide. How many of the service's conditions the CLI inspected, as `<state> <inspected>/<total>`. |
| Condition warning | Wide. One row for each kind of problem the CLI found in the conditions. |
| Declared runtime | The optional `spec.runtime.name`. |
| Model | The optional `spec.model.name`. |
| Generation | `<generation> observed=<observedGeneration>; advisory Unverifiable`: the service's `metadata.generation`, then the `status.observedGeneration` the controller recorded. |

Ready is one of:

| Ready | Meaning |
| --- | --- |
| `True / Valid`, `False / Valid`, `Unknown / Valid` | The status of the Ready condition. |
| `NotRecorded / Unavailable` | The service has no Ready condition yet. |
| `True / Invalid`, `False / Invalid`, `Unknown / Invalid`, `NotRecorded / Invalid` | A Ready condition is malformed, or two disagree. The Condition warning rows in `-o wide` say which. |

Ready doesn't check `status.observedGeneration`, so `True / Valid` can describe the service as it was before your last change. Compare the two numbers in the Generation row.

Condition inspection is `Complete` when every condition checks out, `Partial` when the CLI found a problem, and `LimitExceeded` when there are too many conditions to inspect. The Condition warning rows are:

| Warning | Meaning |
| --- | --- |
| `OversizedConditionRecord` | A condition is too long to inspect. |
| `InvalidConditionRecord` | A condition is malformed. |
| `FutureConditionTimestamp` | A condition's `lastTransitionTime` is later than the time the CLI read it. |
| `ConflictingReadyConditions` | Two Ready conditions differ in status or severity. |
| `DuplicateReadyConditions` | A Ready condition repeats an earlier one. |

### Pods and components

The CLI counts the pods labelled `ome.io/inferenceservice=<name>` in the service's namespace that have a `component` label of `engine`, `decoder` or `router`. An `Issue` row names each reason it left a pod out (see [Warning events and issues](#warning-events-and-issues)).

| Row | What it shows |
| --- | --- |
| Pod observation | How the pod read went, as `<state> count=<pods> truncated=<bool>`, followed by a reason when something went wrong. count is the number of pods read. |
| Event observation | How the Warning event read went, in the same form. count is the number of events kept. |
| Pod targets skipped | Wide. Always `0` in this release. |
| Event targets skip | Wide. How many counted pods the CLI didn't read events for, past the [limit](#observation-bounds) of 15. |
| `engine`, `decoder`, `router` | One row for each component that the spec declares or that has counted pods, as `<ready> / <validity>; Ready pods=<ready>/<total> restarts=<restarts>`. |
| `<component> Pod total` | Wide. The component's counted pods. |
| `<component> Pod Ready` | Wide. Those of its pods whose PodReady condition is `True`. |
| `<component> restarts` | Wide. The total restarts of all containers in its pods, including init and ephemeral containers. |
| `<component> phases` | Wide. Its pods by phase, as `R=<Running> P=<Pending> F=<Failed> S=<Succeeded> U=<Unknown> deleting=<n>`. A pod being deleted counts under `deleting` as well as under its phase. |
| `<component> evidence` | Wide. `Observed` when the CLI read the pods, `Unavailable` when the pod read failed. |

A component's `<ready> / <validity>` comes from its `EngineReady`, `DecoderReady` or `RouterReady` condition, which the CLI checks the same way as Ready, so it takes the same values. It's `NotRecorded / Unavailable` until the controller sets the condition.

For a component on [OMENative](../../concepts/omenative/overview.md), these rows count pods, not [Instances](../../concepts/omenative/instances.md). An Instance can be a leader pod and its workers. [`kubectl ome instance list`](instance.md#list) shows each Instance with its phase, pods and revision.

Pod observation and Event observation show one of these states:

| State | Meaning |
| --- | --- |
| `Reported` | The CLI read everything within its [limits](#observation-bounds). |
| `Partial` | The CLI read only part, or left pods out. |
| `Unavailable` | The read failed. |

`truncated=true` means the read stopped at a limit or failed partway. The reason is one of:

| Reason | Meaning |
| --- | --- |
| `Forbidden` | Your RBAC doesn't allow the read. |
| `Unauthorized` | The API server didn't accept your credentials. |
| `NotFound` | The API server answered with NotFound. |
| `Timeout` | A request ran out of time. |
| `Unavailable` | The API server was throttling requests or unavailable. |
| `Unreadable` | Any other error. |
| `MalformedPayload` | Pods only. The CLI left out some pods, and no request failed. |

### Rollout

| Row | What it shows |
| --- | --- |
| Rollout | `<state> reported=<reported>`: the STATE and REPORTED rows of [`kubectl ome rollout status`](rollout.md#status). |
| Rollout evidence | `<evidence> / <epoch>`: its EVIDENCE and EPOCH rows. |
| Coordination Ready | Wide. Its COORDINATION row. |
| Rollout issue | Wide. One row for each rollout issue. |
| Rollout warning | Wide. `PartialData`, when there's a rollout issue. |

[`rollout status`](rollout.md#status-output-fields) explains each value, and what each rollout issue means. The state is `Unknown` whenever the rollout evidence is `Reported`, because the CLI can't tie the controller's status to the current generation. That's the case for a service with [rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md), and for most others. Read the `reported=` value instead.

### Autoscaling

The autoscaling rows show the [component autoscaling](../../concepts/serving/component-autoscaling.md) status that the controller recorded in `status.components`. To compare it with the live HorizontalPodAutoscaler or KEDA ScaledObject, run [`kubectl ome autoscale status`](autoscale.md#status) with `--live-scaler`.

| Row | What it shows |
| --- | --- |
| Autoscaling | `<state> / <evidence> parent status`, or `Unavailable; parent read, no usable scaler status`. |
| `Scale <component>` | One row for each component with an entry in `status.components`, as `<state> <class>/<managedBy> <current>-><desired> (<replicas>)`. The counts show `-` unless replicas is `Reported` or `Ambiguous`. |
| Scale target | Wide, after each Scale row. `<component> / <state>`: the scale target the status records. |
| Scale conditions | Wide, after each Scale row. `<component> / <state>`: the scaler conditions the status records. |
| Autoscale issue | Wide. `<component> / <code>` for each issue. An issue of the whole service, such as `UnknownComponentStatus`, has nothing before the slash. |

Autoscaling is one of:

| Autoscaling | Meaning |
| --- | --- |
| `Reported / Reported parent status` | Every component's autoscaler status is complete. |
| `Partial / Reported parent status` | A component's autoscaler status is incomplete, or some components have none. |
| `Invalid / Reported parent status` | `status.components` has a key the CLI doesn't know, or a component's autoscaler status is invalid. |
| `Unavailable / Unavailable parent status` | `status.components` has no engine, decoder or router entry, or is too large to read. |
| `Unavailable; parent read, no usable scaler status` | The component entries carry no autoscaler status. |

[`kubectl ome autoscale status`](autoscale.md#status-output-fields) explains the values in a Scale row and the Autoscale issue codes. The state in Scale target and Scale conditions is `Reported`, `NotReported`, `Unavailable` or `Invalid`. `PolicyEvidenceInvalid` and `PolicyConditionConflict` come from an [AutoscalerPolicy](../../concepts/serving/autoscaler-policy.md), which is alpha.

### Placement

!!! note "Alpha"
    Placement is part of multi-cluster routing, which is alpha, still in development, and off by default. Its fields and behavior can change between releases.

A service without placement shows Placement as `NotConfigured / NotApplicable / NotRecorded`. Placement homes, Reported cluster, Placement endpoint and Placement replicas appear when the state is `Reported` or `Partial`.

| Row | What it shows |
| --- | --- |
| Placement | `<state> / <mode> / <phase>`. |
| Placement homes | The candidate clusters the status lists, as `<state> <kept>/<total> truncated=<bool>`. |
| Reported cluster | The cluster the status names, when it names one. |
| Placement endpoint | Whether the status records an endpoint, as `<state> (reported; not probed)`. |
| Placement replicas | When the mode is `Split`. `admitted=<n> ready=<n> (reported)`, with `Unknown` or `Unavailable` in place of a count the CLI can't vouch for. |
| Placement evidence | Wide. `Reported / Unverifiable` when the CLI read a placement status, `Unavailable / NotApplicable` otherwise. |
| Placement mode | Wide. Where the mode comes from: `Declared`, `Defaulted`, `Unavailable` or `NotApplicable`. |

The placement state is:

| State | Meaning |
| --- | --- |
| `NotConfigured` | The service has no placement settings. |
| `NotReported` | The service has placement settings, but the controller hasn't written `status.placement`. |
| `Reported` | The status records a placement that the CLI can read in full. |
| `Partial` | Part of the placement status is missing, cut short or unknown to the CLI. The other placement rows show which part. |
| `Unavailable` | The CLI couldn't make sense of the placement data. The row reads `Unavailable / Unknown / NotRecorded`. |

The mode is `Single`, `All`, `Split`, `Unknown` or `NotApplicable`. [`kubectl ome placement status`](placement.md#status-output-fields) explains the phase, candidate and endpoint values. While the controller admits the service, this report shows the phase `Unknown` and the state `Partial`, where `placement status` shows `Admitting`. This is a known bug.

### Traffic

The traffic rows come from `status.traffic` on the InferenceService, which the controller writes for the [traffic policy](../../concepts/rollouts-and-traffic/traffic-policy.md) in `spec.traffic`.

| Row | What it shows |
| --- | --- |
| Traffic | `<state> / <evidence> parent status`. The evidence is `Unavailable` when the state is, and `Reported` otherwise. |
| Traffic policy Ready | Wide, unless the state is `Unavailable`. The `BackendPolicyReady` condition, as `<status> / <freshness>`. |
| Traffic translator | Wide, unless the state is `Unavailable`. `envoy-gateway`, `istio` or `Unavailable`. |
| Traffic algorithm | Wide, unless the state is `Unavailable`. `Default`, `RoundRobin`, `LeastRequest`, `Random`, `ConsistentHash` or `Unknown`. |

The traffic state is:

| State | Meaning |
| --- | --- |
| `Reported` | Your gateway accepted the current traffic policy. |
| `Pending` | The gateway hasn't accepted or rejected the current policy yet. |
| `Degraded` | The gateway rejected the policy, or OME has no translator for your gateway. |
| `Partial` | The traffic status is from an older generation, or part of it is missing. |
| `Invalid` | The traffic status is malformed. |
| `Unavailable` | The traffic status is missing, lacks a `BackendPolicyReady` condition, or is too large to read. It's missing for a service that declares no traffic behavior. |

On Envoy Gateway, Traffic stays `Pending` even after the gateway accepts the policy. To see the gateway's verdict, follow [Read the traffic status](../../concepts/rollouts-and-traffic/traffic-policy.md#read-the-traffic-status). This is a known bug.

In Traffic policy Ready, the freshness is `Current` for the current generation and `Stale` for an older one. It's `Unverifiable` when the condition records no generation or a newer one, and `Unavailable` when the CLI couldn't read it.

### Runtime and accelerator

| Row | What it shows |
| --- | --- |
| Runtime active | The active runtime, which the controller runs the service with, as `<state> / <active> <reason>`. It's the live runtime object, or a pinned [runtime revision](../../concepts/runtimes/runtime-revisions.md) of it. |
| Active runtime name | Wide, when the active runtime is available. Its name. |
| Active runtime kind | Wide, when the active runtime is available. `ServingRuntime` or `ClusterServingRuntime`. |
| Active origin | Wide, when the active runtime is available. `LiveRuntime` when it comes from the runtime object, `ControllerRevision` when it comes from a pinned runtime revision. |
| Runtime pin | Wide, when the CLI worked out the pin. The pin state that [`runtime effective`](runtime.md#effective-output-fields) reports, such as `Resolved` or `AwaitingPin`. |
| Runtime freshness | Wide, when the CLI worked it out. `Current`, `Stale`, `Unobserved` or `Invalid`. |
| Accelerator | The accelerator settings of the engine and decoder, as `<state> / <evidence> <reason>`. |
| `Accelerator engine`, `Accelerator decoder` | When the Accelerator state is `Reported`, `Partial` or `Invalid`. `<selection> (<class>)`. |
| Declared class | Wide, after each of those rows. `<component> / <declaredClass> (<intent>)`. The class name appears only when the intent is `Class`. |

Runtime active is one of:

| Runtime active | Meaning |
| --- | --- |
| `Reported / Available` | The live and active runtimes are both available, and the CLI found no issue. |
| `Partial / Available`, `Partial / Unavailable` | The CLI found a problem, such as a missing or unreadable model, runtime or revision. The second value says whether the active runtime is available. |
| `NotConfigured / Unavailable` | The service names no model or runtime. |
| `Unavailable / Unavailable AutoSelectionNotProbed` | The service names a model but no runtime, so OME picks one. Run [`kubectl ome runtime explain`](runtime.md#explain) with `--isvc` to see which runtimes match the model, and why. |
| `Unavailable / Unavailable VirtualDeployment` | The service is a VirtualDeployment. |
| `Unavailable / Unavailable ReadFailed`, `Unavailable / Unavailable ProjectionInvalid` | The CLI failed to create its runtime client, or to work out the runtime from what it read. |

Accelerator is one of:

| Accelerator | Meaning |
| --- | --- |
| `NotConfigured / Unavailable` | No component asks for an accelerator class or policy, or the service declares no engine or decoder. |
| `Unavailable / Unavailable AutoSelectionNotProbed` | The service names no runtime, so the CLI doesn't work out the settings. |
| `Unavailable / Unavailable ReadFailed`, `Unavailable / Unavailable ProjectionInvalid` | As in Runtime active, or the CLI failed to work out the accelerator settings. |
| `Reported / Computed` | The settings of every component check out. |
| `Partial / Computed` | The CLI found an issue with a component's settings, such as a selection the status doesn't record. |
| `Invalid / Computed` | A component's intent, selection or requests are invalid. |

In an `Accelerator <component>` row, the selection is `NotReported`, `Unavailable`, `Invalid` or `NotConfigured`, and the class is `NotRequested`. In Declared class, the intent is `Class` for a class by name, `Policy` for a selection policy, `NotConfigured` or `Invalid`. [`kubectl ome accelerator explain`](accelerator.md#explain-output-fields) explains these values.

!!! warning "Accelerator shows `Partial` when a component sets a class or policy"
    OME doesn't record the class it picks, so a component that asks for a class or policy shows Accelerator `Partial / Computed` and a `NotReported` selection. To see that class, check the node selector of the component's pods, as in [Step 4 of Select accelerators](../../guides/deploy-models/select-accelerators.md#step-4-check-the-selected-class). This is a known bug.

### Warning events and issues

The report shows the Warning events of the InferenceService and its counted pods, newest first, within the [limits](#observation-bounds). For Normal events and events on InferenceReplicas, run `kubectl get events -n demo`. An event can remain after the problem is fixed: `-o wide` shows when it was last seen.

| Row | What it shows |
| --- | --- |
| `Warning Pod`, `Warning Inference...` | One row for each event kept, as `<name> <reason>`. An event about the InferenceService shows as `Warning Inference...`, cut to fit the column. |
| Event message | Wide, after each event. The event's message. |
| Event count | Wide, after each event. How many times it happened, as the event records it. |
| Event last seen | Wide, after each event that records it. When it last happened, as an RFC 3339 time. |
| Issue | One row for each kind of problem with what the CLI read. |

An issue about a pod or an event means the CLI left it out of the report. The issues are:

| Issue | Meaning |
| --- | --- |
| `AutoscaleUnavailable` | The autoscaler status is too large to read, so the report skips it. |
| `CollectionLimitExceeded` | Part of what the CLI read is larger than the report holds. |
| `EventIdentityRejected` | An event doesn't match the service or a counted pod. |
| `EventMalformed` | An event is malformed. |
| `InvalidGeneration` | The generation or the observed generation is negative. |
| `PlacementUnavailable` | The placement status lists too many candidate clusters, or candidate counts that don't add up. |
| `PodIdentityRejected` | A pod doesn't match what the CLI asked for, or two pods share a name or UID. |
| `PodMalformed` | A pod's status is malformed. |
| `RolloutUnavailable` | The rollout status is too large to read, so the report skips it. |
| `UnsupportedComponent` | A pod labelled for the service has no `component` label, or one other than `engine`, `decoder` or `router`. |

### Detail rows

The report ends with Full safe values, which says `Use -o json or -o yaml`, and six rows that name the commands in the [section table](#output-fields), with `NAME` for the service's name. `-o wide` adds three rows: Collected at, when the CLI built the report, as an RFC 3339 time; Source generation, the generation of the InferenceService it read; and Source evidence, always `Observed`.

## Limits {#observation-bounds}

The command reads a bounded amount, so it answers quickly even for a large service. These limits show in the output:

| Limit | What you see |
| --- | --- |
| Events of the InferenceService and of at most 15 pods, with pods that aren't Ready or are being deleted first | Event observation is `Partial` with `truncated=true`, and Event targets skip counts the pods left out. |
| 25 Warning events for each object, and 100 in the report | Event observation is `Partial` with `truncated=true`. |
| 30 seconds for the whole command | No report, only `error: status read was cancelled or timed out`. |

When the CLI can read the InferenceService, any other read that fails or stops at a limit still leaves a report, with `Partial` or `Unavailable` in the rows it affects.

## Examples

Show the status of `qwen2-5-7b` in `demo`, just after you created it:

```bash
kubectl ome status qwen2-5-7b -n demo
```

```output
FIELD                VALUE
Name                 qwen2-5-7b
Namespace            demo
Ready                NotRecorded / Unavailable
Ready reason
Declared runtime
Model                qwen2-5-7b
Generation           1 observed=0; advisory Unverifiable
Pod observation      Reported count=0 truncated=false
Event observation    Reported count=0 truncated=false
engine               NotRecorded / Unavailable; Ready pods=0/0 restarts=0
Rollout              Unknown reported=Unknown
Rollout evidence     Reported / Unverifiable
Autoscaling          Unavailable / Unavailable parent status
Placement            NotConfigured / NotApplicable / NotRecorded
Traffic              Unavailable / Unavailable parent status
Runtime active       Unavailable / Unavailable AutoSelectionNotProbed
Accelerator          Unavailable / Unavailable AutoSelectionNotProbed
Full safe values     Use -o json or -o yaml
Rollout detail       kubectl ome rollout status NAME
Autoscale detail     kubectl ome autoscale status NAME
Placement detail     kubectl ome placement status NAME
Traffic detail       kubectl ome traffic status NAME
Runtime detail       kubectl ome runtime effective NAME
Accelerator detail   kubectl ome accelerator explain NAME
```

The controller hasn't written any status yet, so Ready is `NotRecorded / Unavailable`, and Autoscaling and Traffic are `Unavailable`. The `engine` row comes from `spec.engine`. The service names a model but no runtime, so Runtime active and Accelerator show `AutoSelectionNotProbed`.

Print the same report with every row:

```bash
kubectl ome status qwen2-5-7b -n demo -o wide
```

??? note "The wide report"
    ```output
    FIELD                  VALUE
    Name                   qwen2-5-7b
    Namespace              demo
    Ready                  NotRecorded / Unavailable
    Ready reason
    Ready message
    Condition inspection   Complete 0/0
    Declared runtime
    Model                  qwen2-5-7b
    Generation             1 observed=0; advisory Unverifiable
    Pod observation        Reported count=0 truncated=false
    Event observation      Reported count=0 truncated=false
    Pod targets skipped    0
    Event targets skip     0
    engine                 NotRecorded / Unavailable; Ready pods=0/0 restarts=0
    engine Pod total       0
    engine Pod Ready       0
    engine restarts        0
    engine phases          R=0 P=0 F=0 S=0 U=0 deleting=0
    engine evidence        Observed
    Rollout                Unknown reported=Unknown
    Rollout evidence       Reported / Unverifiable
    Coordination Ready     NotApplicable
    Rollout issue          ComponentStatusMissing
    Rollout issue          EpochUnverifiable
    Rollout warning        PartialData
    Autoscaling            Unavailable / Unavailable parent status
    Placement              NotConfigured / NotApplicable / NotRecorded
    Placement evidence     Unavailable / NotApplicable
    Placement mode         NotApplicable
    Traffic                Unavailable / Unavailable parent status
    Runtime active         Unavailable / Unavailable AutoSelectionNotProbed
    Accelerator            Unavailable / Unavailable AutoSelectionNotProbed
    Full safe values       Use -o json or -o yaml
    Rollout detail         kubectl ome rollout status NAME
    Autoscale detail       kubectl ome autoscale status NAME
    Placement detail       kubectl ome placement status NAME
    Traffic detail         kubectl ome traffic status NAME
    Runtime detail         kubectl ome runtime effective NAME
    Accelerator detail     kubectl ome accelerator explain NAME
    Collected at           2026-09-15T12:00:00Z
    Source generation      1
    Source evidence        Observed
    ```

The service has no conditions yet, so Condition inspection is `Complete 0/0`. The rollout issues say why Rollout is `Unknown`: `status.components` has no entry for the engine yet, and the CLI can't tie the rollout status to the current generation.

Print only the Ready section of the JSON report, with `jq`:

```bash
kubectl ome status qwen2-5-7b -n demo -o json | jq '.content.ready'
```

```output
{
  "status": "NotRecorded",
  "validity": "Unavailable",
  "inspection": {
    "state": "Complete",
    "total": 0,
    "inspected": 0,
    "warnings": []
  }
}
```

The JSON leaves out `reason` and `message` while they're empty.

Ask for a service that doesn't exist:

```bash
kubectl ome status qwen2-5-7 -n demo
```

```output
error: status InferenceService was not found
```

The command exits with code 1.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | `status`, after it prints the report, whatever state the service is in. |
| `1` | General error | `status`, when it prints no report: a bad name, namespace, flag, output format or kubeconfig, a service it can't find, read or make sense of, a run over 30 seconds, Ctrl-C, or output it can't write. |

Errors print to stderr as `error: <message>`.

A run over 30 seconds, or Ctrl-C, prints only `error: status read was cancelled or timed out`. To wait for Ready in a script, use [`kubectl ome wait`](wait.md): `kubectl ome wait qwen2-5-7b -n demo --for=condition=Ready`.

## Related guides

- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
