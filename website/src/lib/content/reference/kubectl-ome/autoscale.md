---
title: kubectl ome autoscale
description: Show the autoscaling state the controller reports for each InferenceService component, and explain which layer supplies its autoscaler.
since: v1.3
---

`kubectl ome autoscale` shows how each component of an [InferenceService](../../concepts/serving/inference-services.md#scaling) scales: its autoscaler, where the autoscaler's settings came from, the object it scales and its replica counts. Both subcommands only read. The component autoscaler settings and status fields they read are alpha. [Component autoscaling](../../concepts/serving/component-autoscaling.md) describes the autoscalers, and [`kubectl ome scale`](scale.md) changes a component's replica count for a while.

```text
kubectl ome autoscale SUBCOMMAND INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`status`](#status) | Shows the autoscaling the controller reports for each component, and can check it against the live objects. |
| [`explain`](#explain) | Works out which autoscaler each component should have, and compares it with the report. |

Each subcommand takes the name of one InferenceService and prints a table. `-o json` and `-o yaml` print the whole report, including warnings the table leaves out, as [Output formats](overview.md#output-formats) describes. [Required RBAC](overview.md#required-rbac) lists the permissions each command needs, and [Extra rules](overview.md#extra-rules) lists the one `--live-scaler` adds.

## `status`

```text
kubectl ome autoscale status INFERENCESERVICE [flags]
```

`status` shows what the InferenceService's status reports for each component: the autoscaler's class, what manages it, where its settings came from, the object it scales, its replica counts and conditions. Add `--live-scale` to check the replica count on the component's InferenceReplica, as after a transient scale. Add `--live-scaler` to check the HorizontalPodAutoscaler or ScaledObject itself. The live rows carry their own evidence, and don't change STATE.

For the autoscaler's metrics and condition messages, run `kubectl describe hpa chat-engine -n prod`, or `kubectl describe scaledobject scaledobject-chat-engine -n prod` for KEDA.

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--live-scale` | `false` | Compare parent counts with exact selected InferenceReplica and /scale reads |
| `--live-scaler` | `false` | Inspect exact selected HPA or KEDA ScaledObject (additional read permission) |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#status-output-fields}

The table has a row per field, and a column for the whole service, SERVICE, and for each component: ENGINE, DECODER and ROUTER. STATE always appears. Other rows appear only when a component has a value, and show `-` under SERVICE. ISSUES comes last.

The service's STATE is one of:

| State | Meaning |
| --- | --- |
| `Invalid` | A component is `Invalid`, or the status names an unknown component. |
| `Unavailable` | The status reports no components, or no autoscaler for any of them. |
| `Partial` | A component is `Partial` or `NotReported`. |
| `Reported` | Every component is `Reported`. |

The first state in this list that applies wins. Any state other than `Reported` adds a `PartialData` warning, which only `-o json` and `-o yaml` show.

A component's state is one of:

| State | Meaning |
| --- | --- |
| `Reported` | The status reports the component's autoscaler and scale target, with no issue. |
| `Partial` | Part of the report is missing or ambiguous, as when KEDA has scaled the component to zero and its counts are 0. The evidence rows and ISSUES say what. |
| `NotReported` | The component's status has no autoscaler. |
| `Invalid` | The status holds a value the CLI can't accept. ISSUES names it. |

The component rows are:

| Row | What it shows |
| --- | --- |
| CLASS | The autoscaler class: `HPA`, `KEDA`, `External` or `None`. `Unknown` when the status reports no autoscaler, or a class outside this list. |
| MANAGED-BY | What manages the autoscaler: `ome`, `external` or `none`, or `Unknown`. |
| SPEC-SOURCE | The layer the autoscaler's settings came from: `isvc`, `policy`, `runtime`, `legacy` or `default`, or `Unknown`. [`explain`](#explain) describes the layers. |
| POLICY-STATE, POLICY | The AutoscalerPolicy evidence, described below. |
| TARGET-KIND | The kind of the scale target: `IR` for an InferenceReplica, or `Deployment`. |
| TARGET-NAME | The name of the scale target. |
| TARGET-EVIDENCE | `Reported`, `NotReported` when the status names no target, or `Invalid`. |
| CURRENT, DESIRED | The current and desired replica counts the status copied from the autoscaler. |
| REPLICA-EVIDENCE | `Reported`, `Ambiguous` when a count is 0, `Unavailable` for an autoscaler that OME doesn't manage, `NotReported` without an autoscaler, or `Invalid`. |
| LAST-SCALE | When the autoscaler last scaled, in UTC, as in `Aug31 18:20Z`. |
| COND-EVIDENCE | `Reported`, `NotReported` when the status has no conditions, `Unavailable` for an autoscaler that OME doesn't manage or conditions the CLI can't use, or `Invalid`. |
| ABLE-TO-SCALE, SCALING-ACTIVE, SCALING-LIMITED | The status of an HPA's `AbleToScale`, `ScalingActive` and `ScalingLimited` conditions, as the InferenceService's status records them. |
| READY, ACTIVE, FALLBACK, PAUSED | The status of a ScaledObject's `Ready`, `Active`, `Fallback` and `Paused` conditions, as the InferenceService's status records them. |
| ISSUES | The issues the CLI found, one per row in each column: the service's issues under SERVICE, and each component's under its column. |

ISSUES names the cause of any other value. An autoscaler that OME doesn't manage has no counts or conditions in the status, so `Unavailable` is normal for it, and the component can still be `Reported`. For an OMENative component, the counts are Instances: one pod, or a leader and its workers.

Long cells are shortened with `...`, and `-o wide` shows full values. The LIVE and SCALER rows that the live flags add follow REPLICA-EVIDENCE.

Each ISSUES cell holds a short alias. State is the state the issue leads to. `-o wide` shows the exact codes.

| Alias | Code | State | When |
| --- | --- | --- | --- |
| `UnknownComp` | `UnknownComponentStatus` | `Invalid`, for the service | The status names a component other than engine, decoder or router. Shown under SERVICE. |
| `NoAutoscaler` | `AutoscalerNotReported` | `NotReported` | The status reports no autoscaler for the component. |
| `NoTarget` | `ScaleTargetNotReported` | `Partial` | The status names no scale target. |
| `BadClass` | `ClassInvalid` | `Invalid` | The class is unknown. |
| `BadManager` | `ManagedByInvalid` | `Invalid` | The manager is unknown. |
| `OwnerMismatch` | `OwnershipMismatch` | `Invalid` | The class and manager don't go together. |
| `BadSpecSource` | `SpecSourceInvalid` | `Invalid` | The spec source is unknown. |
| `UnexpectedEv` | `UnexpectedScalerEvidence` | `Invalid` | An `External` or `None` autoscaler has counts, a last scale time or conditions. |
| `ReplicaAmbig` | `ReplicaEvidenceAmbiguous` | `Partial` | A replica count is 0, as after scaling to zero. The status leaves out a count of 0, so the CLI can't tell it from a missing one. |
| `BadReplica` | `ReplicaEvidenceInvalid` | `Invalid` | A replica count or the last scale time is invalid. |
| `BadTarget` | `ScaleTargetInvalid` | `Invalid` | The scale target is invalid. |
| `BadCondition` | `ConditionInvalid` | `Partial` | A condition is malformed, or doesn't belong to the class. |
| `CondConflict` | `ConditionConflict` | `Partial` | A condition appears twice with different values. |
| `BadPolicy` | `PolicyEvidenceInvalid` | `Invalid` | The policy evidence is malformed, doesn't fit the spec, or was written for an earlier generation of the InferenceService. |
| `PolicyClash` | `PolicyConditionConflict` | `Invalid` | The status has more than one `AutoscalerResolved` condition. |

POLICY-STATE and POLICY appear when a component references an AutoscalerPolicy.

!!! note "Alpha"
    These rows report AutoscalerPolicy, which is alpha and off by default. [Turn on the feature](../../concepts/serving/autoscaler-policy.md#turn-on-the-feature) describes how to enable it.

POLICY-STATE says how the policy relates to the autoscaler in effect. POLICY names the referenced policy, except for `Held`:

| POLICY-STATE | Meaning |
| --- | --- |
| `Current` | The policy rendered the autoscaler in effect. |
| `Shadowed` | The component's inline `autoscaler` outranks the policy. |
| `Held` | The component keeps the last render of the policy in POLICY, which can be one referenced before. |
| `Unresolved` | The policy never rendered for the component. |
| `Unsupported` | The component is `MultiNode` (deprecated), which gets no autoscaler. |

`-o wide` adds the reason on the component's `AutoscalerResolved` condition as POLICY-RESOLUTION, and [Conditions and status](../../concepts/serving/autoscaler-policy.md#conditions-and-status) lists the reasons. [Fail-closed behavior](../../concepts/serving/autoscaler-policy.md#fail-closed-behavior) explains holds, and [Inline settings win](../../concepts/serving/autoscaler-policy.md#inline-settings-win) explains the shadowed case. Right after you change the InferenceService, a component with a policy reference is `Invalid`, with the issue `BadPolicy`, until the controller catches up.

`--live-scale` compares the counts in the status with the live counts of an [OMENative](../../concepts/omenative/overview.md) component's InferenceReplica, which it reads with its `scale` subresource. After a [`kubectl ome scale`](scale.md) request, LIVE-SPEC shows the count you asked for. A RawDeployment component shows `Unsupported`. `--live-scaler` reads the autoscaler that OME manages: the HorizontalPodAutoscaler named after the scale target, or the ScaledObject `scaledobject-<target>`. For an InferenceReplica target, both flags first check that it's the one the InferenceService currently selects. They add these rows:

| Row | What it shows |
| --- | --- |
| LIVE-EVIDENCE | What the `--live-scale` reads found, as below. |
| LIVE-SPEC | The number of Instances the InferenceReplica asks for. |
| LIVE-CURRENT | The number of Instances it has, ready or not. |
| LIVE-COUNT | `Equal` when LIVE-SPEC matches DESIRED and LIVE-CURRENT matches CURRENT, `Drift` when either differs, and `Unknown` when the reads didn't succeed or REPLICA-EVIDENCE isn't `Reported`. |
| SCALER-KIND | `HPA` or `KEDA`. For an autoscaler that OME doesn't manage, the class. |
| SCALER-EVIDENCE | What the `--live-scaler` reads found, as below. |
| SCALER-GEN | `Matched` when the HPA has processed its latest spec, otherwise `Unproven`, as always for a ScaledObject. |
| SCALER-CURRENT, SCALER-DESIRED | The HPA's current and desired replicas. A ScaledObject has none. |
| SCALER-ABLE, SCALER-SCALING, SCALER-LIMITED, SCALER-READY, SCALER-ACTIVE, SCALER-FALLBACK, SCALER-PAUSED | The HPA's or ScaledObject's own conditions, as it reports them. |

LIVE-EVIDENCE and SCALER-EVIDENCE are one of:

| Evidence | Meaning |
| --- | --- |
| `Reported` | The reads passed every check. |
| `NotSelected` | The status names no scale target, or, for `--live-scaler`, OME doesn't manage the autoscaler. |
| `Unsupported` | For `--live-scale`, the target is a Deployment. For `--live-scaler`, it's neither a Deployment nor an InferenceReplica. |
| `Invalid` | The object isn't the one the InferenceService selects, or holds a value the CLI can't use, as right after you change the InferenceService. Run the command again. |
| `Stale` | The InferenceReplica or HPA hasn't caught up with its latest change, as right after a scale request. Run the command again. |
| `Changed` | `--live-scale` only: the InferenceReplica changed between the two reads. Run the command again. |
| `Deleting` | An object it read is being deleted. |
| `Forbidden`, `NotFound`, `Unavailable` | A read was forbidden, found nothing, or failed. A cluster without the KEDA API gives `NotFound`. |

`--live-scale` also adds a `PartialData` warning when a component's result isn't `Reported` with `Equal`, and `--live-scaler` when an autoscaler that OME manages isn't `Reported`.

`-o wide` prints one row per component, with full values and exact issue codes. Policy evidence adds columns after SPEC-SOURCE, including the render's policy generation and digests, and the live flags add theirs at the end.

### Examples {#status-examples}

Show the autoscaling state of `chat` in `prod`:

```bash
kubectl ome autoscale status chat -n prod
```

```output
FIELD              SERVICE    ENGINE         DECODER   ROUTER
STATE              Reported   Reported       -         -
CLASS              -          HPA            -         -
MANAGED-BY         -          ome            -         -
SPEC-SOURCE        -          default        -         -
TARGET-KIND        -          IR             -         -
TARGET-NAME        -          chat-engine    -         -
TARGET-EVIDENCE    -          Reported       -         -
CURRENT            -          2              -         -
DESIRED            -          3              -         -
REPLICA-EVIDENCE   -          Reported       -         -
LAST-SCALE         -          Aug31 18:20Z   -         -
COND-EVIDENCE      -          Reported       -         -
ABLE-TO-SCALE      -          True           -         -
SCALING-ACTIVE     -          True           -         -
```

The engine has the default HPA, which OME manages and which scales the InferenceReplica `chat-engine`. The HPA reports 2 current replicas and wants 3, and it last scaled at 18:20 UTC on August 31. The status reports no decoder or router.

Show the same report with exact values:

```bash
kubectl ome autoscale status chat -n prod -o wide
```

```output
STATE      COMPONENT   COMPONENT-STATE   CLASS   MANAGED-BY   SPEC-SOURCE   TARGET                              TARGET-EVIDENCE   CURRENT   DESIRED   REPLICA-EVIDENCE   LAST-SCALE             CONDITION-EVIDENCE   CONDITIONS                            ISSUES
Reported   engine      Reported          HPA     ome          default       InferenceReplica/prod/chat-engine   Reported          2         3         Reported           2026-08-31T18:20:00Z   Reported             AbleToScale=True,ScalingActive=True   -
```

Check the engine's counts against its InferenceReplica. In this report, the status records no last scale time, so the LAST-SCALE row is left out:

```bash
kubectl ome autoscale status chat -n prod --live-scale
```

```output
FIELD              SERVICE    ENGINE        DECODER   ROUTER
STATE              Reported   Reported      -         -
CLASS              -          HPA           -         -
MANAGED-BY         -          ome           -         -
SPEC-SOURCE        -          default       -         -
TARGET-KIND        -          IR            -         -
TARGET-NAME        -          chat-engine   -         -
TARGET-EVIDENCE    -          Reported      -         -
CURRENT            -          2             -         -
DESIRED            -          3             -         -
REPLICA-EVIDENCE   -          Reported      -         -
LIVE-EVIDENCE      -          Reported      -         -
LIVE-SPEC          -          3             -         -
LIVE-CURRENT       -          2             -         -
LIVE-COUNT         -          Equal         -         -
COND-EVIDENCE      -          Reported      -         -
ABLE-TO-SCALE      -          True          -         -
SCALING-ACTIVE     -          True          -         -
```

LIVE-SPEC is the 3 Instances the InferenceReplica asks for, and LIVE-CURRENT the 2 it has. They match DESIRED and CURRENT in the status, so LIVE-COUNT is `Equal`.

Check the engine's HPA, with the same status:

```bash
kubectl ome autoscale status chat -n prod --live-scaler
```

```output
FIELD              SERVICE    ENGINE        DECODER   ROUTER
STATE              Reported   Reported      -         -
CLASS              -          HPA           -         -
MANAGED-BY         -          ome           -         -
SPEC-SOURCE        -          default       -         -
TARGET-KIND        -          IR            -         -
TARGET-NAME        -          chat-engine   -         -
TARGET-EVIDENCE    -          Reported      -         -
CURRENT            -          2             -         -
DESIRED            -          3             -         -
REPLICA-EVIDENCE   -          Reported      -         -
SCALER-KIND        -          HPA           -         -
SCALER-EVIDENCE    -          Reported      -         -
SCALER-GEN         -          Matched       -         -
SCALER-CURRENT     -          2             -         -
SCALER-DESIRED     -          3             -         -
SCALER-SCALING     -          True          -         -
COND-EVIDENCE      -          Reported      -         -
ABLE-TO-SCALE      -          True          -         -
SCALING-ACTIVE     -          True          -         -
```

The HPA's counts agree with the status, and it has processed its latest spec, so SCALER-GEN is `Matched`. The SCALER rows show the conditions the HPA reports itself, here only `ScalingActive`.

Print the first report as JSON:

```bash
kubectl ome autoscale status chat -n prod -o json
```

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "AutoscaleStatusReport",
  "metadata": {
    "namespace": "prod",
    "name": "chat"
  },
  "collectedAt": "2026-08-31T18:30:00Z",
  "sources": [
    {
      "kind": "InferenceService",
      "namespace": "prod",
      "name": "chat",
      "uid": "uid-chat",
      "generation": 7,
      "evidence": "Reported",
      "collectedAt": "2026-08-31T18:30:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "Reported"
    },
    "components": [
      {
        "type": "engine",
        "state": "Reported",
        "class": "HPA",
        "managedBy": "ome",
        "specSource": "default",
        "target": {
          "state": "Reported",
          "apiVersion": "ome.io/v1beta1",
          "kind": "InferenceReplica",
          "namespace": "prod",
          "name": "chat-engine"
        },
        "replicas": {
          "state": "Reported",
          "currentReplicas": 2,
          "desiredReplicas": 3,
          "lastScaleTime": "2026-08-31T18:20:00Z"
        },
        "conditions": {
          "state": "Reported",
          "items": [
            {
              "type": "AbleToScale",
              "status": "True",
              "lastTransitionTime": "2026-08-31T18:15:00Z"
            },
            {
              "type": "ScalingActive",
              "status": "True",
              "lastTransitionTime": "2026-08-31T18:15:00Z"
            }
          ]
        }
      }
    ],
    "issues": []
  },
  "warnings": []
}
```

Scripts can read `content.summary.state`, and each component's `state`, `class`, `target` and `replicas`. With a live flag, each component also has `liveScale` or `liveScaler`. `sources` then also lists each object the flag read, with the evidence `Observed`, or `Unavailable` when the read or its checks failed.

## `explain`

```text
kubectl ome autoscale explain INFERENCESERVICE [flags]
```

`explain` works out which autoscaler each component should have, from the InferenceService and its runtime, and compares it with the autoscaler the status reports. It reads specs and status, not the autoscalers: to check the HPA or ScaledObject itself, use [`status --live-scaler`](#status).

It reads the [serving runtime](../../concepts/runtimes/serving-runtimes.md) the controller selected, the runtimes it [inherits from](../../concepts/runtimes/runtime-inheritance.md) and, for a named runtime, a [pinned revision](../../concepts/runtimes/runtime-revisions.md#live-and-pinned-runtimes) in the OME namespace, which `--ome-namespace` sets.

!!! warning "STATE can flag a correct status"
    On an InferenceService created with only OMENative components, STATE is `Partial` at best, with WHY `status-unobserved`. On a RawDeployment component, STATE can show `Partial` or `Invalid`, with WHY `status-stale` or `status-invalid`, while the status is correct, for example after its autoscaler scales. This is a known bug: compare DESIRED and EXPECTED-TARGET with [`status`](#status) yourself.

A component's autoscaler comes from the first layer that supplies one, as [Which setting wins](../../concepts/serving/component-autoscaling.md#which-setting-wins) describes:

| Layer | Where the autoscaler comes from |
| --- | --- |
| `isvc` | The component's inline `autoscaler`. |
| `policy` | The alpha AutoscalerPolicy that the component's `autoscalerPolicyRef` names. |
| `runtime` | The component's `autoscaler` in the runtime. |
| `legacy` | The [legacy autoscaling annotations](../api/labels-and-annotations.md#legacy-autoscaling), on a RawDeployment component. |
| `default` | An HPA that targets 80% CPU utilization. |

`explain` doesn't read AutoscalerPolicies. When the policy layer supplies the autoscaler, DESIRED shows `?/policy`, WHY shows `policy-unavailable`, and STATE is `Partial` or worse. [`status`](#status) shows the policy evidence the controller reports.

### Flags {#explain-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output format: table, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#explain-output-fields}

The table has a FIELD column and a column for each of the InferenceService's components, such as ENGINE. A SERVICE column follows for issues that no component column shows, such as `decoder:unexpected-component`.

| Row | What it shows |
| --- | --- |
| STATE | The result for the whole InferenceService, repeated in each column. |
| MODE | The component's [deployment mode](../../concepts/architecture/deployment-modes.md): `Raw` for RawDeployment, `Native` for OMENative, `Multi` for `MultiNode` (deprecated), or `Virtual` for VirtualDeployment. |
| POLICY | The alpha [scaling policy](../../concepts/runtimes/serving-runtimes.md#scaling-policy) mode of the whole InferenceService, not an AutoscalerPolicy: `spec.scalingPolicy.mode` from the InferenceService, else from the runtime, else `Independent`. |
| DESIRED | The autoscaler the component should have, as `class/layer`. `?/policy` when the policy layer supplies it, `unsupported` in a mode without autoscaling, and `invalid` when its settings fail validation. |
| RANGE | The [replica bounds](../../concepts/serving/component-autoscaling.md#replica-bounds), as `min..max`, `?` when unknown, or `invalid`. `explain` doesn't read the controller's [replica defaults](../../guides/operate-ome/set-replica-defaults.md), so it shows `1..1` when the InferenceService and its runtime set no bound. |
| ZERO | Whether the component can scale to zero, as below. |
| EXPECTED-TARGET | The object the autoscaler should scale: `Deployment/<name>-<component>` for a RawDeployment component, and `InferenceReplica/<name>-<component>` for an OMENative one. `?`, `unsupported` and `invalid` as for DESIRED. |
| REPORTED | The autoscaler the status reports, as `class/layer/manager`. `-` when the status reports none, `?` when the CLI can't use the status, and `invalid` when the status is invalid. |
| REPORTED-TARGET | The scale target the status reports, as `Kind/name`, with the same `-`, `?` and `invalid`. |
| CUR/DES | The current and desired replica counts the status reports. A `?` follows when either is 0, as in `0/0?`. |
| LAST-SCALE | When the autoscaler last scaled, in RFC 3339. |
| CONDITION-EVIDENCE | `reported`, `missing`, `unknown` or `invalid`, for the COND-EVIDENCE values `Reported`, `NotReported`, `Unavailable` and `Invalid` of [`status`](#status). |
| CONDITIONS | The conditions the status reports, as `Type=Status`. |
| CHECK | How the report compares with the expectation: `match`, `mismatch`, `missing` when the status reports no autoscaler, `unknown` when the CLI can't compare them, or `invalid`. |
| WHY | The first reason, most severe first, then `,+N` when there are N more. `-o json` lists them all. |

STATE is one of:

| STATE | JSON value | Meaning |
| --- | --- | --- |
| `Invalid` | `Invalid` | A setting fails validation, or the status is invalid (`status-invalid`). |
| `Unsupported` | `Unsupported` | Part of the configuration is unsupported: autoscaling in the component's mode, or scale to zero. |
| `Mismatch` | `ReportedMismatch` | The status reports a different class, manager, layer or target than expected, or a component the InferenceService doesn't have. |
| `Partial` | `Partial` | The CLI can't finish the comparison, as when a policy supplies the autoscaler, or when the status is missing or can't be tied to the spec (`status-stale`, `status-unobserved`). |
| `OK` | `Consistent` | The report matches the expectation. |

The first state in this list that applies wins.

ZERO is one of:

| ZERO | Meaning |
| --- | --- |
| `-` | The component doesn't ask to scale to zero. |
| `yes` | The component asks to scale to zero, with `minReplicas: 0` or a KEDA `idleReplicaCount: 0`, and has a KEDA autoscaler with triggers. |
| `invalid` | The component asks to scale to zero without a KEDA autoscaler that has triggers, or another autoscaling setting is invalid. WHY names the reason. |
| `unsupported` | The component asks to scale to zero in a mode other than RawDeployment or OMENative, or the InferenceService sets `minReplicas: 0` on an OMENative component without `idleReplicaCount: 0`. |
| `?` | A policy supplies the autoscaler. |

An OMENative component never runs fewer than one Instance, even when ZERO is `yes`, as [OMENative components can't scale to zero](../../guides/scale-and-migrate/scale-to-zero-with-keda.md#omenative-components-cant-scale-to-zero) explains.

WHY shows these reasons, most severe first:

| WHY | Code | STATE | When |
| --- | --- | --- | --- |
| `class-invalid` | `AutoscalerClassInvalid` | `Invalid` | An autoscaler whose class isn't HPA or KEDA fails validation. |
| `policy-ref-invalid` | `PolicyReferenceInvalid` | `Invalid` | The component's `autoscalerPolicyRef` isn't valid, or the runtime sets one. |
| `keda-triggers` | `KEDATriggersRequired` | `Invalid` | A KEDA autoscaler has no triggers. |
| `keda-config-invalid` | `KEDAConfigurationInvalid` | `Invalid` | The KEDA settings would fail the validation of the ScaledObject that OME creates. |
| `hpa-metric-invalid` | `HPAMetricMalformed` | `Invalid` | An HPA's settings fail validation. |
| `keda-idle-invalid` | `KEDAIdleNotBelowMinimum` | `Invalid` | A KEDA autoscaler with triggers fails validation, for example because `idleReplicaCount` isn't below `minReplicas`. |
| `keda-name-conflict` | `ReservedHPANameCollision` | `Invalid` | `keda.advanced.horizontalPodAutoscalerConfig.name` is the name of the scale target. |
| `legacy-invalid` | `LegacyAutoscalerInvalid` | `Invalid` | The legacy annotations fail the webhook's checks. |
| `bounds-invalid` | `ReplicaBoundsInvalid` | `Invalid` | The replica bounds fail validation. |
| `zero-invalid` | `ScaleToZeroInvalid` | `Invalid` | The scale-to-zero settings fail validation: see ZERO. |
| `status-invalid` | `StatusInvalid`, `ReportedEvidenceInvalid` | `Invalid` | The status looks newer than the spec, or [`status`](#status) finds it `Invalid`. |
| `mode-unsupported` | `DeploymentModeUnsupported` | `Unsupported` | The component's mode isn't RawDeployment or OMENative. |
| `zero-unsupported` | `ScaleToZeroUnsupported` | `Unsupported` | ZERO is `unsupported`. |
| `class-mismatch`, `owner-mismatch`, `source-mismatch`, `target-mismatch` | `ReportedClassMismatch`, `ReportedOwnershipMismatch`, `ReportedSpecSourceMismatch`, `ReportedTargetMismatch` | `Mismatch` | The status reports a different class, manager, layer or target than expected. |
| `unexpected-component` | `ReportedComponentUnexpected` | `Mismatch` | The status reports a component the InferenceService doesn't have. |
| `policy-unavailable` | `PolicyResolutionUnavailable` | `Partial` | The policy layer supplies the autoscaler. |
| `inheritance-unavailable` | `InheritanceUnavailable` | `Partial` | The CLI couldn't read the runtime's inheritance chain, or the chain is malformed. |
| `revision-inconsistent` | `ActiveRevisionInconsistent` | `Partial` | The pinned revision's metadata disagrees with the runtime it records, or can't be checked. |
| `status-missing` | `StatusNotReported` | `Partial` | The status reports no autoscaler for the component. |
| `status-stale` | `StatusStale` | `Partial` | The status looks older than the spec. |
| `status-unobserved` | `StatusUnobserved` | `Partial` | The status has no observed generation, as on an OMENative-only service. |
| `status-partial` | `ReportedEvidencePartial` | `Partial` | The status's evidence for the component is incomplete. |

`explain` exits with code 1 when it can't read a consistent set of objects. The errors include:

| Error | What to do |
| --- | --- |
| `runtime API snapshot changed during collection`, `runtime evidence does not match the inference service snapshot` | Something changed while the CLI read it. Run the command again. |
| `require active runtime configuration: active runtime configuration is unavailable` | The CLI can't find or read a usable runtime or pinned revision. Check that the runtime exists and isn't disabled, and set `--ome-namespace` when OME runs outside `ome`. |
| `runtime selection candidates exceeded the CLI collection limit` | There are too many candidate runtimes to read. Name the runtime in `spec.runtime.name`, and `explain` reads only that one. |

### Examples {#explain-examples}

Explain the autoscaling of `chat` in `prod`. Unlike the `status` examples, this `chat` runs its engine as a RawDeployment:

```bash
kubectl ome autoscale explain chat -n prod
```

```output
FIELD                ENGINE
STATE                OK
MODE                 Raw
POLICY               Independent
DESIRED              HPA/default
RANGE                1..4
ZERO                 -
EXPECTED-TARGET      Deployment/chat-engine
REPORTED             HPA/default/ome
REPORTED-TARGET      Deployment/chat-engine
CUR/DES              2/2
LAST-SCALE           -
CONDITION-EVIDENCE   reported
CONDITIONS           AbleToScale=True
CHECK                match
WHY                  -
```

Neither the InferenceService nor its runtime sets an autoscaler, so the default applies: an HPA that scales the Deployment `chat-engine` between the engine's `minReplicas: 1` and `maxReplicas: 4`. The status is current and reports the same class, layer and target, so CHECK is `match` and STATE is `OK`.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Both subcommands, whenever they print a report, whatever its STATE. |
| `1` | General error | Both subcommands, for an invalid name, namespace or flag, a failed API request, an inconsistent read in `explain`, or Ctrl-C. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md)
- [Bring your own autoscaler](../../guides/scale-and-migrate/bring-your-own-autoscaler.md)
- [Scale to zero with KEDA](../../guides/scale-and-migrate/scale-to-zero-with-keda.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
