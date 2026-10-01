---
title: kubectl ome rollout
description: Show rollout progress, the pinned plan and recent history for an InferenceService, validate its configuration, and run the alpha rollout actions.
since: v1.3
---

`kubectl ome rollout` shows where an [InferenceService](../../concepts/serving/inference-services.md) is in a rollout, and steers a rollout in progress. The read commands show progress, the plan a run follows and recent runs, and check the rollout, traffic and autoscaling configuration. The alpha actions pause and resume rollout work, promote or roll back a canary, and repin a drifted plan. [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md) and alpha [rollout policies](../../concepts/rollouts-and-traffic/rollout-policy.md) describe the plans. Rollout groups and the actions need components that use [OMENative](../../concepts/omenative/overview.md).

```text
kubectl ome rollout SUBCOMMAND INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`status`](#status) | Shows rollout progress for an InferenceService. |
| [`explain`](#explain) | Shows the declared, live and pinned plans, and what holds the rollout. |
| [`history`](#history) | Shows the active run, the last run and the current revisions. |
| [`validate`](#validate) | Checks the rollout, traffic and autoscaling configuration. |
| [`pause`](#pause-and-resume) | Alpha. Holds rollout work across the whole service. |
| [`resume`](#pause-and-resume) | Alpha. Clears a pause. |
| [`promote`](#promote-and-rollback) | Alpha. Passes the manual gate a canary waits at. |
| [`rollback`](#promote-and-rollback) | Alpha. Aborts a canary to its stable revision. |
| [`repin`](#repin) | Alpha. Moves the active run to the current plan. |

Each subcommand takes the name of one InferenceService and prints a table. For the read commands, `-o wide` prints more detail. `-o json` and `-o yaml` print the whole report, including its warnings, as [Output formats](overview.md#output-formats) describes. [Required RBAC](overview.md#required-rbac) lists the permissions each command needs.

The reports mark how the CLI got each value with an [evidence level](overview.md#evidence-levels).

## `status`

```text
kubectl ome rollout status INFERENCESERVICE [flags]
```

`status` shows where each component of the InferenceService is in its rollout: its rollout group, its phase, the current step of a canary, and the revisions it serves. The values come from the controller's status. While a run is active, the steps come from the plan the run pinned.

The command makes one GET of that InferenceService and reads no pods, policies or other child resources. It reports the recorded progress, not an independent check that workloads or traffic converged.

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json, or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#status-output-fields}

The table has a SERVICE column for the whole InferenceService, then a column for each component: ENGINE, DECODER and ROUTER. The first rows describe the service:

| Row | What it shows |
| --- | --- |
| STATE | REPORTED when the CLI can tie it to the current generation, otherwise `Unknown`. |
| REPORTED | The state the controller's status adds up to: the first of `Failed`, `RollingBack`, `RolledBack`, `Paused`, `Unknown`, `InProgress`, `Staged` and `Succeeded` that applies to any group or component. |
| EVIDENCE | `Reported` when STATE rests on the controller's status, `Declared` when the spec settles it. |
| EPOCH | `Unverifiable` when STATE rests on status, otherwise `NotApplicable`. |
| COORDINATION | The RolloutCoordinationReady condition of blue-green, rolling-update and sequential groups: `True`, `False`, `Unknown`, `Unobserved` if unset, `Invalid` when malformed or at odds with the groups' phases, or `NotApplicable` without such groups. |

A service with rollout groups, or with rollout phases or revisions in its status, shows STATE `Unknown`. The CLI can't tie that status to the current generation, so EVIDENCE shows `Reported`, EPOCH shows `Unverifiable`, and ISSUES lists `EpochUnverifiable`. Read REPORTED for the state the status records.

`Staged` means a group finished rolling out up to the [`rollingUpdate.partition`](../../concepts/architecture/omenative-update-strategies.md#pacing-with-rollingupdate) its components set, which holds that many instances on the old revision. `NotConfigured` means the service doesn't use rollouts.

A component row appears only when a component has a value for it:

| Row | What it shows |
| --- | --- |
| GROUP | The index of the component's rollout group. |
| STRATEGY | How the component rolls out. |
| GROUP-PHASE | The group's phase, or `Unknown` when the group has no status. |
| PHASE | The component's own rollout phase. |
| GROUP-CURRENT, GROUP-PREVIOUS | In a sequential group, the component rolling out now and the one before it. |
| STEP | The current canary step, as `step/total`. |
| GATE | What ends the step. |
| CAPACITY | The share of the component's replicas on the new revision at this step. |
| TRAFFIC | The new revision's traffic weight, as `target% -> observed%`: the step's `traffic`, then the weight the controller recorded once the step's capacity was ready. |
| ROLLED-OUT | The revision that finished rolling out. |
| READY | The newest revision whose pods are Ready. |
| PREVIOUS | The revision that rolled out before ROLLED-OUT. |

!!! warning "The traffic weight doesn't route requests"
    OME records a canary step's traffic weight in the InferenceService's status, but doesn't apply it to routing in this release. The new revision gets requests in proportion to its share of ready instances, which the step's `capacity` sets. [Canary progression](../rollouts/canary-progression.md#traffic-split) has the details.

In a canary group, ROLLED-OUT, READY and PREVIOUS don't change, even after the canary finishes. They keep the revisions from before the component joined the group, or stay empty if it started in one. The canary's own revisions show in [`explain`](#explain), as `stable=` and `target=`.

STRATEGY is one of:

| Strategy | Meaning |
| --- | --- |
| `Canary` | Steps move capacity to the new revision a share at a time and set its traffic weight, with a gate after each step. |
| `BlueGreen` | The default for a group with no progression or policy. Each component updates at most 25% of its replicas at a time, while both revisions serve requests. |
| `RollingUpdate` | The controller replaces pods a few at a time, within the surge and unavailable budget. |
| `Sequential` | Single-component blue-green groups that roll out one after another, shown as one group at the index of the first. |
| `Independent` | The component is in no rollout group and rolls out on its own. |
| `Unknown` | The CLI can't tell from the spec and status. |

GATE is one of:

| Gate | Meaning |
| --- | --- |
| `Immediate` | The step advances as soon as it's reached. |
| `Timed` | The step advances when its pause duration ends. |
| `Manual` | The step waits for a [`promote`](#promote-and-rollback). |
| `Analysis` | The step advances when its analysis metrics pass, after an optional `initialDelay` warm-up and a minimum bake of the step's pause duration. |

[Canary progression](../rollouts/canary-progression.md#how-a-step-advances) describes how a step advances, and [Canary metric analysis](../rollouts/canary-analysis.md) the analysis checks.

A canary waiting at any gate, including the last step, or holding after a [`repin`](#repin), is `Paused`, and so is REPORTED. Once the final gate passes, any remaining completion delay reports `Promoting`, with REPORTED `InProgress`.

The last row, ISSUES, lists issue codes for the service and for each component, in their own columns. An issue tied to a rollout group shows as `Code(group=N)`. Any issue also adds a `PartialData` warning. The issues are:

| Issue | Meaning |
| --- | --- |
| `AnalysisInconclusive` | A canary step's analysis is inconclusive. |
| `CanaryStatusMissing` | A canary group is in a phase that needs canary status, and the status has none. |
| `CanaryStatusUnexpected` | The status records a canary where the spec has no canary group. |
| `CanaryStepInvalid` | The canary's current step is outside the steps its group declares, or a step can't be read. |
| `ComponentStatusMissing` | `status.components` has no entry for a component. |
| `EpochUnverifiable` | The CLI can't tie the rollout status to the current generation. |
| `GroupStatusMissing` | A rollout group has no status yet. |
| `GroupStatusUnexpected` | The rollout coordination status names a group that the spec doesn't have. |
| `RevisionNameInvalid` | A revision name in the status is empty or invalid. |
| `SpecMalformed` | The rollout part of the spec is malformed. |
| `StatusMalformed` | The rollout status is malformed or contradicts the spec. |
| `TrafficInvalid` | The entire per-revision traffic list is withheld: revision names are invalid, hashes are duplicated, weights are outside 0 to 100, or their sum is not 100. |

Malformed evidence also forces REPORTED to `Unknown` before phase precedence is applied. Missing-status notices, `AnalysisInconclusive` and `EpochUnverifiable` do not by themselves mark the evidence malformed; they still qualify what the report can establish.

`-o wide` prints a row per component, with every field but COORDINATION as a column.

### Examples {#status-examples}

Show the rollout of `chat` in `prod` while its canary waits at a manual gate:

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

The engine's canary waits at the first of two steps, with 25% of its capacity on the new revision and a recorded traffic weight of 10%. PHASE is `Paused` and GATE is `Manual`, so it waits for a `promote`.

For a service whose engine is in no rollout group, print the report as JSON:

```bash
kubectl ome rollout status chat -n prod -o json
```

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RolloutStatusReport",
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
      "uid": "isvc-uid",
      "generation": 7,
      "evidence": "Observed",
      "collectedAt": "2026-08-31T18:30:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "Unknown",
      "reportedState": "Succeeded",
      "evidence": "Reported",
      "epoch": "Unverifiable",
      "coordinationReady": "NotApplicable"
    },
    "groups": [],
    "components": [
      {
        "type": "engine",
        "strategy": "Independent",
        "phase": "Stable",
        "traffic": []
      }
    ],
    "issues": [
      {
        "code": "EpochUnverifiable"
      }
    ]
  },
  "warnings": [
    {
      "code": "PartialData"
    }
  ]
}
```

The engine's current and update revisions match, so the CLI works out `Stable`, and `reportedState` is `Succeeded`.

## `explain`

```text
kubectl ome rollout explain INFERENCESERVICE [flags]
```

`explain` shows the rollout plan as you declared it, as it resolves now, and as the controller runs it, next to the progress the controller reports. Use it to see which plan a run follows and what holds a rollout.

### Flags {#explain-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json, or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#explain-output-fields}

The table has a section for each view of the plan:

| View | What it shows |
| --- | --- |
| `Declared` | The groups in `spec.rollout`, as written. |
| `Live` | The plan a new run would pin: the current spec, with the controller's policy resolution. Without an active run, PLAN shows `groups=0`. |
| `Effective` | The plan in force: the pinned plan while a run is active (`mode=Pinned; evidence=Reported`), otherwise the live plan (`mode=Live; evidence=Declared`). |

Each view lists these items:

| Item | What it shows |
| --- | --- |
| PLAN | The number of groups, with the plan mode in `Live` and `Effective`, and the evidence in `Effective`. |
| GROUP n | A rollout group: its strategy, its components, and where its plan comes from, as evidence/source. |
| CONFIG | The group's settings, one per line. |
| STEP i/n | A canary step: the capacity on the new revision, its traffic weight, and the gate that ends the step. |
| ISSUES | Issue codes for the view or the group. Any issue also adds a `PartialData` warning. |

The `Effective` view also shows the plan conditions, the holds and the observed progress:

| Item | What it shows |
| --- | --- |
| READY | The RolloutPlanReady condition, as status/reason: `True/Pinned` with a pinned run, `True/NoActiveRun` without one. `False` parks the rollout, and the old revision keeps serving. |
| DRIFT | The RolloutPlanDrift condition: `True/SpecNewerThanRun` or `True/PolicyNewerThanRun` when the spec or a policy changed during the run, otherwise `False/InSync`. The change waits for the next run or a [`repin`](#repin). |
| HOLD | What holds the rollout. |
| PHASE | The group's phase, as the controller reports it. |
| SEQUENCE | In a sequential group, the component rolling out now and the one before it, as `current=` and `previous=`. |
| REVISIONS | A canary group's revisions: `stable=` served when the canary started and is where a rollback returns, `target=` is rolling out, and `rejected=` is a target a rollback rejected. |
| TRAFFIC | The traffic weight the controller recorded for each revision, as `component:revision=percent`. |
| COMPONENT type | A component in no rollout group: its strategy, phase and evidence, then rows for its revisions, traffic and issues. REVISIONS shows the [`status`](#status-output-fields) revisions as `current=`, `ready=` and `previous=`. |

READY is `False` with the reason `PolicyNotFound`, `PolicyNotReady`, `ProgressionMismatch`, `PlanInvalid` or `ProviderUnbound`. READY and DRIFT show `Unobserved; evidence=Unavailable` until the controller sets the condition. They show `Invalid/Unknown`, and ISSUES lists `PlanConditionMalformed`, when the condition is malformed or conflicts with whether a run is active.

The source in a GROUP row is one of:

| Source | Meaning |
| --- | --- |
| `Inline` | The group sets its own progression. If it also names a rollout policy, the inline progression wins, and CONFIG shows the policy as `shadowed=`. |
| `Policy` | A rollout policy supplies the progression. The CLI can't read its steps outside the pinned plan, so the `Live` view, and `Effective` without an active run, list `PolicyBodyUnavailable` in ISSUES. |
| `Defaulted` | The group names no progression or policy, so it rolls out blue-green. The pinned plan records it as `Inline`, with CONFIG `origin=Unknown`. |
| `Unknown` | The pinned plan records another source, so ISSUES lists `ActiveRunMalformed`. |

CONFIG lists a group's settings by key:

| Key | What it shows |
| --- | --- |
| `strategy=defaulted` | The group's source is `Defaulted`. |
| `origin=Unknown` | The CLI can't tell where the progression came from: the group is malformed, or it's a blue-green group in the pinned plan. |
| `policy=` | The policy that supplies the progression, as `RolloutPolicy/name`. The pinned plan adds its generation, as in `policy=RolloutPolicy/guarded@4`. |
| `digest=` | The digest of the group's plan, as in `rp1:aaaaaaaaaaaa`: the digest the run pinned, or outside the pinned plan, the one the controller reports. |
| `shadowed=` | A policy the group names, which its inline progression overrides. It's absent from the pinned plan. |
| `soak=` | How long the controller waits after the group finishes before the next group starts. |
| `surge=`, `unavailable=` | A rolling-update group's `maxSurge` and `maxUnavailable`. |
| `ratio=` | The group's `maintainRatio.tolerance`: how far, in percent, the replica ratio between its components can drift during the rollout. |

The last four settings print as `value(Source;Effect)`, as in `soak=1m0s(Configured;IgnoredFinalGroup)`. A setting that takes effect leaves out the effect, as in `surge=25%(Defaulted)`. One the CLI can't resolve leaves out the value and the parentheses, as in `ratio=Unresolved`.

| Part | Value | Meaning |
| --- | --- | --- |
| Source | `Configured` | The spec sets the value. |
| Source | `Defaulted` | The spec leaves a rolling-update budget unset, so it's `25%`. |
| Source | `Unresolved` | The CLI can't work out the value, as for a ratio left to the operator's default. |
| Effect | `IgnoredFinalGroup` | A soak on the last group of a sequential plan. |
| Effect | `IgnoredPlanShape` | A soak on a canary group or outside a sequential plan, or a ratio on a canary group. |
| Effect | `IgnoredSingleComponent` | A ratio on a group with one component. |
| Effect | `Unknown` | The plan is malformed, or the CLI can't read a policy's steps. |

HOLD is one of:

| Hold | Evidence | Meaning |
| --- | --- | --- |
| `GlobalPause` | `Declared` | The `ome.io/rollout-paused` annotation is `true` or `freeze`. [`resume`](#pause-and-resume) clears it. |
| `PlanParked` | `Reported` | READY is `False`, so the rollout is parked. READY names the cause. |
| `CanaryPreStep` | `Reported` | After a repin to a step with a higher traffic weight than last recorded, the canary holds before the step. [The canary holds after the repin](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md#what-the-controller-does-next) shows how to release it. |
| `ObservedPaused` | `Reported` | The controller reports the group as `Paused`, as for a canary waiting at a gate before its last step. |

`-o wide` prints a row per canary step, or per group without steps, with the group's fields on each row. A component in no rollout group gets a row with VIEW `Observed`.

### Examples {#explain-examples}

Explain the rollout of `chat` in `prod`:

```bash
kubectl ome rollout explain chat -n prod
```

```output
VIEW        ITEM        DETAIL
Declared    PLAN        groups=1
            GROUP 0     Canary; components=engine; source=Declared/Inline
            STEP 1/2    capacity=25%; traffic=10%; gate=Manual
            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate
Live        PLAN        mode=Live; groups=1
            GROUP 0     Canary; components=engine; source=Declared/Inline
            STEP 1/2    capacity=25%; traffic=10%; gate=Manual
            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate
Effective   PLAN        mode=Pinned; evidence=Reported; groups=1
            READY       True/Pinned; evidence=Reported
            DRIFT       True/SpecNewerThanRun; evidence=Reported
            GROUP 0     Canary; components=engine; source=Reported/Policy
            CONFIG      policy=RolloutPolicy/guarded@4
                        digest=rp1:aaaaaaaaaaaa
            PHASE       Canarying
            REVISIONS   stable=aaaaaaaa,target=bbbbbbbb
            TRAFFIC     engine:aaaaaaaa=80%,engine:bbbbbbbb=20%
            STEP 1/2    capacity=50%; traffic=20%; gate=Manual
            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate
            ISSUES      EpochUnverifiable
```

The run pinned its plan from the rollout policy `guarded` at generation 4. That plan puts half the capacity and a 20% traffic weight on the new revision until a manual `promote`, then all of it. The spec now declares a quarter of the capacity and a 10% weight, so DRIFT is `True/SpecNewerThanRun`.

## `history`

```text
kubectl ome rollout history INFERENCESERVICE [flags]
```

`history` shows the rollout records the InferenceService keeps in its status: the active run, the last closed run, where each group's plan came from, and the current revisions. The status keeps only those two runs, so `history` isn't an audit trail.

### Flags {#history-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json, or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#history-output-fields}

Each row is one record, and TYPE says what kind:

| TYPE | What the row shows |
| --- | --- |
| `WINDOW` | All the kept records: STATE is `Partial` when there are any, otherwise `Empty`, or `Unavailable` when the service has no rollout status. |
| `CURR` | The current rollout state, as [`status`](#status) reports it, with its evidence in IDENT and its epoch in DETAIL (`Unverified` or `N/A`). |
| `ACTIVE` | The open run: `Active`, the last part of its run ID in IDENT, when it opened, and its number of groups (`G:`) and targets (`T:`) in DETAIL. |
| `TARGET` | A revision the active run rolls a component out to, with its evidence in STATE. |
| `LAST` | The last closed run: its outcome (`Completed`, `RolledBack` or `Superseded`), when it closed, and its number of groups. |
| `PROV-A`, `PROV-L`, `PROV-C` | Where a group's plan came from in the active run, the last run, or a run starting now: `Inline` or `Policy`, the group, and a short digest. |
| `REV` | A current revision of a component, with its phase and role: `Current`, `Ready` or `Previous`, the ROLLED-OUT, READY and PREVIOUS revisions of [`status`](#status-output-fields). |

TIME is in UTC, and ISS counts the issues in the whole report. In the `PROV` rows, DETAIL names the policy the group uses or shadows. STATE shortens long values, as in `NotConf...` for `NotConfigured`. A policy name over 10 characters prints as its first character, `#` and a hash. REV shortens four phases: `BGStandby` is BlueGreenStandby, `RollingBk` is RollingBack, `ScaleDown` is ScalingDown, and `AwaitNext` is AwaitingNextComponent.

`-o wide` prints every kept field in full, one row per record.

### Examples {#history-examples}

Show the kept history of `chat` in `prod`:

```bash
kubectl ome rollout history chat -n prod
```

```output
TYPE     STATE       COMP     IDENT          TIME          DETAIL       ISS
WINDOW   Partial     -        -              -             bounded      1
CURR     Unknown     -        Reported       -             Unverified   1
ACTIVE   Active      -        0123456789ab   08-31T18:00   G:1 T:1      1
TARGET   Reported    engine   cccccccc       -             run-target   1
LAST     Completed   -        -              08-31T17:00   G:1          1
PROV-A   Inline      g0       cca45ec1fb0f   08-31T18:30   -            1
PROV-L   Inline      g0       cca45ec1fb0f   08-31T17:00   -            1
PROV-C   Inline      g0       cca45ec1fb0f   -             -            1
REV      Unknown     engine   aaaaaaaa       -             Current      1
REV      Unknown     engine   bbbbbbbb       -             Ready        1
REV      Unknown     engine   dddddddd       -             Previous     1
```

The active run `0123456789ab` opened at 18:00 UTC with one group and one target, engine revision `cccccccc`. The run before it completed at 17:00. All three plan records show the same inline plan.

## `validate`

```text
kubectl ome rollout validate INFERENCESERVICE [flags]
```

`validate` checks the rollout, [traffic](../../concepts/rollouts-and-traffic/traffic-policy.md) and [autoscaling](../../concepts/serving/autoscaler-policy.md) configuration of an InferenceService, and the controller's evidence that parts of it took effect. It prints the report, then exits `0` when OVERALL is `Valid`, `2` when OVERALL is `Invalid` or `Unverifiable`, and `1` when it can't run.

### Flags {#validate-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json, or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#validate-output-fields}

Each row is one check. COMP names the component for a per-component check. RESULT is `Valid`, `Invalid`, `Unverifiable` or `NotApplicable`, and SOURCE shows the evidence and its freshness, as in `Computed/Current`. The freshness is `Current`, `Stale`, `Unverifiable` or `NotApplicable`.

OVERALL is `Invalid` when any check is, otherwise `Unverifiable` when any check is, and otherwise `Valid`. When OVERALL is `Invalid`, `validate` then prints `error: rollout validation found invalid configuration` to stderr. When it's `Unverifiable`, it prints `error: rollout validation could not verify all prerequisites`.

| CHECK | What it checks |
| --- | --- |
| ROLLOUT-REFS | Rollout policy references: the kind is empty or `RolloutPolicy`, the name a DNS-1123 subdomain, and the progression `canary`, `blueGreen` or `rollingUpdate`. Admission accepts any non-empty name. |
| ROLLOUT-PLAN | At most 3 groups of 1 to 3 components, one progression per group, 20 canary steps, 10 analysis metrics per step, and the canary, coordination and lifecycle rules. |
| ROLLOUT-ORDER | Groups leave `order` unset. With more than one group, `spec.rollout.groupOrdering` is `Concurrent`, or every group is a single-component blue-green group. |
| ROLLOUT-RESOLVE | Whether the controller resolved the plan, from the RolloutPlanReady condition. `NotApplicable` without rollout groups or an active run. |
| TRAFFIC-SPEC | `spec.traffic` and the traffic annotations. |
| TRAFFIC-READY | Whether the controller applied the traffic settings, from `status.traffic`. `NotApplicable` when the service sets none. `Unverifiable` while a BackendPolicyUnsupportedFields condition reports ignored settings, even if BackendPolicyReady is `True`. |
| SCALING-POLICY | The alpha `spec.scalingPolicy`. |
| AUTOSCALER-SPEC | The autoscaling settings: one row for the service, then one per component. |
| AUTOSCALER-RESOLVE | For a component with an `autoscalerPolicyRef` to an alpha AutoscalerPolicy, whether its AutoscalerResolved condition is `True` for the current generation. |

AUTOSCALER-SPEC's service row checks the `ome.io/autoscalerClass`, `ome.io/metrics` and `ome.io/targetUtilizationPercentage` annotations. Each component's row checks its `autoscaler` block, its replica bounds and its `autoscalerPolicyRef`. [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md#what-admission-rejects), [Canary progression](../rollouts/canary-progression.md#what-admission-rejects) and [Component autoscaling](../../concepts/serving/component-autoscaling.md#what-admission-rejects) list what admission rejects in these fields.

ROLLOUT-RESOLVE, TRAFFIC-READY and AUTOSCALER-RESOLVE check the controller's evidence. They report any problem as `Unverifiable`, including a plan or autoscaler policy that fails to resolve, and `-o wide` shows the cause.

!!! note "An InferenceService with rollout groups never passes"
    With rollout groups, or while a run is active, ROLLOUT-RESOLVE is always `Unverifiable`. OVERALL is then `Unverifiable`, and `validate` exits `2` even when every other check is `Valid`. To gate CI on such a service, fail when any `.content.checks[].result` in the `-o json` output is `Invalid`.

To see whether the pinned plan still matches the spec, check DRIFT in [`explain`](#explain).

`-o json` and `-o yaml` also list warnings: `PartialData` when OVERALL is `Unverifiable`, and `StaleEvidence` when any check's freshness is `Stale`.

`-o wide` splits SOURCE into EVIDENCE and FRESHNESS, and adds each check's issue codes.

### Examples {#validate-examples}

Validate `chat` in `prod`:

```bash
kubectl ome rollout validate chat -n prod
```

```output
CHECK             COMP     RESULT          SOURCE
OVERALL           -        Valid           Computed/Current
ROLLOUT-REFS      -        Valid           Computed/Current
ROLLOUT-PLAN      -        Valid           Computed/Current
ROLLOUT-ORDER     -        Valid           Computed/Current
ROLLOUT-RESOLVE   -        NotApplicable   Computed/NotApplicable
TRAFFIC-SPEC      -        Valid           Computed/Current
TRAFFIC-READY     -        NotApplicable   Computed/NotApplicable
SCALING-POLICY    -        Valid           Computed/Current
AUTOSCALER-SPEC   -        Valid           Computed/Current
AUTOSCALER-SPEC   engine   Valid           Computed/Current
```

ROLLOUT-RESOLVE and TRAFFIC-READY are `NotApplicable`, because this InferenceService has no rollout groups, active run or traffic settings. Every other check is `Valid`, and `validate` exits `0`.

## `pause` and `resume`

!!! note "Alpha"
    These commands are alpha. Their flags and behavior can change between releases.

```text
kubectl ome rollout pause INFERENCESERVICE [flags]
kubectl ome rollout resume INFERENCESERVICE [flags]
```

`pause` holds rollout work across the whole InferenceService by setting the `ome.io/rollout-paused` annotation to `true`. `resume` removes the annotation, which has two depths:

| Value | Effect |
| --- | --- |
| `true` | No update, create or migration work starts, and migrations stop advancing. Updates and creates under way can finish, and the restart policy keeps repairing instances. |
| `freeze` | Repair of existing instances stops too: no repair starts, and one under way finishes. Kubelet container restarts continue. |

`pause` sets `true`; set `freeze` with `kubectl annotate`. A pause covers the [OMENative lifecycle](../../concepts/architecture/omenative-update-strategies.md) of the whole service. Deliberate scale-down and deletion still proceed. Timed canary gates keep aging, so a canary can advance as soon as you resume. [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md) shows both commands in use.

### Flags {#pause-and-resume-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--discard-pending-actions` | `false` | `resume` only. Atomically discard pending promote/rollback mailboxes; requires `--yes` |
| `--dry-run` | `none` | Dry-run mode: none, client or server |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output: table, wide (bounded), json or yaml |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### How the actions run {#how-the-actions-run}

`pause`, `resume`, `promote`, `rollback` and `repin` change the InferenceService the same guarded way:

1. The CLI checks the flags, reads what the action needs, and refuses if the action isn't safe now. A refusal changes nothing and exits `1`.
2. It prints a preview of the exact change to stderr: the context, the namespaces, the target with its UID and resourceVersion, the annotation, and warnings.
3. It asks `Confirm this exact action? [y/N]` on stderr, and goes ahead only on `y` or `yes`. `--yes` confirms without the prompt, and scripts need it.
4. It sends a JSON Patch that tests the UID and resourceVersion from the preview. If the InferenceService changed since the read, the patch fails and the CLI exits `3`.
5. It prints the result, an ActionResult, to stdout.

Any other answer, or a missing terminal, fails with `action not confirmed; noninteractive input requires --yes`. The command has 45 seconds in all, including the prompt, and fails with `context deadline exceeded` when they run out.

`--dry-run client` runs the checks and the prompt, then stops before the patch. `--dry-run server` sends the patch with `dryRun=All`, so the API server checks it and stores nothing.

`applied` in the result means the API server stored the annotation, not that the controller acted on it. Run the `follow-up` command to see what the controller did.

If the patch fails with anything but a refusal or a conflict, it may have been stored, so check [`status`](#status) before you run the command again. [Guarded actions](guarded-actions.md) describes the whole contract, including [unknown outcomes](guarded-actions.md#when-the-outcome-is-unknown) and [the ActionResult](guarded-actions.md#the-actionresult).

Every action also refuses a target that's being deleted, has an unsafe identity, or exceeds the CLI's inspection bounds. The same goes for a target in multi-cluster placement, which is alpha and still in development. [Target checks](guarded-actions.md#target-checks) lists these messages.

### Refusals {#pause-and-resume-refusals}

`pause` and `resume` also refuse in these cases:

| Message | Cause |
| --- | --- |
| `action refused: service is already paused; freeze is preserved` | `pause` when the annotation is already `true` or `freeze`. |
| `action refused: service has no recognized pause` | `resume` when the service isn't paused. |
| `action refused: a rollout promote or rollback mailbox is present` | The promote or rollback annotation is present. On a paused service, `resume --discard-pending-actions --yes` removes it. |
| `action refused: no applicable active rollout or lifecycle work was observed` | `pause` when no rollout or lifecycle work is in progress. |
| `action refused: controller safety evidence is stale or inconsistent` | `pause` when the controller's status contradicts itself or the InferenceService. |
| `action refused: active runtime is unavailable, inconsistent or unbound` | No component uses OMENative, or the controller reports a different runtime than the CLI resolved. |
| `action refused: a logical annotation value is unsafe to preview` | The annotation value to replace or remove is unsafe to show. |

### Examples {#pause-and-resume-examples}

Pause `chat` in `prod`:

```bash
kubectl ome rollout pause chat -n prod
```

The command prints its preview to stderr and asks for confirmation. After your `y`, the result is:

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

Resume without a prompt, as a script would:

```bash
kubectl ome rollout resume chat -n prod --context=prod-cluster --yes
```

```output
ALPHA guarded action preview (not controller convergence)
FIELD               VALUE
Action              rollout resume
Context             prod-cluster
Workload NS         prod
OME NS              ome
Target              InferenceService/chat
UID                 5d9f3c2a-8b41-4f7e-a0c6-1e2b3c4d5e6f
ResourceVersion     184467
Dry-run             none
Affected            engine
Pause depth         true
Remove annotation   ome.io/rollout-paused
Value               "true"
Active records      operations=0 migrations=0
Scope: service-wide OMENative lifecycle, not a full workload freeze.
true holds Update/Create/Migration; RestartPolicy repair continues.
freeze also holds existing-instance repair; resume clears either depth.
Canary timed gates continue aging and may advance immediately on resume.
Deliberate scale-down and deletion teardown can still proceed.
Parent UID/resourceVersion CAS is not a multi-object transaction.
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

The preview comes first, on stderr, from the `ALPHA` line to the last warning. The table after it is the ActionResult, on stdout.

## `promote` and `rollback`

!!! note "Alpha"
    These commands are alpha. Their flags and behavior can change between releases.

```text
kubectl ome rollout promote INFERENCESERVICE [flags]
kubectl ome rollout rollback INFERENCESERVICE [flags]
```

`promote` and `rollback` act on the canary group of the active run, at the step the canary has reached.

`promote` sets the `ome.io/rollout-promote` annotation to the canary revision hash, which passes the manual gate the canary waits at. Each promote advances one step. The controller removes the annotation once it records the advance, and another `promote` refuses until then. With `--override-analysis --yes`, it passes an analysis gate instead, for this step only, skipping its checks, warm-up and bake.

`rollback` sets the `ome.io/rollout-rollback` annotation to `true`. It works at any step while the canary is `Pending`, `Canarying`, `Paused`, `Promoting` or `Failed`. The controller returns every member of the canary group to its own stable revision, and the rollout spec stays as it is.

The group stays on its stable revisions until a different target revision appears, and the controller removes the rollback annotation only then. While the annotation is there, `pause`, `promote`, `rollback` and `repin` refuse, and so do `kubectl ome scale`, `kubectl ome runtime sync` and `kubectl ome migration start`. On a paused service, `resume --discard-pending-actions --yes` removes the annotation. To remove it yourself, run:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/rollout-rollback-
```

```output
inferenceservice.ome.io/chat annotated
```

Removing the annotation doesn't retry the rejected revision.

Both commands follow the [guarded flow](#how-the-actions-run). Their preview adds the canary's run, phase, step, gate, traffic weights and revisions, so you can check the step you act on. It counts steps from 0, as in `0 of 2 (zero-based)`, while `status` and `explain` count from 1. At an analysis step, it also shows the analysis state and each metric's results. [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md) shows both commands in use.

### Flags {#promote-and-rollback-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--dry-run` | `none` | Dry-run mode: none, client or server |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output: table, wide (bounded), json or yaml |
| `--override-analysis` | `false` | `promote` only. Bypass only this active analysis gate, including warm-up and bake; requires `--yes` |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Refusals {#promote-and-rollback-refusals}

Besides the refusals [every action shares](guarded-actions.md#target-checks), `promote` and `rollback` refuse in these cases:

| Message | Cause |
| --- | --- |
| `action refused: promote requires an active indefinite manual gate; analysis requires explicit override` | `promote` when the canary isn't waiting at a gate it can pass, as the list below describes. |
| `action refused: globally paused canary cannot consume a request` | The service is paused. |
| `action refused: a rollout promote or rollback mailbox is present` | A promote request still waits for the controller, or the rollback annotation remains. |
| `action refused: no applicable active rollout or lifecycle work was observed` | No run with a canary group is active, or the canary was rolled back. For `rollback`, also a canary outside the five phases it accepts. |
| `action refused: controller safety evidence is stale or inconsistent` | The canary's status doesn't match the InferenceService or its pinned plan, or the service has more than one canary group. |
| `action refused: active runtime is unavailable, inconsistent or unbound` | A canary component doesn't use OMENative, or the reported runtime differs. |

`promote` passes the gate only when all of these hold:

- The gate matches the flag: `Manual` for plain `promote`, `Analysis` with `--override-analysis --yes`. Timed and immediate gates advance on their own.
- Before the last step, the canary is `Paused`. At the last step, it's `Promoting`, and the step's traffic weight is 100%.
- The controller recorded when the canary entered the step, and both sides of TRAFFIC match.
- The controller finished the previous promote.

The last-step phase check currently differs from the controller: a final gate waits in `Paused`, so the CLI refuses promotion there. See [Final-step promotion](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#final-step-promotion) for the limitation and the direct annotation request.

It also refuses while the canary holds after a repin, which [`explain`](#explain) shows as HOLD `CanaryPreStep`. [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md#what-the-controller-does-next) shows how to release the hold.

### Examples {#promote-and-rollback-examples}

These examples run in a kubeconfig context named `prod-us-east`. Promote a canary waiting at a manual gate:

```bash
kubectl ome rollout promote chat -n prod
```

After the preview and your `y`, the result is:

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

Promote past an analysis gate, skipping its checks for this step:

```bash
kubectl ome rollout promote chat -n prod --override-analysis --yes
```

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

Roll back the canary:

```bash
kubectl ome rollout rollback chat -n prod
```

After the preview and your `y`, the result is:

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

In each result, `revision-hash` is the canary revision. After a rollback, it's the revision the controller rejects.

## `repin`

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

```text
kubectl ome rollout repin INFERENCESERVICE [flags]
```

A run pins its plan when it starts. An edit to the rollout spec or a rollout policy then waits for the next run, and [`explain`](#explain) shows DRIFT. Run `repin` to move the active run to the current plan now. It sets the `ome.io/rollout-repin` annotation to the current plan's digest, and the controller repins only if its own render of the plan has that digest.

The controller removes the annotation and reports the outcome as an event on the InferenceService:

| Event | Type | Meaning |
| --- | --- | --- |
| `RolloutPlanRepinned` | Normal | The run follows the new plan, keeping its identity and progress, and DRIFT returns to `False/InSync`. |
| `RolloutRepinRejected` | Warning | The current plan doesn't render, or another edit changed its digest. Run `repin` again to request the current plan. |

A canary keeps its step number, or takes the new plan's last step if the new plan has fewer steps. If that step's traffic weight is higher than the one last recorded, the canary holds before the step, with HOLD `CanaryPreStep`. When the current plan already matches the pinned plan, the controller removes the annotation without an event.

An annotation left when a run closes stays until the next run opens. Set by hand, the value `now` skips the digest check, as [Repin by annotation](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md#repin-by-annotation) shows.

The live plan must keep the pinned topology: the same number of groups, and the same progression, components, order, soak and ratio in each. [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md) shows the command in use.

`repin` follows the [guarded flow](#how-the-actions-run), with these differences:

- It reads only the InferenceService, and has no `--ome-namespace` flag.
- Its follow-up command is `kubectl ome rollout explain`.
- Its ActionResult shows `run-id`, `pinned-plan-digest`, `requested-plan-digest` and `group-count` in place of `request-id` and `revision-hash`.
- It reports a server error, a timeout or a network error as an unknown outcome, as in `rollout repin request outcome unknown; do not replay, check rollout explain`. Check `explain` before you run `repin` again.

### Flags {#repin-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--dry-run` | `none` | Dry-run mode: none, client or server |
| `-o`, `--output` | `table` | Output: table, wide (bounded), json or yaml |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Refusals {#repin-refusals}

Besides the refusals [every action shares](guarded-actions.md#target-checks), `repin` refuses in these cases:

| Message | Cause |
| --- | --- |
| `rollout repin refused: no active pinned run` | No run is active. |
| `rollout repin refused: empty live plans are not supported by this command` | `spec.rollout.groups` is empty. |
| `rollout repin refused: controller plan evidence is missing, stale or inconsistent` | The run, READY or DRIFT doesn't show a pinned, drifted plan the CLI can check, or the live spec fails validation. |
| `rollout repin refused: the current plan is already pinned` | DRIFT is `False/InSync`, or the live plan has the pinned digest. |
| `rollout repin refused: live and pinned rollout topology differ` | The groups changed shape. |
| `rollout repin refused: this command supports at most one canary group` | The live spec has more than one canary group. |
| `rollout repin refused: a rollout action mailbox is already present` | A repin or promote request still waits for the controller, or the rollback annotation remains. |

### Examples {#repin-examples}

Have the API server check the repin without storing it:

```bash
kubectl ome rollout repin chat -n prod --dry-run server --yes
```

```output
FIELD                   VALUE
action                  rollout repin
target                  InferenceService/prod/chat
dry-run                 server
accepted                Yes
applied                 No
run-id                  chat-0123456789ab
pinned-plan-digest      rp1:56fe0ddd6edd
requested-plan-digest   rp1:1d153db2a5df
group-count             1
message                 API dry-run accepted; no changes persisted.
follow-up               kubectl ome rollout explain chat -n prod --context=pr...
hint                    Use -o json or -o yaml for full values.
```

Repin, confirming at the prompt:

```bash
kubectl ome rollout repin chat -n prod
```

After the preview and your `y`, the result is:

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

The request asks to move run `chat-0123456789ab` from plan `rp1:56fe0ddd6edd` to `rp1:1d153db2a5df`. Run the follow-up `explain` to see whether DRIFT returned to `False/InSync`.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every subcommand that finishes. `validate` returns it only when OVERALL is `Valid`. |
| `1` | General error | Every subcommand, for an invalid name or flag, a failed API request, a refusal, an action you didn't confirm, Ctrl-C, or an action whose outcome is unknown. |
| `2` | Assertion unmet | `validate`, after it prints the report, when OVERALL is `Invalid` or `Unverifiable`. |
| `3` | Mutation conflict | `pause`, `resume`, `promote`, `rollback` and `repin`, when the API server rejects the guarded patch because the InferenceService changed after the CLI read it. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md)
- [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md)
- [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
