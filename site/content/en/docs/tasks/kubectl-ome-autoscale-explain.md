---
title: "Explain Effective Autoscaling"
linkTitle: "Autoscale Explain"
weight: 21
date: 2026-09-26
description: >
  Use kubectl ome autoscale explain to see which layer — inline block, AutoscalerPolicy, runtime, legacy annotation, or default — supplies each component's autoscaler, and whether the controller-reported evidence matches it.
---

Per-component autoscaling in OME resolves through a [five-layer precedence chain](/ome/docs/concepts/autoscaler_policy/#precedence-inline-wins): an inline `autoscaler` block, an `autoscalerPolicyRef`, the runtime's component block, the legacy annotation, or the built-in default. Reading the InferenceService spec alone doesn't tell you which layer actually won, and reading its status alone doesn't tell you whether the controller has caught up with what you declared.

`kubectl ome autoscale explain` answers both questions in one report. For each component (`engine`, `decoder`, `router`) it:

1. resolves the **declared** autoscaling — class, winning precedence layer, replica bounds, scale-to-zero eligibility, and expected scale target — from the InferenceService and the controller-selected active runtime configuration (the live ServingRuntime, or the pinned `ControllerRevision` snapshot when [`autoSync: false`](/ome/docs/concepts/runtime-revision)), and
2. compares it with the **reported** evidence the controller mirrors onto the InferenceService itself (`status.components.<component>.autoscaler` and `scaleTargetRef`).

The command never queries child autoscaler or workload objects — no HorizontalPodAutoscalers, ScaledObjects, Deployments, or InferenceReplicas. For live scaler state read directly from those objects, use `kubectl ome autoscale status` instead (which needs extra RBAC, as noted on the [plugin page](/ome/docs/tasks/kubectl-ome)).

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome).
- The baseline read-only `kubectl-ome-reader` role from the plugin page is sufficient: the command reads the InferenceService, runtimes and models (`ome.io`, `get`/`list`), and — for pinned services — `ControllerRevision` snapshots (`apps`, `get`/`list`). Unlike `autoscale status`, it needs no HPA or KEDA rules.

## Run the command

```bash
kubectl ome autoscale explain chat -n prod
```

The single argument is an InferenceService name; `-n` selects its namespace. Additional flags:

| Flag | Meaning |
|------|---------|
| `-o`, `--output` | `table` (default), `json`, or `yaml`. |
| `--ome-namespace` | Namespace where the OME control plane is installed (default `ome`). Only matters for pinned services: the active configuration's `ControllerRevision` snapshot is read from here. |

Standard kubectl connection flags (`--kubeconfig`, `--context`) also apply.

A healthy single-engine RawDeployment service looks like this:

```
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

The command prints one column per component, and exits successfully even when the verdict is `Mismatch` or `Invalid` — the report *is* the answer. It fails only when evidence cannot be safely collected (the InferenceService doesn't exist, or the runtime evidence cannot be bound to the same API snapshot).

## Reading the report

| Row | Meaning |
|-----|---------|
| `STATE` | Overall verdict, repeated in every column: `OK`, `Partial` (some evidence unavailable), `Mismatch` (reported evidence disagrees with the declaration), `Unsupported`, or `Invalid`. Severity wins: Invalid > Unsupported > Mismatch > Partial > OK. |
| `MODE` | The component's deployment mode: `Raw`, `Native`, `Multi`, or `Virtual`. Only Raw and Native dispatch an OME-managed autoscaler; on Multi and Virtual the declared configuration is `unsupported` (`mode-unsupported`). |
| `POLICY` | The service **scaling-policy mode** — `spec.scalingPolicy` on the InferenceService, else the runtime's, else `Independent`. This is *not* the AutoscalerPolicy: only `Independent` is supported today, and `Proportional` / `Pinned` make the report `Unsupported` (`policy-unsupported`). |
| `DESIRED` | `<class>/<layer>` — the declared autoscaler class (`HPA`, `KEDA`, `External`, `None`) and the precedence layer that supplied it (see below). |
| `RANGE` | Effective `minReplicas..maxReplicas` after defaulting. |
| `ZERO` | Scale-to-zero: `-` not requested, `yes` eligible (KEDA with triggers), `?` unknown, `unsupported`, or `invalid` (requested without the KEDA admission gate). |
| `EXPECTED-TARGET` | The scale target the declaration should produce: `Deployment/<isvc>-<component>` on RawDeployment, `InferenceReplica/<isvc>-<component>` on OMENative. |
| `REPORTED` | `<class>/<layer>/<owner>` from `status.components.<component>.autoscaler` — what the controller says it resolved (`ome`, `external`, or `none` as the owner). `-` when nothing is reported yet, `?` when status can't be trusted. |
| `REPORTED-TARGET` | The controller-published `status.components.<component>.scaleTargetRef`. |
| `CUR/DES` | Mirrored `currentReplicas/desiredReplicas`. A trailing `?` (for example `0/2?`) marks ambiguous evidence — a zero can mean scaled-to-zero or not yet started. |
| `LAST-SCALE` | Mirrored last scale time (UTC). |
| `CONDITION-EVIDENCE` | Whether backend scaler conditions were mirrored at all: `reported`, `missing`, `unknown`, or `invalid`. |
| `CONDITIONS` | The mirrored conditions themselves — HPA: `AbleToScale`, `ScalingActive`, `ScalingLimited`; KEDA: `Ready`, `Active`, `Fallback`, `Paused`. |
| `CHECK` | The per-component declared-vs-reported comparison: `match`, `mismatch`, `missing` (nothing reported), `unknown`, or `invalid`. |
| `WHY` | The highest-priority issue code for the component, with `,+N` for the rest. `-` when there is nothing to explain. Actionable configuration errors sort before mismatches, which sort before missing/stale evidence. |

Issues that belong to no rendered component — for example, reported status for a component the declaration doesn't produce — appear in an extra `SERVICE` column (`decoder:unexpected-component`).

## Which layer is effective

The second element of the `DESIRED` cell is the direct answer to "which configuration is my component actually running on":

| Layer | Meaning |
|-------|---------|
| `isvc` | The inline `spec.<component>.autoscaler` block. |
| `policy` | A rendered [`autoscalerPolicyRef`](/ome/docs/concepts/autoscaler_policy). |
| `runtime` | The resolved ServingRuntime's component-level `autoscaler` block. |
| `legacy` | The deprecated `ome.io/autoscalerClass` annotation (RawDeployment only). |
| `default` | The built-in HPA with a single CPU 80% utilization metric. |

**AutoscalerPolicy refs are not rendered by the CLI.** Rendering depends on controller-local bindings (metric providers), so when a policy ref is the winning layer, `DESIRED` shows `?/policy` with `WHY policy-unavailable` — the declared class is genuinely unknown to a read-only client, and `ZERO` is `?` because the render may request KEDA scale-to-zero that the visible bounds don't show. What actually rendered is on the reported side: once the controller reconciles, `REPORTED` shows for example `KEDA/policy/ome`, and the render provenance digests live on the [InferenceService status](/ome/docs/concepts/autoscaler_policy/#conditions-and-status).

## Whether the reported evidence matches

`CHECK` compares four declared properties against the mirrored status: class, owner (`managedBy`), spec source, and target identity. Each disagreement adds an issue:

- `class-mismatch` — declared HPA but the controller reports KEDA (or vice versa).
- `owner-mismatch` — the reported owner disagrees with what the declared class implies.
- `source-mismatch` — the controller resolved a different precedence layer than the explain resolution predicts.
- `target-mismatch` — the reported scale target is not the expected `<isvc>-<component>` object.

A mismatch is **observational, not causal**: the report describes one bound API snapshot and makes no claim about why the two sides disagree or whether a rollout has converged. The most common benign cause is that the controller simply hasn't reconciled your latest edit yet — which the command detects through status freshness rather than guessing:

- Reported evidence is trusted only when `status.observedGeneration` equals `metadata.generation`.
- When status lags the spec, every reported cell shows `?` and `WHY` shows `status-stale` (re-run after the controller catches up).
- When `observedGeneration` is absent, `status-unobserved`; when it is *ahead* of the generation, the evidence is contradictory and the report is `Invalid` (`status-invalid`).

A component whose declaration is fine but that has no mirrored autoscaler status at all shows `REPORTED -`, `CHECK missing`, and `WHY status-missing`:

```
FIELD                ENGINE
STATE                Partial
MODE                 Raw
POLICY               Independent
DESIRED              HPA/default
RANGE                1..4
ZERO                 -
EXPECTED-TARGET      Deployment/svc-engine
REPORTED             -
REPORTED-TARGET      -
CUR/DES              -
LAST-SCALE           -
CONDITION-EVIDENCE   missing
CONDITIONS           -
CHECK                missing
WHY                  status-missing
```

## WHY codes

| Alias | Meaning |
|-------|---------|
| `class-invalid` | The resolved autoscaler class is not `HPA`, `KEDA`, `External`, or `None`. |
| `keda-triggers` | A KEDA autoscaler with no triggers. |
| `keda-config-invalid` | The winning KEDA block fails the dispatch validation the controller applies. |
| `hpa-metric-invalid` | A malformed HPA metric in the winning block. |
| `keda-idle-invalid` | KEDA `idleReplicaCount` is not below the effective minimum. |
| `keda-name-conflict` | The KEDA advanced HPA-name override collides with the component's scale-target name. |
| `legacy-invalid` | The deprecated `ome.io/autoscalerClass` annotation carries an invalid value. |
| `bounds-invalid` | `minReplicas`/`maxReplicas` are not a valid pair. |
| `zero-invalid` / `zero-unsupported` | Scale-to-zero requested without the KEDA gate / on a mode that cannot honor it. |
| `policy-invalid` / `policy-unsupported` | The scaling-policy mode is unknown / `Proportional` or `Pinned`. |
| `policy-ref-invalid` | A stored `autoscalerPolicyRef` is malformed. |
| `policy-unavailable` | A policy ref won the precedence; the render is not visible to this read-only command. |
| `mode-unsupported` | MultiNode or VirtualDeployment component — no autoscaler dispatch. |
| `class-mismatch`, `owner-mismatch`, `source-mismatch`, `target-mismatch` | Declared-vs-reported disagreements (see above). |
| `unexpected-component` | The controller reports autoscaler status for a component the declaration doesn't produce. |
| `status-missing`, `status-stale`, `status-unobserved`, `status-partial`, `status-invalid` | Reported evidence is absent, lags the spec generation, was never observed, is incomplete, or is contradictory. |
| `inheritance-unavailable` | The live runtime's declared inheritance chain could not be read; declared values may omit inherited context. |
| `revision-inconsistent` | The pinned service's status disagrees with its recorded revision. |

## Machine output

`-o json` (or `yaml`) emits the full report — `apiVersion: cli.ome.io/v1alpha1`, `kind: AutoscaleExplainReport` — which carries evidence the table condenses:

- `content.summary` — the overall state plus `statusFreshness` (`Current`, `Stale`, `Unobserved`, `Invalid`).
- `content.activeConfiguration` — where the declared side came from: `origin: LiveRuntime` or `origin: ControllerRevision`, the runtime reference, the pinned revision (namespace, name, UID), and the runtime's inheritance chain.
- `content.scalingPolicy` — mode and whether it came from the InferenceService, the runtime, or the default.
- `content.components[]` — the structured `desired` (including `metricCount`/`triggerCount`, which the table omits), `reported`, and `reconciliation` blocks; issue codes appear unabbreviated (`ReportedClassMismatch` rather than `class-mismatch`).
- `sources` — every object the report was derived from, with UIDs and generations.
- `warnings` — `PartialData`, `StaleEvidence`, `UnsupportedConfiguration`.

As on the [plugin page](/ome/docs/tasks/kubectl-ome): the table is not a stable scripting interface before GA — script against `-o json`. By design the report never echoes raw runtime specs, autoscaler payloads, resource versions, status messages, or sync tokens.

## Next steps

- Live scaler state read from the HPA / ScaledObject objects themselves: `kubectl ome autoscale status`.
- Reusable autoscaler templates and the full precedence chain: [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy).
- Pinned runtimes and roll-forward: [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision).
