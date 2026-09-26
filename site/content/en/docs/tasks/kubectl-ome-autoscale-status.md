---
title: "Check Autoscaling Status with kubectl-ome"
linkTitle: "Autoscale Status"
weight: 21
date: 2026-09-26
description: >
  Read the controller-reported autoscaling state of every InferenceService component, and add exact live reads with --live-scale and --live-scaler when you need them.
---

`kubectl ome autoscale status` shows, per InferenceService component (engine,
decoder, router), the autoscaling evidence the OME controller has mirrored onto
the InferenceService itself: the autoscaler class, who manages it, where its
spec came from, the scale target, replica counts, last scale time, and scaler
conditions.

By default the command performs exactly **one read** — a GET of the
InferenceService — and never touches HPAs, KEDA ScaledObjects, Deployments, or
InferenceReplicas. That makes the default output cheap and safe to run with
minimal permissions, but it also means the output is only as fresh as the
controller's last status update. Two opt-in flags add bounded, exact live
reads when you need to cross-check:

- `--live-scale` compares the parent-reported replica counts with a direct
  read of the selected InferenceReplica and its `/scale` subresource.
- `--live-scaler` reads the actual HPA or KEDA ScaledObject for OME-managed
  scalers.

See [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome) for installing the
plugin and the baseline read-only RBAC role.

## Basic usage

```bash
kubectl ome autoscale status my-isvc -n prod
```

Example output for a service with one OME-managed HPA on the engine:

```
FIELD             SERVICE    ENGINE        DECODER   ROUTER
STATE             Reported   Reported      -         -
CLASS             -          HPA           -         -
MANAGED-BY        -          ome           -         -
SPEC-SOURCE       -          isvc          -         -
TARGET-KIND       -          IR            -         -
TARGET-NAME       -          chat-engine   -         -
TARGET-EVIDENCE   -          Reported      -         -
CURRENT           -          2             -         -
DESIRED           -          3             -         -
REPLICA-EVIDENCE  -          Reported      -         -
LAST-SCALE        -          Sep26 08:15Z  -         -
COND-EVIDENCE     -          Reported      -         -
ABLE-TO-SCALE     -          True          -         -
SCALING-ACTIVE    -          True          -         -
```

How to read it:

- **STATE** in the `SERVICE` column is the summary: `Reported` (every present
  component reported complete evidence), `Partial` (some evidence is missing
  or ambiguous), `Unavailable` (no component reported autoscaler evidence at
  all), or `Invalid` (contradictory or malformed evidence).
- **CLASS** is `HPA`, `KEDA`, `External`, or `None`; **MANAGED-BY** is `ome`,
  `external`, or `none`. For `External` and `None` classes, replica counts and
  conditions show `Unavailable` — that is the expected shape, not an error:
  OME deliberately reports no scaler evidence for components it does not
  scale.
- **SPEC-SOURCE** says where the effective autoscaler spec came from: `isvc`
  (inline on the InferenceService), `policy` (rendered from an
  [AutoscalerPolicy](/ome/docs/concepts/autoscaler_policy)), `runtime`,
  `legacy`, or `default`.
- **TARGET-KIND** abbreviates `InferenceReplica` as `IR` in the compact table;
  the other supported kind is `Deployment`.
- **LAST-SCALE** is formatted as UTC `MonDD HH:MMZ` in the compact table.
- A **CURRENT** or **DESIRED** of `0` is preserved but reported as
  `Ambiguous` (issue alias `ReplicaAmbig`), because parent status alone cannot
  distinguish deliberate scale-to-zero from a count that was never populated.
  A KEDA service scaled to zero therefore summarizes as `Partial` — that is
  working as intended, not a fault.

The report is deliberately **message-free**: every field comes from a closed
vocabulary, and condition reasons and messages never appear in any output
format. For human-readable condition messages, use
`kubectl describe inferenceservice`.

## The ISSUES rows

When the projection finds anything missing or contradictory, the compact table
adds `ISSUES` rows, one alias per row, in the column of the affected component
(or `SERVICE` for service-level issues). The aliases expand as follows:

| Alias | Issue code |
| --- | --- |
| `UnknownComp` | `UnknownComponentStatus` |
| `NoAutoscaler` | `AutoscalerNotReported` |
| `NoTarget` | `ScaleTargetNotReported` |
| `BadClass` | `ClassInvalid` |
| `BadManager` | `ManagedByInvalid` |
| `OwnerMismatch` | `OwnershipMismatch` |
| `BadSpecSource` | `SpecSourceInvalid` |
| `UnexpectedEv` | `UnexpectedScalerEvidence` |
| `ReplicaAmbig` | `ReplicaEvidenceAmbiguous` |
| `BadReplica` | `ReplicaEvidenceInvalid` |
| `BadTarget` | `ScaleTargetInvalid` |
| `BadCondition` | `ConditionInvalid` |
| `CondConflict` | `ConditionConflict` |
| `BadPolicy` | `PolicyEvidenceInvalid` |
| `PolicyClash` | `PolicyConditionConflict` |

Issue codes outside this closed vocabulary (from a newer controller than the
CLI) render as `X#` followed by a stable 10-digit hex digest rather than
echoing the raw value.

## Output formats

- `-o table` (default) — the compact matrix above, with cells bounded to 13
  characters.
- `-o wide` — one row per component with the full target identity
  (`Kind/namespace/name`), RFC 3339 timestamps, the complete condition list,
  and **exact issue codes** instead of aliases.
- `-o json` / `-o yaml` — the full report, including a `sources` array that
  lists every object the command read (with UID, generation, and collection
  time) and a `warnings` array that carries `PartialData` whenever anything is
  less than fully reported.

As with all kubectl-ome commands, the human tables are not a stable scripting
interface before GA — script against `-o json`.

## Cross-check counts with --live-scale

```bash
kubectl ome autoscale status my-isvc --live-scale
```

For each component whose reported scale target is an `InferenceReplica`,
`--live-scale` performs two extra exact reads — a GET of that named
InferenceReplica and a GET of its `/scale` subresource — and compares the live
counts with what the parent reported. It never lists or discovers objects, and
Deployment-backed targets are not read (their evidence shows `Unsupported`).

The InferenceReplica only counts as evidence if it is verifiably the parent's
own child: it must carry a controller owner reference to the exact
InferenceService UID and be stamped with the parent's current generation.
Immediately after you edit the InferenceService, the read can therefore show
`Invalid` until the controller re-stamps the child — retry after
reconciliation catches up.

The output gains four fields per component:

- **LIVE-EVIDENCE** — one of `Reported`, `NotSelected` (no scale target
  reported), `Unsupported` (Deployment target), `Forbidden` (RBAC denied),
  `NotFound`, `Deleting`, `Stale` (the InferenceReplica's
  `status.observedGeneration` lags its generation), `Changed` (the
  InferenceReplica and `/scale` reads disagree — caught mid-write, retry),
  `Invalid`, or `Unavailable`.
- **LIVE-SPEC** / **LIVE-CURRENT** — the live `/scale` counts, shown only when
  evidence is `Reported`.
- **LIVE-COUNT** — `Equal` or `Drift` versus the parent-reported counts,
  computed only when the parent counts themselves were `Reported`; otherwise
  `Unknown`.

`Equal` is a point-in-time count comparison only. It is not proof of ongoing
freshness, InferenceReplica health, or autoscaler health.

**Extra RBAC:** `get` on `inferencereplicas` and `inferencereplicas/scale` in
the `ome.io` group. The baseline reader role on the
[kubectl-ome page](/ome/docs/tasks/kubectl-ome/#required-rbac) already covers
both through its `ome.io` wildcard; a narrower role must name the `/scale`
subresource explicitly, because RBAC never matches subresources implicitly. A
missing permission does not fail the command — it shows up as
`LIVE-EVIDENCE=Forbidden`.

## Inspect the scaler with --live-scaler

```bash
kubectl ome autoscale status my-isvc --live-scaler
```

For components that are OME-managed (`MANAGED-BY=ome`) with class `HPA` or
`KEDA`, `--live-scaler` reads the actual scaler object by its exact name:

- **HPA class** — a GET of the `autoscaling/v2` HorizontalPodAutoscaler with
  the same name as the scale target.
- **KEDA class** — a GET of the `keda.sh/v1alpha1` ScaledObject named
  `scaledobject-<target-name>` (target names longer than 50 characters keep
  their last 50).

Components with class `External` or `None`, or not managed by OME, are never
read and show `SCALER-EVIDENCE=NotSelected`. For InferenceReplica-backed
targets the command first performs a verified exact InferenceReplica read to
establish the owner chain; if that read fails or the InferenceReplica is being
deleted, the scaler is not read at all. For Deployment-backed targets,
ownership is proven by the scaler's controller owner reference pointing at the
exact InferenceService UID — a same-named scaler owned by anything else
reports `Invalid`.

The output gains per component: **SCALER-KIND**, **SCALER-EVIDENCE**,
**SCALER-GEN**, **SCALER-CURRENT**, **SCALER-DESIRED**, and one column per
live scaler condition (`SCALER-ABLE`, `SCALER-SCALING`, `SCALER-LIMITED` for
HPA; `SCALER-READY`, `SCALER-ACTIVE`, `SCALER-FALLBACK`, `SCALER-PAUSED` for
KEDA). `SCALER-EVIDENCE` uses the same vocabulary as `LIVE-EVIDENCE` above,
minus `Changed` — and `Stale` (observed generation lagging) is only possible
for HPAs.

Interpret the results conservatively:

- `SCALER-GEN=Matched` means **only** that the HPA's
  `status.observedGeneration` equals its `generation`. KEDA ScaledObjects
  always show `Unproven`.
- For KEDA, only the ScaledObject itself is read — KEDA's generated HPA and
  its reconciliation freshness are not observed by this flag.
- `STATE` and the `CURRENT`/`DESIRED` columns remain parent-reported; the
  `SCALER-*` columns are separate live evidence. Count equality between the
  two is not proof of freshness or scaler health.

**Extra RBAC:** exactly the two rules called out in the
[kubectl-ome RBAC section](/ome/docs/tasks/kubectl-ome/#required-rbac):

```yaml
  - apiGroups: ["autoscaling"]
    resources: ["horizontalpodautoscalers"]
    verbs: ["get"]
  - apiGroups: ["keda.sh"]
    resources: ["scaledobjects"]
    verbs: ["get"]
```

Without them the command still succeeds but shows
`SCALER-EVIDENCE=Forbidden`, which is easy to mistake for a broken autoscaler
— check RBAC before debugging the scaler.

Both flags can be combined in one invocation; `--live-scale` evidence is
collected first, then `--live-scaler`.

## AutoscalerPolicy provenance

When a component's autoscaler is rendered from an
[AutoscalerPolicy](/ome/docs/concepts/autoscaler_policy) (alpha), the report
additionally carries policy provenance: `POLICY-STATE` and `POLICY` rows in
the compact table, plus resolution reason, policy generation, and digests in
`-o wide` and `-o json` output. `SPEC-SOURCE=policy` marks these components.
