---
title: "Explain Accelerator Selection"
linkTitle: "Accelerator Explain"
weight: 21
description: >
  See which AcceleratorClass the controller selected for each component of an
  InferenceService, and compare the declared policy with the applied resource requests
---

`kubectl ome accelerator explain` is a read-only report that answers three
questions about one InferenceService:

- What accelerator selection did I **declare** — a policy, an explicit
  `AcceleratorClass`, or nothing?
- What did the controller actually **select** for each serving component, and
  does the referenced `AcceleratorClass` really exist?
- How do the **base** resource requests (from the merged runtime/service
  template) compare to the requests the controller reports it **applied** to
  the pods after selection?

The command is part of the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome).
It never reruns accelerator selection and never treats a locally computed
candidate as controller success: everything labeled "selected" comes from the
InferenceService status, and everything it verifies is read with exact,
bounded GETs under a single 10-second deadline.

## Usage

```bash
kubectl ome accelerator explain my-isvc -n team-a
```

The single argument is the InferenceService name. Output formats are `table`
(default), `wide`, `json` and `yaml` via `-o`. `--ome-namespace` (default
`ome`) tells the command where to read pinned runtime snapshots from; it is
only used when the service is pinned.

A compact table for a service with an engine and a decoder looks like:

```
COMP     POLICY    CLASS      SEL       REQUESTS          ISS
engine   Cheapest  a100-80gb  Reported  nvidia.com/gpu=8  0
decoder  Cheapest  -          Missing   base:cpu=4,+1     1
```

| Column | Meaning |
| --- | --- |
| `COMP` | Serving component: `engine`, then `decoder`. Only these two can carry accelerator selection. |
| `POLICY` | Declared intent: the policy name (`BestFit`, `Cheapest`, `MostCapable`, `FirstAvail`), `Explicit` when the spec pins an `acceleratorClass`, or `-` when nothing is declared. |
| `CLASS` | The controller-reported class; falls back to the declared class when nothing is reported yet. Long names are middle-truncated. |
| `SEL` | Selection state: `Reported`, `Missing` (configured but not in status yet), `None` (not configured), `Unavail` (evidence unavailable), `Invalid`. |
| `REQUESTS` | The applied requests when the controller reported them (first entry plus `,+N` for the rest); otherwise the computed base requests prefixed `base:`; `base:Unknown` / `base:Invalid` when the base could not be computed. |
| `ISS` | Number of distinct issues affecting the component (details in `-o wide` or `-o json`). |

## Declared intent: where the policy and class come from

Intent is read from the spec only:

```yaml
spec:
  acceleratorSelector:        # service-wide intent
    policy: Cheapest
  engine:
    acceleratorOverride:      # component override wins for that component
      acceleratorClass: a100-80gb
```

A component's `acceleratorOverride` takes precedence over the service-level
`acceleratorSelector`, and an explicit `acceleratorClass` takes precedence
over a policy. The wide view labels each value with its origin
(`Declared/Service` or `Declared/Component`). Declared
`acceleratorSelector.constraints` do not appear in this report.

## Reported selection: what the controller actually chose

The selection comes from
`status.components.<component>.selectedAccelerator`, which the controller
writes when it selects an accelerator:

```yaml
status:
  components:
    engine:
      selectedAccelerator:
        acceleratorClass: a100-80gb
        reason: "..."
        resourceRequests:
          nvidia.com/gpu: "8"
```

Two safeguards apply before any of this is echoed back:

- **Freshness gating.** Status is trusted only while
  `status.observedGeneration` equals `metadata.generation`. If you just edited
  the spec and the controller has not reconciled yet, the selection shows as
  `Unavail` with a `StatusStale` issue instead of repeating possibly-outdated
  status. `StatusUnobserved` (never reconciled) and `StatusInvalid`
  (`observedGeneration` ahead of the spec) are gated the same way.
- **Reason redaction.** The free-form `reason` string is never printed.
  The report carries only a SHA-256-derived digest (`rs1:` plus twelve hex
  characters), so a status message can not leak credentials through the CLI.
  Read the field from the InferenceService itself if you need the text.

For each class named in current status (at most two: engine and decoder), the
command performs one exact cluster-scoped GET of the `AcceleratorClass` to
verify it exists — it never lists AcceleratorClasses. The result appears as
`Observed`, or as `NotFound` / `Forbidden` / `UnsupportedAPI` / `Unreadable` /
`Invalid` when the read failed, without failing the whole report.

## Base versus applied requests

The report shows two request sets per component so you can see what
accelerator selection changed:

- **Base** — the serving container's resource requests after merging the
  InferenceService component spec with the **active** runtime configuration,
  before any accelerator selection contributes. This merge is pin-aware: a
  service running live (`spec.runtime.autoSync: true`, the default) uses the
  current ServingRuntime or ClusterServingRuntime, while a
  [pinned service](/ome/docs/concepts/runtime-revision) uses its pinned
  `ControllerRevision` snapshot. An inconsistent snapshot withholds the base
  (`ActiveRevisionInconsistent`) rather than guessing.
- **Applied** (called `EFFECTIVE` in the output) — the requests the controller
  reports it actually applied to pods,
  `status...selectedAccelerator.resourceRequests`.

Services annotated for `VirtualDeployment` short-circuit in the controller
before any runtime merge, so their base requests are reported as unavailable
by design.

## The full view: `-o wide`

`-o wide` prints every field with an evidence label, then the sources the
report was built from and any warnings. Trimmed example:

```
SCOPE      COMP    FIELD             VALUE              EVIDENCE
SUMMARY    -       STATE             Reported           Computed
SUMMARY    -       STATUS_FRESHNESS  Current            Computed
INTENT     engine  MODE              Policy             Declared
INTENT     engine  POLICY            Cheapest           Declared/Service
SELECTION  engine  STATE             Reported           Reported
SELECTION  engine  CLASS             a100-80gb          Reported
SELECTION  engine  REASON_STATE      Reported           Reported
SELECTION  engine  REASON_DIGEST     rs1:09963eb2e26c   Reported/Redacted
CLASS      engine  STATE             Observed           Observed
CLASS      engine  NAME              a100-80gb          Observed
REQUEST    engine  BASE_STATE        Available          Computed
REQUEST    engine  BASE              cpu=4,memory=32Gi  Computed
REQUEST    engine  EFFECTIVE_STATE   Reported           Reported
REQUEST    engine  EFFECTIVE         nvidia.com/gpu=8   Reported
SOURCE     AcceleratorClass  NAME    a100-80gb          Observed
SOURCE     InferenceService  NAME    team-a/my-isvc     Observed
SOURCE     ServingRuntime    NAME    team-a/my-runtime  Observed
...
```

The evidence column tells you where a value came from: `Declared` (spec),
`Reported` (controller status), `Observed` (verified by reading the object),
`Computed` (derived locally from observed inputs), `Unavailable`.

`-o json` and `-o yaml` emit the same data as a typed
`AcceleratorExplainReport` (`apiVersion: cli.ome.io/v1alpha1`, an alpha
contract). As with the other plugin commands, script against `-o json` — the
human-readable tables are not a stable interface.

## Issue codes

Issues appear per component in `-o wide`/`-o json`; the summary `STATE`
becomes `Partial` when any issue is present and `Invalid` when evidence is
malformed.

| Code | Meaning |
| --- | --- |
| `StatusStale` / `StatusUnobserved` / `StatusInvalid` | Status freshness gating withheld the reported selection (see above). |
| `SelectionNotReported` | Accelerator selection is configured but the (current) status carries no `selectedAccelerator` yet. |
| `SelectionUnexpected` | Status reports a selection although nothing in the spec configures accelerator selection. |
| `ReportedClassMismatch` | The declared `acceleratorClass` differs from the class the controller reports it selected. |
| `ClassNotFound` / `ClassForbidden` / `ClassUnsupportedAPI` / `ClassUnreadable` | The verification GET of the reported AcceleratorClass failed with that classification. |
| `ClassInvalid` | The reported class name is malformed, or the returned AcceleratorClass object could not be safely bound to the requested identity. |
| `DeclaredClassInvalid` / `PolicyInvalid` | The spec declares a class name or policy that is not valid. |
| `ActiveConfigurationUnavailable` | The active runtime configuration could not be resolved, so base requests cannot be computed. |
| `ActiveRevisionInconsistent` | The pinned runtime snapshot is inconsistent; base requests are withheld. |
| `BaseRequestsUnavailable` | The merged component template did not yield readable resource requests. |
| `RequestsNotReported` | A selection is reported without `resourceRequests`. |
| `RequestsInvalid` / `NodeSelectorInvalid` | Reported requests or node selector in status are malformed. |
| `UnexpectedComponentEvidence` | A component other than engine or decoder (for example the router) carries `selectedAccelerator` status. |

Report-level warnings summarize the same conditions: `PartialData`,
`StaleEvidence` and `SourceUnavailable`.

## Required RBAC

The command only reads: the InferenceService, the active runtime source (the
ServingRuntime/ClusterServingRuntime, or the pinned ControllerRevision in the
OME namespace), and at most two AcceleratorClasses by exact name. All of these
are covered by the baseline read-only role on the
[kubectl-ome page](/ome/docs/tasks/kubectl-ome). An unreadable
AcceleratorClass degrades that class to `Forbidden`/`Unreadable` in the report
instead of failing the command.
