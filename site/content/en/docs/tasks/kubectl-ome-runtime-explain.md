---
title: "Explain Runtime Selection for a Model"
linkTitle: "Runtime Explain"
weight: 21
date: 2026-09-27
description: >
  Use kubectl ome runtime explain to see every serving runtime OME considered
  for a model, which ones matched, which were rejected and why — before or
  after you deploy an InferenceService.
---

`kubectl ome runtime explain` answers one question: **which serving runtimes
would OME's automatic selection consider for this model, and why does each one
match or not?** It runs the operator's own selection engine
(`pkg/runtimeselector`) against the live cluster — the verdicts are the
controller's, not a CLI reimplementation — and prints every namespace-scoped
ServingRuntime and cluster-scoped ClusterServingRuntime it considered,
including the rejected ones with the reason each was rejected.

Use it when the controller's
[`RuntimeNotFound` diagnostics](/ome/docs/tasks/run-workloads/troubleshoot-runtime-selection/)
are not enough: the event only exists after a failed reconcile of a deployed
InferenceService, while `runtime explain` works before you deploy anything
(`--model`), also works when selection *succeeded* (showing the full ranking,
not just the winner), and names the compatible-but-skipped runtimes that the
controller's excluded list leaves out.

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome).
- The command is read-only and patches nothing. The baseline read-only
  ClusterRole on the plugin page covers everything it reads: the
  (Cluster)BaseModel, the InferenceService (with `--isvc`), `list` on
  ServingRuntimes and ClusterServingRuntimes and, with `--with-effective`,
  `ControllerRevision` snapshots in the OME control-plane namespace.

## Run the command

Target either a model or an InferenceService — exactly one of the two flags is
required, and the command takes no positional arguments:

```bash
# Before deploying: what would auto-selection do for this model in team-a?
kubectl ome runtime explain --model llama-3-70b -n team-a

# For an existing service: same question for the model its spec.model names
kubectl ome runtime explain --isvc llama-chat -n team-a
```

`--model` resolves the name the same way the operator does: a BaseModel in the
selected namespace first, then a ClusterBaseModel. `--isvc` reads the
InferenceService and explains selection for its `spec.model` — a service
without `spec.model` is rejected with an error telling you to pass `--model`
instead.

Example output:

```
RUNTIME     SCOPE       COMPATIBLE  PRIORITY  WEIGHT  REASON
srt-tuned   Namespaced  Yes         2         20      -
srt-llama   Cluster     Yes         1         10      -
srt-legacy  Cluster     No          -         -       runtime is disabled
srt-small   Cluster     No          -         -       model size 70B is outside
                                                      supported range [1B, 13B]
```

## Reading the table

**`Yes` rows are the operator's actual auto-select ranking.** Every runtime
marked `Yes` passed all compatibility checks, is opted in to automatic
selection, and scored above zero. They are printed in selection order:
namespace-scoped matches always rank above cluster-scoped ones (a tenant's own
runtime beats an equally good shared one), and within each scope higher score
wins, with ties broken by the closer `modelSizeRange`, then by name. **The
first row is the runtime automatic selection would choose.** `PRIORITY` is the
matched `supportedModelFormats` entry's `priority` (default 1), and `WEIGHT`
sums the matched format and framework weights, each multiplied by that
priority; `REASON` is `-` for a clean match.

**`No` rows are everything auto-selection would skip.** The controller's
selection API silently drops non-matches, so the command re-evaluates each one
against the same model to recover the reason. They appear after the matches,
namespace-scoped first, sorted by name, with `-` for priority and weight.

The `REASON` for a rejected runtime is the **first** check it failed, in the
same order the controller checks: runtime disabled, accelerator class,
component deployment mode, supported model formats, model size range. The
reason strings are the controller's own — see
[reading the exclusion reasons](/ome/docs/tasks/run-workloads/troubleshoot-runtime-selection/#reading-the-exclusion-reasons)
for the full catalog, including the format-mismatch details.

Three reasons are specific to this command, because they describe runtimes
that are *compatible* but still never auto-selected — the case the
controller's excluded list never mentions:

- `supports the model but has no supportedModelFormats[].autoSelect=true
  entry, so automatic selection skips it (pin it explicitly via
  spec.runtime.name instead)`
- `matching format <name> is not autoSelect-enabled (a different
  supportedModelFormats entry on this runtime has autoSelect=true, but not the
  one that matches this model)`
- `auto-select score is 0` (with `(format priority 0)` appended when the
  matching format's `priority` is explicitly 0)

If no runtimes exist at all, the command prints
`No serving runtimes found in the selected namespace or at cluster scope.` to
stderr and exits 0.

### Services that pin a runtime

When `--isvc` targets a service with an explicit `spec.runtime`, the table is
still worth reading, but it is hypothetical — the controller is not running
auto-selection for that service. The command says so on stderr before the
table:

```
Note: spec.runtime is explicit; automatic selection below is hypothetical.
```

To see which runtime and revision is *actually* driving a pinned service, use
[`kubectl ome runtime effective`](/ome/docs/tasks/kubectl-ome-runtime-effective)
— or append its evidence to this report with `--with-effective` below.

## One consistent snapshot

Runtime candidates come from one all-or-nothing snapshot: pages of 500, at
most 4 pages and 1,000 objects shared across both runtime kinds, with a
10-second timeout on every API request. The cluster-scoped list is read at the
exact resource version of the namespace-scoped list, so both kinds reflect a
single moment, and every read the selection engine performs afterwards — the
ranking and the per-candidate re-evaluation — is served from that immutable
snapshot. The explanation can never mix two revisions of a runtime, and adding
runtimes to the cluster does not add API reads per candidate.

The snapshot is deliberately not best-effort: if the cluster holds more
runtimes than the budget, or a list fails or returns inconsistent pages, the
command errors out (for example
`runtime candidate snapshot exceeds the CLI collection limit`) instead of
printing a table that silently omits candidates.

## Appending effective evidence: --with-effective

`--with-effective` (valid only with `--isvc`) appends a second section to the
report: what runtime the service is *actually* bound to right now, collected
as an independent bounded observation (up to 1,000 items across 2 pages, 10
seconds per request, plus `ControllerRevision` reads for pinned services). It
never changes the selector table above it, and it never implies that pods have
converged on anything.

```bash
kubectl ome runtime explain --isvc llama-chat -n team-a --with-effective
```

```
Effective context (separate observation; selector verdict unchanged):
SCOPE     FIELD           VALUE
Service   SELECTION       Selected
Service   SOURCE          CSR/srt-llama
Source    INHERITANCE     Observed
Source    ROOT-FIRST      CSR/srt-base
Source    ROOT-FIRST      CSR/srt-llama
Service   CAVEAT          Independent snapshot; not rollout convergence
Live      STATE           Available
Live      RUNTIME         CSR/srt-llama
Live      HASH            e0e2b0d6
Live      ENGINE          RawDeployment (Default)
Active    STATE           Available
Active    RUNTIME         CSR/srt-llama
Active    HASH            e0e2b0d6
Active    ENGINE          RawDeployment (Default)
Service   PIN             AutoSync/NotApplicable
Service   SYNC            Absent
Service   STATUS          Current
Service   DRIFT           NotReported
Service   LIVE-RELATION   Equal
```

The rows unique to this section:

- `SELECTION` — how the service's runtime was chosen: `Explicit`
  (`spec.runtime` names it) or `Selected` (automatic selection).
- `SOURCE` — the runtime the service resolves to, as `SR/<namespace>/<name>`
  or `CSR/<name>`.
- `INHERITANCE` — whether the runtime's `ome.io/inherit-from` ancestry was
  observed: `Observed`, `NotRecorded`, or `Unavailable` (with a `REASON` row
  when unavailable). Each `ROOT-FIRST` row is one link of the chain, root
  first.

The `Live`, `Active`, and remaining `Service` rows are the same evidence
`kubectl ome runtime effective` reports — see
[that page](/ome/docs/tasks/kubectl-ome-runtime-effective) for what `PIN`,
`SYNC`, `STATUS`, `DRIFT`, and `LIVE-RELATION` mean.

Two properties to rely on:

- **The verdict never moves.** The effective context is additive. Whatever the
  selector table said, it still says — a drifted or missing live runtime does
  not rewrite the ranking, and the `CAVEAT` row reminds you the two
  observations are independent.
- **Unavailable evidence is not a failure.** If the effective observation
  cannot be collected (missing RBAC, unreachable API), the selector table is
  still printed in full, followed by `Service EVIDENCE Unavailable` — the raw
  resolver error is deliberately never echoed, and the command still exits 0.

For pinned services the effective evidence reads `ControllerRevision`
snapshots from the control-plane namespace; pass `--ome-namespace` (default
`ome`) if OME is installed elsewhere. Pointed at the wrong namespace, the
command still succeeds but reports pin evidence as unavailable.

## Output format

The report is a human-readable table only — this command has no `-o` flag yet,
so there is no structured output to script against. Long `REASON` text wraps
(never truncates) within 80 display columns, or narrower if your terminal is
narrower; effective-context values are middle-truncated to fit their column.

## Related pages

- [Troubleshoot Runtime Selection Failures](/ome/docs/tasks/run-workloads/troubleshoot-runtime-selection/)
  — the controller-side view: the `RuntimeReady` condition and
  `RuntimeNotFound` event, and the full exclusion-reason catalog
- [Runtime Selection Logic](/ome/docs/concepts/serving_runtime/#runtime-selection-logic)
  — how compatibility, auto-select, and scoring are defined
- [Inspect Effective Runtime Evidence](/ome/docs/tasks/kubectl-ome-runtime-effective)
  — the full pin/sync/drift report that `--with-effective` summarizes
- [Audit Runtime Inheritance and Consumers](/ome/docs/tasks/kubectl-ome-runtime-tree)
  — which runtimes inherit from a runtime and which services reference it
- [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) — installation, shared
  flags, exit codes, and the baseline RBAC role
