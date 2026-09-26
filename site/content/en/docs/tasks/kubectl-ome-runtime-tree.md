---
title: "Audit Runtime Inheritance and Consumers"
linkTitle: "Runtime Tree"
weight: 21
description: >
  Use kubectl ome runtime tree to see which runtimes inherit from a
  ServingRuntime or ClusterServingRuntime and which InferenceServices
  reference each runtime in the tree.
---

Before you edit or delete a shared runtime, you want two answers: which other
runtimes inherit from it, and which InferenceServices reference each of those
runtimes. `kubectl ome runtime tree` answers both in one read-only report:

```bash
# Auto-detect when the name resolves to exactly one runtime
kubectl ome runtime tree vllm-runtime

# Select a cluster-scoped runtime explicitly
kubectl ome runtime tree shared --kind ClusterServingRuntime

# Select a namespaced runtime explicitly and emit the automation contract
kubectl ome runtime tree shared --kind ServingRuntime -n team-a -o json
```

The command is part of the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome)
and needs only `list` access to `clusterservingruntimes`, `servingruntimes`
and `inferenceservices` (covered by the baseline reader ClusterRole on that
page). It never prints runtime specs, InferenceService specs or status,
labels, annotations, or resource versions — only object identities and
classification codes — so its output is safe to paste into tickets.

## What counts as an edge

A runtime inherits from another runtime by naming it in the
`ome.io/inherit-from` annotation. The controller resolves that name in a
fixed **resolution context** per runtime:

- A **ClusterServingRuntime** resolves every parent name as a
  ClusterServingRuntime.
- A **ServingRuntime** in namespace `N` resolves each parent name as the
  ServingRuntime of that name in `N` first, falling back to the
  ClusterServingRuntime of that name.

Inheritance walks are bounded to 5 runtimes deep. The tree replays these
controller walks exactly and keeps namespaced and cluster contexts separate,
because namespace shadowing can give the same runtime different visible
ancestors in different contexts.

An InferenceService is a consumer of the runtime its `spec.runtime.name`
resolves to — the exact runtime, never its ancestors. `spec.runtime.kind`
narrows the lookup (`ServingRuntime` resolves in the service's namespace
only; `ClusterServingRuntime` resolves cluster-first; an omitted kind
resolves namespaced-first). Services without `spec.runtime` use automatic
runtime selection and are never attached to the tree.

## Reading the output

Suppose ClusterServingRuntime `srt-llama` inherits from ClusterServingRuntime
`srt-base`, ServingRuntime `team-a/srt-llama-tuned` inherits from
`srt-llama`, and two services reference `srt-llama` and `srt-llama-tuned`:

```bash
kubectl ome runtime tree srt-llama --kind ClusterServingRuntime
```

```text
RUNTIME TREE
Target: ClusterServingRuntime/srt-llama
Context: Cluster (resolution: Complete)
Head: ClusterServingRuntime/srt-llama
ClusterServingRuntime/srt-base
`-- ClusterServingRuntime/srt-llama [selected]
    `-- InferenceService/team-b/llama-eval
Context: Namespaced/team-a (resolution: Complete)
Head: ServingRuntime/srt-llama-tuned
ClusterServingRuntime/srt-base
`-- ClusterServingRuntime/srt-llama [selected]
    `-- ServingRuntime/srt-llama-tuned
        `-- InferenceService/llama-chat
Snapshot: Complete
Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=2
Collection: ServingRuntime scope=AllNamespaces status=Complete pages=1 items=1
Collection: InferenceService scope=AllNamespaces status=Complete pages=1 items=2
```

How to read it:

- Every runtime whose controller walk passes through the target gets its own
  `Head:` section — that is how you enumerate descendants. The full path from
  the observed root down to that head is shown, with the selected runtime
  marked `[selected]`.
- `InferenceService` leaves hang off the exact runtime they reference (the
  head of each path). `team-b/llama-eval` references the
  ClusterServingRuntime directly; `llama-chat` references the tuned child, so
  it appears under `srt-llama-tuned`, not under `srt-llama`.
- Inside a `Context: Namespaced/<ns>` section, ServingRuntimes and services
  in that namespace are printed without the namespace; everything else keeps
  its full identity.
- The `Snapshot:` and `Collection:` lines record exactly which lists the
  report was built from and whether each was complete (see
  [completeness](#completeness-and-failure-modes)).

When a walk cannot be resolved, the path ends at the error boundary and an
`Issue:` line explains why: `ParentMissing` (the annotation names a runtime
that does not exist in that context), `CycleDetected`, or
`MaxDepthExceeded` (more than 5 runtimes).

### Which namespaces are read

A namespaced target (`--kind ServingRuntime`, or auto-detection resolving to
one) lists ServingRuntimes and InferenceServices **only in the selected
namespace** — consumers of a shared ancestor in other namespaces will not
appear. A cluster-scoped target expands both lists across all namespaces;
this also happens when auto-detection resolves the name to a
ClusterServingRuntime, so pass `--kind ServingRuntime` explicitly if you need
the command to stay namespace-scoped (for example under a namespaced Role).

If the name exists as both a ServingRuntime in your namespace and a
ClusterServingRuntime, auto-detection refuses to guess and asks you to pass
`--kind`.

## Finding unattributed users

Services that reference a runtime but cannot be attributed to one are hidden
by default. `--show-unattributed-users` prints them in a separate block that
is never attached to the tree (and adds no extra API reads):

```bash
kubectl ome runtime tree srt-llama --kind ClusterServingRuntime \
  --show-unattributed-users
```

```text
Unattributed users (not attributed to runtime tree):
  [not attributed] InferenceService/team-a/summarize
  state=Unresolved reason=AutomaticSelection
  [not attributed] InferenceService/team-b/legacy
  state=Unresolved reason=RuntimeNotFound
  declared runtime=srt-retired
```

| State | Reason | Meaning |
| --- | --- | --- |
| `Unresolved` | `AutomaticSelection` | No `spec.runtime`; the runtime is chosen by weighted selection, not an explicit reference. |
| `Unresolved` | `RuntimeNotFound` | The declared runtime name (shown as `declared runtime=`) does not exist in the collected snapshot. |
| `Invalid` | `InvalidRuntimeName` | The declared name is not a valid DNS subdomain; the unsafe value is never printed. |
| `Ambiguous` | `DuplicateInferenceService` | Defensive evidence: the snapshot repeated a service identity, which a conforming Kubernetes LIST cannot do. |

`RuntimeNotFound` entries are the ones to chase before deleting a runtime:
they are services that will keep failing to resolve either way, often left
behind by a rename.

## Output formats

`-o table` (default) bounds every line to 80 display columns. A name too long
for its line is clipped in the middle and suffixed with `#` plus the first 8
hex characters of the SHA-256 of the full name, so the same object always
gets the same fingerprint and rows stay correlatable across runs.

`-o wide` prints the complete, unabridged tree; only the opt-in
unattributed-user rows stay bounded, because their non-attribution labels are
safety-critical.

`-o json` / `-o yaml` emit a versioned `RuntimeTreeReport`
(`apiVersion: cli.ome.io/v1alpha1`) whose `content` carries `target`,
`snapshot.collections`, `contexts[].paths[]` (each with `head`, `runtimes`
and `dependents`) and, with the flag, `unattributedUsers`. Human-readable
output is not a stable scripting interface before GA — script against
`-o json`. For example, to list every consumer visible in the tree:

```bash
kubectl ome runtime tree srt-llama --kind ClusterServingRuntime -o json \
  | jq -r '.content.contexts[].paths[].dependents[] | "\(.namespace)/\(.name)"'
```

## Completeness and failure modes

Each of the three lists is collected with bounded pagination: pages of 500,
at most 2 pages and 1,000 items per kind, 10 seconds per request.

The two **runtime** collections must be complete, because a truncated or
unreadable runtime list could silently change inheritance edges (an unseen
namespaced runtime can shadow a cluster one). If either is truncated or the
list call fails, the command exits with an error instead of printing a tree:

```text
runtime tree requires complete runtime evidence: ServingRuntime collection is unavailable (pages=0 items=0); restore list access or narrow the runtime scope, then retry
```

The **InferenceService** collection is dependency evidence only: if it is
truncated or unavailable, the tree still prints with its edges intact, the
collection line shows `status=Truncated` or `status=Unavailable`, the
snapshot degrades to `Snapshot: Partial`, and `Warning: PartialData` plus
`Warning: Truncated` or `Warning: SourceUnavailable` lines are appended.
Treat the consumer list as a lower bound in that case. API server error
details are never echoed into the report.

## Related pages

- [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) — installation, shared
  flags and the baseline RBAC role
- [Serving Runtime](/ome/docs/concepts/serving_runtime) — the runtime custom
  resources this command inspects
- [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision) —
  controlling when consumers adopt runtime changes you found here
