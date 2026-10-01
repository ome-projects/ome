---
title: "List OME Resources with kubectl ome get"
linkTitle: "kubectl-ome get"
weight: 21
date: 2026-09-30
description: >
  The resources, aliases and merged views kubectl ome get supports, and its rules for named lookups, namespaces, label selectors, output formats and columns
---

`kubectl ome get RESOURCE [NAME]` lists OME custom resources with
model-centric columns. Every kind it knows is one entry in a single registry,
so all of them follow the same rules for named lookups, namespaces, selectors
and output formats — this page is the contract. It ships with the
[kubectl-ome plugin](/ome/docs/tasks/kubectl-ome) and accepts the standard
kubectl connection flags (`--kubeconfig`, `--context`, `-n`).

```bash
kubectl ome get isvc                    # table, kubeconfig namespace
kubectl ome get isvc chat               # one named object
kubectl ome get models -A               # merged view across namespaces
kubectl ome get runtimes -l team=nlp    # label-filtered listing
kubectl ome get trafficmaps -o wide     # extra columns
kubectl ome get isvc -o json            # typed output for scripts
```

The command is read-only: a listing issues `list` requests and a named lookup
issues a `get`, all against the `ome.io` API group, so the plugin's baseline
[reader role](/ome/docs/tasks/kubectl-ome/#required-rbac) is all it needs.

## Supported resources and aliases

`RESOURCE` is matched case-insensitively against one canonical (plural) name
or one of its aliases. An unknown resource fails before any API request with
an error that enumerates every canonical name.

| Canonical name | Aliases | Scope |
| --- | --- | --- |
| `inferenceservices` | `inferenceservice`, `isvc`, `isvcs` | Namespaced |
| `models` | `model` | Merged view |
| `basemodels` | `basemodel`, `bm` | Namespaced |
| `clusterbasemodels` | `clusterbasemodel`, `cbm` | Cluster |
| `runtimes` | `runtime` | Merged view |
| `servingruntimes` | `servingruntime`, `srt` | Namespaced |
| `clusterservingruntimes` | `clusterservingruntime`, `csrt` | Cluster |
| `acceleratorclasses` | `acceleratorclass`, `ac` | Cluster |
| `acceleratorquotas` | `acceleratorquota`, `aq` | Cluster |
| `benchmarkjobs` | `benchmarkjob`, `bj` | Namespaced |
| `finetunedweights` | `finetunedweight`, `ftw` | Cluster |
| `inferencereplicas` | `inferencereplica`, `ir` | Namespaced |
| `workloadclusters` | `workloadcluster`, `wc` | Cluster |
| `rolloutpolicies` | `rolloutpolicy`, `rp` | Namespaced |
| `autoscalerpolicies` | `autoscalerpolicy`, `ap` | Namespaced |
| `trafficmaps` | `trafficmap`, `tm`, `tmap` | Namespaced |

## Merged views: models and runtimes

Two entries merge a namespaced kind and its cluster-scoped sibling into one
listing:

- `models` — BaseModels from the target namespace **plus every
  ClusterBaseModel**, always.
- `runtimes` — ServingRuntimes from the target namespace **plus every
  ClusterServingRuntime**, always.

`-A` widens only the namespaced half; the cluster-scoped half is included
either way. A label selector applies to both halves. Both views add a `SCOPE`
column (`Namespaced` or `Cluster`) that the single-kind views omit:

```
$ kubectl ome get models
NAME            SCOPE        ARCH               PARAMS   FORMAT        STATE   AGE
ns-model        Namespaced   LlamaForCausalLM   70B      safetensors   Ready   12d
cluster-model   Cluster      LlamaForCausalLM   70B      safetensors   Ready   40d
```

A named lookup on a merged view resolves the namespaced kind first and falls
back to the cluster-scoped kind only when the namespaced lookup returns
NotFound — the same precedence the operator applies when resolving an
InferenceService's model reference. Any other error on the namespaced lookup
(Forbidden, for example) is reported directly, without the fallback. In table
output the `SCOPE` column shows which kind matched; in `-o json`/`-o yaml`
the object's own `kind` does.

## Named lookups

`kubectl ome get RESOURCE NAME` fetches exactly one object. A name cannot be
combined with `-A` (`a resource name cannot be combined with
--all-namespaces`) or with `-l` (`a resource name cannot be combined with
--selector`); both are rejected before any API request. A missing object is
an error (exit 1), not an empty table.

## Namespaces and label selectors

- **Namespaced resources** list the kubeconfig context's namespace by
  default; `-n` overrides it. `-A`/`--all-namespaces` lists every namespace
  and prepends a `NAMESPACE` column to the table. In a merged view with `-A`,
  cluster-scoped rows show `-` in that column.
- **Cluster-scoped resources** ignore `-n` entirely. `-A` is accepted for
  kubectl parity but does nothing except print a warning to stderr:
  `warning: --all-namespaces is ignored for the cluster-scoped resource
  "acceleratorclasses"`.
- `-l`/`--selector` filters any listing by label selector, exactly as in
  `kubectl get`.

## Output formats

`-o` accepts `table` (the default, also selected by omitting `-o`), `wide`,
`json` and `yaml`; anything else is rejected with `unsupported output format
"..." (supported: table, wide, json, yaml)`.

- **table** — an aligned, human-readable table of the compact columns below.
  An empty result prints `No <resource> found.` (cluster-scoped or `-A`) or
  `No <resource> found in namespace "<ns>".` to **stderr**, writes nothing to
  stdout and exits 0.
- **wide** — the same table plus the wide-only columns marked below.
- **json** / **yaml** — a named lookup prints the object itself; a listing
  prints one valid document: a `v1` `List` envelope whose `items` holds the
  objects. `items` is always an array — an empty listing yields
  `"items": []`, never `null`, so `jq '.items[]'` works unconditionally.
  Every object carries its `apiVersion` and `kind` even though typed client
  responses omit them, so merged-view output stays self-describing.

As everywhere in the plugin, the table is not a stable scripting interface
before GA — script against `-o json` and the
[exit codes](/ome/docs/tasks/kubectl-ome/#exit-codes).

## Columns

Empty or absent values render as `-`: a nil reference, an empty field, or a
zero creation timestamp. In an InferenceService row, `RUNTIME -` specifically
means the runtime is auto-selected rather than pinned —
[`kubectl ome status`](/ome/docs/tasks/kubectl-ome-status) shows the resolved
one. A `?` cell means the object was not the kind the column expected; it is
a defensive placeholder that should not appear in practice. `AGE` is
rendered kubectl-style (`41d`, `10m`).

```
$ kubectl ome get isvc -n team-a
NAME   MODEL           RUNTIME     READY   URL                                AGE
chat   llama-3-3-70b   srt-llama   True    https://chat.team-a.example.com    4d
```

Wide-only columns are listed separately; the compact set is shown by default
and `-o wide` appends the rest.

| Resource | Compact columns | Wide adds |
| --- | --- | --- |
| `inferenceservices` | NAME, MODEL, RUNTIME, READY, URL, AGE | — |
| `models` | NAME, SCOPE, ARCH, PARAMS, FORMAT, STATE, AGE | — |
| `basemodels`, `clusterbasemodels` | NAME, ARCH, PARAMS, FORMAT, STATE, AGE | — |
| `runtimes` | NAME, SCOPE, DISABLED, FORMATS, AGE | — |
| `servingruntimes`, `clusterservingruntimes` | NAME, DISABLED, FORMATS, AGE | — |
| `acceleratorclasses` | NAME, VENDOR, FAMILY, AGE | — |
| `acceleratorquotas` | NAME, ROLE, PARENT, RESOURCE, FLAVOR, NOMINAL, ADMITTED, SOURCE, STATUS-FRESHNESS, READY, DEGRADED, AGE | BORROWED, RESERVED |
| `benchmarkjobs` | NAME, STATE, AGE | — |
| `finetunedweights` | NAME, TYPE, BASEMODEL, AGE | — |
| `inferencereplicas` | NAME, COMPONENT, PARENT, DESIRED, CURRENT, READY, LIFECYCLE, AGE | AVAILABLE, REASON, SERVING, UPDATED, ENCODING, CURRENT-REVISION, UPDATE-REVISION, MIGRATIONS, PAUSED, COORDINATION, LIFECYCLE-FRESHNESS |
| `workloadclusters` | NAME, CONNECTION, REFERENCE, KEY, READY, GENERATION, OBSERVED-GENERATION, AGE | REASON |
| `rolloutpolicies` | NAME, PROGRESSION, READY, DIGEST, REFS, AGE | REASON, IN-USE, STATUS-FRESHNESS |
| `autoscalerpolicies` | NAME, CLASS, READY, ATTACHED, AGE | DIGEST, REASON, IN-USE, STATUS-FRESHNESS |
| `trafficmaps` | NAME, MODE, TARGETS, ROUTABLE, PUBLISHED, AGE | SERVICE, ACTIVE, HEALTHY, OVERRIDE, OVERRIDE-REASON, REASON, GATEWAY, PUBLISHER-FRESHNESS |

Details worth knowing per resource:

- **Models** (`models`, `basemodels`, `clusterbasemodels`) — `ARCH`,
  `PARAMS` and `FORMAT` come from the model spec (architecture, parameter
  size, format name); `STATE` is the model's lifecycle state from status.
- **Runtimes** (`runtimes`, `servingruntimes`, `clusterservingruntimes`) —
  `DISABLED` prints `true`/`false`; `FORMATS` lists the supported model
  format names, clipped after three to `fmt1,fmt2,fmt3,+N`.
- **`acceleratorquotas`** — the table is a flattened budget view: **one row
  per budget**, so a single quota can span several rows. `SOURCE` is
  `Reported` for a status budget, `Declared` for a spec budget shown because
  status has not observed the current generation, or `Unavailable` when the
  quota has neither. `STATUS-FRESHNESS` compares `status.observedGeneration`
  to `metadata.generation` (`Current`, `Stale` or `Unobserved`).
- **`inferencereplicas`** — `DESIRED` is `spec.replicas` (`-` when unset);
  `CURRENT` and `READY` are the status counters. `LIFECYCLE` prints
  `<Condition>=<Status>`, preferring an active `RolloutStalled` condition
  over `Ready` so a serving-but-wedged rollout cannot look healthy, and
  `Unavailable` when neither condition exists. The wide `ENCODING` column
  reports the stored status representation — `DenseV1`, `ColumnarV2`,
  `Unknown` (an unrecognized marker) or `Invalid` (an inconsistent
  representation) — not the validity of individual rows. An unmarked
  status without columnar data is read as `DenseV1`. A standalone replica
  has no InferenceService parent, so its `PARENT` column is `-`.
- **`workloadclusters`** — `CONNECTION`, `REFERENCE` and `KEY` describe the
  cluster source: `KubeConfig` with its Secret reference and key, or
  `ClusterProfile` with the profile name; an ambiguous or empty source
  renders as `Invalid`.
- **`rolloutpolicies`**, **`autoscalerpolicies`**, **`trafficmaps`** — the
  condition-backed columns are generation-guarded: when status has not
  observed the current `metadata.generation`, status columns print `Unknown`
  and value columns print `-` instead of passing off stale state as live.
  Column semantics are documented on each kind's concept page:
  [Rollout Policy](/ome/docs/concepts/rollout_policy),
  [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy) and
  [Traffic Map](/ome/docs/concepts/traffic_map).

## Listings are complete and paged

Every listing drains the API in bounded chunks of 500 objects (kubectl's own
default chunk size), following continue tokens until the collection is
exhausted — merged views drain both halves. If a pagination token expires
mid-listing, the partial snapshot is discarded and the same chunked read
restarts once from the beginning; a second expiration fails the command.
Output — table or `-o json`/`-o yaml` — is therefore always a complete
listing, never a partial or mixed snapshot.

## Errors and exit codes

`get` is an inspection command: it exits 0 on success (including an empty
listing) and 1 on any failure, and never uses the plugin's exit codes 2
and 3. Failures write a single bounded, classified line to stderr that
identifies the request without echoing raw server text, for example:

```
error: get inferenceservices team-a/missing: NotFound
error: list trafficmaps prod: Forbidden
```

See the [plugin exit codes](/ome/docs/tasks/kubectl-ome/#exit-codes) for the
full mapping.

## Related pages

- [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome) — install, connection
  flags, exit codes, RBAC.
- [Read the kubectl ome status Report](/ome/docs/tasks/kubectl-ome-status) —
  one service's conditions, pods and resolved runtime in depth.
- [List Logical Instances with kubectl ome instance
  list](/ome/docs/tasks/kubectl-ome-instance-list) — the controller-reported
  instance inventory behind one InferenceService.
