# Model artifact use claims

`modelclaims` coordinates endpoint preparation and eviction through the same
BaseModel's `ome.io/artifact-use-claims` annotation:

```json
{"endpoint-a": true, "endpoint-b": true}
```

Use a stable endpoint identifier for each claim. The caller resolves the actual
namespaced BaseModel and pins its UID; a replacement with the same name is a
different model. This package has no background worker or automatic expiration.

## Operations

Create `NewCoordinator(apiReader, writer)` with an **uncached** reader.
Its Kubernetes identity needs `get` and `patch` permission for BaseModels.

| Method | Caller supplies | Result |
| --- | --- | --- |
| `Acquire` | Validated BaseModel snapshot and endpoint ID | Merges the claim with one conditional patch; returns conflicts to the caller |
| `Evict` | Model namespace/name and UID, after eligibility evaluation | Sets `ome.io/artifact-residency: Evicted` only when no claims remain |
| `Release` | Model namespace/name, UID and the same endpoint ID | Removes only that endpoint's claim |

`Acquire(ctx, model, endpointID)` uses the exact CR snapshot that the preparation
caller checked for readiness and current residency. It merges the claim and
patches with that snapshot's UID and resourceVersion, without another GET, a
readiness callback or retries. It does not mutate the supplied snapshot. On
conflict, the caller rereads and reevaluates readiness/restoration before deciding
whether to retry. Never replace the resourceVersion while reusing an old Ready
result. The preparation workflow must acquire the claim before returning success;
workload creation consumes that result and does not register claims independently.

`Release` and `Evict` read the current CR and use client-go's bounded conflict
retry to repeat their checks and conditional patch. All writes preserve unrelated
annotations, other endpoint claims, spec and status. Missing claims mean an empty
set; malformed annotation values block changes. This package does not implement
restoration or declare physical files ready.

Release a claim only after that endpoint's creation operations and associated
workloads have ended, including failure/cancellation cleanup. Persist enough
workflow state to retry release after a restart. A failed request or timeout
alone is not permission to release. `Release` is idempotent; a CR NotFound or
UID mismatch must be handled by the caller's workflow without touching a new CR.

## Integration boundary

- `modelusage.Evaluator` protects claimed models even before the serving cache
  sees an ISVC. One claimed model excludes its entire shared artifact from the
  planner. Invalid claim metadata also protects it.
- Wire BaseModel updates through `modelusage.RemovedReferences` too. The final
  claim release supplies a demand-end event when workload removal was previously
  observed while the claim was still held.
- Scout ignores claim-only changes for DownloadOverride. Actual download input
  and lifecycle intent changes retain their existing behavior.
- `Evict` is the single-CR conditional submission primitive. The caller still
  evaluates serving references and all related models. It rechecks the current
  reservation label and vendor source alongside claims. Group submissions are
  not a transaction, and success is not confirmation of file deletion.
- Enable automated eviction only for model users covered by the preparation/claim
  protocol. Apply that restriction to every model in a shared reclamation group;
  an absent claim is not evidence of rollout coverage.
- Endpoint preparation, recoverable release, manual eviction entrypoints and
  lifecycle cleanup/restoration still need to adopt this protocol. Direct
  annotation writes bypass it. This library alone does not enable automated
  eviction or provide end-to-end coordination with workload creation.

## Validation

```sh
go test -race ./pkg/modelclaims ./pkg/modelusage
```

Tests exercise both claim/eviction race orders, concurrent claims and releases,
caller-owned acquisition retries, snapshot preservation, UID replacement, bounded
eviction conflicts and a new coordinator resuming from persisted claims. They
require no cluster.

For the local API-server test, set `KUBEBUILDER_ASSETS` to installed envtest
binaries and run `go test -race ./pkg/modelclaims -run TestConditionalWritesWithAPIServer`.
This starts an isolated API server; it does not use a cluster kubeconfig.
