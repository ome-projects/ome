# Model usage and eviction eligibility

`modelusage` queries current serving references, maintains BaseModel idle
history, and returns `evictionplanner.Eligibility`. It contains no deployed
controller, watches, artifact deletion, or separate in-memory reference graph.

## Controller integration

1. Create `NewReferenceQuery(cache, hasSynced, SourceOptions{...})` using the
   **informer cache** as reader. Set `LeaderWorkerSets: true` whenever LWS is
   enabled; the other required sources are InferenceServices, Pods, Deployments,
   ReplicaSets, StatefulSets, and InferenceReplicas. Add these types to the
   scheme and ensure the cache and RBAC cover every relevant namespace and
   configured source kind, without selectors that hide serving objects.
2. Call `query.RegisterIndexes(ctx, manager.GetFieldIndexer())` before starting
   the cache. Treat registration failure as a setup failure. Wait for all
   configured informers to synchronize; `hasSynced` must report their combined
   synchronization state. The same query object owns registration and the
   queried source set. Do not omit an enabled workload kind to bypass errors.
3. Create `NewEvaluator(apiReader, client, query, nil)` with an **uncached**
   BaseModel reader. `nil` selects the real clock.
4. For updates, call `RemovedReferences(oldObject, newObject, observedAt)`;
   for actual deletions, call `RemovedReferences(deletedObject, nil, observedAt)`.
   Unwrap informer deletion tombstones to the typed object first. Capture
   `observedAt` once when receiving the event, then enqueue each returned
   `Request`. Creates, startup/relist additions, status-only changes, reference
   additions, and duplicate names do not produce release requests.
5. Call `Reconcile(ctx, request, expectedUID, inFlight)`. The comparable request
   can be a workqueue item and must retain its original `ReferenceRemovedAt`
   through retries. If a queue coalesces by model instead, retain the newest
   actual removal time; never replace it with reconciliation time. Errors
   protect the model and retry the same request. A successful protected result
   consumes the event: a remaining source's later removal generates another.
   No package state remembers or reconstructs missed events.
6. Ordinary planning/startup uses `Request{Model: key}`, with no removal time.
   Planner callers supply the inventory UID and current eviction-in-flight
   state. History-only event handling may use an empty expected UID; its result
   must not authorize an inventory candidate. A history-only BaseModel NotFound
   may mean the name resolves to ClusterBaseModel; finish that event instead of
   retrying forever. Candidate NotFound remains protected.
7. Put each candidate's result in `EligibilityByUID`. The planner excludes an
   entire shared artifact when any associated model is protected. On errors,
   retain protection rather than reusing an earlier eligible result.

Each query uses namespace and model-name field indexes plus an unresolved-source
sentinel. It never scans all cluster objects. Missing indexes, list failures,
unsynchronized caches, and unresolved OME identities protect the model.

## Reference rules

ISVC primary and overlay names protect BaseModels in the ISVC namespace. OME
tries namespaced BaseModel first and ClusterBaseModel second, including an
explicit `ClusterBaseModel` kind. ClusterBaseModels are outside eviction scope.

Workloads use the renderer's OME ISVC label, primary `ome.io/base-model-name`
annotation / `base-model-name` label, and local HostPath overlays named
`model-overlay-<model name>`. Both LWS leader and worker templates participate.
InferenceReplica model refs and all stored runner templates participate; its
renderer adds the OME label even when a runner template omits it. Template
references use the owning object's namespace. Arbitrary host mounts and remote
or emptyDir overlay volumes do not establish model identity. Missing or
conflicting primary identity on an OME workload protects its whole namespace.

Unready, terminating, terminal, and zero-replica sources retain their references
until actual removal. Deleting, vendor-URI, reserved, Evicted, and in-flight
BaseModels are protected without history writes.

## History and limits

Only `ome.io/model-usage-history` is patched, conditional on UID and
resourceVersion. Conflicts get at most three total attempts, each with a fresh
BaseModel GET and a fresh current-reference query. Other metadata is preserved.

When references remain, history stays unchanged. With no current references,
a valid removal event monotonically advances `lastObservedDemandEndedAt` using
its original observation time. Duplicate or older requests cannot reset it;
future and pre-creation events are rejected as history evidence. With no valid
end or fallback, ordinary evaluation initializes `idleSince` once. Invalid,
future, and pre-creation history fields are independently treated as absent.

`lastObservedDemandEndedAt` takes precedence over `idleSince` and is returned in
**`Eligibility.IdleSince`**, the planner's recency input. An earlier fallback may
remain stored but is unused once a valid end exists. Active history needs no
clearing because current references protect the model.

The sync predicate gates initial cache synchronization; it is not a separate
watch-health monitor. Cache data can lag, and queries across resource kinds are
not atomic with one another or with a later workload creation or history patch.
Missed events and use intervals during controller downtime are not reconstructed;
startup/relist must not fabricate demand ends. An unresolved source without
known names cannot supply model-specific removal events. History is a scoring
input, and eligibility is an advisory snapshot: execution must revalidate and
coordinate with newly starting demand before submitting eviction intent.

Scout excludes the exact history annotation from download-input comparisons.

## Validation

```sh
go test ./pkg/modelusage ./pkg/evictionplanner
go test -race ./pkg/modelusage
```

Tests use indexed fake cache clients and conditional API patches. They require
no cluster access and cover source extraction, release events, history, retries,
UID replacement, lifecycle exclusions, and shared-artifact protection.
