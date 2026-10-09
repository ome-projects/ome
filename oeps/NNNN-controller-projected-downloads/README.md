# Controller-projected model download scope

An external controller can narrow model storage eligibility to selected node
pools while preserving the original eligibility of ordinary nodes. The agent
does not watch endpoints, resolve customer identity, or change queue priority.
This contract uses the existing storage-selection algorithm; it does not
introduce a separate download scheduler.

## Agent contract

Both arguments are empty by default, preserving existing behavior:

```text
--download-scope-node-label=example.com/scoped-node
--download-scope-pool-label=example.com/pool
```

The existing Helm `modelAgent.env` facility can set the equivalent
`DOWNLOAD_SCOPE_NODE_LABEL` and `DOWNLOAD_SCOPE_POOL_LABEL` variables. Raw
DaemonSets can pass the arguments directly. Configure every deployed GPU/CPU
agent consistently. The deployment must select an image built from this source;
existing chart image defaults are not evidence of feature support.

The controller preserves `spec.storage.nodeSelector` and intersects each original
required affinity term with ordinary nodes or its selected pool set. It atomically
publishes that affinity and the `models.ome.io/download-scope` JSON annotation:

- `version`: `v1`;
- `uid`: the current model UID;
- `applied`: the exact published NodeAffinity;
- `pools`: allowed values of the configured pool label;
- controller-owned baseline fields may support reversible restoration.

For classified nodes, new downloads require both existing storage eligibility and
consistent scope evidence. Missing identity or incomplete evidence blocks new
downloads without treating uncertainty as permission to delete a cache. A known
eligibility loss can remove local state; queued work is revalidated against the
current model UID and eligibility. Node identity changes replay eligibility.

Nodes with neither label retain ordinary eligibility, task execution and selector
semantics. They do not poll node scope or depend on scope model-cache validation.
Projection-only updates do not restart their downloads. Scoped nodes must register
with classification before agents observe them. Once classified, an agent cannot
become ordinary by losing both labels; missing identity blocks new scoped work
without deleting cached files. This contract selects downloads; it is not a
workload-admission or tenant-authorization boundary.

## Ownership and migration

Pool selection, reference retention, optimistic updates, and restoration remain
the external controller's responsibility. The agent adds no endpoint clients or
endpoint readiness dependency. Existing deletion, reuse and cancellation paths
remain in place.

No new model spec/status fields or manager demand controller are required.
The external scope owner handles consumer rollout and restoration. The model
manager continues its existing import validation and readiness responsibilities.

Disable the agent guard through a rollout, then let the owning controller restore
the original affinities before removing its lifecycle permissions. Clearing agent
arguments alone does not undo persisted affinity restrictions.

## Validation

`pkg/modelagent/download_scope_test.go` covers ordinary and scoped eligibility,
incomplete evidence, node promotion, same-name recreation, eligibility-loss cleanup,
queued-task revalidation and concurrent refresh. `download_scope_baseline_test.go`
covers ordinary-node no-op behavior, projection/restoration updates, and sticky
classification. Manager
readiness tests preserve the downstream requirement to sync controller sources
before advertising readiness, independently of liveness.

Image consumers must validate their schemas/RBAC against the exact source commit
before updating their image pin. A deployment must complete agent rollout before
the owning controller changes model projections.
