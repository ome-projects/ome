# Model eviction dry-run controller

This independent controller connects `artifactinventory`, `modelusage` and
`evictionplanner`. It reports what it would evict, without submitting eviction
intent or deleting files. It does update the BaseModel usage-history annotation.

## One planning round

1. List nodes matching the configured selector and read kubelet `imageFs`
   capacity and available bytes over authenticated HTTPS.
2. Build inventories only for nodes at or above H. At most four node tasks run
   concurrently. Failed measurements remain unknown; failed inventories remain
   unevaluated.
3. Check each distinct candidate Model CR once using the existing Evaluator.
   Models with serving references, claims, explicit retention or failed checks
   stay protected.
4. Pass the pressure-node inventories and eligibility results to the existing
   batch planner. It accounts for cross-node effects and prunes redundant choices.
5. Log the report: observations, errors, candidates, exclusions, alternatives,
   selected CRs, projected usage and remaining shortfalls. Projections are not
   evidence that any space has been released.

The leader runs a round after startup synchronization, then waits six hours
between completed rounds. Rounds never overlap. Capacity is revalidated before
planning; samples that expire during collection are excluded from that round.

| Setting | Default |
| --- | --- |
| High / low usage watermark | 75% / 70% |
| Delay between rounds | 6 hours |
| Concurrent node tasks | 4 |
| Individual API / kubelet request timeout | 10 seconds |
| Overall round timeout | 10 minutes |
| Maximum capacity sample age | 5 minutes |

These collection limits are configurable starting values, not a cluster-scale
latency guarantee. Watch startup and planning deadlines should be checked during
staged deployment; a failed or incomplete round does not submit any eviction.

## Usage history between rounds

Reference informers run continuously across all namespaces. Their indexes cover
InferenceServices, Pods, Deployments, ReplicaSets, StatefulSets and
InferenceReplicas. LeaderWorkerSets are included when their API is installed at
startup; restart this controller after installing that CRD.

The initial list also indexes existing serving objects that have no use claim.
Planning waits for all configured caches to synchronize. Removal events and the
last BaseModel claim release enqueue a fresh eligibility query. If other demand
remains, no demand-end time is recorded. Retries keep the original event time.
Only the leader processes history and planning; replicas use a Kubernetes Lease.

The cache is eventually consistent. These reports do not authorize deletion.
Actual execution still needs use-claim integration, fresh submission checks and
lifecycle completion feedback. This binary has no execution-enabling flag.

## Deployment

Build `cmd/model-eviction-controller` or the image in
`dockerfiles/model-eviction-controller.Dockerfile`. The opt-in manifests in
`config/model-eviction-controller` are separate from the default installation and
start with zero replicas. Existing OME APIs and the model ConfigMap namespace
must already exist.

Before enabling one replica:

- Set the built image and a node selector for the intended model pools. The
  sample selector uses `nvidia.com/gpu.present=true`; change it to match the
  cluster. **Model storage must share the filesystem reported as `imageFs`.**
- Ensure the Pod can reach each selected node's kubelet HTTPS port. The reader
  uses InternalIP, then InternalDNS or Hostname, and the node's advertised port
  (10250 if absent). The chosen address must match the serving certificate.
- Use the ServiceAccount CA when it signs kubelet certificates. Otherwise mount
  the appropriate CA bundle and pass `--kubelet-ca-file`. TLS verification cannot
  be disabled. The reader uses the rotating ServiceAccount bearer token.
- Ensure the mandatory reference APIs and their RBAC are available. All serving
  objects must be visible to the cache. Missing permissions prevent readiness.

RBAC grants `get nodes/stats`, reference watches, BaseModel history patches,
ConfigMap reads in `ome`, ReplicationJob reads in `default`, and leader Lease and
event access. It grants neither `nodes/proxy` nor `pods/exec`. Kubernetes RBAC
cannot restrict patch permission to one annotation; the controller code limits
writes to usage history. Adjust namespaced bindings if changing namespaces.

`/healthz` reports process health; `/readyz` requires reference-cache sync. Cache
memory scales with serving objects, independently of the six-hour plan interval.
Managed fields are stripped. The manifest's resource settings are starting values.

## Local validation

```sh
go test -race ./pkg/modeleviction ./pkg/artifactinventory ./pkg/modelusage ./pkg/evictionplanner ./pkg/modelclaims
go build ./cmd/model-eviction-controller
```

Set `KUBEBUILDER_ASSETS` to local envtest binaries to include the API-server test.
That test verifies initial indexing and history updates after ISVC/Pod removal;
it does not use a real cluster. TLS tests use local HTTPS servers. Round tests
exercise the actual inventory, Evaluator and planner through a fake client that
rejects annotation writes other than usage history.
