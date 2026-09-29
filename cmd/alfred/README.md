# Alfred

Alfred is OME's GPU cluster caretaker. It recommends defragmentation and
evacuation of workloads from unhealthy or maintenance-marked nodes. With
explicit execution opt-in, it requests whole-instance migrations through OME's
public migration API; the workload-owning controllers perform the migration.

## How it works

Every replica observes cluster state. Only the elected leader evaluates node
health and defragmentation policies, applies safety budgets, and reports
recommendations or dispatches migration requests. Before dispatch, Alfred uses
an isolated scheduler worker with an explicitly configured Kubernetes or OME
scheduler profile, then rechecks the source and cluster state.

- [`main.go`](main.go): process setup, configuration, workers and leader election.
- [`pkg/alfred`](../../pkg/alfred): snapshots, policies, guards, dispatch journal,
  reporting and metrics.
- [Scheduler simulator](../../pkg/alfred/simulator/README.md): profile matching,
  execution prerequisites, supported inputs and recovery procedures.

Alfred does not import workload or OMENative controller behavior. Its bounded
status reader uses the shared IR wire codec; that read-only codec is the sole
approved controller-package dependency. Migration mechanics remain outside
Alfred.

## Configuration and limits

Start with the [Helm defaults](../../charts/ome-alfred/values.yaml) or
[Kustomize configuration](../../config/alfred). The default is `recommend-only`:
Alfred can publish reports and Events but does not request migrations. Node
health conditions and maintenance conditions, labels and taints are configurable.

Execution requires compatible scheduler workers, explicit migration-v1 startup
configuration, execution policy, admission guards and a retained dispatch
journal. The OME controller also needs valid `lifecycle.audit` configuration;
Alfred's compatibility flag does not discover consumer readiness. See the
[migration API guide](../../site/content/en/docs/tasks/request-an-instance-migration.md).

New migrations respect both the replica's pause flag and the public placement
execution policy. A paused or invalid authority envelope on the replica or its
origin-marked owner prevents submission, including while owner policy projection
is pending. A valid release must clear both observed pauses before new work proceeds.
Already submitted migrations remain tracked; a pause does not cancel them or
stop reconciliation of allocated work. Alfred only reads this placement protocol;
it does not implement placement-controller or migration-controller behavior.
Pause-related recommendations and dispatch diagnostics can currently report
generic ineligibility or source-change reasons rather than identifying the pause.

Current execution supports eligible OMENative instances only. RawDeployment,
LWS and other workloads have no eviction adapter. Simulation reserves no
capacity; target hints are preferences, not scheduling-time health guarantees.
One unresolved request blocks new dispatches. Do not erase the journal to
unblock it. These limits and the recorded qualification results are not a
production-readiness guarantee.

New prepared requests use a versioned semantic source fingerprint. Retries keep
the exact UUID and payload across diagnostic status updates or a valid, fully
observed placement pause/release. They still require unchanged source identities,
specs, gang members, lifecycle state and authority identity, plus fresh policy,
budgets, simulation and submission checks. Generations must remain stable during
each simulation attempt. Condition states and lifecycle timestamps remain fenced;
event stamps, previous-failure diagnostics and condition messages do not.

Pre-upgrade prepared requests retain their original strict fingerprint: Alfred
never rewrites their fence to authorize a retry. Unknown fingerprint versions
cannot be retried, but already published requests still reconcile. `SourceChanged`
also covers unknown or malformed stored fingerprints, so it does not always mean
the live source changed. A pause that outlasts the acknowledgement deadline can
leave a request stalled; this change does not resume stalled requests. The
pre-dispatch checks do not provide an atomic
transaction across owner and replica objects, fence future consumer acceptance,
or reserve a destination. Typed source fingerprints cover fields known to the
running Alfred binary, not unknown fields from a newer API version.

## Build and verify

From the repository root:

```sh
go build ./cmd/alfred
go test ./cmd/alfred/... ./pkg/alfred/...
(cd pkg/alfred/simulator && go test ./...)
```

Follow the [local kind/KWOK harness](../../hack/alfred-kind-e2e/README.md) for
real-controller and scheduler acceptance tests. Its
[qualification record](../../hack/alfred-kind-e2e/QUALIFICATION.md) identifies
the revisions actually tested and remaining gaps. Unit tests and virtual-GPU
acceptance tests do not establish real inference-traffic continuity.
