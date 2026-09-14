# Alfred scheduler simulator

`alfred-simulator` is an offline, one-request scheduler worker for Alfred
placement predictions. It runs the Kubernetes scheduler compiled at **v1.35.4**
against private clients populated only from the JSON request. It does not read a
kubeconfig, use in-cluster credentials, contact an API server, or change a live
cluster.

Alfred can opt into this worker for recommendations. Its leader-only
decision loop captures full public cluster objects through the uncached API
reader, builds predictive relocation requests, and invokes an exact-profile
worker from its startup registry. Supplying a worker or profile never enables
migration execution by itself. The separately guarded execution path below
requires explicit startup opt-in as well as `mode: execute`.

## Enable guarded OMENative migration

First configure and verify the exact worker profiles as below. Then set Helm
`migration.apiVersion: v1` and policy `alfredConfig.mode: execute` with
`alfredConfig.omenativeMigrationEnabled: true`. Recommendation-only remains the
default. Leader election is mandatory. The chart installs a fail-closed
`ValidatingAdmissionPolicy`/Deny binding for the Alfred service account and
precreates `alfred-dispatch-state`. Kubernetes 1.30+ is required; unchecked,
missing or drifted admission configuration withholds execution.

`migration.apiVersion` is the operator's assertion that the existing OME v1
migration API is enabled and compatible. It is **not controller liveness
discovery**; Alfred does not depend on an OME capability Lease or add controller
code. RawDeployment, LWS and non-OME workloads are not actuated by this path.
Native Kubernetes eviction is a separate future adapter, not a gang fallback.

Only original executable policy candidates enter execution. Alfred replays
policy and safety checks against fresh API reads, simulates the complete
instance, rechecks source UIDs/incarnation/revisions/member Pods and scheduling
objects, and submits one UID/resourceVersion-conditioned migration annotation.
Execution also requires source node affinity to match the public runner-template
baseline. A source retaining an earlier migration's overlay, or another
unexplained affinity change, is withheld instead of simulating accumulated hints
that the next migration would not reproduce.
For migration simulation, `migrationFromNode` explicitly identifies the single
excluded source node; all other source Pods remain occupied. The replacement
Pods carry the API's required hostname exclusion and weight-50 soft target
hints. Recommendations without that field retain their all-source exclusion
contract. Update Alfred and its bundled worker together; older workers reject
this new optional request field.

Execution is serial: **one unresolved Alfred request at a time**, even with
larger configured limits. Other active migrations also block new requests.
The journal persists the UUID, exact payload and source fingerprint before a
write attempt. Uncertain retries reuse that intent; only the matching current
InferenceReplica UUID status acknowledges it. Annotation disappearance is not
completion. Default acknowledgement timeout is two minutes
(`migration.acknowledgementTimeout`), and failure backoff is five minutes
(`migration.failureBackoff`); both must be positive and at most one hour.
A timeout becomes `stalled`, remains unresolved and blocks new requests. Late
terminal status can resolve it. No automatic cancellation, annotation deletion
or replacement UUID is performed. Preserve the journal across upgrades and
leader changes; do not clear it merely because a request timed out. If status
was lost or an owner was recreated, an operator must reconcile the request
with the owning controller before repairing that state.
The Helm journal is retained when execution is disabled or the release is
uninstalled. Re-enabling execution must adopt its existing state, not initialize
an empty journal. Retained ConfigMaps may need deliberate ownership/adoption
handling when moving to a different release. Terminal history is retained for
at least an hour and for longer configured cooldown windows; size/entry limits
withhold new work rather than dropping unresolved or still-needed history.

The journal and recommendation records distinguish `withheld`, `submitted`,
`acknowledged`, `completed`, `failed`, and `stalled`, with UUIDs and bounded
reasons. Admission alone is never reported as a submitted migration.

Predicted placements and hints are **not reservations**. The scheduler may
choose other nodes. API v1 also lacks an expected-incarnation/UID field: Alfred
checks identity before submission, but cannot fence a queued request against
changes before the consumer accepts it. These are explicit limits of the
existing API, not guarantees supplied by simulation.

For Kustomize, add the same worker mount and startup flags:
`--migration-api-version=v1`, `--migration-service-account=ome-alfred`,
`--migration-ack-timeout=2m`, `--migration-failure-backoff=5m`.
The base leaves these execution flags absent. If changing namespace or service
account, update the admission policy's exact username and RBAC subjects along
with the explicitly namespaced resources. One configured Alfred identity is
supported per cluster. Alfred cannot install or modify its own admission guard.

### Kustomize journal lifecycle

`config/alfred/dispatch-state.yaml` is a **bootstrap-only initializer**, excluded
from the recurring Kustomize base. The runtime never creates this ConfigMap;
a missing or malformed journal withholds execution. Recommendation-only
installation does not require creating an empty migration journal.

Before the first migration-enabled installation, confirm the target cluster and
namespace and check for retained state:

```sh
kubectl -n ome get configmap alfred-dispatch-state -o yaml
```

Only a confirmed `NotFound`, together with confirmation that there is no earlier
Alfred dispatch history to recover, permits this one-time bootstrap:

```sh
kubectl create -f config/alfred/dispatch-state.yaml
```

Never use `apply`, `replace`, `--force`, or delete/recreate with this initializer.
`AlreadyExists` means **stop and inspect the existing journal**; it is not an
error to suppress or a reason to overwrite it. An API/read failure is not proof
that no journal exists. A custom namespace needs the same explicitly reviewed
namespace change in this bootstrap file as in the deployment, RBAC and guard.

For normal install, upgrade and reinstall, use `kubectl apply -k config/alfred`
or `make install-alfred`. The latter's force-conflict server-side apply cannot
reset the journal because the journal is not in the base resource set. Uninstall
with `kubectl delete -k config/alfred` or `make uninstall-alfred`; both leave the
journal intact. Preserve the namespace too: deleting it or using a broad
ConfigMap cleanup still deletes durable history. Do not apply/delete the
initializer through recursive directory operations or a GitOps resource list.

When adopting an installation that previously managed the journal through
Kustomize or another release, first stop Alfred so both dispatch and journal
reconciliation are paused, then inventory/back up the live object's UID and
exact data, including unrelated keys. Remove the
journal from recurring apply **and deletion/pruning ownership** before enabling
the new resource set; GitOps removal must not prune it. Reuse the existing
ConfigMap without changing its UID or state bytes. Changing Helm release
ownership requires an intentional metadata-only adoption consistent with the
retained release; never bootstrap over the existing data. Re-enable execution
only after its namespace/identity, guard and worker configuration are verified.

Recovery is an explicit operator action, not an installation step. If state is
missing, malformed, or stalled, keep execution disabled and stop Alfred before
modifying the ConfigMap. Preserve all available
journal/backup bytes, and reconcile every retained UUID with request annotations,
authoritative InferenceReplica status and the owning controller. Annotation
disappearance, timeout or absent status alone does not prove cancellation.
Restore or repair reviewed history only after establishing the outcomes of
possibly applied requests and the still-needed budgets/cooldowns; do not clear
unknown entries to unblock execution. Reinstalling Alfred is not journal
recovery, and an empty initializer is not a substitute for lost history.

## Enable recommendation simulation

The Alfred image bundles `/alfred-simulator`. Pin the image by digest and mount
an operator-owned **immutable** ConfigMap containing `workers.json` and one
scheduler configuration per worker. Keep it separate from the hot-reloaded
Alfred policy ConfigMap. Print each identity using the **same image artifact**,
configuration file and empty environment used by Alfred:

```sh
env -i /alfred-simulator --backend alfred-default-v1 \
  --scheduler-config /etc/alfred-simulation/default.yaml --print-profile
```

Copy that result into the trusted startup manifest (substitute the exact
printed identity and gang capability; do not use these placeholders literally):

```json
{
  "workers": [{
    "binaryPath": "/alfred-simulator",
    "schedulerConfigPath": "/etc/alfred-simulation/default.yaml",
    "identity": {
      "schedulerName": "default-scheduler",
      "backend": "alfred-default-v1",
      "schedulerVersion": "v1.35.4",
      "configurationID": "<printed configurationID>"
    },
    "gangScheduling": false
  }]
}
```

Set `simulation.configMapName` in the `ome-alfred` Helm chart to this existing
ConfigMap's name. The chart mounts every key under `/etc/alfred-simulation` and
passes `--simulation-workers=/etc/alfred-simulation/workers.json`. For the
Kustomize installation, add that same read-only volume/mount and flag through
your deployment overlay. The default empty registry flag disables prediction.
Also copy the identity/capability into `alfredConfig.scheduling.profiles` as
described below. The startup probe rejects mismatches; changing the policy
ConfigMap cannot introduce another executable or cause a scheduler fallback.
To change the registry or scheduler configuration, create a new immutable
ConfigMap, update the deployment reference and restart Alfred.

Each decision cycle attempts at most eight candidates in policy order, with
one fresh lossless capture, a 30-second total deadline, and a 30-second maximum
age for both the policy observation and capture. Freshness is checked again
after evaluation. The worker timeout defaults to ten seconds (chart
`simulation.timeout` or `--simulation-timeout`, maximum one minute), subject
to the shorter cycle deadline. The worker receives 80% of the remaining
whole-process budget for evaluation, leaving headroom to return a validated
`Unsupported` timeout result. Captures are non-atomic and predictions reserve
nothing. Source owner UID/generation, revision, incarnation, complete Pod
identity and placement must still match the observation that selected it.

Recommendation JSON records the closed scheduling status/reason, snapshot ID
and time, and at most 128 predicted Pod placements separately from policy
target hints. Larger cohorts are reported `RecommendationTooLarge`; other
unmodelled inputs remain `InputUnsupported`. Results are never executable and
never consume migration budgets. An original advisory reason such as
`OMENativeUnavailable` stays visible even when a prediction is feasible.

The registry runs only explicit executables with fixed arguments, one process
at a time, no shell, an empty environment, bounded input/output, cancellation
and child reaping. Manifest/config files are capped at 1 MiB; requests/results
at 16 MiB; profile output/stderr at 64 KiB. It accepts only one strictly decoded
response tied to the exact request, snapshot and profile. Raw worker errors
are not published in recommendations. This is **not an OS sandbox**: the
trusted binary shares Alfred's filesystem and service-account mounts. Only
deploy the reviewed offline worker; environment stripping does not make an
arbitrary replacement executable safe.

## Build and test

Run these commands from `pkg/alfred/simulator`:

```sh
go test ./...
go vet ./...
go build -o ./bin/alfred-simulator ./cmd/alfred-simulator
```

The module is deliberately separate from the repository root because embedding
`k8s.io/kubernetes` requires the Kubernetes staging-module replacements. It
reuses the OME scheduler plugin through the adjacent `scheduler` module; it does
not import the root `sigs.k8s.io/ome` module.

## Scheduler profiles

A worker identity is derived from the strictly decoded, defaulted scheduler
configuration, the compiled scheduler version, the supported feature-gate
state, and available build VCS/module provenance. Build metadata is not always
available and a local module replacement may be unversioned, so the digest alone
does not prove that two binaries are equivalent. Operators must pin an immutable
worker artifact and use the opaque backend name to identify that deployment.
Print the complete identity before producing requests:

```sh
./bin/alfred-simulator \
  --backend alfred-default-v1 \
  --scheduler-config examples/default-scheduler.yaml \
  --print-profile
```

The output has this shape:

```json
{
  "identity": {
    "schedulerName": "default-scheduler",
    "backend": "alfred-default-v1",
    "schedulerVersion": "v1.35.4",
    "configurationID": "<calculated digest>"
  },
  "gangScheduling": false
}
```

Use `identity.schedulerName` as the key under Alfred's
`scheduling.profiles`, and copy all printed identity fields and
`gangScheduling` into that mapping. Reprint and update the mapping whenever the
scheduler configuration, compiled worker, or feature gates change. A request
must carry the exact printed `identity`; matching only the scheduler version is
not sufficient.

Two supported starting configurations are included:

- `examples/default-scheduler.yaml` preserves the upstream default plugin set.
- `examples/ome-scheduler.yaml` mirrors the chart's OME profile: OMEGangPack is
  enrolled through MultiPoint, DefaultPreemption is disabled, NodeResourcesFit
  uses `MostAllocated`, the OME and resource packing scores are weighted, and
  only PodTopologySpread's preference score is disabled. Hard topology-spread
  filtering remains enabled.

Each configuration must contain exactly one scheduler profile. Unknown plugins,
extenders, and unsupported settings are rejected instead of being silently
ignored.

## Run one simulation

The process accepts exactly one versioned JSON `Request` on stdin and writes
exactly one JSON protocol `Result` on stdout for a valid request:

```sh
./bin/alfred-simulator \
  --backend alfred-default-v1 \
  --scheduler-config examples/default-scheduler.yaml \
  --timeout 10s \
  < request.json > result.json
```

The timeout defaults to 10 seconds and must be positive and no greater than 60
seconds. Input is capped at 16 MiB. Configuration, decoding, validation, and
unexpected evaluation failures are reported on stderr and exit nonzero. A
valid, correctly evaluated `Unsupported` decision is still a protocol result
and exits successfully.

For a repository smoke test, replace the fixture's illustrative identity with
the identity printed by this exact worker before extracting its request:

```sh
PROFILE_JSON="$(./bin/alfred-simulator \
  --backend alfred-default-v1 \
  --scheduler-config examples/default-scheduler.yaml \
  --print-profile)"

jq --argjson profile "$PROFILE_JSON" \
  '.request.profile = $profile.identity | .request' \
  ../scheduling/testdata/simulator-v1.json |
  ./bin/alfred-simulator \
    --backend alfred-default-v1 \
    --scheduler-config examples/default-scheduler.yaml
```

`--print-profile` does not read stdin. There is intentionally no kubeconfig,
API-server, or service-account option.

## Input boundary

Requests contain simulation-only relocation copies of checked, observed Pods.
These are complete scheduling inputs, not bare templates or a claim about the
exact future Pods a workload controller will create. Alfred's input builder
preserves constraints, consistently remaps supported per-instance/gang
identities, and rejects ambiguous models. It does not call a workload renderer,
request an admission preview, or change the existing migration API.

Requests must also carry the full dependency closure needed for the prediction:

- every Node and Namespace referenced by the request;
- every bound Pod whose occupancy or topology can affect placement;
- every source Pod both in `sourcePods` and as an exactly matching snapshot
  object;
- all relevant selector and scheduling objects: Services,
  ReplicationControllers, `apps/v1` ReplicaSets and StatefulSets needed by the
  default PodTopologySpread selector logic, plus a distinct, complete PodGroup
  for an OME gang; and
- the snapshot identity/time, exact worker profile identity, and explicit
  source-node exclusions.

Source Pods and their nodes remain occupied throughout simulation. Replacement
Pods bind only inside fresh private clients. The worker never mutates a source,
preempts or evicts another Pod, calls a live Kubernetes API, or falls back to a
network transport.

The initial supported scope covers the upstream in-tree CPU, memory, extended
resource (including GPU), host-port, node-selector, toleration, required
inter-Pod-affinity, and topology-spread constraints. The OME profile also runs
the real OMEGangPack queue, filter, score, Reserve, Permit, pre-bind, and
post-bind lifecycle for complete gangs.

PVC/CSI or provisioning-dependent placement, dynamic resource claims,
extenders, unregistered plugins, incomplete or reused gangs, preemption, and an
incomplete snapshot dependency closure are unsupported. These cases fail
closed; the worker does not disable plugins or invent missing objects to
manufacture feasibility.

Even a `Feasible` result is only a bounded prediction from the supplied
snapshot. It creates no live scheduler reservation, and cluster state may change
immediately after the result. Workload configuration and admission behavior may
also change before real replacements are created. Unsupported or drifted input
models must not be treated as feasible. Production dispatch still requires a
fresh execution-time source/profile checks, Arbiter
revalidation, and the separate guarded migration dispatcher; no live execution
is provided here.

### Building predictive inputs

The root-module `scheduling/input.Capture` API reads full Nodes, nonterminal
Pods (including pending and terminating Pods), Namespaces, Services, RCs, RSs,
STSs, PodGroups, and public InferenceService/InferenceReplica objects. Use a
lossless API reader with a context deadline; Alfred's normal transformed Pod
cache drops fields required by scheduling and cannot be used here. The reader
needs list permissions for every listed resource, including the PodGroup CRD.
Any failed or partial list makes the capture unavailable.

Captures retain object and list resource versions and a content digest.
Freshness is measured from the first read, not the end of collection. Lists
across resource kinds are not an atomic Kubernetes snapshot.

`scheduling/input.BuildRequest` validates the source and its complete observed
cohort, constructs isolated relocation identities, and selects the configured
profile from those Pods. Original Pods remain in the snapshot: replacement
capacity must be available before the migration owner drains the source. The
builder declines unsupported inputs rather than copying workload-controller
rendering logic.

Excluded source nodes are marked unschedulable **only in the worker's private
Node copies**, before gang domain planning. The final exclusion filter remains
in place. This prevents spare capacity on an excluded source from attracting a
gang whose worker affinity then waits for its rejected leader. Source Pods and
their resource occupancy remain present; neither input objects nor live nodes
are modified.

The dedicated simulator CI runs real-binary roundtrips from these input APIs
and through the production registry and recommendation loop, for single Pods
and complete OME gangs, including insufficient/partial replacement capacity.
Run it locally after building the worker:

```sh
# From the repository root.
ALFRED_SIMULATOR_BINARY="$PWD/pkg/alfred/simulator/bin/alfred-simulator" \
  go test ./pkg/alfred/scheduling/input ./pkg/alfred/engine -run 'TestWorkerIntegration|TestPredictionWorkerIntegration' -v
```
