# Alfred scheduler simulator

`alfred-simulator` is an offline, one-request scheduler worker for Alfred
placement predictions. It runs the Kubernetes scheduler compiled at **v1.35.4**
against private clients populated only from the JSON request. It does not read a
kubeconfig, use in-cluster credentials, contact an API server, or change a live
cluster.

Alfred can opt into this worker for **recommendations only**. Its leader-only
decision loop captures full public cluster objects through the uncached API
reader, builds predictive relocation requests, and invokes an exact-profile
worker from its startup registry. Supplying a worker or profile never enables
migration execution, even with `mode: execute`; the Dispatcher is not wired.

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
