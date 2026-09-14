# Alfred scheduler simulator

`alfred-simulator` is an offline, one-request scheduler worker for Alfred
placement predictions. It runs the Kubernetes scheduler compiled at **v1.35.4**
against private clients populated only from the JSON request. It does not read a
kubeconfig, use in-cluster credentials, contact an API server, or change a live
cluster.

This worker is not connected to production Alfred. The current controller has
no authoritative replacement-Pod renderer/admission input, worker registry, or
worker invocation. Supplying a profile in Alfred configuration therefore does
not enable migration execution.

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

Requests must contain complete, externally rendered and admitted replacement
Pods—not templates, IR fragments, or copies synthesized from source Pods. They
must also carry the full dependency closure needed for the prediction:

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
immediately after the result. Production dispatch still requires authoritative
replacement inputs, a configured worker integration, Arbiter revalidation, and
the separate guarded migration dispatcher; none is provided here.
