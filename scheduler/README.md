# ome-scheduler

The OME accelerator scheduler — the upstream kube-scheduler built as a library
with the OME placement plugin (`OMEGangPack`) registered next to the built-in
plugins.

## Why a separate Go module

Building the Kubernetes scheduler as a library requires importing
`k8s.io/kubernetes`, which needs the staging `replace` block in `go.mod` (pinning
every `k8s.io/*` to the target Kubernetes version — currently 1.35). That block,
and the large transitive graph it pulls in, would poison the root
`sigs.k8s.io/ome` module (which pins k8s via controller-runtime on a
different cadence). So the scheduler is its **own module**, pinned and bumped
deliberately — the same structure scheduler-plugins / Volcano / KAI use.

## Layout

```
scheduler/
├── go.mod                        # own module; k8s 1.35 staging replace block
├── cmd/ome-scheduler/main.go     # entry point: kube-scheduler + our plugin
├── pkg/topology/                 # pure best-fit domain accounting (no k8s deps)
├── pkg/placement/                # per-gang domain pins
├── pkg/plugins/gangpack/         # OMEGangPack placement plugin
└── test/integration/             # envtest + real in-process scheduler
```

## Build & test

```sh
cd scheduler
go build ./...                                     # compile-check every package (no binary emitted)
go build -o bin/ome-scheduler ./cmd/ome-scheduler  # produce the scheduler binary
go test ./...                                       # unit tests
```

Note: `go build ./...` only *checks* that everything compiles — for a multi-package
build Go discards the executable. Use the explicit `-o` form (or `go install
./cmd/ome-scheduler`) to get the binary.

### Integration tests

`test/integration/` runs the plugin inside a **real in-process kube-scheduler**
against an [envtest](https://book.kubebuilder.io/reference/envtest) API server —
the authoritative proof that the choose → pin → enforce → gate → bind loop
actually schedules a gang, covering the informer/`New`/snapshot glue the unit
tests fake. The scheduler-plugins `PodGroup` CRD is sourced from the go.mod
module at test time (not vendored — go.mod is the source of truth, matching OME's
dep-crds policy).

```sh
export KUBEBUILDER_ASSETS="$(../bin/setup-envtest use "$(cat ../.envtest-k8s-version)" -p path)"
go test ./test/integration/... -v
```

## Status

The placement loop is live end to end: `PreFilter` proves which domains can hold
the complete gang and normally best-fit-pins one. When a gang has a hard topology
spread constraint, it instead exposes every feasible domain so the framework's
regular filters can narrow the candidates; `Reserve` then pins the selected
node's domain. `Permit` gates the gang until all members are present, and
`Unreserve` unwinds a half-formed gang. `Reserve` claims the gang's remaining
whole-node capacity so two gangs can't race into one and over-commit it. Gang
membership + size come from the standard
scheduler-plugins `PodGroup`; the domain label is declared per-workload via a
configured PodGroup annotation key; and node free-ness is inferred from the gang
pod's own resource requests. The OME chart maps that generic setting to
`ome.io/topology-key`; alternatively, one global topology key can cover every
gang without any producer annotation. The plugin binary bakes in neither a
producer metadata prefix nor a fabric value. A rejected gang member re-attempts promptly via
precise requeue hints for its siblings, PodGroup, and relevant capacity changes
rather than the scheduler's slow periodic flush; gang-internal transitions that
no cluster event represents (a member set reaching `minMember`, a member
reaching the gate) are turned into explicit activations. `NodeResourcesFit` supplies
node-level `MostAllocated` packing. Placement
decisions, gate outcomes, unwinds, and the count of pinned groups are exported as
`ome_scheduler_*` metrics on the
scheduler's `/metrics` endpoint. Required replica spreading composes with the
built-in PodTopologySpread filter; topology-aware preemption is still ahead.

## Version policy

Pinned to the target Kubernetes minor version (currently 1.35 →
`k8s.io/kubernetes v1.35.4`). The plugin framework API changes between k8s
minors, so version bumps are deliberate, reviewed changes: regenerate the
`replace` block from the matching `sigs.k8s.io/scheduler-plugins` release.
Kubernetes 1.35 currently requires the published `v0.35.4-devel` tag because no
stable scheduler-plugins v0.35 tag is available; note the supply-chain caveat:
`v0.35.4-devel` is a pre-release tag — move to the stable scheduler-plugins
v0.35 tag when one is published.
