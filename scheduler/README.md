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
├── pkg/placement/                # per-gang domain assignments
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
the authoritative proof that the choose → assign → enforce → gate → bind loop
actually schedules a gang, covering the informer/`New`/snapshot glue the unit
tests fake. The scheduler-plugins `PodGroup` CRD is sourced from the go.mod
module at test time (not vendored — go.mod is the source of truth, matching OME's
dep-crds policy).

```sh
export KUBEBUILDER_ASSETS="$(../bin/setup-envtest use "$(cat ../.envtest-k8s-version)" -p path)"
go test ./test/integration/... -v
```

## Status

The placement loop is live end to end: `PreFilter` best-fits and
assigns the gang's domain, `Filter` enforces it, `Permit` gates the gang until all
members are present, and `Unreserve` unwinds a half-formed gang. `Reserve` claims
the gang's whole-node capacity in its assigned domain so two gangs can't race into
one and over-commit it. Gang membership + size come from the standard
scheduler-plugins `PodGroup`; the domain label is declared per-workload via a
configured PodGroup annotation key; and node free-ness is inferred from the gang
pod's own resource requests. The OME chart maps that generic setting to
`ome.io/topology-key`; alternatively, one global topology key can cover every
gang without any producer annotation. The plugin binary bakes in neither a
producer metadata prefix nor a fabric value. A rejected gang member re-attempts promptly via
precise requeue hints for its siblings, PodGroup, and relevant capacity changes
rather than the scheduler's slow periodic flush; gang-internal transitions that
no cluster event represents (a member set reaching `minMember`, a member
reaching the gate, a forming gang's domain reservation draining or unwinding)
are turned into explicit activations. `NodeResourcesFit` supplies
node-level `MostAllocated` packing. Placement
decisions, gate outcomes, unwinds, and the count of assigned groups are exported as
`ome_scheduler_*` metrics on the
scheduler's `/metrics` endpoint. Replica spreading for fault isolation
needs no scheduler support: OME renders it as a standard leader-pod
`topologySpreadConstraint`, whose hard (`DoNotSchedule`) form the retained
PodTopologySpread Filter enforces — a best-fit assignment the constraint vetoes is
released by PostFilter, remembered, and re-planned into a compatible domain
(covered by `test/integration/spread_tsc_test.go`).

Gang preemption is opt-in through the plugin's `preemption` argument (the
chart's `scheduler.plugin.preemption`); without it the plugin never evicts. When
it is on, a gang that fits no domain for lack of room may free one: `PostFilter`
picks the domain whose most important evicted gang ranks lowest, reserves it for the
waiting gang, and deletes every pod of each lower-priority gang there (never
part of a gang), plus lower-priority pods in no gang. The choice is the pure
function in `pkg/preemption`. The reservation keeps other pods out while the
evicted pods shut down. After a scheduler restart the plugin counts pods still
shutting down as leaving, so it reserves the same domain again without new
evictions. `preemption.maxInFlight` caps how many gangs may wait at once for a
freed domain. Covered by the `TestGangPreemption*` cases in
`test/integration/gangpack_test.go`.

## Version policy

Pinned to the target Kubernetes minor version (currently 1.35 →
`k8s.io/kubernetes v1.35.4`). The plugin framework API changes between k8s
minors, so version bumps are deliberate, reviewed changes: regenerate the
`replace` block from the matching `sigs.k8s.io/scheduler-plugins` release.
Kubernetes 1.35 currently requires the published `v0.35.4-devel` tag because no
stable scheduler-plugins v0.35 tag is available; note the supply-chain caveat:
`v0.35.4-devel` is a pre-release tag — move to the stable scheduler-plugins
v0.35 tag when one is published.
