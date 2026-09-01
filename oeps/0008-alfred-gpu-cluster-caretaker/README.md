# OEP-0008: Alfred GPU Cluster Caretaker

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [The GPU fragmentation problem](#the-gpu-fragmentation-problem)
  - [Why pure eviction is insufficient](#why-pure-eviction-is-insufficient)
  - [Why one caretaker, not N controllers](#why-one-caretaker-not-n-controllers)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [What Alfred is](#what-alfred-is)
  - [Implementation status and compatibility baseline](#implementation-status-and-compatibility-baseline)
  - [Architectural posture: observer, recommender, narrow actuator](#architectural-posture-observer-recommender-narrow-actuator)
  - [The engine: snapshot → policies → arbiter → dispatcher + reporter](#the-engine-snapshot--policies--arbiter--dispatcher--reporter)
  - [Core concepts](#core-concepts)
  - [User stories](#user-stories)
    - [Story 1: Operator observes fragmentation, then opts into action](#story-1-operator-observes-fragmentation-then-opts-into-action)
    - [Story 2: Alfred unblocks a pending large workload](#story-2-alfred-unblocks-a-pending-large-workload)
    - [Story 3: A node goes GpuUnhealthy — Alfred evacuates and signals for repair](#story-3-a-node-goes-gpuunhealthy--alfred-evacuates-and-signals-for-repair)
    - [Story 4: Maintenance-window honor](#story-4-maintenance-window-honor)
    - [Story 5: Opt-out for a critical workload](#story-5-opt-out-for-a-critical-workload)
    - [Story 6: LWS legacy workload — recommendation only](#story-6-lws-legacy-workload--recommendation-only)
    - [Story 7: Coexistence with cluster-autoscaler](#story-7-coexistence-with-cluster-autoscaler)
  - [Risks and mitigations](#risks-and-mitigations)
- [Design details](#design-details)
  - [Policy #1: Defragmentation](#policy-1-defragmentation)
    - [Observation layer](#observation-layer)
    - [Fragmentation scoring](#fragmentation-scoring)
    - [Candidate selection](#candidate-selection)
    - [Placement-hint computation](#placement-hint-computation)
    - [Execution](#execution)
  - [Policy #2: Node-health evacuation](#policy-2-node-health-evacuation)
    - [Goal and context](#goal-and-context)
    - [What must be true](#what-must-be-true)
    - [Approach (how)](#approach-how)
    - [The narrow-contracts property (no new RBAC, no cloud credentials)](#the-narrow-contracts-property-no-new-rbac-no-cloud-credentials)
    - [Anticipated objection](#anticipated-objection)
  - [Policy #3: Descheduling (future)](#policy-3-descheduling-future)
  - [The arbiter (arbitration-lite)](#the-arbiter-arbitration-lite)
    - [Why an arbiter at all](#why-an-arbiter-at-all)
    - [The four rules](#the-four-rules)
    - [Why lite, and what a heavier version would add](#why-lite-and-what-a-heavier-version-would-add)
  - [Coexisting with the autoscaler (OEP-0013)](#coexisting-with-the-autoscaler-oep-0013)
  - [Policy and configuration model](#policy-and-configuration-model)
    - [Opt-in / opt-out semantics](#opt-in--opt-out-semantics)
  - [Safety bounds](#safety-bounds)
  - [Concurrent-operation awareness](#concurrent-operation-awareness)
  - [Human intervention and incident posture](#human-intervention-and-incident-posture)
  - [Spot and preemptible nodes](#spot-and-preemptible-nodes)
  - [Multi-tenancy](#multi-tenancy)
  - [Model-download coordination](#model-download-coordination)
  - [Degraded mode](#degraded-mode)
  - [Deployment model](#deployment-model)
  - [Leader election](#leader-election)
  - [RBAC](#rbac)
  - [Observability](#observability)
- [Test plan](#test-plan)
  - [Unit tests](#unit-tests)
  - [Integration tests](#integration-tests)
  - [Chaos / robustness](#chaos--robustness)
  - [Simulation](#simulation)
- [Graduation criteria](#graduation-criteria)
  - [Alpha](#alpha)
  - [Beta](#beta)
  - [GA](#ga)
- [Implementation history](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Alternative 1: Alfred as a scheduler plugin](#alternative-1-alfred-as-a-scheduler-plugin)
  - [Alternative 2: Embed Alfred in the OME manager](#alternative-2-embed-alfred-in-the-ome-manager)
  - [Alternative 3: Alfred as a CRD-driven system](#alternative-3-alfred-as-a-crd-driven-system)
  - [Alternative 4: Don't build Alfred; manual operator intervention](#alternative-4-dont-build-alfred-manual-operator-intervention)
  - [Alternative 5: Extend cluster-autoscaler](#alternative-5-extend-cluster-autoscaler)
  - [Alternative 6: Keep Alfred a single-purpose defragmenter; put node-health and descheduling in separate controllers](#alternative-6-keep-alfred-a-single-purpose-defragmenter-put-node-health-and-descheduling-in-separate-controllers)
- [Open questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** Alfred is designed as the GPU **Cluster Caretaker** — a cluster-level control loop that watches the *physical* GPU layer (nodes, accelerators, workload placement) and, when it drifts into a bad state, arbitrates a safe corrective action and actuates it through narrow write contracts. The current implementation stops after arbitration and reporting. Alfred does not schedule pods, scale replicas, or touch node lifecycle.

The target caretaker is a single observe → arbitrate → actuate engine, not a grab-bag of one-off scripts. One shared, read-only **ClusterSnapshot** (the physical GPU state for a reconcile pass) is fed to a set of **Policies**; each Policy is a pure function `Evaluate(snapshot) → []Candidate`, where a *Candidate* is a proposed action plus its justification. An **Arbiter** selects and sequences across all Policies' candidates under global safety bounds (rate limits, cooldowns, concurrency caps), a **Dispatcher** actuates the winners through one narrow contract — the OEP-0007 migration-request annotation — and the controller that owns each workload executes the request. OMENative is the only implemented consumer of that contract today. `RawDeployment` and legacy LWS candidates remain advisory-only until their lifecycle owner implements and validates an equivalent consumer. A **Reporter** is the only component that emits observability output (K8s Events, decision metrics, the optional recommendations `ConfigMap`), so a Policy holds no client and writes nothing at all.

Three policies, staged: **Policy #1 — Defragmentation** is implemented as observation, scoring, arbitration, and reporting; execution is not yet wired. **Policy #2 — Node-Health Evacuation** is the next policy: on a bad node condition, evacuate eligible OME workloads off the node *and emit a remediation signal* — it does **not** cordon, drain, terminate, or reboot the node. **Policy #3 — Descheduling** (rebalance against drifted constraints) is future, and named here only to show the engine generalizes. Alfred introduces **no new CRDs**: configuration lives in a `ConfigMap`, per-workload gating in annotations, and output in Prometheus metrics, K8s Events, and an optional recommendations `ConfigMap`.

## Motivation

### The GPU fragmentation problem

In GPU-dense clusters running LLM inference, the default K8s scheduler's spread behavior produces a predictable failure mode. Consider 10 nodes × 8 H100s = 80 GPUs. Deploy 10 × 1-GPU workloads spread one-per-node: each node has 7 free GPUs, 70 free in total. Now deploy a workload needing 8 GPUs on one node — the scheduler cannot place it despite 70 GPUs being "free," because no single node has 8 *contiguous* free GPUs. The cluster is not out of capacity; it is fragmented.

With bin-packing, those 10 small workloads occupy 1–2 nodes (e.g., Node1 full at 8 workloads, Node2 holds 2), leaving 8 empty nodes — each of which can accept the 8-GPU workload. So fragmentation splits into two sub-problems:

1. **Initial placement (bin-packing).** At deployment time. Solved at the scheduler level: deploy Volcano, Yunikorn, or scheduler-plugins with `NodeResourcesFit: MostAllocated`. OME supports this today via `PodSpec.SchedulerName`. **Not Alfred's concern.**
2. **Runtime defragmentation.** As workloads come and go, placement drifts from optimal. Scale-downs, deletions, node additions, failed-then-rescheduled pods all leave fragmentation behind. Even with a bin-packing scheduler at placement time, cluster state degrades over its lifetime. **This is Alfred's problem.**

The analog is cluster-autoscaler (CA): CA doesn't handle initial resource requests; it handles dynamic node-utilization state *after* workloads evolve. Alfred is to GPU fragmentation what CA is to node utilization — a control loop that corrects the steady-state drift the scheduler does not revisit.

### Why pure eviction is insufficient

A naive caretaker would use the K8s Eviction API universally, trusting the scheduler to re-place evicted pods more densely. That is tolerable only when the component has replicas to cover the gap: capacity dips by one pod, briefly. For a single-replica workload, eviction is an outage — nothing serves until the replacement pod is Ready somewhere else.

For **multi-pod Instances** (OMENative's multi-node engine/decoder grouping — see [OEP-0007](../0007-omenative-workload-strategy/README.md)) and legacy **LWS groups**, eviction has sharp edges. An *atomic multi-pod group* is a set of pods that must start, stop, and migrate together; evicting one without the others leaves a half-broken workload — which is exactly why blind eviction is unsafe here.

- **LWS with `RecreateGroupOnPodRestart`** (OME's current default for LWS) tears down the *entire* group on any pod deletion. No surge, no rollback: if the scheduler can't place all `N` replacement pods simultaneously, the service stays down.
- **OMENative Instances** have atomic group semantics too — evicting one pod triggers a per-Instance restart, cleaner than LWS's whole-group teardown but still not surge-protected. So pure eviction is unsafe for OMENative multi-pod Instances as well.

The answer is **not** "smarter eviction." The answer is to narrow the action surface to a single verb: every executable migration is requested through the published annotation contract and executed by the controller that owns the workload's lifecycle. OMENative already consumes that contract for its Instances. `RawDeployment` has no equivalent consumer yet, so Raw candidates are advisory-only in this OEP's current implementation phase. Alfred itself never evicts or deletes a pod. This narrow-actuation rule is load-bearing for every policy, not just defragmentation: it is the property that lets the Arbiter reason about safety uniformly (next section).

### Why one caretaker, not N controllers

Tl;dr: defragmentation, node-health evacuation, and descheduling are *different policies over the same physical state*, and the hard parts they share — building a consistent snapshot, enforcing safety bounds, and actuating without corrupting workloads — should be written once, not three times.

The reader's reasonable objection: "These are three unrelated jobs. Why not three small controllers?" Because the genuinely difficult, genuinely dangerous machinery is identical across all three:

1. **One ClusterSnapshot.** Every policy reasons about the same physical GPU layer — node GPU inventory, per-pod GPU consumption, accelerator health, pending-pod pressure. Building that view *consistently* (so two policies don't act on disagreeing state in the same pass) is the same work whether the trigger is fragmentation or a `GpuUnhealthy` condition. Three controllers means three snapshots that can disagree, and three reconcile loops racing each other on the same pods.
2. **One safety layer.** Rate limits, per-workload cooldowns, concurrency caps, and "reject high-uncertainty actions" (*primum non nocere*) are not policy-specific — they bound *actuation*, regardless of which policy proposed it. Centralizing them in the Arbiter means a defrag migration and a health evacuation can't both move the same workload, or together exceed the cluster's churn budget. N independent controllers cannot enforce a shared budget without inventing a coordination protocol between them — i.e., reinventing the Arbiter, badly.
3. **One narrow actuation contract.** Both moving a fragmented workload and evacuating a workload off a bad node reduce to the *same* primitive: write the migration-request annotation and let the controller that owns the workload's lifecycle move it safely. The planned Dispatcher centralizes "how to request a move without breaking the workload." A second actuator would either duplicate that contract or, worse, take a wider, more dangerous surface.

**Policy #1 (Defragmentation)** now implements the observation and recommendation part of the original vision. **Policy #2 (Node-Health Evacuation)** remains target design. On a bad node condition, Policy #2 will evacuate eligible OME workloads through the same narrow Dispatcher contract and emit a remediation signal (a Prometheus metric, plus K8s Events at detection and at drain completion) for whatever system actually owns hardware remediation. It explicitly does **not** cordon, drain, terminate, or reboot the node, and it does **not** call any cloud Compute API. That is the line between the archived design and this one: we keep "react to a `GpuUnhealthy` node," we drop "patch/repair the hardware." (Refined non-goal below.)

Generalizing to a caretaker engine has a cost, and it is worth naming: a single process now carries three policies' blast radius, and a snapshot bug or a Dispatcher bug is a bug for *all* policies at once. The mitigation is the same property that makes the engine tractable — every policy actuates only through the narrow contract and only under the Arbiter's bounds — so a misbehaving policy is bounded to "proposes a bad Candidate," which the Arbiter can reject, not "directly does damage." This is a mitigation, not a full fix: a wrong *snapshot* fools the Arbiter too. TBD: the snapshot-consistency guarantees and how a policy declares a Candidate's confidence are Design Details, not settled here.

### Goals

1. **Run one observe → arbitrate → actuate engine** over the physical GPU layer: a shared read-only ClusterSnapshot, a set of Policies (`Evaluate(snapshot) → []Candidate`), an Arbiter that selects and sequences under global safety bounds, a Dispatcher that actuates through the existing narrow contracts, and a Reporter that is the sole emitter of observability output (Events, decision metrics, the optional recommendations ConfigMap). The engine is the deliverable; policies are pluggable on top of it.
2. **Ship Policy #1 — Defragmentation.** Continuously observe GPU utilization, workload placement, and pending-pod pressure; compute a fragmentation metric; produce candidates identifying which workloads, if relocated, would reduce fragmentation most; and execute only those candidates for which a lifecycle owner exposes the migration-request contract.
3. **Ship Policy #2 — Node-Health Evacuation.** On a bad node condition (consumed from existing signals — node conditions — not newly detected by Alfred), produce candidates that evacuate eligible OME workloads off the affected node through the narrow contract, **and** emit a remediation signal (metric + two Events: `NodeRepairNeeded` at detection, `NodeDrainedForRepair` at drain completion) for the system that owns remediation. Evacuate and signal; never cordon, drain, terminate, or reboot.
4. **Reserve Policy #3 — Descheduling** as a future policy on the same engine (rebalancing workloads whose placement has drifted from current constraints). Named here only to validate that the engine generalizes; **no** behavior is promised in this OEP.
5. **Actuate only through one narrow contract.** The migration-request annotation (the published OEP-0007 contract) is the single verb for every executable path. OMENative consumes it for OMENative Instances. A deployment mode without a consumer is advisory-only; adding a `RawDeployment` consumer is separate implementation work and must preserve the same validation, idempotency, and status contract. Alfred itself never evicts or deletes a pod. Never modify OME-owned reconciliation state by any other path.
6. **Operate safely by default.** Conservative rate limits, per-workload cooldowns, concurrency caps, and explicit rejection of high-uncertainty actions, enforced centrally in the Arbiter across all policies. First principle: *primum non nocere.*
7. **Introduce no new CRDs.** Configuration via `ConfigMap`; per-workload gating via annotations on `InferenceService`; output via Prometheus metrics, K8s Events, and an optional recommendations `ConfigMap`.
8. **Be useful without OMENative** as an observer and recommender for `RawDeployment` workloads. Automatic Raw migration is not an Alpha dependency; it becomes executable only after the InferenceService controller implements the same migration-request consumer contract.
9. **Scale to large clusters** (tested design target: 1000 nodes, 10k pods, 100+ InferenceServices).

### Non-Goals

1. **Not a scheduler, and not in the scheduling hot path.** Alfred does not make initial pod-placement decisions and is not a K8s scheduler extender or plugin. The K8s scheduler (default or custom) places pods; Alfred is a separate, out-of-band controller. *Rationale:* placement is already owned, and competing with the scheduler in-band is how you get oscillation.
2. **No node lifecycle management — Alfred does not cordon, drain, terminate, or reboot nodes.** This stays **true** even with Policy #2: node-health *evacuation* moves OME *workloads* off a node through the narrow Dispatcher contract and emits a remediation signal; it never touches the *node* object's lifecycle. Cordon/drain/terminate/reboot belong to the operator or cluster autoscaler. *Rationale:* node lifecycle is destructive and cluster-wide; keeping Alfred to workload-level actuation is what keeps its blast radius bounded.
3. **No replica scaling — that is [OEP-0013](../0013-autoscaling/README.md)'s domain.** Alfred relocates existing workloads; it never changes how many replicas a workload has. *Rationale:* scaling and defragmentation are separate control loops with separate triggers; merging them couples two independent decisions and double-counts churn budget.
4. **No new CRDs.** Hard constraint. Configuration is a `ConfigMap`, gating is annotations, output is metrics/Events/ConfigMap. *Rationale:* a caretaker should be deployable into an existing OME cluster without an API surface change or a migration.
5. **Node-health *evacuation* is in scope (Policy #2); hardware *remediation* stays deferred.** This **refines** the archived Alfred's blanket "no auto-repair." We keep reacting to a bad node condition by evacuating workloads and signaling; we do **not** patch, repair, soft-reset, image-rotate, or call any cloud Compute API. Remediation — the part that actually fixes or recycles hardware — remains out of scope and is left to the operator/cloud, with an optional remediation plugin a possible *future* extension (not promised here). *Rationale:* evacuation is reversible and lives entirely within Alfred's existing narrow contract; remediation is irreversible, cloud-specific, and a different trust boundary.
6. **Not solving GPU health detection.** Alfred *consumes* existing signals (node conditions) to trigger Policy #2; it adds no new telemetry and runs no health probes of its own. *Rationale:* detection is a separate concern with its own ownership; conflating it would expand scope past a caretaker.
7. **Not an optimization engine in the mathematical sense.** Policies use heuristic candidate selection under safety bounds. "Good enough to reduce fragmentation" is the standard; global optimality is **not** promised. *Rationale:* optimal bin-packing is NP-hard and the cluster state moves under you anyway; a safe heuristic that converges beats an optimum that's stale on arrival.
8. **Not managing workloads beyond InferenceService.** Non-OME GPU workloads are *observed* (they count toward the fragmentation picture in the ClusterSnapshot) but **never** migrated or evacuated. *Rationale:* Alfred only has a safe actuation contract for OME-owned workloads; acting on anything else would exceed that contract.
9. **Not causing preemption.** Users set `priorityClass` on `InferenceService` and the scheduler enforces it; Alfred does not preempt. *Rationale:* preemption is the scheduler's decision, and an out-of-band preemptor would race it.
10. **Not integrated with DRA in v1.** Forward-looking; revisit when Dynamic Resource Allocation matures. *Rationale:* DRA changes how accelerators are modeled, which would reshape the ClusterSnapshot — premature to bind to it now. (TBD: revisit trigger.)

## Proposal

Tl;dr: Alfred is a GPU **Cluster Caretaker** — a leader-elected controller that
runs one loop, observe → arbitrate → actuate, on top of a shared read-only
**ClusterSnapshot**. Independent **Policies** each propose work against that
snapshot; a single **Arbiter** selects and sequences across them; a
**Dispatcher** actuates only through the narrow write contracts OEP-0008 already
owns; a single **Reporter** emits every Event, decision metric, and
recommendation. The caretaker never orchestrates pod lifecycle itself, so its blast radius
is bounded by that surface — and adding the new node-health policy needs **no**
new node-write RBAC and **no** cloud credentials.

### What Alfred is

Alfred is a Kubernetes controller deployed as a leader-elected `Deployment`
(typically 3 replicas, one active) in OME's namespace or a dedicated namespace.
"Caretaker" is the precise word, not branding: Alfred *tends* the GPU cluster —
it watches for conditions that degrade GPU utilization or workload health, and it
nudges the cluster back toward a better state without ever owning the workloads
it nudges. It does not schedule, it does not provision, it does not repair
hardware. It observes, decides, and hands work to whoever already owns the
execution.

Alfred runs two loops:

- **Observation loop** (default 30s): refresh the `ClusterSnapshot` from
  informers, compute fragmentation and health signals, update Prometheus gauges.
  This loop only reads and only emits metrics — it never actuates.
- **Decision loop** (default 5m): hand the latest snapshot to every Policy,
  collect their Candidates, run the Arbiter to pick and order a set of
  Recommendations, publish every outcome through the Reporter (Events, decision
  metrics, the optional recommendations ConfigMap), then let the Dispatcher
  actuate the executable ones (subject to mode, cooldowns, and rate limits).

The split matters for a concrete failure mode: if the decision loop is wedged,
the observation loop keeps the snapshot gauges flowing, so an operator still
*sees* fragmentation and health degrade even when Alfred is not deciding at all.
(In `mode: recommend-only` the decision loop still runs — Policies, Arbiter, and
Reporter keep publishing Recommendations; only the Dispatcher is off.)
Observability is not coupled to actuation.

**Supplemental early pass, not interruption.** The decision loop is
time-driven, but a node-condition change may request an additional immediate
pass. If no pass is running, the early pass starts immediately; if a pass is
running, it starts as soon as that pass completes. A pass is never interrupted,
resumed, or run concurrently with another, and the regular periodic deadline is
left unchanged.
*Rationale:* without this, a `GpuUnhealthy` condition a watch delivers in
milliseconds can sit unnoticed for up to a full `decisionLoopInterval` while
the loop sleeps; with it, health-reaction latency drops from minutes to
seconds while defragmentation stays lazily periodic. (`earlyTickOn:
[NodeConditionChange]`; an empty list disables the supplemental pass.)

Alfred reads:
- Nodes (GPU capacity, allocation, conditions including `GpuUnhealthy`).
- Pods (labeled by OME conventions).
- InferenceServices.
- InferenceReplicas (OMENative Instance lifecycle and migration status).
- OMENative per-owner migration-audit ConfigMaps (durable UUID history).
- The OMENative executor capability Lease (read-only liveness/capability proof).
- BaseModels / ClusterBaseModels.
- PersistentVolumeClaims / PersistentVolumes (PVC-backed model topology).
- Alfred's own ConfigMap for policy configuration.

Alfred writes — and this list is exhaustive (see [the engine
table](#the-engine-snapshot--policies--arbiter--dispatcher--reporter)):
- Prometheus metrics (continuous).
- K8s Events on InferenceServices and Nodes (recommendations, migrations,
  evacuation signals, skip reasons).
- Optional `alfred-recommendations` ConfigMap in its own namespace.
- Migration-request annotations on InferenceServices (only in execute mode; the
  single migration verb, and only for a deployment mode with a confirmed
  consumer).

Everything else is read-only.

### Implementation status and compatibility baseline

This OEP is the target design, not a claim that every stage is implemented.
At the 2026-08-31 baseline, the source tree has the following status:

| Area | Status | Current boundary |
|------|--------|------------------|
| Observation and configuration | Implemented | Every replica builds snapshots and publishes gauges; configuration hot-reloads with last-known-good fallback. |
| Defragmentation Policy #1 | Partially implemented | Scoring, candidate generation, arbitration, and reporting run; placement feasibility is not yet scheduler-complete. |
| Arbiter and Reporter | Partially implemented | Core admission gates and outputs exist; positive-benefit/regression admission and dispatch/outcome-fed ledger state are not connected. |
| Node-Health Policy #2 | Not implemented | Node conditions only exclude unhealthy nodes as defrag targets and enqueue a coalesced early decision request. That request currently reads the latest cached snapshot without first refreshing it; no evacuation candidates or remediation signals are produced. |
| Dispatcher | Not implemented | Alfred does not patch migration-request annotations, and its current ClusterRole grants no InferenceService write. Without a Dispatcher, admitted actions in both `recommend-only` and `execute` mode report `OutcomeWithheld`, with reasons `RecommendOnly` and `DispatcherUnavailable` respectively. |
| OMENative state | Partially implemented | Checked `InferenceReplica.Status` normalization and the live Pod join by stable Instance index and incarnation are implemented. In-memory migration reconstruction reads current status; durable audit-ledger ingestion remains deferred. |
| OMENative executor readiness | Partially implemented | The manager publishes the capability Lease and Alfred reads it directly through its uncached API reader. A just-in-time direct re-read immediately before dispatch remains deferred with the Dispatcher. |
| RawDeployment execution | Deferred | Raw candidates are advisory (`Executable=false`). Neither an Alfred Dispatcher nor a Raw request consumer exists; execution remains deferred until both are implemented and tested. |

The remaining sections describe the target architecture unless they explicitly
say "current implementation." The compatibility baseline for new work is the
current OMENative API, not pod-name reconstruction or legacy aggregate migration
history on `InferenceService`.

### Architectural posture: observer, recommender, narrow actuator

Tl;dr: Alfred observes and recommends; it does not move pods itself. It hands
execution to whoever already owns the workload's lifecycle — because pod eviction
is destructive, and getting it wrong on a multi-pod group breaks the workload.
Alfred should never be the thing that does the breaking.

**Definition.** An *atomic multi-pod group* is a set of pods that must start,
stop, and migrate together (an OMENative Instance, or a legacy LWS group);
evicting one without the others leaves a half-broken workload. This is precisely
why eviction is unsafe for these groups and why their migration must be delegated.

**The posture, stated as three roles.**

1. **Observer.** Alfred reads cluster state into the snapshot. It mutates
   nothing while observing.
2. **Recommender.** Alfred reasons over the snapshot and emits Recommendations —
   metrics, events, and an optional ConfigMap. A recommendation is a *statement*,
   not an action; in `recommend-only` mode the caretaker never gets past this
   role.
3. **Narrow actuator.** When configured to act, Alfred executes a Recommendation
   only by *delegating*:
   - OMENative-managed workloads → **OMENative**, the controller that owns
     their lifecycle, via the OEP-0007 migration-request annotation.
   - `RawDeployment` and legacy LWS components → advisory-only until their
     lifecycle owner implements the same request/acknowledgement contract.

Why not let Alfred orchestrate lifecycle directly? Eviction races with other
cluster activity, and for an atomic group it is *unsafe*: evicting one pod out
from under the group corrupts the workload, and a plain eviction is not
surge-protected — there is no replacement pod waiting. Safe migration of a group
requires the controller that owns it. So that is where the work belongs.

**Failure modes are bounded — by the narrow surface, by design.**

- A bad *recommendation* (e.g. the scorer proposes a counterproductive move) is
  rejected or simply not executed by OMENative: nothing happens to the workload.
- A Candidate for a deployment mode without a confirmed consumer is
  `Executable=false`; it can produce an operator-visible recommendation but no
  migration-request write.

Note the boundary condition explicitly: **this bound holds *because* Alfred never
orchestrates pod lifecycle directly.** The worst Alfred can do through its write
contracts is request a migration — which the owning controller (OMENative or
another explicitly implemented consumer) validates, rate-limits, and can refuse. The day Alfred starts
driving pod lifecycle itself — staging surge pods, deleting group members in
sequence — the guarantee is gone and the blast radius becomes unbounded. The
narrow surface is not an accident of the current implementation; it is the load-
bearing safety property of the whole design.

### The engine: snapshot → policies → arbiter → dispatcher + reporter

Tl;dr: One read-only snapshot feeds many independent policies; one arbiter is the
*only* component that reasons across policies; one dispatcher is the *only*
component that actuates; one reporter is the *only* component that emits
observability output. Policies never write at all.

The engine is a five-component pipeline — the Dispatcher and the Reporter sit
side by side at its end — and the separation between stages is the design, not
an implementation detail:

```
ClusterSnapshot ──▶ [ Policy₁ Defragmentation ]──┐
   (read-only)      [ Policy₂ Node-Health Evac ]──┼──▶ Arbiter ──▶ Dispatcher ──▶ actuation
                    [ Policy₃ Descheduling (fut)]──┘  (select+    (the only       contracts
                                                  │    sequence)   actuator)    (migration request)
                                                  │       │
                        advisory candidates ──────┤       │ outcomes (admitted /
                        (Executable=false)        │       │ withheld / rejected + reason)
                                                  │◀──────┘
                                                  ▼
                                                 Reporter ──▶ observability contracts
                                                 (the only      (Events, decision metrics,
                                                  observability   recommendations ConfigMap)
                                                  writer)
```

**The Policy interface.** Every policy implements exactly one method:

```go
// A Policy reads the shared snapshot and proposes work. It never writes.
type Policy interface {
    Evaluate(snapshot *ClusterSnapshot) []Candidate
}
```

The contract is deliberately tiny, and the consequence is the point:

- A policy is a **pure function of the snapshot**: given a snapshot, it returns
  Candidates and mutates nothing. That makes each policy **independently
  testable** against a synthetic snapshot fixture — no fake clients, no informer
  plumbing, no apiserver. The defragmentation scorer can be exercised on an
  adversarial cluster state in a table-driven unit test, and the node-health
  policy can be handed a snapshot with a `GpuUnhealthy` node and asserted to
  return exactly the evacuation Candidates expected.
- A policy **cannot actuate**, because the interface gives it no client and no
  write path. Even a buggy or malicious policy can, at worst, propose bad
  Candidates — which the Arbiter and Dispatcher are positioned to filter. This is
  the same safety lever as the narrow posture above, pushed one layer inward.
- A policy does **not** emit observability either. `Evaluate` returns Candidates
  and nothing else — no Event, no metric increment, no ConfigMap write; the
  Reporter (below) emits all of that on the engine's behalf, from the returned
  Candidates and the Arbiter's recorded outcomes. This keeps the purity claim
  literal — a policy holds no client of any kind — so the table-driven tests
  exercise exactly the code production runs.
- Adding a policy is additive: register a new `Evaluate` implementation. The
  Arbiter, Reporter, and Dispatcher are unchanged. **Descheduling (Policy #3)** is named
  here precisely to show the seam — it is future work, not in this OEP's scope,
  and it slots in as one more `Evaluate` with zero change to the actuation path.

**The three policies.**

1. **Defragmentation (Policy #1).** Detects GPU fragmentation (free GPUs
   scattered across nodes so that a large contiguous request cannot schedule) and
   proposes consolidating migrations. This is the original OEP-0008 workload.
2. **Node-Health Evacuation (Policy #2).** Detects a node that has gone
   `GpuUnhealthy` (node condition) and proposes evacuating eligible OMENative
   Instances off it, plus *signalling* that the node needs repair (an Event on
   the node and an Alfred-owned ConfigMap record). It **evacuates and signals —
   it does not repair.** The actual hardware repair (cordon-and-reset via a
   cloud Compute API) is left to
   whatever node-lifecycle controller the operator already runs. This is the key
   reframing of the old `ome-operator` auto-repair: the evacuation reuses the
   exact same delegated migration path as defragmentation, so Policy #2
   needs **no new node-write RBAC and no cloud credentials.**
3. **Descheduling (Policy #3, future).** Reserved seam for policy-driven eviction
   of workloads that violate placement rules over time. Out of scope here; named
   only to validate the interface.

**The Arbiter** is the *only* cross-policy reasoner. Each policy is myopic by
design — it sees the whole snapshot but only its own concern. The Arbiter takes
the union of all Candidates and resolves them globally: it deduplicates moves
that two policies both proposed, orders by priority (a `GpuUnhealthy` evacuation
should usually preempt a routine defrag consolidation), enforces cluster-wide
caps and per-node/per-workload cooldowns, and runs the regression check
(simulate the post-move snapshot, reject moves that make fragmentation worse).
Its output is an ordered list of **Recommendations**, plus a recorded reason for
every Candidate it drops, defers, or withholds — the Reporter turns those
reasons into Events and metrics. Centralizing cross-policy reasoning in one
place means thrashing prevention and rate limiting are written once, not
re-implemented per policy.

**The Dispatcher** is the *only* component that actuates against the wider
cluster — the sole writer of the migration-request annotation, the one
executable contract. It takes the Arbiter's ordered Recommendations and actuates the
executable ones through the narrow write contracts — by workload type, exactly
as the posture section describes.

**The Reporter** is the *only* component that emits observability output: every
K8s Event, every decision metric, and (if enabled) every
`alfred-recommendations` ConfigMap entry comes from this one stage. It is fed
from two directions. From the arbitrated path it receives the Arbiter's
outcomes (admitted, withheld, rejected — each with its recorded reason) and the
Dispatcher's actuation results. And **advisory Candidates** (`Executable=false`
— a Raw/LWS defrag recommendation, a node remediation signal) are routed to it
directly, bypassing arbitration entirely. The bypass is deliberate: an advisory
carries no action to admit, so sending it through the Arbiter could only hurt —
a benefit-cost admission threshold would silently drop it, or an inert
recommendation would debit the budget real actions need. Concentrating
actuation in one component and observability in another is what makes the
write-contract table below an *exhaustive* and *auditable* surface rather than
a hope: every write traces to exactly one stage, and a Policy traces to
neither.

**The narrow write contracts** (everything the Dispatcher and the Reporter —
and therefore Alfred — write to the wider cluster):

| Target | Resource | Write | Writer | Why | New RBAC for Policy #2? |
|--------|----------|-------|--------|-----|-------------------------|
| Alfred namespace | ConfigMap (`alfred-recommendations`) | `get`, `update`, `patch`, `delete` | Reporter | Alfred's pre-created output | No |
| Alfred namespace | Lease (`alfred.ome.io`, pre-created without `spec`) | `get`, `update` | engine (leader election) | Leader election | No |
| Any namespace | Events (on ISVC **and** Node) | `create`, `patch` | Reporter | Surface recommendations, migrations, evacuation + repair signals | No — `events` already granted |
| Any namespace | InferenceService annotations | `patch` (narrow to `ome.io/migration-request-v1-*`) | Dispatcher | OEP-0007 migration verb — the single executable contract for supported workload owners, defrag **and** evacuation | No — same annotation both policies |

The load-bearing property: **Policy #2 (Node-Health Evacuation) introduces no new
write contract.** It evacuates by reusing the same migration annotation the
Dispatcher writes for defragmentation — the owning controller (currently
OMENative) executes the actual moves — and it signals repair by writing an
Event on the node — a verb Alfred already has. It
does **not** cordon nodes via the node-write API, and it does **not** call any
cloud Compute API. Contrast the old `ome-operator` auto-repair, which cordoned
nodes and triggered Compute resets and therefore needed both node-write RBAC
and cloud credentials. Folding node-health into the caretaker as a policy keeps
the entire surface read-only-plus-four-narrow-writes — the same surface OEP-0008
already justified for defragmentation alone.

**One pass at a time.** A decision pass — snapshot build, policy evaluation,
one Arbiter admission run, dispatch — is non-overlapping by construction: the
loop is non-reentrant, so if a pass overruns `decisionLoopInterval`, the next
tick is delayed until it completes, never started concurrently (cadence
degrades under load; correctness does not). A coalesced early request adds one
serialized pass without resetting the periodic timer. *Rationale:* every safety
bound — the shared budget, the capacity check net of in-flight claims, the
cooldown bookkeeping — assumes admissions are totally ordered. Two concurrent
Arbiter passes are two admitting authorities racing one ledger: both read "2
of 3 in flight," both admit, the cap is silently broken — the
N-independent-controllers failure this engine exists to eliminate,
reintroduced inside one process.

**Failover re-derives; it never resumes.** On leader change, the new leader
builds a fresh snapshot and recomputes from scratch: a pass is never resumed,
and no Candidate survives the leader that computed it — Candidates are derived
cache with the lifetime of one pass, not persisted state that could go stale.
This imposes a standing requirement: **all arbitration bookkeeping must be
reconstructible from the cluster**, because leader memory is defined to be
losable. Pending mailbox writes reconstruct from migration-request annotations;
accepted and terminal OMENative work reconstructs from
`InferenceReplica.Status.Migrations`; long-term rate and cooldown history
reconstructs from the workload audit ledger. The same request UUID is
deduplicated across all three surfaces. Where durable history is incomplete,
the new leader assumes the relevant budget window is spent — degrading toward
quiescence, never toward a storm.

**The guarantee's honest boundary.** Lease-based election is advisory, not
fenced: a paused old leader may dispatch briefly after losing the Lease.
Inside that window, safety rests on the write contracts themselves —
UUID-idempotent migration requests, status-backed source/surge operation
fencing, oldest-first selection of accepted Manual records, and the owning
controller's execution-capacity gates. OMENative may queue distinct UUIDs
rather than reject them merely because another request is active; it selects
one record per pass and bounds allocated surge work. Alfred's own budget may
transiently overshoot and re-converges on the next pass from observed state.
Dispatched migrations are otherwise
unaffected by any of this: executions from earlier passes proceed concurrently
and are *observed* via the next snapshot, never awaited — decision latency
stays seconds-long regardless of how long executions run.

### Core concepts

Each term defined before use, with the consequence baked in.

- **ClusterSnapshot.** An immutable, in-memory model of cluster state at one
  instant — nodes (GPU capacity, allocation, largest-contiguous-free, health,
  cordon/scale-down/preemptible flags, occupants), workloads (broken into
  Components and Instances), model availability, and pending pods. *Consequence:*
  it is built once per loop and shared read-only across all policies, so every
  policy reasons over the **same** view — two policies can never disagree about
  what the cluster looked like, and a policy can never mutate state another policy
  is reading.
- **Migration state.** One request UUID may appear in three places as it moves
  through the system: the InferenceService annotation is the pending mailbox,
  `InferenceReplica.Status.Migrations` is authoritative after OMENative accepts
  it, and the workload audit ledger is durable history. *Consequence:* Alfred
  joins and deduplicates these surfaces by UUID; annotation disappearance is
  never interpreted as success, and the recommendations ConfigMap is never used
  as execution truth.
- **Policy.** A pluggable decision module implementing `Evaluate(snapshot) →
  []Candidate`. *Consequence:* a policy is a pure, side-effect-free function of
  the snapshot — independently unit-testable and structurally incapable of
  actuating (it has no client).
- **Candidate.** A single proposed action — e.g. "migrate Component X's Instance
  Y off Node Z." Each Candidate carries a **benefit** (expected improvement: GPUs
  freed, fragmentation reduced, or a node drained for repair) and a **cost**
  (disruption risk: pods restarted, model re-pull, in-flight requests). *Consequence:*
  benefit and cost are explicit and comparable, so the Arbiter can rank across
  policies on a common axis instead of trusting each policy's self-assessment.
  A Candidate flagged `Executable=false` is *advisory* — a finding to surface
  (a Raw/LWS defrag recommendation, a node remediation signal), not an action to
  dispatch; the engine routes it directly to the Reporter, so it never enters
  arbitration and never consumes budget.
- **Recommendation.** A Candidate that has survived the Arbiter — passed
  opt-in/eligibility, cooldown, rate-limit, capacity, and regression checks — and
  is ready to emit or dispatch. *Consequence:* a Recommendation is the only thing
  the Dispatcher will act on; an un-arbitrated Candidate never reaches an
  actuation write — the only surface an advisory Candidate can reach is the
  Reporter's observability output.
- **Arbiter.** The single cross-policy reasoner: it takes the union of all
  policies' Candidates and selects + sequences them into ordered Recommendations,
  resolving conflicts, deduplicating, prioritizing (health evac usually over
  routine defrag), and enforcing global caps and cooldowns. *Consequence:* there
  is exactly **one** place where cross-policy tradeoffs and rate limits live, so
  thrashing prevention is not re-derived per policy.
- **Dispatcher.** The single actuator: it executes Recommendations by writing
  the migration-request annotation; the controller that owns the workload
  performs the actual move. In the current compatibility baseline, only
  OMENative is such a consumer; RawDeployment and LWS remain advisory-only.
  *Consequence:* all
  actuation writes funnel through one component, making the write surface
  exhaustively auditable alongside the Reporter's observability writes.
- **Reporter.** The single observability emitter: it turns returned Candidates,
  Arbiter outcomes, and Dispatcher results into K8s Events, Prometheus decision
  metrics, and the optional `alfred-recommendations` ConfigMap. Advisory
  Candidates reach it directly, without arbitration. *Consequence:* policies and
  the Arbiter stay pure decision logic — values in, values out — and every
  operator-visible surface is produced in exactly one place, so emission can be
  tested (and deduplicated) once, not per policy.
- **Fragmentation Score.** A cluster-level number in [0, 1] summarizing fixable
  fragmentation opportunity plus eligible pending pressure. `0` means Alfred
  models no executable relocation that would help — which includes perfectly
  packed, fully utilized, and fragmented-but-immovable clusters — while values
  toward `1` mean greater fixable opportunity or pressure (mechanism in Design
  Details). *Consequence:* it is the
  scalar the Defragmentation policy thresholds on and the metric an operator
  watches; it is a *signal*, not an action, so a rising score in
  `recommend-only` mode produces visibility without disruption.
- **Workload.** The unit Alfred reasons about: typically one InferenceService,
  broken down into its **Components** and **Instances**. Alfred reasons at the
  Component/Instance level, not the bare-pod level. *Consequence:* a multi-pod
  Instance is treated as one atomic unit to be moved together, which is exactly
  why its migration is delegated to OMENative rather than evicted pod-by-pod.
- **Atomic multi-pod group.** A set of pods that must start, stop, and migrate
  together (an OMENative Instance, or a legacy LWS group). *Consequence:*
  evicting one member without the others leaves a half-broken workload — so for
  these groups, migration must go through the lifecycle owner (OMENative), never
  through Alfred-driven eviction.

### User stories

#### Story 1: Operator observes fragmentation, then opts into action

An operator deploys OME with Alfred enabled and `mode: recommend-only`. Over a
week, workloads come and go; the `alfred_cluster_fragmentation_score` gauge rises
from 0.2 to 0.6. The operator reviews the Defragmentation policy's
Recommendations in `kubectl get events` and the `alfred-recommendations`
ConfigMap, gains confidence, and switches to `mode: execute`. Alfred's Dispatcher
begins writing OEP-0007 migration-request annotations to OMENative-managed
workloads. Fragmentation drops over the next hour. The observe-before-act split
is deliberate: the operator saw the recommendations the engine *would* have
executed before granting it permission to execute them.

#### Story 2: Alfred unblocks a pending large workload

A new InferenceService for Llama4 needs 8 GPUs on a single node. No node has 8
contiguous free GPUs despite 70 GPUs free cluster-wide. The Defragmentation
policy sees the Pending pod in the snapshot and proposes Candidates whose
migration would free 8 contiguous GPUs on one node. The Arbiter ranks them by
benefit (capacity unblocked) against cost (small workloads disrupted) and emits
Recommendations; the Dispatcher migrates the small OMENative workloads via the
migration verb, consolidating Node1. Llama4 schedules on Node3 within minutes.

#### Story 3: A node goes GpuUnhealthy — Alfred evacuates and signals for repair

Node7's node condition flips to `GpuUnhealthy` (a GPU has failed). The
Node-Health Evacuation policy sees the condition in the snapshot and returns
Candidates for every OME occupant on Node7. Eligible OMENative Instances can
evacuate; RawDeployment, LWS, and ineligible OMENative findings stay advisory.
The Arbiter prioritizes executable health Candidates over routine defrag
Candidates and emits ordered Recommendations. The Dispatcher writes migration
requests for the eligible OMENative Instances; OMENative surges them away via
OEP-0007. Alfred
*signals twice*: `NodeRepairNeeded` on Node7 at detection — before any drain,
giving the repair pipeline its lead time while workloads still run — and
`NodeDrainedForRepair` only once the node is empty of every OME workload. If an
advisory-only occupant remains, Alfred withholds the completion Event and keeps
reporting the blocker. **Alfred does not cordon the node and does not call any cloud Compute API
to reset it.** Repair is delegated to whatever node-lifecycle controller the
operator already runs. The whole story uses only the existing narrow write
contracts: migration annotation, node Event. No new RBAC, no cloud
credentials, and no pod-level write by Alfred.

#### Story 4: Maintenance-window honor

An operator sets a weekday business-hours maintenance window. During the window,
high fragmentation produces Recommendations (metrics + events) but the Arbiter
withholds them from the Dispatcher — Alfred observes and recommends, but does not
act. After the window, dispatch resumes under the standard rate limit. An
`emergencyPendingAgeMinutes` override lets the Arbiter release a Recommendation
during the window if a Pending pod has waited past the threshold.

#### Story 5: Opt-out for a critical workload

A business-critical InferenceService is annotated `alfred.ome.io/movable:
"false"`. Every policy's `Evaluate` excludes it from Candidate generation
entirely (the eligibility filter reads the annotation off the snapshot). The
Reporter emits an Event on the ISVC explaining the exclusion — useful when an
operator wonders why no Recommendation was produced despite obvious
fragmentation.

#### Story 6: LWS legacy workload — recommendation only

An InferenceService using `deploymentMode: MultiNode` (LWS-backed) is highly
fragmented. A policy produces a Candidate, but the Dispatcher will **not**
execute it, because Alfred has no safe delegated path for an LWS group (LWS is
not OMENative; there is no migration verb to delegate to). The event explains:

> LWS-backed workload cannot be migrated safely by alfred. Migrate the workload
> to OMENative strategy for automatic defragmentation, or handle manually.

The operator migrates the workload to OMENative (per OEP-0007 §Strategy Migration
Procedure) or handles the defragmentation manually. This is the posture in
action: where Alfred has no safe delegate, it degrades to recommend-only rather
than reaching for an unsafe direct action.

#### Story 7: Coexistence with cluster-autoscaler

Cluster-autoscaler (CA) is enabled and scales down underutilized nodes. The
Defragmentation policy's consolidation feeds this: as Alfred packs workloads onto
fewer nodes, CA notices the emptied nodes and removes them. The policy reads each
node's `cluster-autoscaler.kubernetes.io/scale-down-disabled` annotation off the
snapshot and excludes such nodes from placement hints, so the two controllers do
not fight over the same node.

### Risks and mitigations

Each risk in failure-mode → mitigation form. "Mitigation, not full fix" is noted
where true.

**Risk: a policy proposes a counterproductive move (scorer bug).** A
Defragmentation Candidate that, if executed, would *increase* fragmentation.
*Mitigation:* the Arbiter runs a regression check — the post-move
snapshot and reject any Recommendation that worsens the Fragmentation Score.
Because policies are pure functions of the snapshot, the same scorer is exercised
in table-driven unit tests over adversarial synthetic snapshots before it ever
ships. `recommend-only` is the default for early deployments, so a bad scorer
produces visible-but-inert Recommendations, not disruption.

**Risk: thrashing.** Alfred migrates workload A, creating fragmentation
elsewhere, triggering a migration of workload B that undoes A.
*Mitigation:* the Arbiter owns this — per-workload cooldown (no migration within
30 minutes of the last; health evacuation uses a shorter floor — see Safety
bounds), per-node cooldown (10 minutes after a migration lands on a node), and
trend-based gating (act only when a signal is consistently above
threshold, not on a spike). Centralizing cooldowns in the Arbiter means a new
policy inherits anti-thrash for free; it cannot forget to implement it.

**Risk: two policies propose conflicting actions on the same workload.** The
Defragmentation policy wants to keep a workload where it packs best; the
Node-Health policy wants it evacuated off a sick node.
*Mitigation:* the Arbiter is the *only* cross-policy reasoner and resolves the
conflict by priority — health evacuation usually preempts routine defrag — and
deduplicates moves both policies proposed. No policy sees another policy's
Candidates, so conflicts cannot be resolved (or mishandled) inside a policy.

**Risk: migration storm.** Many Candidates approved at once; OMENative is
overwhelmed.
*Mitigation:* the Arbiter enforces a cluster-wide cap (default 3 in-flight, 10
per hour). The Dispatcher observes OMENative `RateLimited` rejections and backs
off. A circuit breaker trips if the failure rate exceeds 50% of the recent 10
dispatched actions — pause for 1 hour, emit a critical event.

**Risk: consecutive node failures exhaust the hourly budget.** One 8-workload
drain spends 8 of the default 10 hourly slots; a second node failing in the
same rolling hour gets the remainder immediately and the rest as the window
releases them — its drain finishes roughly at first-failure +60m, doubling its
degraded-workload exposure. Alfred cannot distinguish independent back-to-back
failures (where faster response would be safe) from the front edge of a
correlated event — rack power, cooling, a driver rollout about to take ten
more nodes — and for the correlated case the throttle is the *correct*
response: surviving capacity is precious, destinations may be about to fail
too, and a mass event needs operators, not a migration storm. A reserved
emergency budget would optimize the independent case by disarming the defense
for the correlated one, so we accept under-response instead. *Mitigation, not
full fix:* size `maxMigrationsPerHour` for the largest legitimate burst you
intend to tolerate (a full drain of the densest node plus routine defrag in
the same hour); withheld evacuations are published with `reason: RateLimited`,
so "N evacuations pending, budget-gated" is a visible, alertable state rather
than silence; and Alfred's caps bound Alfred, not the cluster —
`kubectl drain` remains available. Remediation signaling is decoupled from the
budget by splitting it into two Events: `NodeRepairNeeded` at *detection*
(lead time for the repair pipeline; the node still runs workloads) and
`NodeDrainedForRepair` at *completion* (the safe-to-act handoff). This also
resolves the latent inconsistency between the Proposal ("evacuate and emit a
remediation signal") and Story 3 ("once the node is drained, Alfred signals")
— each sentence is true of one of the two Events.

**Risk: evacuation blocked by the very fragmentation defrag exists to fix.**
OMENative evacuations need
their replacement footprint free on a healthy node *before* the source frees.
On a fragmented cluster that footprint may exist only in scatter — 12 free
GPUs as 2+3+2+3+2 cannot surge an 8-GPU Instance — so the candidate downgrades
to advisory with `NoSurgeHeadroom`, correctly but unhelpfully: the
consolidation that would create the slot is defrag's job, and nothing steers
defrag toward that specific hole. The emergency boost keys on Pending pods,
and the stranded Instance is not Pending — it is running-degraded on failing
hardware, invisible to every urgency mechanism. *Mitigation:* a blocked
evacuation is fed back into the next snapshot as a **virtual pending pod** of
its footprint (the snapshot already carries Alfred-caused state —
`ActiveMigration` — so this adds a field, not a concept). Existing machinery
then does the rest unmodified: demand weighting steers `Frag(c, s)` toward the
needed size, pending pressure gives the blockage bounded age-based urgency,
and the emergency boost lets the defrag move that opens the slot jump the
queue — Arbiter priority hands the freed headroom to the evacuation on the
following pass. RawDeployment, LWS, and otherwise ineligible occupants remain
advisory; there is no free-then-place executable path in Alpha. The residual
case — a packed cluster with nothing movable — ends in the advisory surface where it belongs,
and `alfred_surge_headroom_gpus == 0` is its dashboard signature. The feedback
is re-derived, not remembered: scoring runs before simulation within a pass,
so pass N's downgrades steer pass N+1 (a one-pass lag), the sick condition
re-produces the blockage every pass while it lasts, and on failover migration
state reconstructs from InferenceReplica status and the workload audit ledger.

**Risk: broken GPU discovered only after migration.** Alfred migrates onto Node5,
which has a broken NVLink; NCCL fails.
*Mitigation:* the snapshot carries node health (including `GpuUnhealthy`), so
unhealthy nodes are excluded from placement hints during Candidate generation.
This is also exactly why **Node-Health Evacuation is a first-class policy** rather
than an afterthought — the same health signal that excludes a node as a *target*
also makes it a *source* to evacuate. Post-migration, if a migrated workload fails
within a window, Alfred marks the node suspect and backs off from it. Mitigation,
not full fix: a fault that is invisible to the node condition can still bite.

**Risk: conflict with HPA / scheduler / other controllers.**
*Mitigation:* the snapshot records active HPA scaling and the Arbiter defers
every policy's Candidate for that Component while scaling is in progress.
Alfred respects `scale-down-disabled` on nodes. Alfred's per-workload exclusion
prevents competing UUIDs from its own policies; OMENative then revalidates live
source identity, fences source/surge operations in status, selects accepted
Manual records oldest-first, and applies execution-capacity gates. A future
consumer must provide equivalent validation and durable operation fencing before
its deployment mode can become executable.

**Risk: migration-request annotation write lost or delayed.**
*Mitigation:* the OEP-0007 annotations are UUID-keyed and idempotent (Q-021).
Until the UUID appears in `InferenceReplica.Status.Migrations` or as a terminal
workload-audit entry, the Dispatcher retries the same UUID after its
acknowledgement timeout. Accepted work follows the InferenceReplica phase and
deadline; annotation deletion alone never means completion.

**Risk: Alfred / owning-controller version skew on the migration wire contract.**
*Mitigation:* the Dispatcher writes only the OEP-0007 v1 request shape
(`ome.io/migration-request-v1-*`, `schemaVersion: "v1"`). The consuming
controllers reject unknown `schemaVersion` values explicitly; additive unknown fields within a
version are ignored, behavior-changing fields require a new version. On
`UnsupportedSchemaVersion`, Alfred degrades that workload to recommend-only and
emits an event.

**Risk: Alfred is a cluster-wide SPOF.**
*Mitigation:* leader-elected with 3 replicas; a crashed leader fails over in
~15s. Alfred is **not** on the critical path — workloads run fine if Alfred is
down for hours, because Alfred only *optimizes*, it does not *serve*. The
caretaker going dark degrades GPU efficiency over time; it never takes a workload
offline.

**Risk: snapshot staleness produces a bad decision.** The snapshot is a point-in-
time view; the cluster can change between snapshot and dispatch.
*Mitigation:* the snapshot is rebuilt every observation-loop period (default
30s), bounding staleness for recommendation. Immediately before each patch, the
future Dispatcher must directly re-read the capability Lease and the live
workload and InferenceReplica state; the latest cached snapshot alone is not
actuation authorization. That just-in-time guard remains deferred with the
Dispatcher. Even direct reads leave a small race window, which OMENative's own
lock and the bounded blast radius of the narrow posture constrain downstream.

**Risk: a third-party or future policy misbehaves.** Policy #3 (Descheduling), or
any later addition, has a bug.
*Mitigation:* the Policy interface gives a policy **no** write path — the worst it
can do is return bad Candidates, which the Arbiter filters and which, even if
dispatched, hit only the bounded narrow-write surface. The structural guarantee
of the engine is that *blast radius is independent of how many policies exist.*

## Design details

The engine — shared `ClusterSnapshot` → `Policies` → `Arbiter` → `Dispatcher` + `Reporter` — is fixed; what varies is the set of policies plugged into it. This section specifies each policy as a self-contained implementation of the `Policy` interface, then the cross-cutting concerns (safety, RBAC, tests) that the engine enforces around all of them.

### Policy #1: Defragmentation

Tl;dr: Policy #1 implements `Evaluate(snapshot) → []Candidate`. It reads the
shared `ClusterSnapshot`, scores fixable GPU fragmentation plus eligible pending
pressure, and — only when that score crosses a threshold — returns a ranked
slice of candidate migrations to the Arbiter. It does not move pods. Execution
belongs to the shared Dispatcher, which delegates to whoever owns the workload's
lifecycle.

This is the first policy. Its demand-weighted scoring formula is retained, but
selection and execution eligibility are refreshed for current OMENative, the
advisory-only Raw/LWS boundary, and fail-closed placement checks. It now returns
`[]Candidate` to the Arbiter instead of dispatching directly. Keep that boundary
in mind while reading — every "emit", "score", and "rank" below produces a
`Candidate`, never a side effect.

**Definitions (used throughout this section).**

- *Candidate*: a proposed move of one Component Instance — "Component X's Instance Y off Node Z" — carrying a *benefit score* (expected fragmentation improvement), a *cost score* (disruption risk), an `Executable` flag, and a ranked list of `HintTargetNodes`. A Candidate is a value, not an action; the Arbiter decides whether any Candidate is dispatched.
- *Instance*: per the OEP-0007 hierarchy, a Component (router/engine/decoder) is a set of Instances; an Instance is the atomic unit Alfred reasons about and migrates. An OMENative Instance may contain one pod or an atomic multi-pod group. Its stable index, incarnation, and lifecycle state come from the owning `InferenceReplica.Status`, while live placement and readiness come from Pods joined by the `ome.io/instance-index` and `ome.io/instance-incarnation` labels — never from pod-name order. A RawDeployment Instance is a single pod (one replica of the component) and is advisory-only until a Raw migration consumer exists.
- *Movable*: a workload Alfred is permitted to migrate. Default true; set false by the `alfred.ome.io/movable: "false"` annotation. A non-movable workload is excluded from candidate enumeration entirely — Policy #1 never produces a Candidate for it.

#### Observation layer

Policy #1 is a pure function of the shared `ClusterSnapshot`: it reads, it does not watch. The engine owns the informer wiring and refresh cadence; the policy only consumes the snapshot the engine hands it on each evaluation tick. Defining the read surface up front matters because every score below is computed against exactly these fields, and a field the snapshot does not carry is a signal the policy cannot use.

The snapshot the policy consumes:

```go
type ClusterSnapshot struct {
    Timestamp         time.Time
    Nodes             map[string]*NodeState
    Workloads         map[types.NamespacedName]*WorkloadState
    Models            map[string]*ModelAvailability
    PendingPods       []*PendingPodInfo
    OMENativeExecutor OMENativeExecutorState
}

type OMENativeExecutorState struct {
    Available   bool                            // fresh Lease + compatible wire version
    WireVersion string
    RenewTime   time.Time
    Reason      string                          // absent, stale, incompatible, or ready
}

type NodeState struct {
    Name                  string
    GPUPool               string                 // node-derived pool key (GPU product / instance-shape label)
    TotalGPUs             int
    AllocatedGPUs         int
    FreeGPUs              int
    LargestContiguousFree int                    // topology-aware; optional — scoring Step 1 hook (Q-040)
    Health                NodeHealthObservation
    Cordoned              bool
    ScaleDownDisabled     bool                   // CA annotation observed
    Preemptible           bool                   // spot / preemptible label observed
    OMEManagedPods        []OMEPodInfo
    OtherOccupants        []OtherPodInfo         // non-OME GPU workloads
}

type NodeHealthObservation struct {
    State              string                    // Clear | Suspect | Unknown | Unhealthy
    Conditions         []NodeConditionObservation
    SuspectUntil       *time.Time
}

type NodeConditionObservation struct {
    Type               string
    Status             string                    // True | False | Unknown
    LastTransitionTime time.Time
}

type WorkloadState struct {
    ISVC             *omev1beta1.InferenceService
    Components       map[string]*ComponentState  // router, engine, decoder
    Movable          bool                        // from annotation
    Priority         float64                     // from annotation or default
    LastMigration    time.Time                   // per-workload cooldown
    ActiveMigrations []*MigrationInFlight        // deduped by request UUID
}

type ComponentState struct {
    DeploymentMode   string                      // OMENative, RawDeployment, MultiNode (LWS), etc.
    InferenceReplica *omev1beta1.InferenceReplica // OMENative source of truth
    StatusFresh      bool                        // observedGeneration and encoding validated
    Instances        []*InstanceState            // stable indexes from status
}

type InstanceState struct {
    InstanceIndex    int32                       // stable index from normalized IR status
    Incarnation      int64                       // status value; must match joined Pods
    Phase            string                      // normalized IR status
    RunningRevision  string                      // normalized IR status
    Admitted         bool                        // normalized IR status
    ActiveOrdinal    int32                       // normalized IR status
    ServingPods      int32                       // normalized IR status
    AvailablePods    int32                       // normalized IR status
    OperationActive  bool                        // normalized IR status
    DesiredPods      int32                       // derived from the runner templates
    ObservedPods     int32                       // live Pods joined by index + incarnation
    ReadyPods        int32                       // derived from current live Pod conditions
    NodesSet         map[string]int               // live node -> pod count
    TotalGPUs        int32                       // derived from the joined live Pods
}

type PendingPodInfo struct {
    Namespace    string
    Name         string
    ISVC         types.NamespacedName
    GPUsNeeded   int32
    PendingSince time.Time
}

type ModelAvailability struct {
    Name           string
    Backend        string                        // PerNode | PVC
    NodesReady     []string                      // PerNode only: from BaseModel.Status.NodesReady
    NodesFailed    []string                      // PerNode only
    PVCAccessModes []string                      // PVC only: RWX/ROX migratable, RWO/RWOP pins the workload
    PVCTopology    []string                      // PVC only: CSI-reachable zone/node set; empty = unconstrained
}
```

The following properties of the read surface are load-bearing for later sections:

- **Non-OME GPU workloads count against capacity but are never candidates.** Kubeflow Notebook pods, generic Jobs, and any other non-OME GPU consumer appear in `NodeState.OtherOccupants` and are folded into `AllocatedGPUs` / `FreeGPUs` so the fragmentation score reflects reality. But Policy #1 enumerates candidates only from `WorkloadState` (i.e. OME InferenceServices). *Failure mode if we got this wrong:* if non-OME pods were invisible to scoring, Alfred would underestimate fragmentation and sit quiescent on a genuinely packed cluster; if they were eligible for migration, Alfred would try to evict a notebook it does not own. The snapshot threads the needle by counting them for scoring and excluding them from selection.
- **OMENative state is a checked join, not either API alone.** The snapshot
  decodes either DenseV1
  `InstanceStatuses` or ColumnarV2 `InstanceStatusColumns`, preserving stable and
  sparse Instance indexes, incarnation, lifecycle phase, operation, serving and
  available counts. It reads `Status.Migrations` for accepted and terminal
  migration work. Current Pods are then joined through the stable Instance index
  and incarnation labels to derive physical placement, GPU footprint, and live
  readiness. For a single-pod Instance, normalized `ActiveOrdinal` selects the
  canonical pod slot; for a multi-pod Instance, Alfred requires the complete
  runner/ordinal set described by the templates. An ambiguous or extra surge set
  is busy, not steady. `ReadyPodCount` and `NodesOccupied` in Instance status are
  retained compatibility fields and are not treated as persisted truth. An
  OMENative Candidate is executable only when both sides agree, status reflects the current
  generation, the encoding is valid, migration policy permits the action, and
  the selected Instance is admitted, fully ready, available, and serving with no
  active lifecycle operation, rollout, scale transition, or migration. A
  missing/stale status, label mismatch, incomplete Pod set, or readiness
  mismatch fails closed to an advisory Candidate.
- **Executor liveness needs a positive signal.** CRD discovery and a coherent
  InferenceReplica snapshot prove API compatibility, not that the controller is
  still running. The OME manager now publishes a namespaced capability Lease
  only while the InferenceReplica executor is enabled and cache-synced. Once per
  observation, Alfred reads the named Lease directly through its uncached API
  reader. It requires the exact `ome.io/migration-request-schema: v1` marker, a
  nonempty holder identity, and a `renewTime` no older than the configured
  maximum age (30 seconds by default and at minimum, or three 10-second
  renewal periods). A `renewTime` up to and including five
  seconds in the future is accepted; greater future skew fails closed. An
  absent, stale, or incompatible Lease makes every Candidate advisory. The
  future Dispatcher must repeat this direct read just before it patches a
  migration request; that just-in-time guard remains deferred.
- **Placement feasibility fails closed.** GPU room, model locality, and storage
  topology are necessary but not sufficient. Before a Candidate becomes
  executable, Alfred evaluates the complete OMENative runner template: scalar
  resources, node selectors, required affinity, taints and tolerations, PVC/PV
  topology, evaluable required pod affinity/anti-affinity, and the atomic
  multi-pod footprint. A required constraint Alfred cannot evaluate downgrades
  the Candidate to advisory rather than producing a speculative target.
- **`ModelAvailability` is storage-aware, because PVC-backed models have no per-node readiness.** Per-node models (model-agent downloads) report readiness through `Status.NodesReady` and the `models.ome.io/...=Ready` node label — and the ISVC controller stamps that label as a hard `nodeSelector` on the pods, so the scheduler enforces it independently of Alfred. PVC-backed models intentionally never populate `NodesReady` (there is no per-node copy to report, and the ISVC controller likewise skips the readiness selector for them). *Failure mode if we got this wrong:* filtering PVC-backed workloads' targets by `NodesReady` would yield zero feasible targets and silently mark every PVC-backed workload `NoFeasibleTarget` forever. So the snapshot records the storage backend and, for PVC, the access modes and CSI topology — and the target filter switches on it (mechanism in Placement-hint computation).

#### Fragmentation scoring

The score answers one question: **how much of the cluster's free GPU capacity could defragmentation make usable that is not usable now?** It is a cluster-level number in `[0, 1]` — 0 = "no migration Alfred can execute would improve anything" (whether perfectly packed, fully utilized, or fragmented-but-immovable), 1 = maximal fixable fragmentation — and it gates the entire policy. Below the threshold, `Evaluate` returns an empty slice and Alfred stays quiescent; above it, the policy proceeds to candidate selection. Two properties are deliberate, and each corrects a failure mode of naive "score the packing" formulas:

- **Fragmentation is relative to demand.** A free GPU is only meaningfully free if a workload shape this cluster actually serves can use it, so the score is computed against the demand-size distribution, not against nodes in isolation. (Formally: the whole-GPU case of the statistical fragmentation measure of Weng et al., USENIX ATC '23, "Beware of Fragmentation: Scheduling GPU-Sharing Workloads with Fragmentation Gradient Descent".)
- **The score measures opportunity, not state.** Operators watch this gauge to decide whether to switch from `recommend-only` to `execute`, so it must measure what `execute` could achieve. A cluster Alfred cannot improve — fully packed, fully utilized, or fragmented solely by workloads Alfred may not move — scores 0 by construction; what it cannot fix is surfaced separately (Step 3), not through the gate.

Everything is computed **per hardware pool, never mixed across pools** — the pool key is derived from the node (GFD GPU product label, else instance shape), never from `AcceleratorClass`: shape-scoped classes (H100x1..x8) can overlap-claim the same node and cannot partition hardware. Free L4s cannot seat an H100 demand, and a healthy L4 pool must not mask a fragmented H100 pool.

**Step 1 — per-size unusable free capacity.** For each pool `c` and each demand size `s` in the within-node ladder `L = [1, 2, 4, 8]`:

```text
Slots(c, s) = sum over nodes n of pool c: floor(FreeGPUs(n) / s)    for s <= maxCap(c)
Slots(c, s) = floor(fullyFreeNodes(c, maxCap) / ceil(s / maxCap))    for s >  maxCap(c)
Frag(c, s)  = 1 - Slots(c, s) * s / TotalFree(c)     // defined 0 when TotalFree(c) = 0
```

`Frag(c, s)` reads: the fraction of pool-c free GPUs that a size-s demand cannot use. The signature trap scores itself — ten 8-GPU nodes each with 7 free: `Slots(8) = 0`, so `Frag(8) = 1.0`; 70 free GPUs, every one of them invisible to an 8-GPU workload. Three properties fall out of the formula's shape: `Frag(c, 1) = 0` identically, so small sizes need no hand-tuned down-weighting; `floor()` handles heterogeneous node sizes with no special cases; and sizes larger than every node in the pool switch to the second case: they draw slots only from fully-free nodes of the pool's largest node shape (`maxCap`), because a smaller fully-free node cannot host a max-shape member pod. *Topology hook (Q-040):* if a node agent later supplies `LargestContiguousFree`, it substitutes for `FreeGPUs` here; on NVSwitch fleets the two are identical, so the hook matters only for PCIe and MIG pools.

**Step 2 — demand weighting.**

```text
w(c, s)       = (1 - lambda) * demandShare(c, s) + lambda * prior(s)
F_observed(c) = sum over s: w(c, s) * Frag(c, s)
```

`demandShare(c, s)` is the fraction of pool-c GPU demand at size `s`, counted from running plus pending OME instance footprints snapped up to the ladder. A **multi-node Instance decomposes into its per-pod, per-node footprints** — a 2×8 Instance is 16 GPUs of size-8 demand, a 2×4 Instance is 8 GPUs of size-4 demand. This is not an approximation of convenience: within-node fragmentation can only deny a multi-node workload one node-shape at a time — what it needs from the cluster is "a node with 8 free," twice — so per-pod footprint is the unit of blocking, and `Slots(8)` already counts the nodes that could host each pod. (Whether two such nodes share an RDMA fabric is cross-node adjacency, out of scope per Q-039.) An LWS-backed instance also counts here — it is real demand — even though Step 3 will refuse to move it. The **prior** is a static distribution over sizes — default `{1: 0.1, 2: 0.1, 4: 0.2, 8: 0.6}`, blended at `lambda = 0.3` (`demandBlendLambda`) — expressing what shapes this fleet exists to serve, independent of what happens to be running right now. *Why a prior must exist:* pure `demandShare` is blind to shapes not currently deployed — a cluster running only 1-GPU jobs would weight `Frag(8)` at zero and report "no fragmentation" right up until the first 8-GPU model arrives and pends. The prior keeps latent large-shape fragmentation visible; its mass sits on the largest within-node size because that is the only shape fragmentation can hurt. `lambda` dials between reactive-only (`0`) and static-weights-only (`1`). The prior need not stay hand-typed: registered `BaseModel`/`ClusterBaseModel` resources declare GPU footprints, so a catalog-derived prior ("what could be deployed here") is a natural refinement — deferred to implementation.

**Step 3 — reclaimable vs. observed.** Hypothetically repack only Instances
that could become executable under the current compatibility baseline:
OMENative, `Movable=true`, a checked InferenceReplica-plus-Pod view, a fresh
compatible executor Lease, steady lifecycle state, and a supported placement
proof. Hold everything else fixed in
place: non-OME occupants, `Movable=false` workloads, RawDeployment, and
LWS-backed Instances. FFD is the same heuristic family candidate simulation
already uses, and global optimality is explicitly not promised (see Non-Goals:
not an optimization engine). Recompute Steps 1–2 on the repacked free
distribution:

```text
F_best(c)        = F_observed(c) recomputed on the repacked snapshot, over the same TotalFree(c)
F_reclaimable(c) = max(0, F_observed(c) - F_best(c))
```

`F_reclaimable` is what migration could actually fix, and it is what gates the policy. The remainder — `F_observed - F_reclaimable` — is fragmentation only an operator can fix in Alpha (RawDeployment, LWS-backed groups, non-OME occupancy) and flows to the Reporter's advisory surface instead of the gate. This split is what keeps Alfred from waking every tick on a cluster it cannot improve, while still telling the operator that manual action would help. Both sides normalize by the *observed* `TotalFree(c)`: a repack can consume free capacity (seating a pod off an excluded node), and letting the denominator shrink with it would fabricate reclaimable fragmentation out of a shrinking base with no slot improvement. `F_best` is a bound, not a plan: realizing it may take many moves, paced by the Arbiter's churn budget across ticks, each tick re-deriving candidates from a fresh snapshot.

**Step 4 — pending pressure.** A pending pod is *eligible* iff the repacked state of Step 3 could seat it (`Slots_best(c, s_p) >= 1`). Ineligible pendings are capacity shortage, not fragmentation — that is [OEP-0013](../0013-autoscaling/README.md)'s problem, and they must not wake Alfred, because no migration can seat them. (A pending multi-node gang arrives as several pending pods and is checked per-pod, so a gang can read eligible when the repack could seat only part of it. This optimism is a deliberate roughness in the gate: the score decides whether to wake, and the candidate simulator — not the score — verifies real placements.) Blocked evacuations are fed back as **virtual pending pods** of their footprint (see the risks section): a `NoSurgeHeadroom` downgrade in pass N enters pass N+1's `PendingPods` with age counted from first blockage, scored identically — steering both the demand weights and `P` toward exactly the hole the evacuation needs.

```text
u(p) = 1 - exp(-age_minutes(p) / tau)      // tau = pendingUrgencyTauMinutes, default 30
P(c) = 1 - prod over eligible p: (1 - u(p))
```

Bounded per pod and in aggregate — an ancient pod asymptotes to 1 instead of growing without limit — and the age unit is stated. (`P` and the `emergencyPendingAgeMinutes` boost in candidate selection are complementary, not redundant: `P` decides *whether the policy wakes*, the boost decides *what the woken policy does first*.)

**Combined score.**

```text
Score(c)           = 1 - (1 - F_reclaimable(c)) * (1 - P(c))
FragmentationScore = max over pools c: Score(c)
```

Noisy-OR across the two terms: fixable shape damage or a starving fixable pod each independently justify waking the policy, and both live in `[0, 1]`, so the score needs no clamp and cannot exceed 1. `max` over pools rather than mean: you defragment the worst pool, and a healthy pool must not dilute a sick one. Published gauges: `alfred_fragmentation_observed{pool, size}`, `alfred_fragmentation_reclaimable{pool}`, `alfred_pending_pressure{pool}`, and the combined `alfred_cluster_fragmentation_score` — an operator sees *which size in which pool* is blocked and what share of it Alfred could fix, before switching from `recommend-only` to `execute`.

**Threshold gate.** If `FragmentationScore > policy.fragmentationThreshold` (default `0.25`), candidate selection proceeds. At or below it, `Evaluate` returns an empty `[]Candidate` and Alfred is quiescent. The default has a concrete reading: "repacking could recover at least a quarter of demand-weighted free capacity, or a fixable pod has been pending for roughly nine minutes or more." A fully-utilized cluster, an idle cluster whose spread blocks nothing yet, and a cluster fragmented entirely by workloads Alfred cannot move all sit below threshold by construction — migration is disruptive, and the gate fires only when migration is what would help.

#### Candidate selection

When the score is above threshold, the policy turns the snapshot into a ranked `[]Candidate`. The steps below run on each evaluation tick:

1. **Enumerate.** For every movable workload (`Movable=true`), enumerate each
   Component Instance as one prospective Candidate and carry its migration,
   placement, and termination state forward. The Arbiter applies the shared
   busy/cooldown/terminating gates to every policy's Candidates; see
   [Safety bounds](#safety-bounds). Keeping these Candidates visible lets the
   Reporter explain a skip instead of turning it into silence.
2. **Classify by deployment mode** — this sets the `Executable` flag, it does not drop the candidate:
   - **OMENative**: potentially executable via the OMENative migration verb,
     subject to the checked InferenceReplica-plus-Pod view, a fresh compatible
     executor Lease, and fail-closed placement checks.
   - **RawDeployment** (any replica count): `Executable=false` until a Raw
     migration-request consumer exists. Still emit the Candidate so operators
     see the opportunity.
   - **LWS-backed multi-pod**: `Executable=false`, but *still emitted* as a Candidate so the recommendation surfaces to operators. *Why include something we won't execute:* LWS's `RecreateGroupOnPodRestart` tears down the whole group on eviction with no surge protection — migrating it automatically is unsafe — but an operator still needs to see that this workload is a defrag opportunity and that the safe fix is to move it to OMENative.
   - **Knative**: not managed by Alfred; not enumerated.
3. **Simulate.** For each candidate, predict the post-migration cluster state and recompute `F_observed` on that hypothetical snapshot; this gives `F_observed_after`. (Per-candidate benefit deliberately uses the *observed* score, never the reclaimable one — `F_best` stays out of per-candidate math, so the repack heuristic cannot jitter rankings.) An executable OMENative migration is **place-then-free**: every replacement member must fit on valid targets while the source still holds its GPUs, and only then does simulation free the source and re-score. A candidate with no surge-feasible placement is downgraded to advisory with reason `NoSurgeHeadroom`. RawDeployment and LWS simulations may estimate operator-visible benefit, but they never create capacity claims or executable Recommendations.

   Getting this order wrong is a real failure mode: free-then-place simulation of a surge move overestimates feasibility exactly when defragmentation matters most — on a highly utilized cluster.
4. **Score** each candidate with a benefit-minus-cost rule:

   ```
   BenefitScore = F_observed_before - F_observed_after   // positive = improvement
   CostScore    = f(disruption_risk, pods_impacted, migration_mode)
   FinalScore   = BenefitScore - CostWeight * CostScore
   ```

   Alpha has one executable mechanism: **OMENative surge**, for either a
   single-pod or multi-pod Instance. It needs the complete replacement footprint
   as headroom while the source still runs; step 3 enforces that as feasibility,
   while the cost term prices the affected serving footprint. RawDeployment and
   LWS are not cost-scored for admission — they are advisory
   (`Executable=false`) and route straight to the Reporter, outside arbitration
   and budget (see Execution).
   A Candidate with `FinalScore <= 0` is never executable: moving it would buy
   no modeled improvement after disruption cost. It remains reportable with an
   explicit non-positive-benefit reason rather than entering arbitration.
5. **Boost emergencies.** If a candidate's migration would unblock a pod — real, or a virtual pending from a blocked evacuation (see the risks section) — that has been Pending longer than `emergencyPendingAgeMinutes`, multiply its `FinalScore` by a boost factor. This is what lets Story 2 — a Llama4 stuck Pending despite 70 free GPUs — jump the queue ahead of routine consolidation. (Complementary to the scoring gate's pending-pressure term `P`: `P` decides whether the policy wakes at all; the boost decides what the woken policy does first.)
6. **Rank** by `FinalScore` descending, breaking ties toward the smaller surge footprint — smaller moves fit more often, disrupt less, and each completion frees GPUs that make larger moves feasible later. High-utilization defragmentation is designed to converge across decision cycles, smallest-first, not in one pass.
7. **Apply policy-local eligibility** before returning: deployment support,
   steady lifecycle state, model/storage reachability, and placement certainty.
   Shared cooldown, maintenance-window, tenant-boundary, and rate-limit gates
   remain in the Arbiter so they are enforced once across policies and their
   rejection reasons reach the Reporter.
8. **Return** the surviving ranked slice as `[]Candidate`. That return value is the policy's entire output — Policy #1 itself emits no Event, no metric, and no ConfigMap entry (it holds no client; see the engine's purity contract). The engine then routes the slice: executable Candidates enter the Arbiter; advisory ones (`Executable=false`) go straight to the Reporter. The Reporter emits a `FragmentationRecommendationProduced` K8s Event on each target InferenceService, increments `alfred_recommendations_produced_total`, and (if enabled) writes the `alfred-recommendations` ConfigMap entry — for every Candidate outcome it sees: produced, admitted, withheld, or rejected, each with its reason. Whether a returned Candidate is acted on remains the Arbiter's call, and dispatch the Dispatcher's.

#### Placement-hint computation

Each Candidate carries `HintTargetNodes`: a ranked, *advisory* list of nodes the migration should prefer. Advisory is the key word — Alfred computes a good target, but the K8s scheduler makes the final placement, and Alfred re-evaluates from the observed outcome rather than insisting. Computing a hint:

1. **Enumerate** nodes that can physically accommodate the Instance's footprint (GPU count and hardware pool).
2. **Filter** out nodes that would make the migration pointless or unsafe:
   - **Required scheduling constraints fail closed** — evaluate CPU, memory and
     scalar requests; node selectors; required node affinity; taints and
     tolerations; PVC/PV topology; evaluable required pod affinity and
     anti-affinity; and the complete multi-pod/gang footprint from the
     InferenceReplica runner templates. If a required constraint cannot be
     evaluated soundly, the Candidate becomes advisory instead of receiving a
     speculative target hint.
   - **Model not available** — storage-aware, switching on `ModelAvailability.Backend`. *Per-node models*: the target must have the model ready, per `BaseModel.Status.NodesReady` or the node label `models.ome.io/{ns}.basemodel.{name}=Ready` (OEP-0007 Q-017) — migrating to a node that must first pull a multi-hundred-GB model defeats the purpose, and the pod's own readiness `nodeSelector` would block the placement anyway. *PVC-backed models*: `NodesReady` is intentionally empty and must **not** be used as a filter; the target set is the nodes that can mount the volume — for RWX/ROX storage, any node satisfying the PVC's CSI topology, with no model pull ever needed. *RWO (and RWOP) PVCs pin the workload*: the volume attaches to one node at a time and the source pod still holds it while a surge replacement starts, so no surge-shaped mechanism can run — the candidate is downgraded to advisory with reason `VolumePinned`.
   - **Unhealthy or cordoned**: excluded. Excluding unhealthy nodes from placement is existing defragmentation behavior — and it is the seam Policy #2 builds on: a node Policy #1 already refuses as a *target* is exactly the kind of node Policy #2 will reason about as a *source* to drain. Nodes inside their post-evacuation **suspicion window** are excluded too, even after the condition clears (see Policy #2's "stay suspicious" rule). (Mechanism for Policy #2 in its own section.)
   - **CA scale-down in progress**: a node with `scale-down-disabled` being processed is excluded, so Alfred and the cluster-autoscaler do not fight over it.
   - **Spot / preemptible**: excluded from targets by default (see the spot/preemptible section) — moving a workload onto a node that can vanish is a poor trade.
3. **Rank** by a bin-packing heuristic whose direction depends on the goal: prefer partially-filled nodes when the goal is to consolidate, or prefer empty nodes when the goal is to free contiguous capacity elsewhere.
4. **Return** the top N (default `3`).

#### Execution

Policy #1 does not execute. It returns `[]Candidate`; the Arbiter selects which Candidate(s) to act on across all policies; the shared **Dispatcher** carries out the move. This separation is the safety boundary of the whole design: a bad *recommendation* is just a value the Arbiter can decline, and the policy never holds an eviction client. The Dispatcher writes exactly one thing — the OEP-0007 migration-request annotation — and the controller that owns the workload's lifecycle executes the move. Which controller consumes a request is determined by the component's deployment mode, so there is no ambiguity; Alfred performs no pod-level action on any path:

**OMENative-managed → migration-request annotation (OEP-0007 Q-004).** The Dispatcher does not evict multi-pod groups; it asks the controller that owns the group's lifecycle to migrate it, by PATCHing a request annotation onto the ISVC:

```go
uuid := generateUUID()
payload := MigrationRequest{
    SchemaVersion:   "v1",
    Component:       "engine",
    Instance:        0,
    Reason:          "fragmentation",
    FromNode:        "node1",
    HintTargetNodes: []string{"node3", "node7"},
    RequestedAt:     time.Now().UTC().Format(time.RFC3339Nano),
    RequestedBy:     "alfred-controller",
}
patch := fmt.Sprintf(`{
    "metadata": {
        "annotations": {
            "ome.io/migration-request-v1-%s": %q
        }
    }
}`, uuid, marshalJSON(payload))
client.Patch(ctx, isvc, types.MergePatchType, []byte(patch))
```

The InferenceReplica controller observes the annotation, validates it, records a
manual entry in `InferenceReplica.Status.Migrations`, persists the request in
the workload audit ledger, and then deletes the mailbox annotation. Alfred
follows the same request UUID across those surfaces to learn what happened; it
does not infer success from annotation deletion.

Retry semantics (OEP-0007 Q-021):
- Until the UUID appears in `InferenceReplica.Status.Migrations` or as a terminal
  workload-audit entry, the request is unacknowledged. The Dispatcher may
  re-PATCH the **same** UUID after its acknowledgement timeout; UUID idempotency
  makes that safe across leader failover and cache lag.
- Once accepted, the InferenceReplica migration `Deadline` and terminal phase
  govern completion. Alfred never invents a separate completion timeout and
  never treats a missing annotation as completion.

**RawDeployment → advisory-only in the current phase.** No production
InferenceService controller consumes the migration-request annotation for
RawDeployment today. Alfred therefore marks Raw candidates
`Executable=false`, emits the proposed source and target hints, and does not
write an annotation. A future Raw executor may adopt the same contract, but it
must land with live-state validation, UUID idempotency, PDB-safe disruption,
durable status/audit reporting, and end-to-end tests before this OEP marks Raw
execution supported.

**Legacy LWS-backed → never dispatched, advisory only.** An LWS-backed Candidate arrives flagged `Executable=false`, and the engine routes it directly to the Reporter — it never enters the Arbiter and never reaches the Dispatcher, so it consumes no rate-limit budget and starts no cooldown. The Reporter emits the recommendation event with a message directing the operator to migrate the workload to OMENative, and increments `alfred_lws_recommendations_total{isvc, action: manual}` so a dashboard can alert that manual defragmentation is needed. *Why route around arbitration:* an advisory carries no action to admit, and pushing it through benefit-cost admission would either silently drop it or let an inert recommendation debit the budget real actions need. *Why refuse rather than try:* LWS's `RecreateGroupOnPodRestart` tears down the entire group on a single eviction with no surge protection — Alfred would cause the very outage it exists to prevent.

**Failure handling at dispatch:**
- **Request validation or execution gate fails** (policy disabled, invalid
  Instance or source, unsupported shape, or OMENative capacity/rate limit): the
  owning controller records the UUID as `Failed`. Alfred reports that terminal
  reason and applies failure cooldown. Accepted records may queue oldest-first;
  queuing is not reported as a `MigrationInProgress` rejection.
- **Request accepted but `InferenceReplica.Status.Migrations` reports `Failed`**
  (surge Instance never Ready, rollout stall, or deadline expiry): increment the
  failure metric, emit an event, and put that workload in cooldown so Alfred
  does not immediately re-attempt a move that just failed.

### Policy #2: Node-health evacuation

Tl;dr: when a node's GPU goes bad, get the OME workloads off it and tell the
operator the node needs repair — but do not touch the node. Policy #2 is
"evacuate + signal," and that two-word actuation depth is the whole point: it
stays inside the exact write contracts Policy #1 already uses, so it needs no
new RBAC and no cloud credentials.

**Definition.** *Policy #1* is defragmentation (the bin-packing relocation of
the rest of this document): its trigger is a fragmentation score crossing a
threshold. *Policy #2* is node-health evacuation: same machinery, different
trigger. A *bad node condition* is a node-level signal the snapshot already
carries — `GpuUnhealthy` is the canonical one — that marks a node's
accelerators as unusable for OME workloads. *Evacuate + signal* means Alfred
produces evacuation Candidates for the workloads on that node and emits a
remediation signal for whoever owns the hardware; it does **not** repair,
replace, cordon, drain, terminate, or reboot the node.

#### Goal and context

The failure mode Policy #2 addresses: a node reports `GpuUnhealthy` (failed GPU,
bad NVLink, ECC errors past threshold) and the OME workloads pinned to it are
now running on — or scheduled onto — hardware that will fail their NCCL
collectives or crash their pods. Defrag already *avoids* such a node: unhealthy
nodes are excluded from placement hints, so Policy #1 never migrates a workload
*toward* one. But avoidance is not evacuation. Nothing in Policy #1 moves the
workloads that are *already* stranded on the bad node, and nothing tells the
operator the node is bad and needs repair. The pods sit there degrading until a
human notices.

In scope: detect the bad condition (already in the snapshot), emit evacuation
Candidates for the OME workloads on that node, and emit a remediation signal so
the node gets repaired or replaced. Out of scope: deciding *how* to repair the
hardware, and performing any node-level write. That belongs to the operator, the
cloud provider, or the cluster autoscaler — see the anticipated objection below.

#### What must be true

1. The trigger is a node condition already present in the snapshot. *Rationale:*
   Non-Goal #6 stands — Alfred adds no new GPU-health telemetry. Policy #2
   consumes `GpuUnhealthy` through `NodeState.Health`; it does not probe GPUs
   itself.
2. Evacuation uses the same execution surface as defrag, unchanged: every
   executable evacuation Candidate goes out as the OEP-0007 migration-request
   annotation and is executed by the owning controller. OMENative surge is the
   supported path in this phase; RawDeployment and LWS findings are
   recommendation-only until their lifecycle owners expose a compatible
   consumer. *Rationale:* the safety argument — workloads are moved by their
   owning controller, never evicted out from under themselves by an outsider —
   must hold identically for health evacuation, because the destructiveness of
   the action does not depend on what triggered it.
3. The remediation signal is observable by the operator/cloud/cluster-autoscaler
   without Alfred holding any node-write permission. *Rationale:* this is the
   property that keeps Non-Goal #3 ("Alfred does not cordon, drain, or terminate
   nodes") literally true while still making the bad node *actionable*.
4. Node health is a small state machine, not a boolean. A configured condition
   at `True` makes the node unhealthy and evacuation-eligible. `Unknown`
   quarantines the node as a target and emits a remediation signal, but does not
   authorize migration. A transition to `False` after an incident enters the
   suspicion window; only an expired suspicion window is clear. Condition
   transition time is retained so this state is reconstructible after restart.
   *Rationale:* treating `Unknown` as healthy is unsafe, while treating it as
   sufficient evidence for disruption is also unsafe.
5. A condition-change early tick refreshes the snapshot before policy
   evaluation. Refresh, publication, and evaluation are serialized; a failed
   refresh skips the early decision. An early pass does not postpone the regular
   decision cadence. *Rationale:* waking quickly but evaluating the pre-change
   snapshot provides neither fast detection nor safe evacuation.

#### Approach (how)

On a bad condition for node N, Alfred does two things, both within its existing
contracts:

a) **Evacuate.** Produce evacuation Candidates for every OME workload occupying
   N, scored and dispatched through the same Candidate pipeline Policy #1
   uses (eligibility check, simulation against the *healthy* remainder of the
   cluster, capacity check, dispatch via the migration-request annotation).
   Ineligible Instances and deployment modes without a consumer remain visible
   as advisory Candidates; they are never silently dropped or speculatively
   dispatched. Source enumeration starts from the OME occupants physically on
   N and resolves each pod to its stable InferenceReplica Instance. Every
   resulting Candidate carries `FromNode=N`; Alfred never substitutes a
   "primary" node guessed from where the rest of a multi-pod Instance happens
   to consume the most GPUs.
   Health-evacuation Candidates carry a distinct reason (`NodeUnhealthy`, vs
   `Fragmentation`) so operators can tell defrag moves from forced evacuations in
   metrics and Events — but they flow through the same code path. Within the
   class, candidates order: feasible-target candidates first, then higher
   `alfred.ome.io/priority` workloads (the most important leave the sick node
   first), then smaller surge footprints (likelier to fit, and each completion
   frees room for the larger moves).

b) **Signal.** Return, alongside the evacuation Candidates, one advisory
   Candidate per bad node (`Executable=false`, reason `RemediationSignal`). The
   engine routes it directly to the Reporter. At first detection the Reporter
   emits a `NodeRepairNeeded` Warning Event on the Node. When the refreshed
   snapshot shows no OME workloads left on the node, it emits
   `NodeDrainedForRepair`. It also maintains an entry in Alfred's own
   `alfred-recommendations` ConfigMap keyed by node name with the health state,
   affected workloads, suspicion deadline, and timestamps. The policy itself
   writes none of these surfaces. Reporter reconciliation treats each policy
   result as the complete desired signal set, so cleared nodes remove stale
   records and repeated observations do not spam Events.

c) **Stay suspicious.** An evacuated node stays excluded from every policy's
   *target* hints for `nodeSuspicionWindowMinutes` (default 30) — even after
   the condition clears. *Failure mode this guards:* a flapping node becomes a
   pump — condition clears, defrag consolidates workloads onto the newly
   "empty" node, condition trips, Alfred evacuates again. The per-workload
   cooldown never prevented this (it only stops the *evacuated* workloads from
   returning; other workloads could still be packed onto the flapper); the
   suspicion window does.

The only genuinely new behavior versus Policy #1 is the **trigger** (a node
condition, not a fragmentation score), the **remediation Event**, and the
**suspicion window**. Everything
downstream — Candidate scoring, simulation, the global safety bounds, dispatch
— is reused verbatim. That reuse is deliberate: it is what bounds the blast
radius of adding a second policy to roughly zero new write surface.

#### The narrow-contracts property (no new RBAC, no cloud credentials)

State it plainly, because it is the load-bearing reason Policy #2 is cheap and
safe to add: **evacuate + signal stays entirely within the three write contracts
Alfred already holds.** Those three, and nothing more:

| Write surface | Used by | Node-level? | New for Policy #2? |
|---|---|---|---|
| OEP-0007 migration-request annotation on the ISVC | defrag + evacuation for workload owners with a validated consumer (OMENative in Alpha) | no | no |
| Kubernetes Events | recommendations + remediation signal | Event *targets* a Node; it is **not** a write to the Node's spec/status | the Node-targeted Event is new; the verb (`create events`) is not |
| Alfred's own `alfred-recommendations` ConfigMap | recommendations + node remediation record | no | no |

There is **no** `patch nodes`, **no** `update nodes/status`, **no** cordon (which
is a node spec write), **no** drain, **no** cloud-API call, **no** pod-level
write of any kind. RawDeployment and LWS remain advisory in Alpha, and therefore
**no** cloud credential anywhere in Policy #2. Emitting an Event whose `involvedObject`
is a Node requires only the `create events` permission Alfred already has for its
recommendations — it does not require write access to the Node itself.

Why this matters as a failure mode, not just a tidiness claim: the day Policy #2
acquires `patch nodes` or a cloud credential, Alfred becomes a thing that can
cordon the wrong node or terminate a healthy instance, and the bounded-blast-
radius guarantee from the architectural posture section evaporates. Keeping the
write set frozen at three contracts is what *makes* "Alfred does not manage node
lifecycle" an enforceable invariant rather than an aspiration — an auditor can
verify it from the RBAC manifest alone.

#### Anticipated objection
Q. If Alfred only signals and never repairs, what closes the loop — couldn't a
bad node sit forever with workloads refusing to land on it?

A. The loop is closed by whoever already owns nodes, not by Alfred. The Node
Event and ConfigMap record are the handoff; cluster-autoscaler replacing the node
or an operator repairing it is the completion. If nobody acts, the node stays
excluded from placement hints. Eligible OMENative Instances may already have
moved, but RawDeployment, LWS, or ineligible OMENative occupants can remain; in
that case Alfred withholds `NodeDrainedForRepair` and keeps their advisory
blockers visible. Policy #2 reduces exposure where a validated migration path
exists. It does not guarantee that every workload is safe or that the hardware
gets fixed; those residual actions belong to the operator and node owner.

### Policy #3: Descheduling (future)

Tl;dr: a reserved interface slot, not a design. Listed now only so the Policy
abstraction is shaped to hold more than two triggers.

*Policy #3* is generic descheduling: "this placement now violates a constraint it
satisfied when the pod was scheduled" — anti-affinity drift (two pods that should
be spread ended up co-located after unrelated churn), priority inversion (a
low-priority workload squatting on capacity a high-priority one now needs), or a
topology constraint that newer labels invalidated. The trigger is a *constraint
violation discovered after scheduling*, distinct from fragmentation (Policy #1)
and node health (Policy #2). The actuation depth would again be evacuate-style
relocation through the same Candidate pipeline — but the *detection* (which
constraints to evaluate, how to score a violation's severity, how to avoid
fighting the scheduler that placed the pod) is not designed here. Policy #3 is
**Phase 2** and is named only to confirm that the Policy interface — trigger →
Candidates → Arbiter → dispatch — generalizes cleanly past defrag, so we don't
have to reshape it later. Anything beyond this paragraph about Policy #3 is out
of scope for this OEP.

### The arbiter (arbitration-lite)

Tl;dr: with more than one policy producing Candidates, something has to decide
which Candidate wins when they conflict. The arbiter does that with four
deterministic rules and **no** prediction — it sequences actions, it does not
forecast them.

**Definition.** The *arbiter* is the stage between policy Candidate-production and
dispatch. *Arbitration-lite* means it resolves conflicts by fixed
priority/exclusion/safety rules that are fully determined by the current
snapshot — given the same snapshot it always makes the same choice — as opposed
to a predictive planner that models future cluster state. We are explicitly
choosing the lite version for v1.

#### Why an arbiter at all

The failure mode it prevents: two policies, run independently, issue
contradictory or unsafe-in-aggregate actions in the same cycle. Concretely —
Policy #1 (defrag) wants to migrate workload A *onto* node N to improve packing,
while Policy #2 (health) is simultaneously evacuating node N because it just went
`GpuUnhealthy`. Without arbitration, Alfred migrates A directly into a node it is
in the middle of evacuating: the worst possible move. Or: two policies each emit
a Candidate for the same workload, and the workload gets two concurrent
migration requests, violating Alfred's one-action-per-workload invariant. The arbiter exists
to make those impossible *before* dispatch, deterministically.

#### The four rules

a) **Priority — health preempts defrag.** A health-evacuation Candidate
   (Policy #2) always outranks a defrag Candidate (Policy #1) that touches the
   same workload or node. *And* the arbiter never lets defrag migrate a workload
   *toward* a node that any health Candidate is evacuating this cycle. *Rationale:*
   getting a workload off failing hardware is strictly more urgent than improving
   bin-packing, and migrating toward a node under evacuation is self-defeating.

b) **Mutual exclusion — at most one in-flight action per workload, and per
   *target* node, per cycle.** If two Candidates target the same workload, only
   the higher-priority one survives arbitration; the other is dropped (it will
   be re-proposed next cycle if still valid). Likewise at most one admitted
   move may *land on* a given node per cycle. The exclusion is deliberately
   target-scoped: a node under health evacuation may *source* several moves in
   one cycle, bounded by the in-flight cap — serializing a drain to one
   workload per cycle would stretch an 8-workload evacuation across 40 minutes
   for no safety benefit, because the risk this rule guards is target
   over-commit, not source outflow. *Rationale:* concurrent actions on one
   workload create duplicate intent and competing capacity claims. OMENative can
   queue distinct UUIDs, so Alfred must enforce its own per-workload exclusion
   before dispatch; target-node serialization prevents simultaneous claims from
   overcommitting the same destination.

c) **Global safety bounds — the existing cooldown, rate-limit, and capacity-check
   apply across all policies, not per policy.** The cluster-wide caps
   (`maxInFlightMigrations`, `maxMigrationsPerHour`), the per-workload and
   per-node cooldowns, and the pre-flight capacity check are evaluated on the
   *combined* stream of surviving Candidates after priority and exclusion — never
   reset or duplicated per policy. *Rationale:* if each policy got its own budget,
   adding Policy #2 would silently double the migration rate the cluster
   tolerates, and the thrashing/migration-storm mitigations from the risks
   section would be defeated. The safety envelope is a property of Alfred as a
   whole, so it is enforced once, globally, at the arbiter.

d) **OEP-0013 awareness — defer to the autoscaler.** Before a Candidate clears
   the arbiter, Alfred reads the autoscaler's intent (read-only; see the next
   section) and skips any workload the autoscaler is *actively scaling*.
   *Rationale:* migrating a Component while OEP-0013 is adding or removing its
   replicas races two controllers over the same pods. *Failure-mode default:* if
   the autoscaler-intent signal is unavailable (CRD absent, read error, stale
   beyond the snapshot window), the arbiter treats the workload as **busy** and
   defers it. We choose the safe default — skip on uncertainty — because acting
   on a workload that might be mid-scale is the costlier error; a deferred
   Candidate simply reappears next cycle.

#### Why lite, and what a heavier version would add

Options for resolving multi-policy conflict:

- a) **Arbitration-lite (deterministic sequencing).** Tradeoff: it never plans
  ahead — it can pick a locally-correct action this cycle that a forecasting
  planner would skip because of where the cluster is heading.
- b) **Predictive planner (forecast post-action cluster state across policies,
  optimize globally).** Tradeoff: needs a cluster-state model and a forecasting
  loop, is far harder to test adversarially, and its failure modes (a confidently
  wrong forecast) are exactly the kind of high-uncertainty action Goal #6 tells us
  to reject.

We choose (a). v1 is sequencing only — priority, exclusion, global bounds,
autoscaler-defer — and that is enough to make multi-policy operation safe and
predictable. A **Phase 3 predictive planner** could sit on top, consuming the
same Candidate stream and adding forecasting before the arbiter's deterministic
rules; nothing in arbitration-lite forecloses it. But forecasting is not in v1,
and we will not pretend the lite arbiter does more than order actions.

### Coexisting with the autoscaler (OEP-0013)

Tl;dr: read-only, one direction. Alfred consults the autoscaler's intent before
acting and **never** writes into OEP-0013's surface. Two controllers, two jobs,
one boundary.

The division of labor, as a one-liner: **Alfred decides *where* pods live and
when to move or evacuate them; the autoscaler (OEP-0013) decides *how many*; the
scheduler decides *initial placement*; the operator/cloud repairs hardware.**
Each owns a different verb, and the verbs don't overlap.

The seam is deliberately asymmetric and minimal: Alfred *reads* OEP-0013's intent
(is this Component being actively scaled right now?) as the input to arbiter rule
(d), and that is the entire interaction. Alfred does not create, patch, or delete
ScaledObjects, HorizontalPodAutoscalers, or any OEP-0013-owned object; it does
not set replica counts; it does not write back any "I'm migrating this" flag into
the autoscaler's surface. *Rationale:* a read-only, one-directional seam has no
write-write race to reason about — the only failure mode is a stale read, and the
arbiter's safe default (treat unavailable/stale intent as "busy, defer") already
bounds that. The moment the seam became bidirectional — Alfred writing into the
autoscaler's state, or the autoscaler asking Alfred to move pods — we would own a
two-controller write contract with all the version-skew and ordering hazards that
implies. We are not building that. The autoscaler scales; Alfred places and
evacuates; they meet only at a single read.

See also: OEP-0013 (Component-Scoped Autoscaling) for the autoscaler-intent
surface Alfred reads. Open question (TBD): the exact shape of the
"actively-scaling" read — whether Alfred infers it from observed ScaledObject /
HPA status conditions and recent replica churn (no OEP-0013 cooperation needed,
works today) or OEP-0013 publishes an explicit intent marker (cleaner, requires
coordination). v1 uses the inference path because it needs nothing from OEP-0013;
an explicit marker is a Phase 2 refinement if the inference proves too noisy.

### Policy and configuration model

Tl;dr: one ConfigMap drives the whole caretaker; each Policy is enabled
independently; each workload can be gated independently; aggressiveness is a
scoring knob, not an on/off switch.

**Definitions.** A *Policy* is a pluggable producer of recommendations that
reads the shared `ClusterSnapshot` and emits ranked candidates — Policy #1 is
defragmentation, Policy #2 is node-health evacuation. A *recommendation* is a
proposed action (migrate this Instance, evacuate this node) carrying a
benefit/cost score and a target hint. Policies never act; they only propose.

Why a single ConfigMap rather than a CRD or per-Policy config objects: the
caretaker is an opt-in operational tool, and operators tune it the way they tune
cluster-autoscaler — edit one config, watch it reload, no API surface to
version. The cost: config is untyped YAML, so a schema-version field and a
last-known-good fallback do the validation work a CRD would get for free
(mechanism below).

Configured via the `alfred-config` ConfigMap. The schema extends the existing
`config.yaml` with a per-Policy block; the global scoring, rate-limiting, and
window keys stay where they were (they now feed the Arbiter — see
[Safety bounds](#safety-bounds)):

```yaml
config.yaml: |
  schemaVersion: 1

  # Overall mode
  mode: recommend-only    # recommend-only | execute

  # Loop cadence
  decisionLoopInterval: 5m
  observationLoopInterval: 30s
  earlyTickOn: [NodeConditionChange]  # adds a serialized early pass; periodic cadence is unchanged ([] disables)

  # Per-policy enable + tuning
  policies:
    defragmentation:
      enabled: true
      fragmentationThreshold: 0.25
      aggressiveness: balanced     # conservative | balanced | aggressive
      scoring:
        sizeLadder: [1, 2, 4, 8]      # within-node sizes; larger demands use fully-free node groups
        demandBlendLambda: 0.3        # lambda — 0: pure observed demand; 1: pure prior
        sizePrior:                    # static prior over demand sizes (scoring Step 2)
          "1": 0.1
          "2": 0.1
          "4": 0.2
          "8": 0.6
        pendingUrgencyTauMinutes: 30  # tau in the pending-pressure term
    nodeHealth:
      enabled: true
      aggressiveness: balanced
      # which node conditions trigger evacuation; consumes existing signals,
      # does not detect health itself (see Non-Goals).
      triggerConditions: [GpuUnhealthy]
      signalOnly: false            # true = signal only, never dispatch
      healthCooldownFloorMinutes: 5   # per-workload cooldown floor for NodeUnhealthy candidates
      nodeSuspicionWindowMinutes: 30  # evacuated nodes stay out of target hints, even after the condition clears

  # Per-workload defaults
  defaultMovable: true
  recentPlacementCooldownMinutes: 10  # authorship-blind placement cooldown, Arbiter-enforced (see Human intervention)

  # Execution surfaces (all annotation-mediated; the owning controllers execute)
  rawDeploymentMigrationEnabled: false # reserved; Raw is advisory until a consumer exists
  omenativeMigrationEnabled: true      # OMENative controller: Instance surge
  omenativeCapabilityLeaseName: ome-inferencereplica-executor
  # Omitted namespace follows Alfred's runtime namespace. Set it explicitly
  # only when the OME manager publishes the Lease in another namespace.
  # omenativeCapabilityLeaseNamespace: manager-system
  omenativeCapabilityMaxStaleness: 30s # minimum; absent/stale/incompatible = recommend-only
  lwsRecommendationsEnabled: true      # produce recommendations for LWS (never execute)

  # Output
  recommendationsConfigMapEnabled: true
  recommendationsConfigMapName: alfred-recommendations

  # Logging
  logLevel: info
  structuredLogging: true
```

The global safety, spot, multi-tenancy, and maintenance-window keys are
unchanged and documented in their own sections below; they are deliberately
*not* nested under `policies` because they are applied once, by the Arbiter,
across every Policy's output.

**Aggressiveness is a scoring knob, not a kill switch.** `conservative` raises
the benefit/cost threshold a candidate must clear before the Arbiter will admit
it; `aggressive` lowers it. It does not bypass cooldowns, rate limits, or the
capacity check — those are global and non-negotiable (see
[Safety bounds](#safety-bounds)). Naming the failure mode: if aggressiveness
*could* relax safety bounds, an operator chasing utilization would set
`aggressive` and silently disable the very gates that keep the caretaker from
churning the cluster. So it can't.

**Per-Policy enable + per-workload gating are orthogonal axes.** Disabling
`policies.nodeHealth` stops the caretaker from proposing evacuations for anyone;
annotating one workload `alfred.ome.io/movable: "false"` opts that workload out
of *all* Policies. An operator needs both: cluster-wide "don't run node-health
yet" and per-workload "never touch this one."

Per-workload overrides via InferenceService annotations:

```yaml
metadata:
  annotations:
    alfred.ome.io/movable: "false"                       # opt out of all policies
    alfred.ome.io/priority: "0.3"                        # lower = more protected
    alfred.ome.io/cooldown-minutes: "60"                 # per-workload override
    alfred.ome.io/opt-out-reason: "critical production"  # operator note
    alfred.ome.io/tenant-group: "team-alpha"             # for cross-tenant opt-in
```

Annotations win over ConfigMap defaults: the per-workload signal is closer to
the workload owner than the cluster-wide default, so it takes precedence.

**Reload semantics.** Alfred watches the ConfigMap and reloads on change. An
invalid `schemaVersion`, or a config that fails schema validation, triggers
fallback to last-known-good with an event on the ConfigMap and
`alfred_policy_reload_total{outcome="failure"}`. Failure mode being mitigated: a
fat-fingered edit must never leave the caretaker running with a half-parsed
policy — better to keep serving the previous known-good config and shout about
it than to act on garbage.

#### Opt-in / opt-out semantics

Default: opt-in per `defaultMovable: true`. A workload is eligible for *any*
Policy unless one of these holds:

- Annotation `alfred.ome.io/movable: "false"`.
- The workload is in cooldown (per-workload or per-node).
- The workload is in active migration.
- Its strategy is unsupported for execution — RawDeployment and LWS are
  recommendation-only; Serverless is not managed.
- It is backed by an RWO (or RWOP) PVC — the volume can attach to one node at a
  time, so no surge-shaped mechanism can move it; advisory-only
  (`MigrationSkippedVolumePinned`). Moving it means downtime, which stays a
  manual operator action — Alfred has no downtime-migration mode.

Operators preferring opt-in-only set `defaultMovable: false`; then a workload
needs `alfred.ome.io/movable: "true"` explicitly. Rationale: a conservative
cluster wants the caretaker silent until each workload owner has signed off,
even at the cost of the caretaker doing nothing on day one.

### Safety bounds

Tl;dr: the safety bounds are *global*, not per-Policy. Every recommendation —
defragmentation or node-health — passes through the same Arbiter gate before any
Dispatcher acts. Primum non nocere: the caretaker must never be the thing that
destabilizes the cluster.

**Why global, not per-Policy.** If each Policy enforced its own rate limit, two
Policies firing in the same loop could each stay under their own cap while
together exceeding the cluster's safe migration rate — node-health evacuating
three Instances while defragmentation migrates three more, six concurrent moves
when the cluster can absorb three. The failure mode is real: a node-health event
and a fragmentation threshold can trip at once. So the bounds live in the
Arbiter, which sees *all* Policies' output and enforces one budget across them.

**Definition.** The *Arbiter* is the single component that receives every
Policy's ranked recommendations, applies the global gates below in order, and
admits or rejects each candidate. A rejected candidate is recorded with its
drop reason — the Reporter emits the corresponding event and metric — and it
does not retry inside the loop.

The gates, applied by the Arbiter to the merged candidate stream:

- **In-flight cap (cluster-wide)**: default 3. The Arbiter tracks in-flight
  actions across OMENative and any future validated execution surface, across
  all Policies, and refuses to admit
  more.
- **Per-hour cap (cluster-wide)**: default 10 actions per rolling hour, summed
  over all Policies. Sizing note: the two caps are different dials — the
  in-flight cap bounds instantaneous parallelism (and keeps the
  in-flight-claims arithmetic tractable), while the hourly ledger bounds
  sustained churn, and at typical migration durations it, not the in-flight
  cap, is the binding long-run limit: a per-move regression check cannot see
  a treadmill, the ledger can. Size it for the largest legitimate burst you
  intend to tolerate — a full drain of the densest node plus routine defrag
  in the same hour (8-dense nodes with a few defrag moves per hour → ~12;
  the default 10 deliberately under-serves that, and widening is an explicit
  operator choice that also widens what a pathological loop can move before
  the circuit breaker trips). Treat any formula as a hypothesis, not a rule:
  calibrate from `alfred_migrations_*` after a few weeks of
  `recommend-only`, and weigh the consecutive-failures risk (risks section)
  when choosing.
- **Per-workload cooldown — class-aware**: default 30 minutes after the last
  action touching a workload, inclusive of failures. Health-evacuation
  candidates (`Reason: NodeUnhealthy`) are gated by a shorter clock,
  `healthCooldownFloorMinutes` (default 5), instead of the standard window: a
  workload defragmentation moved 10 minutes ago **is** eligible for a
  node-health evacuation — making it wait out a routine-optimization cooldown
  on failing hardware would be the wrong trade, and defrag makes the case
  likely rather than rare (consolidation packs recently-moved, cooldown-locked
  workloads onto one node, and GPU faults tend to surface exactly under the
  load transitions a fresh migration causes). The floor is still a real gate —
  inside it even health waits: the prior move is still settling, and the floor
  bounds churn under a false-positive detector to ~12 moves/hour before the
  global caps bind. An admission under the floor but inside the standard
  window emits `CooldownOverriddenForEvacuation`, recording the node, the
  condition, and how much standard cooldown was bypassed. The carve-out
  changes only when *this workload* may move again — the global caps and the
  circuit breaker apply to the admitted candidate unchanged.
- **Per-node cooldown — target-scoped**: default 10 minutes after an action
  lands on a node as a *target*, or leaves it as the source of a routine
  (defrag-class) move. A node under health evacuation is exempt as a source —
  the point is to drain it; its outflow is bounded by the in-flight cap
  instead.
- **Placement cooldown (authorship-blind)**: default 10 minutes
  (`recentPlacementCooldownMinutes`) after any pod of an Instance is
  scheduled, regardless of who placed it — operator, scheduler churn after a
  drain, or Alfred's own completed migration (harmlessly double-covered by
  `LastMigration`). Class-aware like the per-workload cooldown:
  health-evacuation candidates wait only the `healthCooldownFloorMinutes`
  floor. See [Human intervention](#human-intervention-and-incident-posture).
- **Terminating exclusion**: no action is admitted for an Instance with any
  pod carrying a `DeletionTimestamp`; past
  `max(2 × terminationGracePeriodSeconds, 5m)` of deletion age the Reporter
  emits a `StuckTerminating` advisory instead. Action never; silence never.
- **Capacity check**: before admitting a candidate, the Arbiter re-checks the
  `ClusterSnapshot` for a feasible target with enough contiguous free GPUs and a
  ready model copy. Every executable Alpha path is OMENative surge and therefore
  **place-then-free**: the complete replacement must fit while the source still
  holds its GPUs. The check is **net of in-flight claims**: GPUs that a
  still-running migration's replacement will occupy count as allocated, so two
  admitted candidates can never both "fit" into the same free block. A candidate
  scored against a stale snapshot whose target has since filled is rejected
  here, not dispatched into a guaranteed stall.
- **Circuit breaker**: if the recent-10-actions failure rate exceeds 50%, the
  Arbiter pauses *all* execution for 60 minutes and emits a critical event.
  Failure mode: a systematically bad target (a node that looks free but rejects
  scheduling) would otherwise let the caretaker thrash; the breaker stops the
  bleeding.
- **Dry-run mode**: `mode: recommend-only` disables execution entirely — Policies
  still produce recommendations, the Arbiter still scores and gates them, and
  the Reporter still publishes every outcome; only the Dispatcher never fires.
  Useful during rollout to watch what the caretaker *would* do.

These defaults are deliberately low. The caretaker optimizes a steady-state
property (fragmentation) and reacts to a rare event (node health); neither
justifies moving more than a handful of workloads per hour. An operator who
wants faster convergence raises the caps explicitly and owns the consequence.

One bound that is *not* the Arbiter's: maintenance windows. Defragmentation runs
only inside configured `maintenanceWindows` (default business hours UTC,
weekdays), because a steady-state optimization can wait. Node-health evacuation
overrides the window — a failing accelerator is not a "wait until Monday"
problem. `emergencyPendingAgeMinutes` (default 10) likewise overrides the window
for defragmentation when a pending pod has starved past the threshold. The
window is a per-Policy urgency policy; the rate limits, cooldowns, and capacity
check above are global.

### Concurrent-operation awareness

The caretaker is one of several actors moving pods around. Acting blind to the
others is how it picks a target the Cluster Autoscaler is about to delete, or
migrates an ISVC the HPA is mid-scale on. Each is a concrete defer-or-exclude
rule, applied by the Arbiter regardless of which Policy proposed the candidate:

- **HPA**: the Arbiter checks HPA status conditions / scale history for the
  target InferenceService. If the HPA scaled within the last 2 minutes, that
  ISVC is deferred — migrating a workload mid-scale fights the autoscaler and
  produces churn neither actor wanted.
- **Cluster Autoscaler**: nodes annotated
  `cluster-autoscaler.kubernetes.io/scale-down-disabled: "true"` are excluded
  from placement hints. Nodes in active CA scale-down (detected via CA's
  deletion-candidate label) are also excluded — placing a workload on a node the
  CA is about to drain guarantees a second move.
- **In-flight migrations**: a pending request annotation, a non-terminal
  `InferenceReplica.Status.Migrations` entry, or a non-terminal workload-audit
  ledger row makes the affected workload busy. Alfred deduplicates these
  surfaces by request UUID and defers new actions. For OMENative, the
  InferenceReplica entry is authoritative after request acceptance.
- **Node maintenance**: nodes carrying common maintenance taints
  (`node.kubernetes.io/unschedulable`, custom `ome.io/maintenance`) are excluded
  as targets.

Note that node-health evacuation is itself a node-maintenance-adjacent action;
the awareness rules apply to its *targets*, not its source — the whole point of
Policy #2 is to leave the unhealthy source.

### Human intervention and incident posture

The operator is the one concurrent actor the previous section does not name,
and the rule for them is different in kind: Alfred defers to controllers
case-by-case, but it must *never need to be stopped* for a human to act
safely. Manual changes — `kubectl drain`, cordons, deletions, hand-placed
pods — are just cluster state: informers absorb them in seconds, the next
pass builds its snapshot from the post-surgery reality, and because
Candidates are re-derived every pass and never queued, whatever the human
already fixed simply stops producing candidates. There is no invalidation
problem because there is nothing to invalidate.

**Mid-flight collisions degrade safely, by layers.** The window that matters
is a dispatch from pass N executing after the human changed the world. Four
independent mechanisms bound it: hints are advisory and the scheduler honors
a cordon instantly, so Alfred cannot push a pod onto a node the operator
just closed; the annotation contract re-validates at execution time, so a
stale decision becomes a terminally failed request (for example,
`MigrationFromNodeMismatch` or a capacity gate), not a wrong action; a failed action puts its
workload in cooldown, backing Alfred off the exact objects under human
surgery; and mass failures of in-flight actions trip the circuit breaker —
Alfred pauses itself for an hour, a de facto auto-yield to whoever is
operating. Note the breaker's limit: it sees *failures*, not interference —
collisions that succeed (Alfred busily re-consolidating what the operator
just spread) never trip it, which is why the exclusions below and the
explicit quiesce exist.

**Two Arbiter-enforced exclusions make coexistence quiet, not merely
convergent.** Both are policy-blind — enforced once, at the Arbiter, on the
combined candidate stream, like every other safety bound; policies may
pre-filter as an optimization but the Arbiter is the guarantee:

- **Placement cooldown, authorship-blind.** Any Instance with a pod
  scheduled within `recentPlacementCooldownMinutes` (default 10) is
  excluded, regardless of who placed it. Without this, a pod the operator
  just carefully placed looks freshly movable, post-drain spread looks like
  fragmentation, and Alfred starts "cleaning up" the operator's work
  mid-incident. Class-aware like the per-workload cooldown: routine
  candidates wait the full window; health-evacuation candidates wait only
  the `healthCooldownFloorMinutes` floor — a fresh placement is still
  settling, but a settling pod on failing hardware should not wait out a
  routine-optimization window. Ten minutes guards settling, not thrash,
  which is why it is shorter than the 30-minute action cooldown.
- **Never act on anything already dying.** An Instance with any pod
  carrying a `DeletionTimestamp` produces no admitted action — this is what
  stops Policy #2 from racing a human drain of the same node with duplicate
  disruption. The exclusion is absolute for *action* (Alfred's contracts
  cannot even express force-delete, by design), but not for *observation*:
  past `max(2 × terminationGracePeriodSeconds, 5m)` of deletion age the
  Reporter emits a `StuckTerminating` advisory on the InferenceService — a
  wedged finalizer is a human's problem, and silence about it was never the
  intent.

**Quiescing actuation is a config flip, not a process stop:**

| Tool | Effect | When |
|------|--------|------|
| `mode: recommend-only` | Dispatcher off; Policies, Arbiter, Reporter keep running | the incident default — the only flip that also quiesces evacuation |
| Narrow or clear `maintenanceWindows` | defragmentation stops (it dispatches only inside windows); evacuation deliberately overrides windows | suppress routine churn, keep evacuations |
| `policies.<name>.enabled: false` | disables one policy entirely | e.g. defrag off, evacuation on |
| `Movable=false` annotation | per-workload shield | targeted protection; too much toil at mass scale |

Runbook: `kubectl -n <alfred-namespace> edit configmap alfred-config` →
`mode: recommend-only`; it takes effect on the next pass, no restart. Do
**not** scale Alfred to zero: killing the process kills the Reporter, and
the Reporter is most valuable mid-incident — the observation loop keeps
gauges flowing and the decision loop keeps publishing what Alfred *would*
do, a running second opinion while you operate ("Alfred also thinks these
six workloads should leave node B, in this order"). Recommend-only turns
Alfred from actor into incident advisor.

**Recovery is re-derivation.** When surgery ends, flip the mode back: the
next pass re-derives everything from the post-surgery world, and the budget
and cooldown ledgers reconstruct from cluster history exactly as on leader
failover — no restart, no state cleanup, no memory of the interruption.

### Spot and preemptible nodes

A spot/preemptible node can vanish on the cloud provider's schedule, so the
caretaker should never *land* a stable workload there and should *prefer* to
evacuate one already there before the provider preempts it. Policy-driven:

```yaml
spotPolicy:
  avoidAsTarget: true
  preferAsSource: true
  preemptibleLabels: [node.kubernetes.io/preemptible, cloud.google.com/gke-preemptible]
```

**Detection**: the caretaker matches each node's labels against
`spotPolicy.preemptibleLabels` (and, optionally, the standard K8s
`node.kubernetes.io/preemptible` label).

**Behavior**:

- `avoidAsTarget: true` excludes preemptible nodes from `hint_target_nodes`.
  Stable workloads land on non-preemptible nodes.
- `preferAsSource: true` boosts the priority of workloads on preemptible nodes
  so they evacuate before a preemption event, not after.
- Per-workload override via `alfred.ome.io/spot-policy: avoid|migrate|ignore`.

This is a scoring input shared by both Policies: defragmentation prefers spot
nodes as evacuation sources, and node-health treats a failing spot node the same
as any failing node — it just gets there first because the spot boost already
raised its priority.

### Multi-tenancy

Tenant boundary = namespace by default. The caretaker's recommendations and
actions are scoped to the ISVC's own namespace: it does not migrate workload A
in `ns1` to benefit workload B in `ns2`. Rationale: cross-tenant moves let one
team's optimization disrupt another team's workload, which no operator wants as
a silent default.

Operators with cross-tenant intent set `alfred.ome.io/tenant-group: <name>` on
ISVCs across namespaces; the caretaker treats same-group ISVCs as
cross-migratable (still subject to every other filter). The hard override
`allowCrossTenantOptimization: false` makes the namespace boundary absolute even
when tenant-group annotations are present — a cluster admin can veto cross-tenant
moves regardless of what individual teams annotate.

### Model-download coordination

The caretaker must not migrate a workload to a node that lacks the model — the
pod would land and immediately block on a multi-gigabyte download, turning a
migration into an outage. So, for per-node models, placement hints filter by
`BaseModel.Status.NodesReady` / `ClusterBaseModel.Status.NodesReady` (OEP-0007
Q-017). If no ready node is feasible as a target:

- Skip the candidate.
- Emit event `NoFeasibleTarget` on the ISVC with the reason.
- Do *not* trigger a pre-download in v1. The operator pre-provisions the model
  (via BaseModel / ClusterBaseModel distribution) before expecting migrations to
  that node.

This applies to both Policies: a node-health evacuation that has nowhere with a
ready model copy is skipped and surfaced, not forced. Anticipated objection: "a
failing node with no migration target leaves the workload stranded." Honest
answer for v1 — yes; the caretaker signals `NoFeasibleTarget` and the owning
controller's normal recovery applies. Auto-triggered pre-download (the caretaker
asking model-agent to distribute the model to a target before retrying) is a
future iteration, out of scope for v1.

**PVC-backed models are the exception on both counts.** They intentionally
never populate `NodesReady` — there is no per-node copy, and the ISVC
controller also skips the readiness `nodeSelector` for them — so the readiness
filter must not apply; applying it would make every PVC-backed workload
permanently `NoFeasibleTarget`. Their real constraint is volume reachability.
For **RWX or ROX** storage (shared filesystems, or read-only-many volumes —
model weights are read-only at inference time, so ROX behaves exactly like RWX
here), any node satisfying the PVC's CSI topology is a valid target and no
model pull is ever needed — these are, if anything, the *easiest* workloads to
migrate. For **RWO** (or `ReadWriteOncePod`) storage, the volume attaches to
one node at a time and the source pod still holds the attachment while a surge
replacement starts, so surge-shaped migration cannot work — the replacement
would sit in `ContainerCreating` on a Multi-Attach error until timeout.
RWO-backed workloads are therefore advisory-only, skipped with
`MigrationSkippedVolumePinned`, and Alfred offers **no downtime-migration
mode, not even opt-in**: moving one means downtime, and that stays an operator
decision, performed manually outside Alfred.

### Degraded mode

The caretaker executes migrations only through a confirmed lifecycle-owner
consumer. In this phase that consumer is OMENative. CRD discovery alone is not
an availability proof: a cluster can retain the InferenceReplica CRD while the
controller is disabled or unavailable, and a current status object can outlive
the process that wrote it. Alfred establishes execution readiness from **both**:

1. a readable InferenceReplica whose observed generation is current and whose
   status representation is valid; and
2. a fresh `ome-inferencereplica-executor` capability Lease carrying a supported
   migration wire version, renewed only while that controller is enabled and
   cache-synced.

A missing, stale, or incompatible input fails closed. The capability Lease is a
shipped Alpha prerequisite: the manager publisher renews it every 10 seconds,
and Alfred performs a direct uncached read during every observation. Alfred
accepts only schema `v1`, a nonempty holder identity, and a present `renewTime`
whose age does not exceed `omenativeCapabilityMaxStaleness`. The default and
minimum accepted maximum age is 30 seconds, preserving three renewal periods;
shorter settings are rejected instead of making a healthy executor flap into
degraded mode. A timestamp up to and including five seconds in the future is
accepted; greater future skew, like a missing, stale, or incompatible Lease,
fails closed. The future Dispatcher must perform another direct read
immediately before its patch; that just-in-time recheck is not implemented yet.

The target Lease is namespaced and defaults to Alfred's runtime namespace
(`ome/ome-inferencereplica-executor` in the shipped manifests). A split-namespace
installation must set `omenativeCapabilityLeaseNamespace` to the OME manager's
namespace explicitly. The Lease carries
`ome.io/migration-request-schema: v1`. Its holder identity names the active OME
manager replica. Alfred considers it fresh only when `renewTime` is present and
no older than `omenativeCapabilityMaxStaleness`; it does not infer freshness
from the Lease object's resource version. The OME manager stops renewing when
the InferenceReplica controller is disabled, not cache-synced, or shutting down.

**If OMENative execution readiness cannot be established:**

- OMENative Instances run recommend-only, even those annotated `movable: "true"`.
- RawDeployment and LWS remain recommend-only, as they do in the normal Alpha
  configuration; neither is an execution fallback.
- `alfred_omenative_unavailable` gauge is set to 1.
- Event `OMENativeUnavailable` is emitted on the *transition* into degraded
  mode, not on every decision loop — otherwise the event stream becomes noise.
- ConfigMap `omenativeMigrationEnabled: true` is a no-op while degraded.

Both Policies degrade the same way: node-health evacuation of an OMENative
Instance also requires the executor, so in degraded mode it falls back to
recommend-only and signals the operator rather than acting.

### Deployment model

Ships as:

- Binary: `cmd/alfred/main.go` → `alfred` container image.
- Helm chart: `charts/ome-alfred/` (or a sub-chart of `ome-resources`).
- Deployment: 3 replicas, leader election enabled.
- ServiceAccount, namespace Role/RoleBinding, and ClusterRole/ClusterRoleBinding
  per [RBAC](#rbac) below.
- ConfigMap `alfred-config` with default values.

The caretaker installs alongside OME (same Helm release) or independently; it is
a separate Deployment, not embedded in the main OME manager, so it has an
independent release cadence, failure domain, and resource footprint. Hard
dependencies:

- OME `InferenceService` CRD installed.
- A fresh, valid InferenceReplica status surface for OMENative execution
  (otherwise degraded mode — see above).
- A fresh, compatible OMENative executor capability Lease for execution
  (otherwise degraded mode — see above).

### Leader election

`client-go/tools/leaderelection` with a Lease resource:

- Lease duration: 15s
- Renew deadline: 10s
- Retry period: 2s

The deployment pre-creates the spec-less `alfred.ome.io` Lease. Alfred's
namespace Role grants `get` and `update` on that resource name only; it does not
grant `create`.

Only the leader runs the decision loop — Policies, Arbiter, and Dispatchers — so
exactly one replica is acting on the cluster at a time. All replicas run the
observation loop, so Prometheus can scrape any replica for snapshot-derived
gauges. This mirrors cluster-autoscaler's pattern.

On leader loss, in-flight action tracking is transient state; the new leader
rebuilds it from pending migration-request annotations,
`InferenceReplica.Status.Migrations`, and the workload audit ledger, deduplicated
by request UUID. The recommendations ConfigMap is an operator-facing record, not
an authoritative migration ledger. Failure mode
being mitigated: a leader handoff must not double-dispatch an in-flight
migration, so the global in-flight cap is reconstructed from durable cluster
state, not from the dead leader's memory.

### RBAC

Tl;dr: the caretaker re-scope adds *no* new write permissions — and the unified
annotation path *removes* one: `pods/eviction` is gone from the ClusterRole,
because every pod-level action is executed by the owning controllers. Policy #2
is **evacuate + signal** — it migrates via the existing migration-request
annotation and otherwise emits an Event and Alfred-owned record for the node
remediation owner. In particular:
**no node-write permission and no pod-write permission.** The caretaker never
cordons, drains, patches a Node, or evicts a pod; it reads cluster state and
requests moves on workloads.

The current Alpha manifest deliberately omits `inferenceservices` patch access
while the Dispatcher and its mandatory admission guard are absent. The target
effective namespace Role plus ClusterRole below applies when those two pieces
land atomically; it is the defragmenter scope minus the eviction verb:

```yaml
# Nodes (read-only) — no write, no cordon, no drain
- apiGroups: [""]
  resources: [nodes]
  verbs: [get, list, watch]

# Pods (read-only) — Alfred performs no pod-level action
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]

# Model-volume topology (read-only)
- apiGroups: [""]
  resources: [persistentvolumeclaims, persistentvolumes]
  verbs: [get, list, watch]

# ConfigMaps (read for policy loading, write only to pre-created Alfred-owned ConfigMaps)
- apiGroups: [""]
  resources: [configmaps]
  verbs: [get, list, watch]
- apiGroups: [""]
  resources: [configmaps]
  verbs: [update, patch, delete]
  resourceNames:
    - alfred-config
    - alfred-recommendations

# Events
- apiGroups: [""]
  resources: [events]
  verbs: [create, patch]

# Leader election (Alfred namespace Role)
- apiGroups: [coordination.k8s.io]
  resources: [leases]
  resourceNames: [alfred.ome.io]
  verbs: [get, update]

# OMENative executor capability (read-only, configured namespace)
- apiGroups: [coordination.k8s.io]
  resources: [leases]
  resourceNames: [ome-inferencereplica-executor]
  verbs: [get]

# OME CRDs (read)
- apiGroups: [ome.io]
  resources:
    - inferenceservices
    - inferenceservices/status
    - inferencereplicas
    - inferencereplicas/status
    - servingruntimes
    - clusterservingruntimes
    - basemodels
    - clusterbasemodels
  verbs: [get, list, watch]

# OME InferenceService annotation patching (narrow contract)
- apiGroups: [ome.io]
  resources: [inferenceservices]
  verbs: [patch]
```

The write contracts in detail — each is the *only* mutation the caretaker can
perform on that resource:

| Resource | Verbs | What the write can do | Why no broader grant |
|---|---|---|---|
| `nodes` | get, list, watch | nothing — read-only | Policy #2 evacuates *workloads*, it never touches the Node object; node cordon/drain belongs to maintenance tooling, not the caretaker |
| `pods` | get, list, watch | nothing — read-only | physical placement and current readiness; Alfred never performs pod-level lifecycle actions |
| `persistentvolumeclaims`, `persistentvolumes` | get, list, watch | nothing — read-only | model-volume access modes and topology for placement feasibility |
| `inferencereplicas` | get, list, watch | nothing — read-only | stable OMENative Instance identity, lifecycle state, and authoritative migration status |
| OMENative capability `lease` | get | nothing — read-only | proves that a compatible InferenceReplica executor is currently enabled and renewing |
| `inferenceservices` | patch | add/retry one `ome.io/migration-request-v1-*` annotation only | the consuming controller owns acknowledgement deletion; Alfred's patch must not touch spec, status, labels, finalizers, or other annotations (enforced cluster-side, below) |
| `configmaps` (named) | update, patch, delete | mutate only `alfred-config` / `alfred-recommendations` | the caretaker does not create ConfigMaps at runtime; Helm pre-creates them |
| `events` | create, patch | emit observability events | events are the audit trail; no other side effect |
| leader-election `lease` (named) | get, update | renew the pre-created spec-less `alfred.ome.io` Lease | no create or access to other Leases |

**Why the re-scope needs nothing new — and drops a verb.** Node-health
evacuation could, in a naive design, demand `nodes` write (to cordon the failing
node) and `pods` delete (to force the workload off). It demands neither:
evacuation writes the same `migration-request` annotation as defragmentation,
and the owning controller performs the pod-level actions under authority it
already holds as workload owner. RawDeployment and LWS remain advisory until
such a consumer exists. Alfred holds **no pod-level write at all**. The narrow
surface that
bounded the defragmenter's blast radius bounds the caretaker's even more
tightly. The RBAC table above is the security contract; adding node-write or
pod-write to it would be the moment the caretaker stops being safe-by-design.

**Authorization boundary.** The dedicated Alfred service account is the only
principal allowed to add or retry an OMENative migration request in v1. The
configured OME manager service account must be allowed to delete a request after
the InferenceReplica controller has persisted its acknowledgement. No other
principal may add, change, or delete a migration annotation, even if generic
InferenceService patch RBAC exists.

**RBAC invariant.** The caretaker's only allowed `patch` effect on
`InferenceService` is to add or retry one
`ome.io/migration-request-v1-<uuid>` annotation. A retry may preserve or replace
only that UUID's identical canonical payload; Alfred does not acknowledge its
own request by deleting it. An internal patch-gateway helper guards this at
runtime, but it is *not* the primary security boundary.

**Mandatory cluster-side enforcement.** Execute mode requires a
`ValidatingAdmissionPolicy` (K8s 1.30+) installed by the Helm chart. If the
policy or binding is absent, the caretaker must refuse to start in
`mode: execute` and fall back to recommend-only. The reference policy object
belongs in the Helm chart template
(`charts/ome-alfred/templates/admission-policy.yaml`, or the equivalent subtree
under `charts/ome-resources/` if packaged there). The policy must enforce three
invariants:

1. Only Alfred's service account may add or retry
   `ome.io/migration-request-v1-*` annotations, and a retry must keep the same
   UUID and canonical payload.
2. Only the configured OME manager service account may delete a consumed
   migration annotation; that acknowledgement update may not add or alter a
   migration request.
3. Neither service account may change `spec`, `status`, labels, finalizers,
   owner references, or non-migration annotations as part of its permitted
   mailbox write.

The exact CEL expression is implementation detail and must be covered by
negative integration tests; the design contract is the enforcement behavior
above, not a sample snippet. The policy applies to both the OME manager's
full-object acknowledgement `UPDATE` and Alfred's `PATCH`. The latter must be
validated against both JSON merge patch and JSON Patch semantics. If the chosen
admission checks cannot soundly prove the invariants above for a patch type the
caretaker might send, execute mode must reject that patch type rather than rely
on ambiguous CEL behavior.

**ConfigMap write boundary.** The caretaker does not create ConfigMaps at
runtime. Helm pre-creates `alfred-config` and, if recommendation snapshots are
enabled, `alfred-recommendations`; the caretaker only updates/patches/deletes
those named objects.

### Observability

Tl;dr: every recommendation metric now carries a `policy` label so operators can
tell defragmentation churn from node-health churn at a glance; the fragmentation
gauges are re-keyed by hardware pool and size (observed / reclaimable /
pending pressure — see Fragmentation scoring); migration metrics are unchanged;
two node-health counters are added. Events
and the recommendations ConfigMap stay as the human-readable surfaces. Every
decision-side surface in this section is emitted by the engine's single
Reporter stage — policies and the Arbiter produce values and recorded reasons;
only the Reporter writes. The snapshot-derived gauges come from the observation
loop.

**Why the `policy` label.** With two Policies feeding one Arbiter, an
undifferentiated `alfred_recommendations_produced_total` can't answer "is the
caretaker moving pods because of fragmentation or because nodes are failing?" —
the single most important operational question. So the recommendation and
outcome counters gain `policy="defragmentation"|"nodehealth"`. The
snapshot-derived gauges (fragmentation scores, capacity) are inherently
Policy-agnostic; capacity gauges keep their existing labels, and the scoring
gauges carry the per-pool/per-size keys defined in Fragmentation scoring.

**Prometheus metrics**:

Snapshot / fragmentation gauges:

- `alfred_cluster_fragmentation_score` (gauge, 0-1; the gate value — max over pools of the combined score)
- `alfred_fragmentation_observed{pool,size}` (gauge; `Frag(c,s)` — which size in which pool is blocked)
- `alfred_fragmentation_reclaimable{pool}` (gauge; the share migration could fix — observed minus reclaimable is the operator-only, advisory-stream gap)
- `alfred_pending_pressure{pool}` (gauge; `P(c)`)
- `alfred_gpu_capacity{node,status}` (gauge; status: total/allocated/free/contiguous_max)
- `alfred_pending_pod_count` (gauge)
- `alfred_pending_pod_gpu_requirements{size}` (gauge)
- `alfred_surge_headroom_gpus{pool}` (gauge, new; the largest replacement footprint that could surge right now, per hardware pool — 0 means surge-shaped migration is infeasible and such candidates degrade to advisory)

Recommendation / migration counters (now `policy`-labeled):

- `alfred_recommendations_produced_total{policy,workload,component,reason,executable}` (counter)
- `alfred_recommendations_accepted_total{policy,workload,component}` (counter)
- `alfred_recommendations_rejected_total{policy,workload,component,reason}` (counter)
- `alfred_migration_calls_total{policy,workload,mode,surface}` (counter; `surface=omenative` in Alpha; future validated consumers add values without changing the annotation contract)
- `alfred_migration_outcome_total{policy,workload,mode,outcome}` (counter; outcome: completed/failed/timeout)
- `alfred_lws_recommendations_total{isvc,action}` (counter; action: manual)

Node-health counters (new):

- `alfred_nodehealth_evacuations_total{node,workload,surface,outcome}` (counter; the count of evacuation actions Policy #2 actually dispatched)
- `alfred_nodehealth_signals_total{node,reason}` (counter; the count of signal-only outcomes — degraded mode, no feasible target, or `signalOnly: true` — where the caretaker emitted a signal instead of acting)
- `alfred_cooldown_overrides_total{policy}` (counter; admissions under the health cooldown floor — each also emits `CooldownOverriddenForEvacuation`)

The split between `_evacuations_total` and `_signals_total` is the load-bearing
distinction: an evacuation moved a workload; a signal punted to the owning
remediation system. An operator watching `_signals_total` climb while
`_evacuations_total` stays flat knows the caretaker *wants* to act but can't —
no target, or degraded mode — which is exactly when a human should look.

Loop / operational metrics (unchanged):

- `alfred_observation_loop_duration_seconds` (histogram)
- `alfred_decision_loop_duration_seconds` (histogram)
- `alfred_leader_status{pod}` (gauge; 0/1)
- `alfred_policy_reload_total{outcome}` (counter; outcome: success/failure)
- `alfred_circuit_breaker_state` (gauge; 0: closed/1: open)
- `alfred_omenative_unavailable` (gauge; 0/1)

**Events** on InferenceService:

- `FragmentationRecommendationProduced`
- `MigrationRequested`, `MigrationCompleted`, `MigrationFailed`, `MigrationRejected`
- `MigrationSkippedOptOut`
- `MigrationSkippedMaintenanceWindow`
- `MigrationSkippedCooldown`
- `MigrationSkippedRateLimit`
- `MigrationSkippedVolumePinned` (RWO/RWOP PVC — no surge-shaped mechanism can move the workload; relocation is a manual, operator-only action)
- `RawDeploymentMigrationUnsupported`
- `LWSMigrationUnsupported`
- `NoFeasibleTarget`
- `NoSurgeHeadroom` (surge-shaped candidate downgraded to advisory — no target can hold the replacement while the source still runs)
- `NodeHealthEvacuationRequested` (Policy #2 dispatched an evacuation for this workload)
- `CooldownOverriddenForEvacuation` (health evacuation admitted inside the standard per-workload cooldown; records the node, the condition, and how much cooldown was bypassed)

**Events** on Alfred's own ConfigMap:

- `PolicyReloadFailed`
- `CircuitBreakerOpened` / `CircuitBreakerClosed`

**Events** cluster-wide:

- `OMENativeUnavailable`

**Events** on Nodes:

- `NodeRepairNeeded`
- `NodeDrainedForRepair`

**Logs**: structured JSON. Every multi-step operation carries a correlation ID
(recommendation UUID) so a single migration can be traced from Policy through
Arbiter to Dispatcher.

**Recommendations ConfigMap (optional)**: if `recommendationsConfigMapEnabled: true`, the
Reporter maintains the `alfred-recommendations` ConfigMap in Alfred's namespace —
a single structured snapshot operators can read without scraping events. Each
recommendation now records which Policy produced it:

```yaml
data:
  recommendations.json: |
    {
      "generated_at": "2026-04-16T12:00:00Z",
      "cluster_fragmentation_score": 0.62,
      "threshold": 0.5,
      "recommendations": [
        {
          "id": "rec-001",
          "policy": "defragmentation",
          "workload": "prod/llama-70b",
          "component": "engine",
          "instance": 0,
          "from_node": "node1",
          "hint_target_nodes": ["node3", "node7"],
          "benefit_score": 0.18,
          "cost_score": 0.1,
          "final_score": 0.08,
          "reason": "fragmentation",
          "executable": true,
          "strategy": "OMENative",
          "status": "accepted"
        }
      ]
    }
```

## Test plan

Tl;dr: the old suite covered one policy (Defrag) end-to-end; the engine
refactor adds three things that can break independently — the **Arbiter**
(cross-policy conflict resolution), **Node-Health Evacuation** (a second
policy that actuates), and the **OEP-0013 read-only seam** (Alfred must not
fight Component-Scoped Autoscaling). Each gets its own coverage below, on top
of the existing Defrag tests, which are preserved verbatim because Defrag is
now Policy #1, not a special case.

Terminology used throughout this section:

- **Policy** : a pluggable unit implementing the `Policy` interface
  (`Evaluate(snapshot) -> []Candidate`). Defrag is Policy #1, Node-Health
  Evacuation is Policy #2, Descheduling is Policy #3.
- **Candidate** : a policy's request to act on a (workload, target) — the
  rename of the old single-policy "Recommendation", generalized so the
  Arbiter can compare candidates from different policies.
- **Arbiter** : the engine stage that takes all candidates for one cycle,
  resolves conflicts, enforces mutual exclusion, and emits at most one action
  per (workload, node).
- **Reporter** : the engine stage that emits every Event, decision metric, and
  recommendations-ConfigMap entry, from the other stages' returned values and
  recorded reasons. Advisory candidates (`Executable=false`) route to it
  directly, bypassing the Arbiter.

### Unit tests

Target coverage per package (engine refactor splits the old monolith; the
new packages carry their own thresholds):

- `engine/arbiter`: ≥ 90% — new; the conflict-resolution logic is the highest-risk addition.
- `engine/reporter`: ≥ 85% — new; every candidate outcome must surface exactly once, and only through this stage.
- `policy/defrag`: ≥ 90% — the old `scorer`/`recommender` logic, ported behind the `Policy` interface.
- `policy/nodehealth`: ≥ 90% — new.
- `policy/descheduling`: ≥ 90% — new, lands in Phase 2.
- `observer`: ≥ 90%
- `executor`: ≥ 85%
- `metrics`: ≥ 80%
- `controller`: ≥ 80%

Synthetic cluster snapshot harness for unit testing without real K8s. The
harness is shared across all policies and the arbiter, because they all
consume the same immutable snapshot — see Alternative 6 for why that sharing
is load-bearing.

New unit coverage for the engine refactor:

1. **Arbiter priority ordering.** Two policies propose actions on disjoint
   workloads; verify the Arbiter emits both, ordered by the configured policy
   priority (Node-Health > Defrag > Descheduling). *Failure mode if absent:* a
   low-priority cosmetic defrag could be scheduled ahead of an urgent
   node-health evacuation, delaying evacuation off a failing GPU.
2. **Arbiter mutual exclusion on a workload.** Defrag and Node-Health both
   propose an action on the **same workload** in one cycle; verify exactly one
   action is emitted (the higher-priority one) and the loser is dropped with a
   recorded reason. *Failure mode if absent:* two policies move the same
   workload at once — double migration, corrupted lifecycle.
3. **Arbiter mutual exclusion on a node.** Two policies target the **same
   node** as a migration destination such that the node would be over-committed
   if both ran; verify the Arbiter serializes or drops to keep the node within
   capacity.
4. **OEP-0013 skip.** A workload's Component is mid-scaling per the OEP-0013
   read-only seam (autoscaler is actively changing replica count); verify every
   policy's candidate against that workload is filtered before it reaches the
   Arbiter. *Failure mode if absent:* Alfred migrates a pod the autoscaler is
   simultaneously scaling — the two controllers thrash.
5. **Node-health candidate selection.** Given a snapshot with one node carrying
   a `GpuUnhealthy=True` condition, verify Node-Health Evacuation identifies
   every OME occupant on that node, marks only eligible OMENative Instances
   executable, and never selects workloads on healthy nodes. Verify `Unknown`
   quarantines and signals without evacuation, recent `False` remains suspect,
   and only expired suspicion becomes clear.
6. **Node-health returns evacuate + signal.** Verify the policy returns both an
   evacuation candidate **and** the advisory remediation-signal candidate that
   the Reporter turns into the Node Event (Phase 1; the pluggable cloud
   node-remediation provider is Phase 3). The two are coupled: evacuating
   without signalling leaves a bad node in service for new pods.
7. **Advisory candidates bypass arbitration but are always reported.** A policy
   returns an `Executable=false` Candidate (a Raw/LWS-backed defrag opportunity,
   or a node remediation signal); verify it consumes no rate-limit budget,
   starts no cooldown, never reaches the Dispatcher — and the Reporter still
   emits its Event and metric. *Failure mode if absent:* benefit-cost admission
   silently drops the advisory before an operator ever sees it, or an inert
   recommendation debits the migration budget real actions need.
8. **Surge feasibility is place-then-free.** Given a snapshot where the target
   fits the Instance only if the source is freed first, verify a surge-shaped
   OMENative candidate is downgraded
   to advisory with `NoSurgeHeadroom` and never dispatched; given genuine
   headroom, verify it is executable. Verify Raw and LWS candidates remain
   advisory regardless of apparent headroom, and that in-flight OMENative surge
   claims are subtracted from available capacity. *Failure mode if absent:* Alfred
   dispatches migrations that stall in `SurgePending` on exactly the clusters
   that most need defragmentation.
9. **Health cooldown floor.** A workload defrag-moved at T+0 receives a
   health-evacuation candidate: verify rejection at T+4 (inside the floor),
   admission at T+6 with `CooldownOverriddenForEvacuation` emitted — and that
   a *defrag* candidate at T+6 is still rejected until T+30. Verify one
   evacuation wave per flapping node (the node evacuation record, not the
   workload cooldown, is the guard) and that a node inside its suspicion
   window is excluded from every policy's target hints. *Failure mode if
   absent:* workloads sit on failing GPUs waiting out a routine-optimization
   cooldown, or a flapping node pumps evacuate/refill cycles.
10. **InferenceReplica normalization and eligibility.** Decode equivalent
    DenseV1 and ColumnarV2 status into the same stable, sparse Instance set.
    Join live Pods by Instance index and incarnation, and derive current
    placement/readiness from those Pods rather than compatibility-only status
    fields. Verify single-pod `ActiveOrdinal` selection and complete multi-pod
    runner membership. Reject stale generation, invalid encoding,
    label/incarnation mismatch, incomplete Pods, paused or migration-disabled components,
    partially ready/serving Instances, active operations, and rollout/scale
    transitions as advisory-only.
11. **Migration-state reconstruction.** Present the same UUID in the request
    annotation, `InferenceReplica.Status.Migrations`, and the workload audit
    ledger; verify it counts once, the InferenceReplica phase wins after
    acceptance, and terminal audit history survives status pruning and leader
    failover.
12. **Fresh early health decision.** Verify a node-condition early tick performs
    `refresh → publish → evaluate`, a refresh failure performs no decision,
    concurrent refreshes serialize, and the early pass does not reset the
    regular decision cadence.
13. **Executor capability fails closed.** With a valid CRD and current
    InferenceReplica status, verify an absent, stale, or wire-incompatible
    OMENative capability Lease still produces advisory Candidates only; a fresh
    compatible Lease enables otherwise eligible Candidates.

The existing single-policy unit expectations (fragmentation scoring, threshold
gating, placement-hint computation, cooldown, rate limiting) remain under
`policy/defrag`, augmented by the compatibility cases above.

### Integration tests

The target integration suite deploys Alfred in a kind cluster and verifies the
following behavior end to end:

1. **End-to-end observation**: deploy Alfred in a kind cluster, deploy mock
   InferenceServices (via test CRDs), verify
   `alfred_cluster_fragmentation_score` changes with workload deployments.
2. **Recommendation generation**: construct a 3-single-GPU-on-3-nodes
   scenario. Verify Alfred (Defrag policy) produces 2 consolidation candidates.
3. **Pending-pod prioritization**: create a Pending 8-GPU pod; verify Alfred
   boosts candidates that would unblock it.
4. **RawDeployment recommendation-only**: in both `recommend-only` and
   `execute` configuration, verify Alfred reports the candidate but writes no
   migration-request annotation because no Raw consumer exists.
5. **OMENative migration invocation**: OMENative workload; verify Alfred
   writes `ome.io/migration-request-v1-<uuid>` on the InferenceService. Mock
   OMENative accepts it into `InferenceReplica.Status.Migrations`, persists the
   workload audit row, and clears the annotation. Verify Alfred follows the UUID
   to its terminal phase and applies cooldown once.
6. **LWS recommendation-only**: LWS-backed workload; verify recommendation
   emitted but no migration request; event reflects `LWSMigrationUnsupported`.
7. **Opt-out**: `alfred.ome.io/movable: "false"` excludes the workload from
   candidates; event explains.
8. **Per-workload cooldown**: execute a migration, verify no new attempt for
   the same workload within cooldown.
9. **Maintenance window**: active window; Alfred continues recommending, does
   not execute.
10. **Rate limiting**: 10 candidates; only 3 migrations in-flight.
11. **Policy hot reload**: update ConfigMap; Alfred reloads without restart.
12. **Leader election**: 3 replicas; kill leader; new leader elected within 20s.
13. **Post-migration health monitoring**: migration succeeds but target GPU
    fails within window; Alfred marks node suspect, backs off.
14. **HPA coexistence**: HPA scaling the target ISVC; Alfred defers.
15. **CA coexistence**: node marked `scale-down-disabled`; excluded from hints.
16. **OMENative-unavailable degraded mode**: stop the controller while leaving
    the CRD and its last current-looking InferenceReplica status installed.
    Once the capability Lease becomes stale, Alfred enters degraded mode and
    all workload types remain recommendation-only. CRD or status presence alone
    must not enable dispatch.
17. **Circuit breaker**: simulate 6+ failed migrations; verify circuit opens,
    pauses execution.
18. **Non-OME workload**: Kubeflow Notebook Pod on a node; Alfred observes but
    does not migrate; fragmentation score accounts for its GPU consumption.
19. **Multi-tenancy default**: workload in namespace A cannot be migrated to
    benefit workload in namespace B without explicit tenant-group annotation.
20. **Spot node avoidance**: preemptible-labeled node excluded from targets;
    preemptible source prioritized.
21. **Admission hardening and acknowledgement**: an unauthorized caller cannot
    add, change, or delete `ome.io/migration-request-v1-*`. Alfred may add/retry
    an identical UUID but may not delete it. The configured OME manager may
    delete a consumed request but may not add or alter one.
22. **Alfred narrow patch enforcement**: Alfred caller attempts to modify
    `spec` or non-migration annotations in the same patch; request rejected by
    admission policy.
23. **Unsupported schema version**: Alfred writes a request with unsupported
    `schemaVersion`; OMENative rejects it and Alfred marks the workload
    recommend-only for OMENative execution until versions are compatible.

New integration coverage for the multi-policy engine:

24. **Node-health end-to-end (the headline Phase 1 test).** A node transitions
    to `GpuUnhealthy`. Verify, in order: (a) Alfred (Node-Health policy)
    identifies every OME occupant, (b) eligible OMENative Instances are
    evacuated through the migration-request annotation while Raw/LWS/ineligible
    findings remain advisory, (c) a `NodeRepairNeeded` Event is emitted naming
    the node, (d) `NodeDrainedForRepair` is emitted only after no OME occupant
    remains and is withheld while an advisory blocker remains, and (e) the bad
    node receives **no re-placement** —
    Alfred excludes it from every policy's target set for as long as the
    condition holds. *Failure mode if absent:* Alfred evacuates a node and then
    immediately migrates a different workload back onto it.
25. **Arbiter cross-policy in a live cluster.** Stage a cluster where Defrag
    wants to consolidate workload W onto node N while Node-Health wants to
    evacuate a workload off node N in the same cycle; verify the Arbiter lets
    Node-Health win and Defrag's candidate is dropped for that cycle with a
    recorded reason, then re-evaluated next cycle.
26. **OEP-0013 seam in a live cluster.** A Component is actively scaling
    (replica count changing); verify no Alfred policy actuates against its
    workload until scaling settles. This is the integration-level twin of unit
    test 4.
27. **Descheduling (Phase 2).** A workload violating its placement
    preference (e.g., a soft anti-affinity that drifted) is selected by the
    Descheduling policy and migrated; verify it does not fire on workloads that
    still satisfy their constraints.
28. **PVC-backed model migration.** An ISVC backed by an RWX (or ROX) PVC model
    migrates with no model-ready filtering — verify the target set is the
    CSI-topology-reachable nodes and the migration completes with no model
    re-download. An ISVC backed by an RWO PVC is never dispatched — verify the
    candidate surfaces as advisory with `MigrationSkippedVolumePinned` and no
    surge is attempted. *Failure mode if absent:* the readiness filter silently
    marks every PVC-backed workload `NoFeasibleTarget`, or a surge against an
    RWO volume deadlocks in `ContainerCreating` on a Multi-Attach error.

### Chaos / robustness

Existing:

- Kill Alfred during in-flight migration; verify recovery.
- API server slow response; verify backoff.
- OMENative rejects all requests; verify circuit breaker triggers.
- InferenceService deleted mid-migration; verify Alfred cleans up gracefully.

New, targeting the multi-policy failure surface:

- **Flapping node conditions don't thrash.** A node's `GpuUnhealthy` condition
  flaps on/off faster than the cooldown window. Verify Alfred evacuates **at
  most one wave** per window and does not oscillate between evacuate and
  re-place. The guard is the *node evacuation record*, not the per-workload
  cooldown (which health candidates may shorten to the floor): the record is
  keyed by node and held across the flap — the condition clearing does not
  reset it — and the node-suspicion window keeps the cleared node out of every
  policy's target hints, so freshly cleared capacity is not immediately
  refilled and re-evacuated. *Failure mode if absent:* a node with
  intermittently-reported health flaps Alfred into an evacuate/re-place loop,
  churning workloads.
- **Defrag and Node-Health never act on the same workload or node in one
  cycle.** Run a chaos scenario with both policies enabled, randomized node
  health, and randomized fragmentation; over many cycles, assert the invariant
  that no (workload, node) pair is touched by two policies in the same cycle.
  This is the live, randomized version of unit tests 2 and 3 — the Arbiter is
  the only thing standing between two policies and a double-move, so it is
  fuzzed, not just unit-tested.

### Simulation

- 10 random cluster states; verify post-Alfred state has lower or equal
  fragmentation.
- Adversarial states designed to trick the Defrag scorer; verify safe
  rejection or correct handling.
- Long-running simulation (100 decision cycles) — verify no thrashing (bounded
  migration count per workload), now with all enabled policies active so the
  thrashing bound is asserted against the **combined** action stream, not just
  Defrag's.

## Graduation criteria

The maturity ladder maps onto the phasing: Alpha ships the engine refactor
plus the first two policies; Beta adds Descheduling and production soak; GA
optionally adds the predictive planner and a cloud remediation provider. The
phasing is deliberate — each phase is independently useful, and a phase can
slip without blocking the one below it.

### Alpha

Scope: the engine refactor (`Policy` interface + `Arbiter` + `Reporter`), Defrag ported to
**Policy #1**, **Policy #2 Node-Health Evacuation** (evacuate + remediation
signal), the checked InferenceReplica-plus-Pod snapshot, OMENative executor
capability Lease, Dispatcher, **arbitration-lite** (priority ordering + mutual
exclusion, no forecasting), and
the **OEP-0013 read-only seam**.

- Unit tests ≥ 80% coverage, including the new `engine/arbiter`,
  `engine/reporter`, `policy/defrag`, and `policy/nodehealth` packages.
- DenseV1 and ColumnarV2 OMENative status normalize to the same stable Instance
  model; Pods join by stable index and incarnation for live readiness and
  placement; migration state and cooldowns reconstruct from
  `InferenceReplica.Status.Migrations` and the workload audit ledger across
  leader failover.
- A fresh, wire-compatible OMENative capability Lease gates every executable
  Candidate; CRD and status presence alone fail the execution-readiness test.
- `RawDeployment` and LWS are advisory-only. A future Raw executor is additive
  and is not an Alpha graduation dependency.
- Integration tests 1–10 passing, plus the node-health end-to-end test (24),
  the cross-policy arbiter test (25), and the OEP-0013 seam test (26).
- The two new chaos tests passing: flapping-node cooldown, and the
  no-same-cycle-collision invariant.
- Deployment via Helm documented.
- Feature gate `AlfredGPUDefragmenter` disabled by default in the OME Helm chart.
- Policy schema documented, including how to enable/disable each policy and set
  the Arbiter priority order.
- `recommend-only` is the default mode; `execute` is opt-in. This applies to
  **every** policy, Node-Health included: an alpha operator can run Node-Health
  in recommend-only and watch the Events before enabling the Dispatcher.

### Beta

Scope adds **Policy #3 Descheduling**.

- Integration tests 1–28 (Descheduling and PVC-backed migration included) +
  chaos + simulation passing.
- 2+ production users running Alfred for ≥ 60 days without Sev1/Sev2 incidents
  attributed to Alfred — across **at least Defrag and Node-Health enabled
  together**, so arbitration is exercised in production, not just in CI.
- Dashboard + runbook published, covering per-policy metrics and the Arbiter's
  drop-reason counters.
- RBAC policy review by a security reviewer.
- OMENative at Beta maturity (execute path validated end-to-end).
- The Event-based remediation signal (`NodeRepairNeeded` followed by
  `NodeDrainedForRepair`) is validated in production; the pluggable-provider
  shape may still be TBD.

### GA

Scope optionally adds, and is not blocked on, a **predictive planner**
(forecasting layer) and a **pluggable cloud node-remediation provider**. Both
are Phase 3 / "GA-and-beyond": GA does not require them to ship, but the
engine must not preclude them.

- 6 months of Beta.
- Test coverage ≥ 85%.
- Policy schema and the `Policy`/`Arbiter`/`Reporter` interfaces stabilized —
  adding a fourth policy must not require an interface change.
- OMENative at GA.
- Deprecation story documented (if Alfred is ever replaced).
- If the predictive planner ships: its forecasts feed the Arbiter as
  advisory-only input, and an operator can disable it without losing the
  reactive policies. If it does not ship by GA, that is acceptable — see Open
  Questions on whether it is worth building at all.

## Implementation history

- 2026-04-16: OEP-0008 initial draft (single-purpose defragmenter).
- 2026-04-16: Design questions consolidated; 37 decisions locked.
- 2026-04-16: OEP-0008 updated to reflect locked decisions.
- 2026-06-04: Reframed from "Alfred GPU Defragmenter" to "Alfred GPU Cluster
  Caretaker": Defrag becomes one policy behind a `Policy` interface, an
  `Arbiter` resolves cross-policy conflicts, Node-Health Evacuation added as
  Policy #2, Descheduling reserved as Policy #3, and the predictive planner and
  cloud remediation provider deferred to Phase 3. Phasing mapped onto
  Alpha/Beta/GA.
- 2026-08-14: Engine gains an explicit **Reporter** stage: all observability
  emission (Events, decision metrics, the recommendations ConfigMap) moves out
  of `Evaluate` into the engine, making the policies-are-pure-functions
  guarantee literal; advisory (`Executable=false`) candidates — LWS
  recommendations, node remediation signals — bypass arbitration and route
  directly to the Reporter.
- 2026-08-15: Actuation design unified on the single migration-request
  annotation. A RawDeployment consumer was proposed but not implemented;
  `pods/eviction` remains outside Alfred's RBAC. Candidate simulation plus the
  Arbiter capacity check become surge-aware
  (place-then-free ordering, in-flight claims, `NoSurgeHeadroom` downgrades,
  smallest-footprint tie-breaking).
- 2026-08-15: Model-readiness target filtering made storage-aware: per-node
  models keep the `NodesReady` / node-label filter; PVC-backed models are
  exempt (their `NodesReady` is intentionally empty) and filter by CSI volume
  reachability instead; RWO-PVC-backed workloads are advisory-only
  (`VolumePinned`), because a surge replacement cannot attach a volume the
  source pod still holds.
- 2026-08-16: PVC access-mode classification refined: ROX treated as migratable
  alongside RWX (model weights are read-only at inference time); RWOP pins like
  RWO. Decision recorded: Alfred ships **no** downtime-migration mode for
  RWO-backed workloads, not even opt-in — moving one is downtime and stays a
  manual operator action.
- 2026-08-16: Cooldown made class-aware: health-evacuation candidates use a
  5-minute floor instead of the 30-minute per-workload cooldown (audited via
  `CooldownOverriddenForEvacuation`); flap protection restated on the per-node
  evacuation record; a post-evacuation node-suspicion window keeps drained
  nodes out of target hints even after the condition clears; per-node mutual
  exclusion and the per-node cooldown scoped to *targets*, so a failing node
  can drain multiple workloads per cycle; within-class ordering of evacuation
  candidates defined (feasible first, higher priority first, smaller footprint
  first).
- 2026-08-20: Scoring keyed by node-derived hardware pool instead of
  AcceleratorClass: shape-scoped classes (H100x1..x8) can overlap-claim the
  same node and cannot partition hardware, so the pool key comes from the
  node's GPU product label (GFD) or instance shape. Metric label
  `acceleratorclass` renamed to `pool`; the snapshot reads
  PersistentVolumeClaims/PersistentVolumes for model topology and no longer
  reads AcceleratorClasses.
- 2026-08-31: Compatibility baseline refreshed for current OMENative.
  `InferenceReplica.Status` is authoritative for stable Instance identity,
  lifecycle state, and migrations; live Pods joined by Instance index and
  incarnation are authoritative for physical placement and current readiness.
  The checked InferenceReplica-plus-Pod snapshot and the manager-published,
  directly read capability Lease are implemented. Request annotations remain a
  mailbox; durable workload audit-ledger ingestion is deferred. RawDeployment
  and LWS are advisory-only until their lifecycle owner implements the request
  contract. Alfred's current ClusterRole withholds InferenceService patch until
  the Dispatcher and admission guard land together. Current implementation gaps
  (Node-Health, Dispatcher, the just-in-time capability recheck, and outcome-fed
  safety state) are recorded explicitly.
- TBD: Complete Alpha implementation (Policy #2, Dispatcher, the just-in-time
  capability recheck, durable audit-ledger ingestion, and outcome-fed safety
  state).
- TBD: First Beta user.
- TBD: Beta (Policy #3 Descheduling).
- TBD: GA.

## Drawbacks

1. **Cluster-wide logical controller.** Alfred is one actor making
   cluster-wide decisions. Bugs have broad blast radius. Similar risk profile
   to cluster-autoscaler. Multi-policy raises the stakes: the Arbiter is now a
   single point through which every policy's actions flow, so an Arbiter bug
   can mis-order or drop actions from **all** policies at once.
2. **Requires OMENative for execution in Alpha.** Without a fresh, compatible
   executor Lease plus a valid InferenceReplica-and-Pod snapshot, Alfred remains
   useful for observation and recommendations but performs no workload
   migration. This is true for Node-Health evacuation too, not just Defrag.
3. **Heuristic, not optimal.** Greedy candidate selection may miss non-obvious
   consolidations. Trade: simplicity and speed over global optimality. The
   predictive planner (Phase 3) is the proposed answer, but it is not yet
   justified — see Open Questions.
4. **Operational overhead.** Operators learn Alfred's metrics, policy schema,
   and failure modes — now multiplied across three policies and an Arbiter
   priority order they must configure.
5. **Policy complexity.** The ConfigMap schema has many knobs, and the
   multi-policy engine adds per-policy enablement plus the Arbiter priority
   list. Good defaults mitigate, but the failure surface is larger than the
   single-policy design.
6. **Cross-controller interaction space.** Alfred, OMENative, HPA, CA,
   OEP-0013 Component-Scoped Autoscaling, and the scheduler form a complex
   interaction graph. Subtle bugs possible. Mitigated by conservative defaults,
   the OEP-0013 read-only seam, and extensive observability.
7. **Narrow write contract is brittle.** The annotation contract with the owning
   controller (OMENative today; future consumers must implement the same rules)
   is the primary coupling point. Race conditions are possible during annotation
   write/clear; mitigated by idempotent UUID semantics. Node-Health reuses this
   same contract, so it inherits both the safety property and the brittleness.

## Alternatives

### Alternative 1: Alfred as a scheduler plugin

Run Alfred inside a custom K8s scheduler, intercepting scheduling events to
apply bin-packing at placement time.

- **Pros:** Scheduling is the right place for placement logic.
- **Cons:** Only affects new pods; does not handle runtime defragmentation —
  and does nothing for node-health evacuation, which is inherently about
  workloads already running. Would need combination with a separate actor for
  migration anyway.

Rejected — the core problem is runtime caretaking (defrag, evacuation,
descheduling), not initial placement.

### Alternative 2: Embed Alfred in the OME manager

Run Alfred's logic inside the main OME controller manager.

- **Pros:** Single deployment, shared informer cache.
- **Cons:** Alfred's failure takes down OME core. Operational profiles differ.
  Less flexible release cadence.

Rejected — keeping them separate is cleaner.

### Alternative 3: Alfred as a CRD-driven system

Alfred's policy = CRD (`DefragmentationPolicy`); recommendations = CRD
(`RecommendationHistory`).

- **Pros:** K8s-idiomatic configuration.
- **Cons:** Violates the "no new CRDs" hard constraint; adds API surface;
  high-frequency data (recommendations/candidates) in etcd is expensive.

Rejected — no new CRDs.

### Alternative 4: Don't build Alfred; manual operator intervention

Operators manually trigger migrations and evacuations during scheduled
maintenance.

- **Pros:** Zero new components.
- **Cons:** Does not scale; reactive, not preventive; fragmentation
  continuously drifts, and node-health evacuation cannot wait for a maintenance
  window — a failing GPU degrades a running workload now.

Rejected — the problem scales with cluster size and workload churn, and
node-health is time-sensitive.

### Alternative 5: Extend cluster-autoscaler

Fork / contribute to CA to add GPU-aware defragmentation.

- **Pros:** Leverages CA's existing architecture.
- **Cons:** CA is node-focused (scale nodes), not pod-focused (move pods).
  Different problem. Fork is high-cost.

Rejected — architectural mismatch.

### Alternative 6: Keep Alfred a single-purpose defragmenter; put node-health and descheduling in separate controllers

Leave Alfred as the defragmenter it was, and ship Node-Health Evacuation and
Descheduling as their own standalone controllers, each with its own
deployment, snapshot, and actuation path.

- **Pros:** Smaller blast radius per controller; each can fail or be disabled
  in isolation; no Arbiter to build.
- **Cons:** All three policies need the **same** machinery — the cluster
  snapshot (observe nodes/GPUs/workloads), the safety bounds (cooldown, rate
  limiting, circuit breaker, maintenance windows, opt-out), and the **same
  narrow actuation surface** (the migration-request annotation executed by the
  owning controllers).
  N controllers means N copies of that machinery to build, test, and keep in
  sync. Worse: with no shared engine, there is **no place to do cross-policy
  arbitration** — two independent controllers can evacuate and consolidate the
  same workload in the same instant, exactly the double-move the Arbiter
  exists to prevent. The coordination problem doesn't disappear; it just moves
  out of one process and into an unowned gap between three.

Rejected — the policies share the snapshot, the safety machinery, and the
narrow actuation, so splitting them duplicates the engine three ways and
forfeits the one thing only a shared engine can provide: arbitration across
policies.

## Open questions

Some of these are the long-standing research-level questions carried from the
single-policy design (the P3 items from `design-questions.md`); the rest are
new, surfaced by the multi-policy reframing. None block design completeness;
they bound what is **not** yet decided.

New, from the Caretaker reframing:

1. **Is the predictive planner worth Phase 3 at all?** The greedy heuristic is
   reactive: it defrags and evacuates after fragmentation or failure is
   observed. A forecasting layer would let the Arbiter preempt — e.g.,
   evacuate ahead of a predicted GPU failure. Open question: does the
   prediction accuracy justify the complexity and the false-positive cost
   (evacuating a node that would have been fine)? The engine is built to accept
   forecasts as advisory Arbiter input, but whether we ever build the forecaster
   is genuinely undecided. GA does not depend on it.

2. **The cloud-remediation consumer schema.** Phase 1 is settled: Alfred emits
   `NodeRepairNeeded` at detection and `NodeDrainedForRepair` after the node has
   no OME occupants, and mirrors structured health state in its recommendations
   ConfigMap. Alfred does not annotate the Node. The remaining question is what
   contract a Phase 3 pluggable cloud-remediation provider consumes — these
   Events, the structured ConfigMap record, or a richer external interface. The
   provider form can lag and does not reopen the Phase 1 write boundary.

3. **DRA (Dynamic Resource Allocation) interaction.** K8s 1.31+
   `ResourceClaims` reshape GPU allocation, which affects **both** how Alfred
   reads "GPU allocation" for fragmentation scoring **and** how Node-Health
   reasons about which workloads hold a claim on a failing GPU. Revisit as DRA
   adoption progresses; this is now a cross-policy concern, not just a Defrag one.

4. **OEP-0013 seam depth.** The read-only seam (Alfred skips workloads whose
   Component is mid-scaling) is the minimum to avoid thrashing with
   Component-Scoped Autoscaling. Open question: is "skip while scaling" enough,
   or should Alfred read OEP-0013's desired-replica intent to plan around
   it (e.g., not consolidate onto a node a Component is about to scale into)?
   Phase 1 ships the minimal seam; deeper coupling is deferred.

Carried from the single-policy design (research-level, not blocking):

5. **Fragmentation score formula validation** (Q-049). Defaults ship with the
   proposed knobs (`demandBlendLambda`, the size prior, `pendingUrgencyTauMinutes`,
   `fragmentationThreshold: 0.25`); alpha-user feedback will refine them. What's
   the evaluation methodology? TBD during alpha. A catalog-derived prior
   (from registered `BaseModel` footprints) is the named follow-up.

6. **GPU topology awareness within a node (NVLink, PCIe)** (Q-040). v1 treats
   intra-node GPUs as fungible; scoring Step 1 already defines the seam
   (`LargestContiguousFree` substitutes for `FreeGPUs` when an agent supplies
   it). On NVSwitch fleets the two are identical, so this matters only for
   PCIe and MIG pools. Source of topology data still open (node-local NVML
   joined with kubelet PodResources requires an agent; gpu-feature-discovery
   alone is not allocation-aware).

7. **RDMA fabric topology across nodes** (Q-039). Multi-node migration targets
   need a full RDMA mesh. Data source unclear. Research.

8. **Historical learning from past migration outcomes** (Q-042). Adjust scoring
   weights or cost model based on real-world outcomes. v2+ feature; data
   collection starts in v1. Note: this overlaps with the predictive-planner
   question above — if we collect outcome data in v1, it is the training signal
   the forecaster would need.

9. **Cost accounting — runtime vs static** (Q-043). v1 uses static
   per-workload-shape cost constants. v2 measures actual disruption cost from
   past migrations.

10. **Dedicated UI beyond metrics + events** (Q-044). v1 = metrics + events +
    ConfigMap. Dashboards operator-owned. No dedicated UI.

11. **Unified "OME Ops" controller** (Q-045). The Caretaker reframing is a step
    toward this: Defrag, Node-Health, and Descheduling now share one engine.
    The open question becomes whether auto-patch / auto-repair / maintenance
    tooling also fold in as further policies, or stay separate. Current design
    is compatible — the `Policy` interface is the obvious extension point.

12. **Auto-patch / auto-repair re-introduction** (Q-046, Q-022 on plugin
    architecture). Could become Policy #4/#5 under the new engine rather than
    separate OEPs. Alfred is designed not to preclude this.

13. **Model pre-download coordination** (Q-032 extension). v2 may add
    auto-triggered pre-download to target nodes before retrying migrations
    blocked by model unavailability — relevant to Node-Health too, where an
    evacuation's target may lack the model.

14. **Final name** (Q-048). "Alfred" is the working name, inherited from
    ome-operator. The Caretaker reframing reopens this: candidates now include
    `GPUCaretaker`, `WorkloadConsolidator`, `OMEScheduler`. Finalize before GA.

15. **Kueue interaction detail** (Q-027). Alfred respects `SuspendedLabel`.
    Full integration with queueing and quotas depends on OEP-0003
    AcceleratorClass maturity. Revisit then.

16. **Priority-class boost semantics** (add as Q-053 if this matures).
    Operator-set `priorityClass` on an InferenceService influences scheduler
    placement; should Alfred also weight candidate-selection by `priorityClass`?
    Current design: no. Priority is for placement/preemption, not caretaking.
    May revisit — and if it does, it is an Arbiter-level concern (which
    policy's candidate wins), not a per-policy one.
