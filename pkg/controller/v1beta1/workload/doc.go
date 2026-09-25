// Package workload is the orchestration and planning root of the
// source-agnostic per-Component reconciler. It drives the lifecycle of
// one logical workload — observe → holds → plan → operations →
// escalation → wake — without knowing whether the workload's spec lives
// on an InferenceService Component or on an InferenceReplica object.
//
// The root holds exactly the pass's orchestration and its plan:
//
//   - reconcile.go — Reconcile and Execute: the dispatch loop, the hold
//     pass wiring, the numbered action arms, teardown and scale-down.
//   - pass_update.go, pass_restart.go, pass_migrate.go — the
//     dispatcher's arms for the three passes that admit work against a
//     budget or a record before handing a row to workload/ops.
//   - plan.go, plan_decision.go — BuildPlan and Plan: the desired shape
//     and the Decision (which owners run this pass, pause and freeze
//     narrowing, the trigger evaluation).
//   - snapshot.go, observation.go — the one memoized observation per
//     reconcile, and the publication view the adapters project counters
//     from.
//
// Everything else lives in a subpackage: the vocabulary and the
// ownership table in types, the hold pass in holds, the evidence
// classifiers in evidence, the row writers in status, the verb passes in
// ops, the end-of-pass repair in escalation, the headless Service in
// service, and the leaf readers in query, drain, podreadiness, revision,
// podgroup, gang and audit.
//
// Boundary contract: the workload tree owns its own status / phase /
// operation / key types and does NOT import the OME CRD API package
// (`pkg/apis/ome/v1beta1`) from any file exported to callers. Adapters
// at the edge convert between the owner-CRD shape and the
// workload-owned types. Per-instance transition fields — Phase,
// Operation, RunningRevision — are written only by this package tree;
// the component-level revision pair (CurrentRevision, UpdateRevision)
// only by the owner's controller.
//
// Callers populate a types.ReconcileInput — identity, projected
// DesiredSpec / ObservedState, and callback closures (MutateInstance,
// RemoveInstance, WriteAggregateCondition, WarnInstanceFailed) — and
// hand it to Reconcile. Workload code reads ReconcileInput and never
// reaches back into the caller's types.
//
// Cross-Component coordination gate decisions reach the dispatcher via
// the UpdateGate callback on ReconcileInput; the workload package never
// imports `inferenceservice/reconcilers/omenative/coordination/`.
package workload
