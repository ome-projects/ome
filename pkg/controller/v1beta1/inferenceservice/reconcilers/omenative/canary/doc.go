// Package canary executes spec.rollout.canary: a progressive rollout that steps
// per-revision pod capacity and the external traffic weight, with Manual/Auto
// promotion and rollback.
//
// It mirrors the omenative/coordination package shape: a pure Reconcile that
// reads spec + observed per-revision pod counts and mutates isvc.Status
// in-memory; the controller persists status afterward. It reuses coordination's
// per-revision Services and traffic-weight writer rather than re-implementing
// them.
//
// The capacity/traffic/promotion machinery is component-agnostic. In a
// multi-Component (PD) group the primary Component carries the step machine
// while every other bumped Component stages its own canary capacity, and
// every member publishes the step's traffic weights on its own revisions
// (see dispatch.go) and its rolled-out revision fields at the ladder's
// edges (see revisions.go).
package canary
