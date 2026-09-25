// Package escalation is the action half of the end of a reconcile: it
// consumes the evidence and the holds the pass has already recorded and
// decides what ends an attempt. It never records a hold and never
// classifies evidence — workload/holds and workload/evidence own those —
// it reads their results and acts.
//
// What it decides, one file each:
//
//   - escalation.go — the pass itself (Run): per row, the fast path on a
//     stuck pod, the broad path on an elapsed operation deadline, and the
//     failed-while-serving exemption that refuses both.
//   - gang.go, scheduler.go — the two arms that end an attempt on a hold
//     no repair can lift: a PodGroup name another controller owns, and a
//     pod the scheduler could not place for longer than the operator's
//     grace.
//   - disposition.go — what a disposable attempt's failure means:
//     relocation first, then a RetryBlock against the revision when the
//     cause is workload-caused, then the plain Failed stamp.
//   - deadline.go — the deadline arithmetic: when an operation's clock
//     has elapsed, and the parking step that stops it while an external
//     authority holds the attempt.
//   - refresh.go — the one write on a row that is already Failed: the
//     generic deadline reason replaced by the concrete cause once the
//     kubelet names one.
//   - budget.go — the per-Component MaxSurge / MaxUnavailable /
//     Partition arithmetic the plan and the verb dispatcher size their
//     admissions by.
//   - retryblock_prune.go — the supersede-prune of RetryBlocks whose
//     revision nothing resolves as a target any more.
//
// The reconcile root calls Run once at the end of every eligible pass and
// PruneSupersededRetryBlocks after it; adapters call
// ReconcileGatedDeadlines from their own status pass.
package escalation
