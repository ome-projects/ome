package audit

// ReasonForceDelete is the Reason stamped on stuck-Terminating
// force-delete records — the terminal ledger rows scale-down escalation
// writes after force-deleting a pod wedged on a dead node
// (and the report-only rows for finalizer-pinned pods).
//
// ForceDelete entries are RECORDS, never work orders, and every ledger
// reader must treat them as inert:
//   - written terminal (Phase=Completed), so the in-flight readers
//     (HasInFlightMigrationForInstance, InFlightEntry) never resume
//     them as migrations, and the adapter's upgrade import (which
//     additionally filters Reason=ForceDelete) never synthesizes a
//     status.migrations entry from one — so they can never reach the
//     migration capacity gate, which counts status entries;
//   - not AutoRecover, so the relocation budget / node-exclusion
//     helpers (CountAutoRecoverAttempts, RecentAutoRecoverFromNodes,
//     NewestAutoRecoverEntry, RemoveAutoRecoverEntries) never count
//     them.
//
// RequestUUID carries the force-deleted pod's UID (a valid UUID), so
// one pod object yields at most one row across passes and restarts.
const ReasonForceDelete = "ForceDelete"

// OutcomeForceDeleteUnreachable is the Outcome stamped when the
// force-delete policy removed a pod (grace 0, UID-preconditioned) whose
// node could not finish it: a Terminating pod past its deletion deadline
// plus the overdue slack, one still inside its grace period on a node
// whose kubelet had stopped reporting for the threshold when the policy
// skips the grace period there, or an unreported pod holding a name a
// rebuild needs. It records that the controller deleted an API object
// whose containers it never saw stop.
const OutcomeForceDeleteUnreachable = "force-delete-unreachable"

// OutcomeForceDeleteFinalizerReport is the Outcome stamped on the
// report-only row for a pod that is overdue but pinned by foreign
// finalizers. The row is a dedup marker keyed by the pod UID: the
// Warning event is emitted once, and this persisted row suppresses
// repeats across passes and controller restarts.
const OutcomeForceDeleteFinalizerReport = "finalizer-report"
