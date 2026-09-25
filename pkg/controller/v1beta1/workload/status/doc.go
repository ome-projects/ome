// Package status is the one place an Instance row is written.
//
// Every stamp is a named function. A pass decides, then calls the stamp;
// it holds no mutation closure of its own, and the layout gate in the
// behavior lock refuses one anywhere else in the workload tree. That is
// what makes the row's write set readable in one file tree instead of at
// every call site, and what lets the identity precondition below be
// stated once.
//
// Three kinds of write, kept apart on purpose so a reader can tell a
// decision from an observation, and named by kind:
//
//   - decision.go — what the engine decided: phase, operation, step,
//     target revision, deadline. Edge triggers key on these. A decision
//     writer is Stamp<State> (StampFailed, StampDeadline, StampStep);
//     the few that move a row by a rule rather than to one named state
//     are named for the rule (DemoteUnbacked, StartGangSurge,
//     RestoreGangSurgeTarget, ResetGangSurgeSource), and a pin's inverse
//     is Clear<Pin> (ClearMigrationPin). The Failed stamps live here and
//     carry their termination record in the same write, so a decision
//     and its cause cannot land apart. The in-mutation helper a stamp
//     composes is named for the state it enters (EnterReady) or the
//     field it builds (<X>Mutation).
//   - evidence.go — what the engine observed: the termination record on
//     LastFailure, the waiting token an authority reports, the capacity
//     refusal the create pass carries forward. An evidence writer is
//     Record<Fact>; its inverse is Release<Fact> or Clear<Fact>.
//   - announced.go — the once-only markers: a warning or event that must
//     reach an operator once per episode and not once per pass. An
//     announcement is Announce<X>.
//
// The RetryBlock is the one non-row record written here, and its writers
// are RetryBlock<Verb>. The decisions that write two rows at once have
// their own files: gangsurge.go for the source-and-marker pair of a
// gang surge, migration.go for the source-and-surge pair of a
// migration, each with the step, promote, reset and cleanup stamps of
// its pair. The verb passes' own stamps are filed by verb: create.go
// holds the create-attempt claim, promote, replacement and rollback as
// the mutations the Create pass batches; delete.go holds the scale-down
// wave's admission and removal, each one transaction over every row of
// the wave; reset.go holds the operator reset. terminal.go holds the
// writes that end a row — finalize, recycle, overdue — and
// precondition.go holds the identity every one of them is preconditioned
// on.
//
// aggregate.go is the read side of the same status: the per-pod
// predicates, the per-Instance counter set, the desired-count lookup and
// the Component-level convergence gates every publisher derives from.
//
// The package imports only workload/types, the leaf readers (query,
// drain, podreadiness) and the metrics sink, so the reconcile root, the
// verb passes, the hold pass, the escalation pass and the gang pass can
// all reach it.
package status
