// Package ops holds the verb passes of the workload engine: create,
// update (surge, gang, in-place, recreate), restart, migrate and delete.
// Each pass does one thing — advance its own operation one step for the
// rows it owns — and one file holds one verb.
//
// What a pass does NOT decide:
//
//   - who drives a row. The ownership table in workload/types answers
//     that once per reconcile; the decision layer groups the rows by
//     owner and hands each pass its list.
//   - whether an external wait is on a row. workload/holds evaluates
//     every hold once, at the top of the reconcile; a pass reads the
//     token it wrote and never writes or releases one.
//   - what an observed pod means. workload/evidence classifies a stuck
//     pod, readiness limbo, an unfolded gate, node death and the
//     apiserver rejection classes; a pass consumes the classification.
//   - how a row is written. workload/status is the one writer, and every
//     stamp re-checks its identity precondition on the fresh row inside
//     the mutation.
//
// Two files here are named for what they do rather than for a verb,
// because more than one verb reaches them: terminal_recycle.go, the
// paced recycle of a terminal or admission-rejected pod occupying a
// target name, and force_delete.go, the policy-gated escalation over a
// pod the node can no longer be asked about. render.go is the pod and
// object renderer every placing pass shares. The operator reset that
// releases the Create or Restart attempt a Failed row parked lives in
// workload/status (reset.go).
package ops
