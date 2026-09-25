package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// authority is one hold: the token it records, the rows it may record on,
// and how it reads its own condition off the pass.
type authority struct {
	// token is the value recorded on InstanceOperation.Waiting.
	token string

	// mayOwn reports whether a row with this owner may carry the token.
	// It is the whole of the writability rule: the token-precedence guard
	// in mark decides everything else, so an authority states only which
	// owners its wait can be true of.
	mayOwn func(owner types.RowOwner, plan types.ComponentPlan) bool

	// everyRow marks an authority that speaks for rows the fact-bearing
	// holds leave untouched — a deferred scale-down victim and a Failed
	// row. Only the pause does: it releases its own token wherever it
	// left one, and its record rule refuses those rows on its own terms.
	everyRow bool

	// report is the authority's reading of the row.
	report func(ctx context.Context, in PassInput, row Row) (reading, error)
}

// reading is what one authority has to say about one row this pass:
// waiting, its wait is over, or nothing at all — in which case the row
// keeps whatever it carries.
type reading struct {
	// waiting is the wait the authority has to report.
	waiting bool
	// evidence is the authority's explanation, left on LastFailure with
	// the token. Nil records the token alone.
	evidence *types.InstanceTermination
	// release retires this authority's own token.
	release bool
}

// table is the six holds in the order they are consulted within one
// pass. Order decides only who reaches an unheld row first when two
// facts arrive on it in the same pass — an incumbent token is never taken
// except by the one ordered pair in takesRowFrom. The order follows where
// each fact arises: the quota refusal and the silent node where a create
// is attempted, the source out of rotation where a surge is driven, and
// the scheduler and the PodGroup at the end of the pass. A fact that
// reaches an operator first on one build must reach them first on the
// next, so the order is an invariant of the row's report, not of this file.
//
// Each row states the invariant its mayOwn rule encodes.
var table = []authority{
	// Quota: admission refused the operation's pod create. Any owner
	// that creates a pod can be refused, so the wait is true of every
	// row, a teardown and an unowned row included.
	quotaHold,
	// Node unknown: a silent node still holds a name the rebuild needs.
	// Refused on a teardown — a pod on its way out occupies no name
	// anyone is waiting to rebuild — and on a row with no attempt.
	nodeUnknownHold,
	// Source unrouted: only a surge takes its source out of rotation, so
	// only a row the update pass owns can report one out of it.
	sourceUnroutedHold,
	// Scheduler: the cluster has nowhere to put a pod. A teardown's pods
	// need no placement and DeleteBatch paces its own removal past any
	// grace, so a Delete row is refused; a row with no owner has no
	// attempt to hold.
	schedulerHold,
	// PodGroup: the gang's deterministic name is still being collected.
	// Refused on the two durable claims nothing read off a PodGroup may
	// take a row from — DeleteBatch's teardown and the migration record,
	// which is the timeout authority for both pair rows.
	gangHold,
	// Pause: the weakest, and the only token that reports a decision
	// rather than a fact. Its owner rule is which attempts a pause
	// actually stands in front of — see pauseHeldOperation.
	pauseHold,
}

// takesRowFrom reports whether token may take a row already reporting
// current. Exactly one pair is ordered, and it is ordered because the
// two are not the same kind of statement: an operator pause reports a
// decision to stop rolling forward, while every other token reports
// something about the Instance — its quota was refused, it has no
// placement, it is serving nothing. The fact is what an operator needs
// to see, so it takes the row; the pause goes on holding the rollout
// either way. Everything else is first-writer-wins: the authority that
// got there first owns the wait being reported, and replacing its token
// would tell an operator the row is queued behind something it is not.
func takesRowFrom(token, current string) bool {
	if current == "" || current == token {
		return true
	}
	return current == types.WaitingReasonPaused && token != types.WaitingReasonPaused
}

// Enters reports whether recording token on a row that currently
// reports current would be the transition INTO that wait: the row is
// unheld, or held only by the pause. It is the precedence rule read from
// outside the pass, by the one site that learns of a wait before this
// pass can report it — the create site announcing a quota refusal — so
// an operator hears of a wait exactly when it becomes the row's report,
// and not for a row already queued behind another authority's fact.
func Enters(token, current string) bool {
	return current != token && takesRowFrom(token, current)
}

// mark records a's token as the operation's Waiting token through the
// row writer, which owns the write and its re-judgement; this function
// owns which authority may make it.
//
// It reports two things about the row the write landed on: entered, the
// transition INTO the hold (the edge a caller announces), and held,
// whether the row carries THIS token afterwards. held is the honest
// answer to "is my authority the one reporting this row's wait" —
// eligibility is judged on the fresh row, so a caller cannot derive it
// from its own pass-start observation, and treating a refused mark as a
// hold would suppress another authority's escalation.
//
// A no-op when the slot is gone, has no operation, is refused by the
// owner rule, or already carries a token this one may not take.
func mark(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, idx int32, a authority, evidence *types.InstanceTermination) (entered, held bool, err error) {
	return status.RecordWaiting(ctx, input, idx, a.token,
		func(owner types.RowOwner) bool { return a.mayOwn(owner, plan) },
		takesRowFrom, evidence)
}

// release retires token once its authority reports the wait over.
//
// Edge-triggered off the observation as well as the write: a pass over a
// row that was never held reaches no mutation at all, so the common case
// — every reconcile of every healthy row — costs nothing.
func release(ctx context.Context, input types.ReconcileInput, idx int32, token string) error {
	if !observedHoldingToken(input, idx, token) {
		return nil
	}
	return status.ReleaseWaiting(ctx, input, idx, token)
}

// observedHoldingToken reports whether this reconcile's observation of
// the row already carries token, which is the only state a release can
// act on.
func observedHoldingToken(input types.ReconcileInput, idx int32, token string) bool {
	row := input.ObservedState.Instance(idx)
	return row != nil && row.Operation != nil && row.Operation.Waiting == token
}

// anyAttemptOwner is the owner rule of every authority that reports a
// fact about the Instance: any row with an attempt in flight can be
// waiting on one, and a teardown is the row none of them may take.
func anyAttemptOwner(owner types.RowOwner, _ types.ComponentPlan) bool {
	return ownerHasAttempt(owner)
}

// ownerHasAttempt reports whether the row has an attempt in flight that
// an authority can hold. A teardown is excluded from every fact-bearing
// hold: DeleteBatch owns a removal's progress, its stuck-Terminating
// escalation and its completion, and it may pace that removal past any
// grace.
func ownerHasAttempt(owner types.RowOwner) bool {
	switch owner {
	case types.OwnerNone, types.OwnerDelete:
		return false
	}
	return true
}
