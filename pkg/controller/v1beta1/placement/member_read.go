package placement

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// MemberReadRetry bounds the re-reads of a member after a transient failure
// within one reconcile. MaxAttempts counts every read, the first one included,
// so zero or one reads once. The wait before each re-read starts at
// InitialBackoff and doubles up to MaxBackoff.
type MemberReadRetry struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// backoff is the wait before re-read n, counted from one: InitialBackoff
// doubled n-1 times, never above MaxBackoff.
func (p MemberReadRetry) backoff(n int) time.Duration {
	wait := p.InitialBackoff
	for i := 1; i < n && wait < p.MaxBackoff; i++ {
		wait *= 2
	}
	return min(wait, p.MaxBackoff)
}

// retryMemberRead runs read against one member, giving every attempt its own
// placeTimeout deadline under ctx, so a caller must pass the reconcile context
// rather than one already bounded by placeTimeout. A transient failure is read
// again while attempts remain and ctx is live; any other result is returned at
// once, including NotFound and failed identity or ownership checks.
func retryMemberRead[T any](ctx context.Context, r *Reconciler, cluster string, read func(context.Context) (T, error)) (T, error) {
	policy := r.MemberReadRetry
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		out, err := read(attemptCtx)
		cancel()
		if err == nil || attempt >= policy.MaxAttempts || !transientMemberReadError(err) || ctx.Err() != nil {
			return out, err
		}
		wait := policy.backoff(attempt)
		r.Log.V(1).Info("member read failed; retrying", "cluster", cluster, "attempt", attempt, "backoff", wait, "error", err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return out, err
		case <-timer.C:
		}
	}
}

// transientMemberReadError reports whether a failed member read may succeed
// when repeated shortly: a timeout, a dropped or refused connection,
// throttling, or a server-side failure.
func transientMemberReadError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, context.DeadlineExceeded),
		apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), apierrors.IsTooManyRequests(err),
		apierrors.IsInternalError(err), apierrors.IsServiceUnavailable(err), apierrors.IsUnexpectedServerError(err),
		utilnet.IsConnectionReset(err), utilnet.IsConnectionRefused(err), utilnet.IsProbableEOF(err):
		return true
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) && status.Status().Code >= http.StatusInternalServerError {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (r *Reconciler) observationNow() time.Time {
	if r.ObservationClock != nil {
		return r.ObservationClock.Now()
	}
	return time.Now()
}

// unknownCandidate carries a home's last known state behind a failed read.
// The ready count is the routing weight, so the count from the last successful
// read stays only until ObservationGrace has passed since the first failed
// read. ObservationFailingSince records that first failure in status, so a
// controller restart neither restarts nor ends the grace.
func (r *Reconciler) unknownCandidate(isvc *v1beta1.InferenceService, previous v1beta1.CandidatePlacement) v1beta1.CandidatePlacement {
	candidate := *previous.DeepCopy()
	candidate = normalizeCandidatePhase(candidate)
	candidate.ObservationKnown = false
	now := r.observationNow()
	if candidate.ObservationFailingSince == nil {
		since := metav1.NewTime(now).Rfc3339Copy()
		candidate.ObservationFailingSince = &since
		// A home that was already unknown but has no stored start cannot have its
		// grace measured, for example when the stored CRD lacks the field, so it
		// gets none rather than a grace that restarts on every pass.
		if !previous.ObservationKnown {
			candidate.ReadyReplicas = 0
		}
	}
	if !now.Before(candidate.ObservationFailingSince.Add(r.ObservationGrace)) {
		candidate.ReadyReplicas = 0
	}
	if !hasPolicyRefs(isvc) {
		candidate.Autoscaling = nil
	}
	if !hasRolloutPolicyRefs(isvc) {
		candidate.Rollout = nil
	}
	return candidate
}

// graceWake collects, over one reconcile, the earliest time a written home
// that cannot be read loses its retained ready count.
type graceWake struct {
	at time.Time
}

type graceWakeKey struct{}

// noteGraceEnds records when the observation grace of each unreadable home in
// a status write ends, so the reconcile wakes in time to withdraw its ready
// count if reads still fail.
func (r *Reconciler) noteGraceEnds(ctx context.Context, candidates []v1beta1.CandidatePlacement) {
	wake, _ := ctx.Value(graceWakeKey{}).(*graceWake)
	if wake == nil || r.ObservationGrace <= 0 {
		return
	}
	now := r.observationNow()
	for _, candidate := range candidates {
		if candidate.ObservationKnown || candidate.ObservationFailingSince == nil {
			continue
		}
		end := candidate.ObservationFailingSince.Add(r.ObservationGrace)
		if end.After(now) && (wake.at.IsZero() || end.Before(wake.at)) {
			wake.at = end
		}
	}
}

// wakeForGrace requeues no later than the earliest grace end recorded for the
// reconcile. A result that already retries sooner is unchanged.
func (r *Reconciler) wakeForGrace(res ctrl.Result, wake *graceWake) ctrl.Result {
	if wake == nil || wake.at.IsZero() || res.Requeue {
		return res
	}
	// A grace that ended during this reconcile still needs a pass; zero would
	// mean no requeue at all, so the wait is kept positive.
	remaining := max(wake.at.Sub(r.observationNow()), time.Nanosecond)
	if res.RequeueAfter <= 0 || remaining < res.RequeueAfter {
		res.RequeueAfter = remaining
	}
	return res
}
