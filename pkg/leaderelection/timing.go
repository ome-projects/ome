// Package leaderelection configures the timing of controller-runtime's leader
// election loop for OME's manager binaries.
package leaderelection

import (
	"flag"
	"fmt"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Timing carries the three durations that govern the leader election loop.
//
// A zero field is not written to the manager options, so controller-runtime's
// own default stands and this repo carries no duration of its own to drift from
// what the chart renders. The right values depend on the apiserver latency of
// the cluster the manager runs in, which only the deployment knows.
//
// The durations constrain each other — a lease that can expire while its holder
// is still trying to renew it hands the lock to a standby while the old leader
// still believes it is active — so they are supplied as a set or not at all.
type Timing struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// BindFlags registers the timing flags on fs.
func (t *Timing) BindFlags(fs *flag.FlagSet) {
	fs.DurationVar(&t.LeaseDuration, "leader-elect-lease-duration", t.LeaseDuration,
		"How long the lease stays valid after its last renewal, and so the longest a standby waits before "+
			"taking over from a leader that stopped renewing. No in-code default; the chart supplies the "+
			"value. Zero/unset falls back to controller-runtime's default. Must exceed "+
			"--leader-elect-renew-deadline, and is supplied together with the other two timings or not at all.")
	fs.DurationVar(&t.RenewDeadline, "leader-elect-renew-deadline", t.RenewDeadline,
		"How long the leader keeps retrying a failed renewal before giving up leadership and exiting. "+
			"controller-runtime caps each renewal request at half this value, so a window holds two attempts "+
			"and the loop survives exactly one hung apiserver request — size it against the apiserver stalls "+
			"the cluster actually sees, since every stall longer than this costs a leader restart. No in-code "+
			"default; the chart supplies the value. Zero/unset falls back to controller-runtime's default.")
	fs.DurationVar(&t.RetryPeriod, "leader-elect-retry-period", t.RetryPeriod,
		"Gap between renewal attempts inside a renew window, and the interval at which a standby polls for "+
			"an expired lease. No in-code default; the chart supplies the value. Zero/unset falls back to "+
			"controller-runtime's default.")
}

// Validate reports whether the durations form a set the elector will accept.
// client-go applies the same constraints, but only when it builds the elector
// partway through Manager.Start, by which point the process is already serving
// probes; checking at startup turns a bad chart value into an immediate,
// named failure.
func (t *Timing) Validate() error {
	supplied := 0
	for _, d := range []time.Duration{t.LeaseDuration, t.RenewDeadline, t.RetryPeriod} {
		if d != 0 {
			supplied++
		}
	}
	if supplied == 0 {
		return nil
	}
	if supplied != 3 {
		return fmt.Errorf("leader election timings are supplied as a set: got lease-duration=%s renew-deadline=%s retry-period=%s",
			t.LeaseDuration, t.RenewDeadline, t.RetryPeriod)
	}
	if t.LeaseDuration <= 0 || t.RenewDeadline <= 0 || t.RetryPeriod <= 0 {
		return fmt.Errorf("leader election timings must be positive: got lease-duration=%s renew-deadline=%s retry-period=%s",
			t.LeaseDuration, t.RenewDeadline, t.RetryPeriod)
	}
	if t.LeaseDuration <= t.RenewDeadline {
		return fmt.Errorf("leader election lease-duration (%s) must exceed renew-deadline (%s), otherwise the lease "+
			"expires while its holder is still renewing", t.LeaseDuration, t.RenewDeadline)
	}
	if jittered := time.Duration(leaderelection.JitterFactor * float64(t.RetryPeriod)); t.RenewDeadline <= jittered {
		return fmt.Errorf("leader election renew-deadline (%s) must exceed retry-period jittered by %.1fx (%s), "+
			"otherwise a window holds no complete attempt", t.RenewDeadline, leaderelection.JitterFactor, jittered)
	}
	return nil
}

// Apply copies the supplied durations onto opts, leaving any that were not
// supplied for controller-runtime to default.
func (t *Timing) Apply(opts *manager.Options) {
	if t.LeaseDuration > 0 {
		opts.LeaseDuration = &t.LeaseDuration
	}
	if t.RenewDeadline > 0 {
		opts.RenewDeadline = &t.RenewDeadline
	}
	if t.RetryPeriod > 0 {
		opts.RetryPeriod = &t.RetryPeriod
	}
}
