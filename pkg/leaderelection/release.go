package leaderelection

import "flag"

// BindReleaseOnCancel registers the flag that makes a leader give up its lease
// at the end of a graceful stop, writing into releaseOnCancel. Every manager
// binary binds the flag here, so the chart passes one name to all of them.
// Off is controller-runtime's own default; the chart supplies the value. A
// binary that turns it on checks its timing with Settings.Validate, because
// the release is safe only under a lease that outlasts a leader whose renewals
// fail.
func BindReleaseOnCancel(fs *flag.FlagSet, releaseOnCancel *bool) {
	fs.BoolVar(releaseOnCancel, "leader-elect-release-on-cancel", *releaseOnCancel,
		"Give up the leader lease at the end of a graceful stop: SIGTERM, SIGINT, or a controller or server that "+
			"fails. A standby then takes over at its next poll of the lease (see --leader-elect-retry-period) "+
			"instead of waiting for the lease to expire. The manager releases the lease once everything it runs "+
			"has stopped or controller-runtime's shutdown timeout has run out, whichever comes first, and the "+
			"process exits right after. If the pod's termination grace period runs out first, the lease is not "+
			"released and expires as usual. When renewals fail, the leader also tries to release the lease before "+
			"it stops. After the last renewal a standby reads, the leader can then act for up to one leader election "+
			"request timeout (half --leader-elect-renew-deadline, at least 1s) plus --leader-elect-retry-period plus "+
			"twice --leader-elect-renew-deadline. With --leader-elect, the manager refuses to start unless "+
			"--leader-elect-lease-duration, in whole seconds, exceeds that sum. It also needs all three timing flags "+
			"set, because it cannot check controller-runtime's defaults without copying them. Without --leader-elect "+
			"this flag does nothing and the check is skipped. Default false, controller-runtime's own default.")
}

// Settings is what the flags say about leader election: whether it runs,
// whether a stopping leader gives its lease back, and the loop's timing.
type Settings struct {
	Enabled         bool
	ReleaseOnCancel bool
	Timing          Timing
}

// Validate refuses a timing the elector would reject and, when a leader gives
// its lease back on a clean stop, a timing under which a standby could take the
// lease while a leader whose renewals fail is still acting. The second rule
// applies only when leader election runs: without it the release flag does
// nothing, and a single-replica install may leave the timing unset.
func (s Settings) Validate() error {
	if err := s.Timing.Validate(); err != nil {
		return err
	}
	if s.Enabled && s.ReleaseOnCancel {
		return s.Timing.ValidateReleaseOnCancel()
	}
	return nil
}
