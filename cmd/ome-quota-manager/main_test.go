package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/leaderelection"
)

// chartTiming is the leader election timing charts/ome-quota-manager renders.
var chartTiming = leaderelection.Timing{
	LeaseDuration: 120 * time.Second,
	RenewDeadline: 40 * time.Second,
	RetryPeriod:   8 * time.Second,
}

func TestManagerOptionsReleaseTheLeaseOnCancelWhenAsked(t *testing.T) {
	for _, release := range []bool{false, true} {
		o := &options{
			enableLeaderElection:          true,
			leaderElectionID:              "ome-quota-manager-leader-lock",
			leaderElectionTiming:          chartTiming,
			leaderElectionReleaseOnCancel: release,
		}

		got := o.managerOptions()

		assert.Equal(t, release, got.LeaderElectionReleaseOnCancel, "release on cancel = %v", release)
		assert.True(t, got.LeaderElection)
		assert.Equal(t, "ome-quota-manager-leader-lock", got.LeaderElectionID)
		require.NotNil(t, got.LeaseDuration)
		assert.Equal(t, chartTiming.LeaseDuration, *got.LeaseDuration)
	}
}

func TestValidateLeaderElectionTimingWithReleaseOnCancel(t *testing.T) {
	shortLease := leaderelection.Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second}
	cases := []struct {
		name    string
		elect   bool
		timing  leaderelection.Timing
		release bool
		wantErr string
	}{
		{name: "the chart's timing with the release is accepted", elect: true, timing: chartTiming, release: true},
		{
			name:    "a 40s renew deadline under a 60s lease is refused with the release",
			elect:   true,
			timing:  shortLease,
			release: true,
			wantErr: "--leader-elect-release-on-cancel needs --leader-elect-lease-duration above 108s, and it is 60s",
		},
		{name: "the same timing without the release is accepted", elect: true, timing: shortLease},
		{
			name:    "no timing with the release names the three flags to set",
			elect:   true,
			release: true,
			wantErr: "needs --leader-elect-lease-duration, --leader-elect-renew-deadline and --leader-elect-retry-period",
		},
		{
			name:    "a lease shorter than its renew deadline is refused before the release rule",
			elect:   true,
			timing:  leaderelection.Timing{LeaseDuration: 10 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second},
			release: true,
			wantErr: "must exceed renew-deadline",
		},
		{name: "with leader election off the release flag and no timing are accepted", release: true},
		{name: "with leader election off the release flag and a short lease are accepted", timing: shortLease, release: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := &options{
				enableLeaderElection:          tc.elect,
				leaderElectionTiming:          tc.timing,
				leaderElectionReleaseOnCancel: tc.release,
			}

			err := o.validateLeaderElectionTiming()

			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
