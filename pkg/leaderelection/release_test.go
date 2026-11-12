package leaderelection

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindReleaseOnCancel(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{name: "unset leaves the lease to expire on its own"},
		{name: "the flag alone turns the release on", args: []string{"--leader-elect-release-on-cancel"}, want: true},
		{name: "an explicit false keeps the release off", args: []string{"--leader-elect-release-on-cancel=false"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var release bool
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			BindReleaseOnCancel(fs, &release)

			require.NoError(t, fs.Parse(tc.args))

			assert.Equal(t, tc.want, release)
		})
	}
}

func TestSettingsValidate(t *testing.T) {
	chartTiming := Timing{LeaseDuration: 120 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second}
	shortLease := Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second}
	cases := []struct {
		name     string
		settings Settings
		wantErr  string
	}{
		{
			name:     "the chart's timing with the release is accepted",
			settings: Settings{Enabled: true, ReleaseOnCancel: true, Timing: chartTiming},
		},
		{
			name:     "a lease a failing leader can outlive is refused with the release",
			settings: Settings{Enabled: true, ReleaseOnCancel: true, Timing: shortLease},
			wantErr:  "--leader-elect-release-on-cancel needs --leader-elect-lease-duration above 108s, and it is 60s",
		},
		{
			name:     "the same lease without the release is accepted",
			settings: Settings{Enabled: true, Timing: shortLease},
		},
		{
			name:     "no timing with the release names the three flags to set",
			settings: Settings{Enabled: true, ReleaseOnCancel: true},
			wantErr:  "needs --leader-elect-lease-duration, --leader-elect-renew-deadline and --leader-elect-retry-period",
		},
		{
			name:     "with leader election off the release flag and an unset timing are accepted",
			settings: Settings{ReleaseOnCancel: true},
		},
		{
			name:     "with leader election off the release flag and a short lease are accepted",
			settings: Settings{ReleaseOnCancel: true, Timing: shortLease},
		},
		{
			name:     "with leader election off a partial timing is still refused",
			settings: Settings{ReleaseOnCancel: true, Timing: Timing{RenewDeadline: 40 * time.Second}},
			wantErr:  "supplied as a set",
		},
		{
			name: "a lease shorter than its renew deadline is refused before the release rule",
			settings: Settings{Enabled: true, ReleaseOnCancel: true,
				Timing: Timing{LeaseDuration: 10 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second}},
			wantErr: "must exceed renew-deadline",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.settings.Validate()

			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
