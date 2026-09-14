package leaderelection

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func TestBindFlagsParsesEachDuration(t *testing.T) {
	var timing Timing
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	timing.BindFlags(fs)

	require.NoError(t, fs.Parse([]string{
		"--leader-elect-lease-duration=60s",
		"--leader-elect-renew-deadline=40s",
		"--leader-elect-retry-period=8s",
	}))

	assert.Equal(t, 60*time.Second, timing.LeaseDuration)
	assert.Equal(t, 40*time.Second, timing.RenewDeadline)
	assert.Equal(t, 8*time.Second, timing.RetryPeriod)
}

func TestBindFlagsLeavesDurationsZeroWhenUnset(t *testing.T) {
	var timing Timing
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	timing.BindFlags(fs)

	require.NoError(t, fs.Parse(nil))

	assert.Zero(t, timing.LeaseDuration)
	assert.Zero(t, timing.RenewDeadline)
	assert.Zero(t, timing.RetryPeriod)
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		timing  Timing
		wantErr string
	}{
		{
			name: "unset is valid and defers to controller-runtime",
		},
		{
			name:   "a consistent set is valid",
			timing: Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second},
		},
		{
			name:   "controller-runtime's own defaults are valid",
			timing: Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second},
		},
		{
			name:    "a partial set is rejected",
			timing:  Timing{RenewDeadline: 40 * time.Second},
			wantErr: "supplied as a set",
		},
		{
			name:    "a negative duration is rejected",
			timing:  Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: -8 * time.Second},
			wantErr: "must be positive",
		},
		{
			name:    "a lease that expires before the renew deadline is rejected",
			timing:  Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "must exceed renew-deadline",
		},
		{
			name:    "a lease equal to the renew deadline is rejected",
			timing:  Timing{LeaseDuration: 40 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "must exceed renew-deadline",
		},
		{
			name:    "a renew window too short for one jittered attempt is rejected",
			timing:  Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 9 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "must exceed retry-period jittered",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.timing.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestApplySetsOnlySuppliedDurations(t *testing.T) {
	timing := Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second}

	var opts manager.Options
	timing.Apply(&opts)

	require.NotNil(t, opts.LeaseDuration)
	require.NotNil(t, opts.RenewDeadline)
	require.NotNil(t, opts.RetryPeriod)
	assert.Equal(t, 60*time.Second, *opts.LeaseDuration)
	assert.Equal(t, 40*time.Second, *opts.RenewDeadline)
	assert.Equal(t, 8*time.Second, *opts.RetryPeriod)
}

func TestApplyLeavesOptionsUntouchedWhenUnset(t *testing.T) {
	var timing Timing

	var opts manager.Options
	timing.Apply(&opts)

	// nil is what makes controller-runtime fall back to its own defaults.
	assert.Nil(t, opts.LeaseDuration)
	assert.Nil(t, opts.RenewDeadline)
	assert.Nil(t, opts.RetryPeriod)
}
