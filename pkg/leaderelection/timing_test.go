package leaderelection

import (
	"context"
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	crleaderelection "sigs.k8s.io/controller-runtime/pkg/leaderelection"
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

func TestValidateReleaseOnCancel(t *testing.T) {
	cases := []struct {
		name    string
		timing  Timing
		wantErr string
	}{
		{
			name:   "a lease longer than a request timeout, retry and twice renew is valid",
			timing: Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 8 * time.Second},
		},
		{
			name:   "a lease one second longer than a request timeout, retry and twice renew is valid",
			timing: Timing{LeaseDuration: 59 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 8 * time.Second},
		},
		{
			name:   "a lease equal to a request timeout, retry and twice renew is rejected",
			timing: Timing{LeaseDuration: 58 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "--leader-elect-release-on-cancel needs --leader-elect-lease-duration above 58s, and it is 58s: " +
				"10s (request timeout, half of --leader-elect-renew-deadline and at least 1s) + 8s " +
				"(--leader-elect-retry-period) + 40s (twice --leader-elect-renew-deadline). Raise " +
				"--leader-elect-lease-duration above 58s or lower --leader-elect-renew-deadline",
		},
		{
			name:   "a fraction of a second does not count toward the lease",
			timing: Timing{LeaseDuration: 58*time.Second + 999*time.Millisecond, RenewDeadline: 20 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "above 58s, and it is 58s: 10s (request timeout, half of --leader-elect-renew-deadline and at least " +
				"1s) + 8s (--leader-elect-retry-period) + 40s (twice --leader-elect-renew-deadline). client-go stores " +
				"the lease in whole seconds, so 58.999s counts as 58s. Raise --leader-elect-lease-duration above 58s",
		},
		{
			name:    "a lease that outlasts retry and twice renew but not the request timeout is rejected",
			timing:  Timing{LeaseDuration: 50 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "above 58s, and it is 50s: 10s (request timeout",
		},
		{
			name:   "the chart's default timing is rejected with each amount in seconds and the remedy",
			timing: Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "--leader-elect-release-on-cancel needs --leader-elect-lease-duration above 108s, and it is 60s: " +
				"20s (request timeout, half of --leader-elect-renew-deadline and at least 1s) + 8s " +
				"(--leader-elect-retry-period) + 80s (twice --leader-elect-renew-deadline). Raise " +
				"--leader-elect-lease-duration above 108s or lower --leader-elect-renew-deadline",
		},
		{
			name:   "amounts with a fraction of a second print in seconds and add up",
			timing: Timing{LeaseDuration: 7 * time.Second, RenewDeadline: 2500 * time.Millisecond, RetryPeriod: 1500 * time.Millisecond},
			wantErr: "above 7.75s, and it is 7s: 1.25s (request timeout, half of --leader-elect-renew-deadline and at " +
				"least 1s) + 1.5s (--leader-elect-retry-period) + 5s (twice --leader-elect-renew-deadline).",
		},
		{
			name:   "a request timeout is at least one second",
			timing: Timing{LeaseDuration: 5 * time.Second, RenewDeadline: 1500 * time.Millisecond, RetryPeriod: time.Second},
			wantErr: "above 5s, and it is 5s: 1s (request timeout, half of --leader-elect-renew-deadline and at least " +
				"1s) + 1s (--leader-elect-retry-period) + 3s (twice --leader-elect-renew-deadline).",
		},
		{
			name:   "a lease that covers the one second request timeout is valid",
			timing: Timing{LeaseDuration: 6 * time.Second, RenewDeadline: 1500 * time.Millisecond, RetryPeriod: time.Second},
		},
		{
			name: "unset timing names all three flags to set",
			wantErr: "--leader-elect-release-on-cancel needs --leader-elect-lease-duration, --leader-elect-renew-deadline " +
				"and --leader-elect-retry-period, because it checks the lease against the other two. Set " +
				"--leader-elect-lease-duration, --leader-elect-renew-deadline and --leader-elect-retry-period to " +
				"positive durations",
		},
		{
			name:    "a partial set names the flags still to set",
			timing:  Timing{LeaseDuration: 60 * time.Second},
			wantErr: "Set --leader-elect-renew-deadline and --leader-elect-retry-period to positive durations",
		},
		{
			name:    "a negative duration names its flag",
			timing:  Timing{LeaseDuration: 60 * time.Second, RenewDeadline: -20 * time.Second, RetryPeriod: 8 * time.Second},
			wantErr: "Set --leader-elect-renew-deadline to a positive duration",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.timing.ValidateReleaseOnCancel()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// The release-on-cancel rule counts controller-runtime's timeout for one
// leader election request. This test fails when controller-runtime gives the
// leader election client a different timeout than the rule counts.
func TestRequestTimeoutMatchesTheTimeoutControllerRuntimeSets(t *testing.T) {
	cases := []struct {
		name          string
		renewDeadline time.Duration
		want          time.Duration
	}{
		{name: "half the renew deadline", renewDeadline: 20 * time.Second, want: 10 * time.Second},
		{name: "one second when half the renew deadline is shorter", renewDeadline: 1500 * time.Millisecond, want: time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lock, err := crleaderelection.NewResourceLock(&rest.Config{Host: "https://127.0.0.1:6443"}, discardingRecorders{},
				crleaderelection.Options{
					LeaderElection:          true,
					LeaderElectionID:        "lock",
					LeaderElectionNamespace: "team-a",
					RenewDeadline:           tc.renewDeadline,
				})
			require.NoError(t, err)
			leaseLock, ok := lock.(*resourcelock.LeaseLock)
			require.True(t, ok, "controller-runtime builds a %T, not a Lease lock", lock)
			coordination, ok := leaseLock.Client.(*coordinationv1client.CoordinationV1Client)
			require.True(t, ok, "the Lease lock talks through a %T", leaseLock.Client)
			restClient, ok := coordination.RESTClient().(*rest.RESTClient)
			require.True(t, ok, "the coordination client talks through a %T", coordination.RESTClient())

			assert.Equal(t, tc.want, restClient.Client.Timeout, "the timeout controller-runtime sets")
			assert.Equal(t, tc.want, requestTimeout(tc.renewDeadline), "the timeout the rule counts")
		})
	}
}

// The release-on-cancel rule compares the lease in whole seconds. This test
// fails when client-go writes a lease that keeps a fraction of a second.
func TestWholeSecondsMatchesTheLeaseClientGoWrites(t *testing.T) {
	lease := 58*time.Second + 999*time.Millisecond
	lock := &recordingLock{}
	leading := make(chan struct{})
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: lease,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   8 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) { close(leading) },
			OnStoppedLeading: func() {},
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		elector.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	select {
	case <-leading:
	case <-time.After(10 * time.Second):
		t.Fatal("the elector did not take a lease that nobody holds")
	}

	records := lock.written()
	require.NotEmpty(t, records)
	assert.Equal(t, 58, records[0].LeaseDurationSeconds, "the lease client-go writes")
	assert.Equal(t, 58*time.Second, wholeSeconds(lease), "the lease the rule counts")
}

// discardingRecorders gives the leader election lock event recorders that drop
// every event.
type discardingRecorders struct{}

func (discardingRecorders) GetEventRecorderFor(string) record.EventRecorder {
	return &record.FakeRecorder{}
}

func (discardingRecorders) GetEventRecorder(string) events.EventRecorder {
	return &events.FakeRecorder{}
}

// recordingLock is a leader election lock that nobody holds. It keeps every
// record the elector writes.
type recordingLock struct {
	mu      sync.Mutex
	records []resourcelock.LeaderElectionRecord
}

func (l *recordingLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	return nil, nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "lock")
}

func (l *recordingLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	return l.Update(ctx, ler)
}

func (l *recordingLock) Update(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, ler)
	return nil
}

func (l *recordingLock) RecordEvent(string) {}

func (l *recordingLock) Identity() string { return "manager-a" }

func (l *recordingLock) Describe() string { return "team-a/lock" }

func (l *recordingLock) written() []resourcelock.LeaderElectionRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]resourcelock.LeaderElectionRecord(nil), l.records...)
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
