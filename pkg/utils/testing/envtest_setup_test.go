package testing

import (
	"errors"
	gotesting "testing"
	"time"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestForBusyHosts_RaisesTheWaitsItIsNotGiven(t *gotesting.T) {
	env := ForBusyHosts(&envtest.Environment{})
	if env.ControlPlaneStartTimeout != busyHostStartTimeout {
		t.Errorf("start timeout = %s, want %s", env.ControlPlaneStartTimeout, busyHostStartTimeout)
	}
	if env.ControlPlaneStopTimeout != busyHostStopTimeout {
		t.Errorf("stop timeout = %s, want %s", env.ControlPlaneStopTimeout, busyHostStopTimeout)
	}
	if env.CRDInstallOptions.MaxTime != busyHostCRDWait {
		t.Errorf("CRD wait = %s, want %s", env.CRDInstallOptions.MaxTime, busyHostCRDWait)
	}
	args := env.ControlPlane.Etcd.Configure()
	if got := args.Get("election-timeout").Get(nil); len(got) != 1 || got[0] != "5000" {
		t.Errorf("election-timeout = %v, want [5000]", got)
	}
	if got := CRDInstallForBusyHosts(nil).MaxTime; got != busyHostCRDWait {
		t.Errorf("CRD install wait = %s, want %s", got, busyHostCRDWait)
	}
}

func TestForBusyHosts_KeepsTheWaitsTheCallerSet(t *gotesting.T) {
	env := ForBusyHosts(&envtest.Environment{
		ControlPlaneStartTimeout: time.Second,
		ControlPlaneStopTimeout:  2 * time.Second,
		CRDInstallOptions:        envtest.CRDInstallOptions{MaxTime: 3 * time.Second},
	})
	if env.ControlPlaneStartTimeout != time.Second || env.ControlPlaneStopTimeout != 2*time.Second || env.CRDInstallOptions.MaxTime != 3*time.Second {
		t.Errorf("waits = %s/%s/%s, want the caller's 1s/2s/3s",
			env.ControlPlaneStartTimeout, env.ControlPlaneStopTimeout, env.CRDInstallOptions.MaxTime)
	}
}

func TestStartOrRebuild(t *gotesting.T) {
	windowMissed := errors.New("default namespace didn't register within deadline: context deadline exceeded")
	other := errors.New("unable to start control plane itself: boom")
	for _, tt := range []struct {
		name      string
		starts    []error // the outcome of each start, in order
		wantEnv   int     // the 1-based attempt whose control plane is returned; 0 for an error
		wantErr   error
		wantStops []int // the attempts whose control plane was stopped
	}{
		{name: "the first start is kept", starts: []error{nil}, wantEnv: 1},
		{name: "a missed default-namespace window is rebuilt once", starts: []error{windowMissed, nil}, wantEnv: 2, wantStops: []int{1}},
		{name: "a second missed window is the error", starts: []error{windowMissed, windowMissed}, wantErr: windowMissed, wantStops: []int{1, 2}},
		{name: "any other start error is not retried", starts: []error{other}, wantErr: other, wantStops: []int{1}},
	} {
		t.Run(tt.name, func(t *gotesting.T) {
			var built []*envtest.Environment
			var stopped []int
			configs := map[*envtest.Environment]*rest.Config{}
			newEnv := func() *envtest.Environment {
				env := &envtest.Environment{}
				built = append(built, env)
				configs[env] = &rest.Config{Host: tt.name}
				return env
			}
			start := func(env *envtest.Environment) (*rest.Config, error) {
				attempt := len(built)
				if attempt > len(tt.starts) {
					t.Fatalf("start called %d times, want at most %d", attempt, len(tt.starts))
				}
				if err := tt.starts[attempt-1]; err != nil {
					return nil, err
				}
				return configs[env], nil
			}
			stop := func(env *envtest.Environment) error {
				for i, b := range built {
					if b == env {
						stopped = append(stopped, i+1)
					}
				}
				return nil
			}
			env, cfg, err := startOrRebuild(newEnv, start, stop)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantEnv == 0 {
				if env != nil || cfg != nil {
					t.Fatalf("env/cfg = %v/%v, want none with an error", env, cfg)
				}
			} else if env != built[tt.wantEnv-1] || cfg != configs[env] {
				t.Fatalf("returned attempt %d's control plane? env=%p cfg=%v", tt.wantEnv, env, cfg)
			}
			if len(built) != len(tt.starts) {
				t.Errorf("starts = %d, want %d", len(built), len(tt.starts))
			}
			if !equalInts(stopped, tt.wantStops) {
				t.Errorf("stopped attempts = %v, want %v", stopped, tt.wantStops)
			}
		})
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
