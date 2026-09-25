package routing

import (
	"strings"
	"testing"
	"time"
)

func validObserverConfig() ObserverConfig {
	return ObserverConfig{
		MaxConcurrentReconciles: 8,
		MaxConcurrentRequests:   16,
		MaxResponseBytes:        64 * 1024,
		MinPeriod:               time.Second,
		MaxSamples:              100,
	}
}

func observerTestProbeConfig() ProbeConfig {
	return ProbeConfig{
		Path:             "/probe",
		Method:           "GET",
		AcceptStatuses:   []int{200},
		GateStatuses:     []int{503},
		Period:           2 * time.Second,
		Timeout:          time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		AllFailedPolicy:  AllFailedPolicyPreserveTraffic,
	}
}

func observerTestCapacityConfig() CapacityConfig {
	return CapacityConfig{
		Path:    "/capacity",
		Method:  "GET",
		Format:  FormatReport,
		Period:  2 * time.Second,
		Timeout: time.Second,
		Samples: 10,
		Quorum:  2,
		MaxAge:  20 * time.Second,
	}
}

func TestObserverConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ObserverConfig)
		want   string
	}{
		{name: "valid", mutate: func(*ObserverConfig) {}},
		{
			name:   "max concurrent reconciles must be positive",
			mutate: func(c *ObserverConfig) { c.MaxConcurrentReconciles = 0 },
			want:   "routing.observer.maxConcurrentReconciles",
		},
		{
			name:   "max concurrent requests must be positive",
			mutate: func(c *ObserverConfig) { c.MaxConcurrentRequests = -1 },
			want:   "routing.observer.maxConcurrentRequests",
		},
		{
			name:   "max response bytes must be positive",
			mutate: func(c *ObserverConfig) { c.MaxResponseBytes = 0 },
			want:   "routing.observer.maxResponseBytes",
		},
		{
			name:   "minimum period must be positive",
			mutate: func(c *ObserverConfig) { c.MinPeriod = -time.Second },
			want:   "routing.observer.minPeriod",
		},
		{
			name:   "max samples must be positive",
			mutate: func(c *ObserverConfig) { c.MaxSamples = 0 },
			want:   "routing.observer.maxSamples",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validObserverConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tt.want)
			}
			if got := err.Error(); !strings.Contains(got, tt.want) {
				t.Fatalf("Validate() = %q, want error containing %q", got, tt.want)
			}
		})
	}
}

func TestConfigValidateRequiresObserverLimitsOnlyWhenEnabled(t *testing.T) {
	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("disabled Config.Validate() = %v, want nil", err)
	}
	if err := (Config{Enabled: true}).Validate(); err == nil {
		t.Fatal("enabled Config.Validate() = nil, want missing observer limit error")
	}
	if err := (Config{
		Enabled:   true,
		Observer:  validObserverConfig(),
		Publisher: PublisherConfig{ResyncInterval: time.Minute},
	}).Validate(); err != nil {
		t.Fatalf("configured Config.Validate() = %v, want nil", err)
	}
}

func TestConfigValidateRequiresPublisherResyncOnlyWhenEnabled(t *testing.T) {
	if err := (Config{Publisher: PublisherConfig{Name: "custom"}}).Validate(); err != nil {
		t.Fatalf("disabled Config.Validate() = %v, want nil", err)
	}

	cfg := Config{Enabled: true, Observer: validObserverConfig()}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "routing.publisher.resyncInterval") {
		t.Fatalf("enabled Config.Validate() = %v, want missing publisher resync error", err)
	}

	cfg.Publisher.ResyncInterval = time.Minute
	if err := cfg.Validate(); err != nil {
		t.Fatalf("configured Config.Validate() = %v, want nil", err)
	}

	disabled := Config{Publisher: PublisherConfig{ResyncInterval: -time.Second}}
	if err := disabled.Validate(); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("negative disabled Config.Validate() = %v, want negative publisher resync error", err)
	}
}

func TestConfigValidateEnforcesObserverPeriodFloor(t *testing.T) {
	t.Run("probe", func(t *testing.T) {
		probe := observerTestProbeConfig()
		probe.Period = 500 * time.Millisecond
		probe.Timeout = 250 * time.Millisecond
		cfg := Config{
			Enabled:   true,
			Observer:  validObserverConfig(),
			Probe:     probe,
			Publisher: PublisherConfig{ResyncInterval: time.Minute},
		}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "routing.observer.minPeriod") {
			t.Fatalf("Validate() = %v, want probe period floor error", err)
		}

		cfg.Probe.Period = cfg.Observer.MinPeriod
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() at probe period floor = %v, want nil", err)
		}
	})

	t.Run("capacity", func(t *testing.T) {
		capacity := observerTestCapacityConfig()
		capacity.Period = 500 * time.Millisecond
		capacity.Timeout = 250 * time.Millisecond
		capacity.MaxAge = 10 * time.Second
		cfg := Config{
			Enabled:   true,
			Observer:  validObserverConfig(),
			Capacity:  capacity,
			Publisher: PublisherConfig{ResyncInterval: time.Minute},
		}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "routing.observer.minPeriod") {
			t.Fatalf("Validate() = %v, want capacity period floor error", err)
		}

		cfg.Capacity.Period = cfg.Observer.MinPeriod
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() at capacity period floor = %v, want nil", err)
		}
	})
}

func TestConfigValidateCapsEffectiveCapacitySamples(t *testing.T) {
	capacity := observerTestCapacityConfig()
	cfg := Config{
		Enabled:   true,
		Observer:  validObserverConfig(),
		Capacity:  capacity,
		Publisher: PublisherConfig{ResyncInterval: time.Minute},
	}

	cfg.Observer.MaxSamples = capacity.Samples - 1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "routing.observer.maxSamples") {
		t.Fatalf("Validate() = %v, want sample window cap error", err)
	}

	cfg.Observer.MaxSamples = capacity.Samples
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() at sample window cap = %v, want nil", err)
	}

	cfg.Capacity.Samples = capacity.Samples + 1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "routing.observer.maxSamples") {
		t.Fatalf("Validate() = %v, want explicit sample window cap error", err)
	}
}
