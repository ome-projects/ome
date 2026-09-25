package routing

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestResolveConfigEnabled(t *testing.T) {
	trueValue, falseValue := true, false
	tests := []struct {
		name          string
		globalEnabled bool
		spec          *v1beta1.InferenceServiceSpec
		wantEnabled   bool
	}{
		{name: "nil spec inherits enabled", globalEnabled: true, wantEnabled: true},
		{name: "nil enabled inherits enabled", globalEnabled: true, spec: routingISVCSpec(&v1beta1.RoutingSpec{}), wantEnabled: true},
		{name: "false opts out", globalEnabled: true, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &falseValue})},
		{name: "true preserves enabled", globalEnabled: true, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue}), wantEnabled: true},
		{name: "nil spec inherits disabled", globalEnabled: false},
		{name: "nil enabled inherits disabled", globalEnabled: false, spec: routingISVCSpec(&v1beta1.RoutingSpec{})},
		{name: "true cannot bypass global gate", globalEnabled: false, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue})},
		{name: "false remains disabled", globalEnabled: false, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &falseValue})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			global := Config{Enabled: tt.globalEnabled}
			if global.Enabled {
				global.Observer = testGlobalRoutingConfig().Observer
				global.Publisher.ResyncInterval = time.Minute
			}
			got, err := ResolveConfig(global, tt.spec, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, got.Enabled)
		})
	}
}

func TestResolveConfigValidationFollowsEffectiveEnablement(t *testing.T) {
	trueValue, falseValue := true, false
	stagedCapacity := func() *v1beta1.RoutingCapacitySpec {
		capacity := validRoutingCapacitySpec()
		capacity.Format = "Unavailable"
		return capacity
	}
	stagedPublisher := &v1beta1.RoutingPublisherSpec{Options: map[string]string{
		"routingClass": "staged",
	}}

	tests := []struct {
		name            string
		global          Config
		routing         *v1beta1.RoutingSpec
		wantEnabled     bool
		wantErrContains string
		wantResolver    bool
	}{
		{
			name: "disabled installation skips invalid global observer plugin",
			global: func() Config {
				cfg := testGlobalRoutingConfig()
				cfg.Enabled = false
				cfg.Capacity.Format = CapacityFormat("Unavailable")
				return cfg
			}(),
		},
		{
			name: "enabled installation validates inherited global observer plugin",
			global: func() Config {
				cfg := testGlobalRoutingConfig()
				cfg.Capacity.Format = CapacityFormat("Unavailable")
				return cfg
			}(),
			wantEnabled:     true,
			wantErrContains: "routing.capacity.format \"Unavailable\" is not registered",
		},
		{
			name:        "per-ISVC opt-out skips staged observer and publisher semantics",
			global:      testGlobalRoutingConfig(),
			routing:     &v1beta1.RoutingSpec{Enabled: &falseValue, Capacity: stagedCapacity(), Publisher: stagedPublisher},
			wantEnabled: false,
		},
		{
			name: "inline true cannot bypass disabled installation",
			global: func() Config {
				cfg := testGlobalRoutingConfig()
				cfg.Enabled = false
				return cfg
			}(),
			routing:     &v1beta1.RoutingSpec{Enabled: &trueValue, Capacity: stagedCapacity(), Publisher: stagedPublisher},
			wantEnabled: false,
		},
		{
			name:            "enabled policy validates observer before publisher semantics",
			global:          testGlobalRoutingConfig(),
			routing:         &v1beta1.RoutingSpec{Enabled: &trueValue, Capacity: stagedCapacity(), Publisher: stagedPublisher},
			wantEnabled:     true,
			wantErrContains: "spec.routing.capacity.format \"Unavailable\" is not registered",
		},
		{
			name:         "enabled valid policy resolves publisher semantics",
			global:       testGlobalRoutingConfig(),
			routing:      &v1beta1.RoutingSpec{Enabled: &trueValue, Publisher: stagedPublisher},
			wantEnabled:  true,
			wantResolver: true,
		},
		{
			name:            "generic API shape remains mandatory while opted out",
			global:          testGlobalRoutingConfig(),
			routing:         &v1beta1.RoutingSpec{Enabled: &falseValue, Capacity: &v1beta1.RoutingCapacitySpec{Path: "/capacity"}, Publisher: stagedPublisher},
			wantErrContains: "method is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolverCalled := false
			got, err := ResolveConfig(tt.global, routingISVCSpec(tt.routing),
				func(globalOptions, inlineOptions map[string]string) (map[string]string, error) {
					resolverCalled = true
					for key, value := range inlineOptions {
						globalOptions[key] = value
					}
					return globalOptions, nil
				})
			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				assert.False(t, resolverCalled)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, got.Enabled)
			assert.Equal(t, tt.wantResolver, resolverCalled)
			if tt.wantResolver {
				assert.Equal(t, "staged", got.Publisher.Options["routingClass"])
			} else {
				assert.Equal(t, tt.global.Publisher.Options, got.Publisher.Options,
					"disabled routing must retain global publisher semantics without applying staged options")
			}
		})
	}
}

func TestResolveConfigObserverBlocks(t *testing.T) {
	global := testGlobalRoutingConfig()

	t.Run("nil blocks inherit", func(t *testing.T) {
		got, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{}), nil)
		require.NoError(t, err)
		assert.Equal(t, global.Probe, got.Probe)
		assert.Equal(t, global.Capacity, got.Capacity)
	})

	t.Run("disabled blocks clear", func(t *testing.T) {
		got, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Probe:    &v1beta1.RoutingProbeSpec{Disabled: true},
			Capacity: &v1beta1.RoutingCapacitySpec{Disabled: true},
		}), nil)
		require.NoError(t, err)
		assert.Equal(t, ProbeConfig{}, got.Probe)
		assert.Equal(t, CapacityConfig{}, got.Capacity)
	})

	t.Run("inline blocks replace atomically", func(t *testing.T) {
		got, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Probe: &v1beta1.RoutingProbeSpec{
				Path:             "/ready",
				Method:           "HEAD",
				AcceptStatuses:   []int32{204},
				GateStatuses:     []int32{500, 503},
				Period:           metav1.Duration{Duration: 2 * time.Minute},
				Timeout:          metav1.Duration{Duration: 7 * time.Second},
				FailureThreshold: 4,
				SuccessThreshold: 5,
				AllFailedPolicy:  v1beta1.RoutingAllFailedPolicyDrain,
			},
			Capacity: &v1beta1.RoutingCapacitySpec{
				Path:    "/capacity",
				Method:  "POST",
				Format:  string(FormatReport),
				Period:  metav1.Duration{Duration: 3 * time.Minute},
				Timeout: metav1.Duration{Duration: 8 * time.Second},
				Samples: 7,
				Quorum:  3,
				MaxAge:  metav1.Duration{Duration: 10 * time.Minute},
			},
		}), nil)
		require.NoError(t, err)

		assert.Equal(t, ProbeConfig{
			Path:             "/ready",
			Method:           "HEAD",
			AcceptStatuses:   []int{204},
			GateStatuses:     []int{500, 503},
			Period:           2 * time.Minute,
			Timeout:          7 * time.Second,
			FailureThreshold: 4,
			SuccessThreshold: 5,
			AllFailedPolicy:  AllFailedPolicyDrain,
		}, got.Probe)
		assert.Equal(t, CapacityConfig{
			Path:    "/capacity",
			Method:  "POST",
			Format:  FormatReport,
			Period:  3 * time.Minute,
			Timeout: 8 * time.Second,
			Samples: 7,
			Quorum:  3,
			MaxAge:  10 * time.Minute,
		}, got.Capacity)
	})

	t.Run("partial inline block does not merge global fields", func(t *testing.T) {
		_, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Probe: &v1beta1.RoutingProbeSpec{Path: "/replacement"},
		}), nil)
		require.EqualError(t, err,
			"spec.routing.probe.method is required")

		_, err = ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Capacity: &v1beta1.RoutingCapacitySpec{Path: "/replacement"},
		}), nil)
		require.EqualError(t, err,
			"spec.routing.capacity.method is required")
	})
}

func TestResolveConfigPublisherOptions(t *testing.T) {
	global := testGlobalRoutingConfig()
	routingSpec := &v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"routingClass": "latency-sensitive",
			"serviceAlias": "service.example.com",
		}},
	}
	var callbackResult map[string]string
	resolver := func(globalOptions, inlineOptions map[string]string) (map[string]string, error) {
		assert.Equal(t, global.Publisher.Options, globalOptions)
		assert.Equal(t, routingSpec.Publisher.Options, inlineOptions)
		for key, value := range inlineOptions {
			globalOptions[key] = value
		}
		inlineOptions["routingClass"] = "callback-mutated"
		callbackResult = globalOptions
		return globalOptions, nil
	}

	got, err := ResolveConfig(global, routingISVCSpec(routingSpec), resolver)
	require.NoError(t, err)
	assert.Equal(t, global.Publisher.Name, got.Publisher.Name)
	assert.Equal(t, map[string]string{
		"globalOnly":   "retained",
		"routingClass": "latency-sensitive",
		"serviceAlias": "service.example.com",
	}, got.Publisher.Options)
	got.Publisher.Options["routingClass"] = "caller-mutated"
	assert.Equal(t, "latency-sensitive", callbackResult["routingClass"],
		"effective config must not alias the publisher's returned map")
	assert.Equal(t, "global", global.Publisher.Options["routingClass"],
		"publisher callback must receive a copy of global options")
	assert.Equal(t, "latency-sensitive", routingSpec.Publisher.Options["routingClass"],
		"publisher callback must receive a copy of inline options")

	_, err = ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"routingClass": "latency-sensitive",
		}},
	}), nil)
	require.EqualError(t, err,
		"spec.routing.publisher.options cannot be set because the selected publisher does not support per-InferenceService options")

	rejectOutOfScope := func(_, _ map[string]string) (map[string]string, error) {
		return nil, errors.New("serviceAlias is outside the permitted scope")
	}
	_, err = ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"serviceAlias": "other.example.com",
		}},
	}), rejectOutOfScope)
	require.EqualError(t, err,
		"spec.routing.publisher.options: serviceAlias is outside the permitted scope")
}

func TestResolveConfigRunsSharedRoutingValidation(t *testing.T) {
	validProbe := func() *v1beta1.RoutingProbeSpec {
		return &v1beta1.RoutingProbeSpec{
			Path:             "/ready",
			Method:           "GET",
			AcceptStatuses:   []int32{200},
			GateStatuses:     []int32{500},
			Period:           metav1.Duration{Duration: time.Minute},
			Timeout:          metav1.Duration{Duration: time.Second},
			FailureThreshold: 2,
			SuccessThreshold: 2,
			AllFailedPolicy:  v1beta1.RoutingAllFailedPolicyPreserveTraffic,
		}
	}
	tests := []struct {
		name        string
		spec        *v1beta1.InferenceServiceSpec
		wantContain string
	}{
		{
			name: "disabled probe is exclusive",
			spec: routingISVCSpec(&v1beta1.RoutingSpec{
				Probe: &v1beta1.RoutingProbeSpec{Disabled: true, Path: "/ready"},
			}),
			wantContain: "disabled must not be combined",
		},
		{
			name: "HTTP status is bounded",
			spec: func() *v1beta1.InferenceServiceSpec {
				probe := validProbe()
				probe.AcceptStatuses = []int32{99}
				return routingISVCSpec(&v1beta1.RoutingSpec{Probe: probe})
			}(),
			wantContain: "invalid HTTP status",
		},
		{
			name: "HTTP method is restricted",
			spec: func() *v1beta1.InferenceServiceSpec {
				probe := validProbe()
				probe.Method = "DELETE"
				return routingISVCSpec(&v1beta1.RoutingSpec{Probe: probe})
			}(),
			wantContain: "method",
		},
		{
			name: "publisher option size is bounded",
			spec: routingISVCSpec(&v1beta1.RoutingSpec{
				Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
					"routingClass": strings.Repeat("x", v1beta1.MaxRoutingPublisherOptionValueBytes+1),
				}},
			}),
			wantContain: "byte value limit",
		},
		{
			name: "new and deprecated capacity factors conflict",
			spec: &v1beta1.InferenceServiceSpec{
				Routing: &v1beta1.RoutingSpec{
					CapacityFactors: map[string]resource.Quantity{},
				},
				Placement: &v1beta1.PlacementSpec{
					//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
					CapacityFactors: map[string]resource.Quantity{},
				},
			},
			wantContain: "must not both be set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisherCalled := false
			_, err := ResolveConfig(testGlobalRoutingConfig(), tt.spec,
				func(_, _ map[string]string) (map[string]string, error) {
					publisherCalled = true
					return nil, nil
				})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantContain)
			assert.False(t, publisherCalled)
		})
	}
}

func TestResolveConfigMutationIsolation(t *testing.T) {
	global := testGlobalRoutingConfig()
	global.Capacity = CapacityConfig{Options: map[string]string{"source": "global"}}
	routingSpec := &v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{
			Options: map[string]string{"routingClass": "inline"},
		},
	}
	resolver := func(globalOptions, inlineOptions map[string]string) (map[string]string, error) {
		for key, value := range inlineOptions {
			globalOptions[key] = value
		}
		return globalOptions, nil
	}

	first, err := ResolveConfig(global, routingISVCSpec(routingSpec), resolver)
	require.NoError(t, err)
	second, err := ResolveConfig(global, routingISVCSpec(routingSpec), resolver)
	require.NoError(t, err)

	first.Probe.AcceptStatuses[0] = 299
	first.Probe.GateStatuses[0] = 599
	first.Capacity.Options["source"] = "first"
	first.Publisher.Options["routingClass"] = "first"
	first.Publisher.Options["globalOnly"] = "first"
	routingSpec.Publisher.Options["routingClass"] = "source-mutated"

	assert.Equal(t, []int{200, 204}, global.Probe.AcceptStatuses)
	assert.Equal(t, []int{500, 503}, global.Probe.GateStatuses)
	assert.Equal(t, map[string]string{"source": "global"}, global.Capacity.Options)
	assert.Equal(t, map[string]string{
		"globalOnly":   "retained",
		"routingClass": "global",
	}, global.Publisher.Options)
	assert.Equal(t, map[string]string{"source": "global"}, second.Capacity.Options)
	assert.Equal(t, map[string]string{
		"globalOnly":   "retained",
		"routingClass": "inline",
	}, second.Publisher.Options)
}

func TestResolveProbePolicyEnablementAndInheritance(t *testing.T) {
	trueValue, falseValue := true, false
	global := testGlobalRoutingConfig()
	tests := []struct {
		name        string
		global      Config
		spec        *v1beta1.InferenceServiceSpec
		wantEnabled bool
		wantProbe   ProbeConfig
	}{
		{
			name:        "nil spec inherits global probe",
			global:      global,
			wantEnabled: true,
			wantProbe:   global.Probe,
		},
		{
			name:        "nil inline enablement inherits global probe",
			global:      global,
			spec:        routingISVCSpec(&v1beta1.RoutingSpec{}),
			wantEnabled: true,
			wantProbe:   global.Probe,
		},
		{
			name:        "inline true retains global probe",
			global:      global,
			spec:        routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue}),
			wantEnabled: true,
			wantProbe:   global.Probe,
		},
		{
			name:      "inline false opts out",
			global:    global,
			spec:      routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &falseValue}),
			wantProbe: global.Probe,
		},
		{
			name: "inline true cannot bypass global routing gate",
			global: func() Config {
				cfg := global
				cfg.Enabled = false
				return cfg
			}(),
			spec:      routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue}),
			wantProbe: global.Probe,
		},
		{
			name:   "unconfigured probe remains disabled",
			global: Config{Enabled: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveProbePolicy(tt.global, tt.spec)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, got.Enabled)
			assert.Equal(t, tt.wantEnabled, got.IsEnabled())
			assert.Equal(t, tt.wantProbe, got.Probe)
			if tt.wantEnabled {
				assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, got.PolicyDigest)
			} else {
				assert.Empty(t, got.PolicyDigest)
			}
		})
	}
}

func TestResolveProbePolicyInlineReplacementAndDisable(t *testing.T) {
	global := testGlobalRoutingConfig()
	inline := validRoutingProbeSpec()
	inline.Path = "/deep-ready"
	inline.Method = "HEAD"
	inline.AcceptStatuses = []int32{201, 202}
	inline.GateStatuses = []int32{502, 504}
	inline.Period = metav1.Duration{Duration: 3 * time.Minute}
	inline.Timeout = metav1.Duration{Duration: 9 * time.Second}
	inline.FailureThreshold = 4
	inline.SuccessThreshold = 5
	inline.AllFailedPolicy = v1beta1.RoutingAllFailedPolicyDrain

	got, err := ResolveProbePolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Probe: inline}))
	require.NoError(t, err)
	assert.True(t, got.IsEnabled())
	assert.Equal(t, ProbeConfig{
		Path:             "/deep-ready",
		Method:           "HEAD",
		AcceptStatuses:   []int{201, 202},
		GateStatuses:     []int{502, 504},
		Period:           3 * time.Minute,
		Timeout:          9 * time.Second,
		FailureThreshold: 4,
		SuccessThreshold: 5,
		AllFailedPolicy:  AllFailedPolicyDrain,
	}, got.Probe)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, got.PolicyDigest)

	disabled, err := ResolveProbePolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Probe: &v1beta1.RoutingProbeSpec{Disabled: true},
	}))
	require.NoError(t, err)
	assert.False(t, disabled.IsEnabled())
	assert.Equal(t, ProbeConfig{}, disabled.Probe)
	assert.Empty(t, disabled.PolicyDigest)
}

func TestResolveProbePolicyIgnoresUnrelatedInlineBlocks(t *testing.T) {
	global := testGlobalRoutingConfig()
	wantGlobal := cloneConfig(global)
	inline := validRoutingProbeSpec()
	inline.Path = "/inline-ready"

	got, err := ResolveProbePolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Probe: inline,
		Capacity: &v1beta1.RoutingCapacitySpec{
			Path:   "/invalid-capacity-without-method",
			Format: "Unavailable",
		},
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"": strings.Repeat("x", v1beta1.MaxRoutingPublisherOptionValueBytes+1),
		}},
	}))
	require.NoError(t, err)
	assert.True(t, got.IsEnabled())
	assert.Equal(t, "/inline-ready", got.Probe.Path)
	assert.Equal(t, wantGlobal.Capacity, global.Capacity)
	assert.Equal(t, wantGlobal.Publisher, global.Publisher)
	assert.Equal(t, wantGlobal, global)
}

func TestResolveProbePolicyValidationScope(t *testing.T) {
	global := testGlobalRoutingConfig()

	_, err := ResolveProbePolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Probe: &v1beta1.RoutingProbeSpec{Path: "/partial"},
	}))
	require.EqualError(t, err, "spec.routing.probe.method is required")

	_, err = ResolveProbePolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Probe: &v1beta1.RoutingProbeSpec{Disabled: true, Path: "/conflicting"},
	}))
	require.EqualError(t, err,
		"spec.routing.probe.disabled must not be combined with probe configuration fields")

	invalidGlobal := global
	invalidGlobal.Probe.Method = "DELETE"
	_, err = ResolveProbePolicy(invalidGlobal, nil)
	require.EqualError(t, err,
		"routing.probe.method \"DELETE\" must be GET, HEAD, or POST")

	invalidGlobal.Enabled = false
	got, err := ResolveProbePolicy(invalidGlobal, nil)
	require.NoError(t, err)
	assert.False(t, got.IsEnabled())
	assert.Empty(t, got.PolicyDigest)
}

func TestResolveProbePolicyEnforcesObserverMinimumPeriod(t *testing.T) {
	global := testGlobalRoutingConfig()
	global.Observer.MinPeriod = 2 * time.Minute

	_, err := ResolveProbePolicy(global, nil)
	require.EqualError(t, err,
		"routing.probe.period (1m0s) must be at least routing.observer.minPeriod (2m0s)")

	inline := validRoutingProbeSpec()
	inline.Period = metav1.Duration{Duration: 90 * time.Second}
	inline.Timeout = metav1.Duration{Duration: time.Second}
	_, err = ResolveProbePolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Probe: inline}))
	require.EqualError(t, err,
		"spec.routing.probe.period (1m30s) must be at least routing.observer.minPeriod (2m0s)")

	inline.Period = metav1.Duration{Duration: 2 * time.Minute}
	got, err := ResolveProbePolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Probe: inline}))
	require.NoError(t, err)
	assert.True(t, got.IsEnabled())
}

func TestResolveProbePolicyDigestIsCanonicalAndComplete(t *testing.T) {
	base := testGlobalRoutingConfig()
	base.Probe.AcceptStatuses = []int{204, 200}
	base.Probe.GateStatuses = []int{503, 500}
	wantAccept := append([]int(nil), base.Probe.AcceptStatuses...)
	wantGate := append([]int(nil), base.Probe.GateStatuses...)

	resolved, err := ResolveProbePolicy(base, nil)
	require.NoError(t, err)
	assert.Equal(t, wantAccept, base.Probe.AcceptStatuses)
	assert.Equal(t, wantGate, base.Probe.GateStatuses)
	assert.Equal(t, wantAccept, resolved.Probe.AcceptStatuses)
	assert.Equal(t, wantGate, resolved.Probe.GateStatuses)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, resolved.PolicyDigest)

	reordered := base
	reordered.Probe.AcceptStatuses = []int{200, 204, 200}
	reordered.Probe.GateStatuses = []int{500, 503, 503}
	reorderedResolved, err := ResolveProbePolicy(reordered, nil)
	require.NoError(t, err)
	assert.Equal(t, resolved.PolicyDigest, reorderedResolved.PolicyDigest,
		"set ordering and duplicate representations must not change the digest")

	mutations := map[string]func(*ProbeConfig){
		"path": func(probe *ProbeConfig) {
			probe.Path = "/other"
		},
		"method": func(probe *ProbeConfig) {
			probe.Method = "HEAD"
		},
		"accept statuses": func(probe *ProbeConfig) {
			probe.AcceptStatuses = []int{200, 201}
		},
		"gate statuses": func(probe *ProbeConfig) {
			probe.GateStatuses = []int{500, 502}
		},
		"period": func(probe *ProbeConfig) {
			probe.Period += time.Second
		},
		"timeout": func(probe *ProbeConfig) {
			probe.Timeout += time.Second
		},
		"failure threshold": func(probe *ProbeConfig) {
			probe.FailureThreshold++
		},
		"success threshold": func(probe *ProbeConfig) {
			probe.SuccessThreshold++
		},
		"all failed policy": func(probe *ProbeConfig) {
			probe.AllFailedPolicy = AllFailedPolicyDrain
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := base
			changed.Probe = cloneProbeConfig(base.Probe)
			mutate(&changed.Probe)
			got, err := ResolveProbePolicy(changed, nil)
			require.NoError(t, err)
			assert.NotEqual(t, resolved.PolicyDigest, got.PolicyDigest)
		})
	}
}

func TestResolveCapacityPolicyEnablementAndInheritance(t *testing.T) {
	trueValue, falseValue := true, false
	global := testGlobalRoutingConfig()
	tests := []struct {
		name         string
		global       Config
		spec         *v1beta1.InferenceServiceSpec
		wantEnabled  bool
		wantCapacity CapacityConfig
	}{
		{
			name:         "nil spec inherits global capacity",
			global:       global,
			wantEnabled:  true,
			wantCapacity: global.Capacity,
		},
		{
			name:         "nil inline enablement inherits global capacity",
			global:       global,
			spec:         routingISVCSpec(&v1beta1.RoutingSpec{}),
			wantEnabled:  true,
			wantCapacity: global.Capacity,
		},
		{
			name:         "inline true retains global capacity",
			global:       global,
			spec:         routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue}),
			wantEnabled:  true,
			wantCapacity: global.Capacity,
		},
		{
			name:         "inline false opts out",
			global:       global,
			spec:         routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &falseValue}),
			wantCapacity: global.Capacity,
		},
		{
			name: "inline true cannot bypass global routing gate",
			global: func() Config {
				cfg := global
				cfg.Enabled = false
				return cfg
			}(),
			spec:         routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue}),
			wantCapacity: global.Capacity,
		},
		{
			name:   "unconfigured capacity remains disabled",
			global: Config{Enabled: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveCapacityPolicy(tt.global, tt.spec)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, got.Enabled)
			assert.Equal(t, tt.wantEnabled, got.IsEnabled())
			assert.Equal(t, tt.wantCapacity, got.Capacity)
			if tt.wantEnabled {
				assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, got.PolicyDigest)
			} else {
				assert.Empty(t, got.PolicyDigest)
			}
		})
	}
}

func TestResolveCapacityPolicyInlineReplacementAndDisable(t *testing.T) {
	global := testGlobalRoutingConfig()
	inline := validRoutingCapacitySpec()
	inline.Path = "/deep-capacity"
	inline.Method = "POST"
	inline.Period = metav1.Duration{Duration: 3 * time.Minute}
	inline.Timeout = metav1.Duration{Duration: 9 * time.Second}
	inline.Samples = 7
	inline.Quorum = 3
	inline.MaxAge = metav1.Duration{Duration: 10 * time.Minute}

	got, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline}))
	require.NoError(t, err)
	assert.True(t, got.IsEnabled())
	assert.Equal(t, CapacityConfig{
		Path:    "/deep-capacity",
		Method:  "POST",
		Format:  FormatReport,
		Period:  3 * time.Minute,
		Timeout: 9 * time.Second,
		Samples: 7,
		Quorum:  3,
		MaxAge:  10 * time.Minute,
	}, got.Capacity)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, got.PolicyDigest)

	disabled, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Capacity: &v1beta1.RoutingCapacitySpec{Disabled: true},
	}))
	require.NoError(t, err)
	assert.False(t, disabled.IsEnabled())
	assert.Equal(t, CapacityConfig{}, disabled.Capacity)
	assert.Empty(t, disabled.PolicyDigest)
}

func TestResolveCapacityPolicyIgnoresUnrelatedInlineBlocks(t *testing.T) {
	global := testGlobalRoutingConfig()
	global.Probe.Method = "DELETE"
	wantGlobal := cloneConfig(global)
	inline := validRoutingCapacitySpec()
	inline.Path = "/inline-capacity"

	got, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Capacity: inline,
		Probe: &v1beta1.RoutingProbeSpec{
			Path:   "/invalid-probe-without-statuses",
			Method: "DELETE",
		},
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"": strings.Repeat("x", v1beta1.MaxRoutingPublisherOptionValueBytes+1),
		}},
	}))
	require.NoError(t, err)
	assert.True(t, got.IsEnabled())
	assert.Equal(t, "/inline-capacity", got.Capacity.Path)
	assert.Equal(t, wantGlobal.Probe, global.Probe)
	assert.Equal(t, wantGlobal.Publisher, global.Publisher)
	assert.Equal(t, wantGlobal, global)
}

func TestResolveCapacityPolicyValidationScopeAndAttribution(t *testing.T) {
	global := testGlobalRoutingConfig()

	_, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Capacity: &v1beta1.RoutingCapacitySpec{Path: "/partial"},
	}))
	require.EqualError(t, err, "spec.routing.capacity.method is required")

	_, err = ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Capacity: &v1beta1.RoutingCapacitySpec{Disabled: true, Path: "/conflicting"},
	}))
	require.EqualError(t, err,
		"spec.routing.capacity.disabled must not be combined with capacity configuration fields")

	invalidGlobal := global
	invalidGlobal.Capacity.Method = "DELETE"
	_, err = ResolveCapacityPolicy(invalidGlobal, nil)
	require.EqualError(t, err,
		"routing.capacity.method \"DELETE\" must be GET, HEAD, or POST")

	inline := validRoutingCapacitySpec()
	inline.Format = "Unavailable"
	_, err = ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline}))
	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.routing.capacity.format \"Unavailable\" is not registered")

	inline = validRoutingCapacitySpec()
	inline.Options = map[string]string{"unexpected": "value"}
	_, err = ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline}))
	require.EqualError(t, err,
		"spec.routing.capacity.options is not accepted for format \"Report\"")

	falseValue := false
	invalidInline := &v1beta1.RoutingCapacitySpec{Path: "/partial"}
	_, err = ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Enabled:  &falseValue,
		Capacity: invalidInline,
	}))
	require.EqualError(t, err, "spec.routing.capacity.method is required")

	invalidGlobal.Enabled = false
	got, err := ResolveCapacityPolicy(invalidGlobal, nil)
	require.NoError(t, err)
	assert.False(t, got.IsEnabled())
	assert.Empty(t, got.PolicyDigest)
}

func TestResolveCapacityPolicyEnforcesObserverBounds(t *testing.T) {
	t.Run("global minimum period", func(t *testing.T) {
		global := testGlobalRoutingConfig()
		global.Observer.MinPeriod = 3 * time.Minute

		_, err := ResolveCapacityPolicy(global, nil)
		require.EqualError(t, err,
			"routing.capacity.period (2m0s) must be at least routing.observer.minPeriod (3m0s)")
	})

	t.Run("inline minimum period is attributed to spec", func(t *testing.T) {
		global := testGlobalRoutingConfig()
		global.Observer.MinPeriod = time.Minute
		inline := validRoutingCapacitySpec()

		_, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline}))
		require.EqualError(t, err,
			"spec.routing.capacity.period (30s) must be at least routing.observer.minPeriod (1m0s)")

		inline.Period = metav1.Duration{Duration: time.Minute}
		inline.MaxAge = metav1.Duration{Duration: 2 * time.Minute}
		got, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline}))
		require.NoError(t, err)
		assert.True(t, got.IsEnabled())
	})

	t.Run("global maximum samples", func(t *testing.T) {
		global := testGlobalRoutingConfig()
		global.Observer.MaxSamples = global.Capacity.Samples - 1

		_, err := ResolveCapacityPolicy(global, nil)
		require.EqualError(t, err,
			"routing.capacity.samples (6) must not exceed routing.observer.maxSamples (5)")
	})

	t.Run("inline maximum samples is attributed to spec", func(t *testing.T) {
		global := testGlobalRoutingConfig()
		global.Observer.MaxSamples = 9
		inline := validRoutingCapacitySpec()

		_, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline}))
		require.EqualError(t, err,
			"spec.routing.capacity.samples (10) must not exceed routing.observer.maxSamples (9)")

		global.Observer.MaxSamples = 10
		got, err := ResolveCapacityPolicy(global, routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline}))
		require.NoError(t, err)
		assert.True(t, got.IsEnabled())
	})
}

func TestCapacityPolicyDigestIsCanonicalAndComplete(t *testing.T) {
	base := CapacityConfig{
		Path:    "/capacity",
		Method:  "POST",
		Format:  "Custom",
		Options: map[string]string{"zeta": "last", "alpha": "first"},
		Period:  2 * time.Minute,
		Timeout: 5 * time.Second,
		Samples: 8,
		Quorum:  3,
		MaxAge:  10 * time.Minute,
	}

	digest, err := capacityPolicyDigest(base)
	require.NoError(t, err)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, digest)
	assert.Equal(t, []capacityPolicyOption{
		{Key: "alpha", Value: "first"},
		{Key: "zeta", Value: "last"},
	}, canonicalCapacityOptions(base.Options))

	reordered := cloneCapacityConfig(base)
	reordered.Options = make(map[string]string, len(base.Options))
	reordered.Options["alpha"] = "first"
	reordered.Options["zeta"] = "last"
	reorderedDigest, err := capacityPolicyDigest(reordered)
	require.NoError(t, err)
	assert.Equal(t, digest, reorderedDigest,
		"map insertion order must not change the digest")

	withoutOptions := cloneCapacityConfig(base)
	withoutOptions.Options = nil
	emptyOptions := cloneCapacityConfig(base)
	emptyOptions.Options = map[string]string{}
	nilDigest, err := capacityPolicyDigest(withoutOptions)
	require.NoError(t, err)
	emptyDigest, err := capacityPolicyDigest(emptyOptions)
	require.NoError(t, err)
	assert.Equal(t, nilDigest, emptyDigest,
		"nil and empty option maps represent the same policy")

	mutations := map[string]func(*CapacityConfig){
		"path": func(capacity *CapacityConfig) {
			capacity.Path = "/other"
		},
		"method": func(capacity *CapacityConfig) {
			capacity.Method = "GET"
		},
		"format": func(capacity *CapacityConfig) {
			capacity.Format = "Other"
		},
		"option key": func(capacity *CapacityConfig) {
			capacity.Options = map[string]string{"beta": "first", "zeta": "last"}
		},
		"option value": func(capacity *CapacityConfig) {
			capacity.Options["alpha"] = "changed"
		},
		"period": func(capacity *CapacityConfig) {
			capacity.Period += time.Second
		},
		"timeout": func(capacity *CapacityConfig) {
			capacity.Timeout += time.Second
		},
		"samples": func(capacity *CapacityConfig) {
			capacity.Samples++
		},
		"quorum": func(capacity *CapacityConfig) {
			capacity.Quorum++
		},
		"max age": func(capacity *CapacityConfig) {
			capacity.MaxAge += time.Second
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := cloneCapacityConfig(base)
			mutate(&changed)
			got, err := capacityPolicyDigest(changed)
			require.NoError(t, err)
			assert.NotEqual(t, digest, got)
		})
	}
}

func TestResolveCapacityPolicyMutationIsolation(t *testing.T) {
	global := testGlobalRoutingConfig()
	global.Enabled = false
	global.Capacity.Format = "Global"
	global.Capacity.Options = map[string]string{"source": "global"}

	first, err := ResolveCapacityPolicy(global, nil)
	require.NoError(t, err)
	second, err := ResolveCapacityPolicy(global, nil)
	require.NoError(t, err)
	first.Capacity.Options["source"] = "first"
	assert.Equal(t, map[string]string{"source": "global"}, global.Capacity.Options)
	assert.Equal(t, map[string]string{"source": "global"}, second.Capacity.Options)

	inline := validRoutingCapacitySpec()
	inline.Format = "Inline"
	inline.Options = map[string]string{"source": "inline"}
	spec := routingISVCSpec(&v1beta1.RoutingSpec{Capacity: inline})
	first, err = ResolveCapacityPolicy(global, spec)
	require.NoError(t, err)
	second, err = ResolveCapacityPolicy(global, spec)
	require.NoError(t, err)
	first.Capacity.Options["source"] = "first"
	assert.Equal(t, map[string]string{"source": "inline"}, inline.Options)
	assert.Equal(t, map[string]string{"source": "inline"}, second.Capacity.Options)

	inline.Options["source"] = "source-mutated"
	assert.Equal(t, map[string]string{"source": "inline"}, second.Capacity.Options)
}

func routingISVCSpec(routing *v1beta1.RoutingSpec) *v1beta1.InferenceServiceSpec {
	return &v1beta1.InferenceServiceSpec{Routing: routing}
}

func validRoutingProbeSpec() *v1beta1.RoutingProbeSpec {
	return &v1beta1.RoutingProbeSpec{
		Path:             "/ready",
		Method:           "GET",
		AcceptStatuses:   []int32{200},
		GateStatuses:     []int32{500},
		Period:           metav1.Duration{Duration: time.Minute},
		Timeout:          metav1.Duration{Duration: time.Second},
		FailureThreshold: 2,
		SuccessThreshold: 2,
		AllFailedPolicy:  v1beta1.RoutingAllFailedPolicyPreserveTraffic,
	}
}

func testGlobalRoutingConfig() Config {
	return Config{
		Enabled: true,
		Observer: ObserverConfig{
			MaxConcurrentReconciles: 2,
			MaxConcurrentRequests:   4,
			MaxResponseBytes:        64 * 1024,
			MinPeriod:               100 * time.Millisecond,
			MaxSamples:              10,
		},
		Probe: ProbeConfig{
			Path:             "/health",
			Method:           "GET",
			AcceptStatuses:   []int{200, 204},
			GateStatuses:     []int{500, 503},
			Period:           time.Minute,
			Timeout:          5 * time.Second,
			FailureThreshold: 2,
			SuccessThreshold: 3,
			AllFailedPolicy:  AllFailedPolicyPreserveTraffic,
		},
		Capacity: CapacityConfig{
			Path:    "/capacity",
			Method:  "GET",
			Format:  FormatReport,
			Period:  2 * time.Minute,
			Timeout: 5 * time.Second,
			Samples: 6,
			Quorum:  2,
			MaxAge:  5 * time.Minute,
		},
		Publisher: PublisherConfig{
			Name:           "publisher",
			ResyncInterval: time.Minute,
			Options: map[string]string{
				"globalOnly":   "retained",
				"routingClass": "global",
			},
		},
	}
}

func validRoutingCapacitySpec() *v1beta1.RoutingCapacitySpec {
	return &v1beta1.RoutingCapacitySpec{
		Path:    "/capacity",
		Method:  "GET",
		Format:  string(FormatReport),
		Period:  metav1.Duration{Duration: 30 * time.Second},
		Timeout: metav1.Duration{Duration: 3 * time.Second},
		Samples: 10,
		Quorum:  2,
		MaxAge:  metav1.Duration{Duration: time.Minute},
	}
}
