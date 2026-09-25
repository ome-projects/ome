package controllerconfig

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
)

// TestNewLifecycleConfig pins the load contract for the "lifecycle"
// ConfigMap key: present + valid parses into LifecycleConfig; absent
// yields (nil, nil) — unconfigured, the caller fails safe (Held) —
// never an error and never a fabricated default.
func TestNewLifecycleConfig(t *testing.T) {
	tests := []struct {
		name          string
		configMapData map[string]string
		expectNil     bool
		expectedError bool
		validate      func(*testing.T, *LifecycleConfig)
	}{
		{
			name: "valid updateRetry block",
			configMapData: map[string]string{
				LifecycleConfigName: `{"updateRetry":{"maxAttempts":3,"initialDelay":"1m","maxDelay":"30m","multiplier":2.0}}`,
			},
			validate: func(t *testing.T, cfg *LifecycleConfig) {
				require.NotNil(t, cfg.UpdateRetry)
				assert.Equal(t, int32(3), cfg.UpdateRetry.MaxAttempts)
				assert.Equal(t, "1m", cfg.UpdateRetry.InitialDelay)
				assert.Equal(t, "30m", cfg.UpdateRetry.MaxDelay)
				assert.Equal(t, 2.0, cfg.UpdateRetry.Multiplier)
			},
		},
		{
			name: "valid scale settings",
			configMapData: map[string]string{
				LifecycleConfigName: `{"scaleUpPodBatchSize":100,"scaleDownPodBatchSize":75,"scaleDownRequeueInterval":"7s"}`,
			},
			validate: func(t *testing.T, cfg *LifecycleConfig) {
				require.NotNil(t, cfg.ScaleUpPodBatchSize)
				assert.Equal(t, int32(100), *cfg.ScaleUpPodBatchSize)
				require.NotNil(t, cfg.ScaleDownPodBatchSize)
				assert.Equal(t, int32(75), *cfg.ScaleDownPodBatchSize)
				require.NotNil(t, cfg.ScaleDownRequeueInterval)
				assert.Equal(t, "7s", *cfg.ScaleDownRequeueInterval)
			},
		},
		{
			// Absent lifecycle key: unconfigured, NOT an error. The nil
			// config is the fail-safe-Held signal for the IR reconciler.
			name:          "absent lifecycle key yields nil config, no error",
			configMapData: map[string]string{},
			expectNil:     true,
		},
		{
			name: "lifecycle key without updateRetry yields config with nil UpdateRetry",
			configMapData: map[string]string{
				LifecycleConfigName: `{}`,
			},
			validate: func(t *testing.T, cfg *LifecycleConfig) {
				assert.Nil(t, cfg.UpdateRetry)
			},
		},
		{
			name: "malformed json is an error",
			configMapData: map[string]string{
				LifecycleConfigName: `{not-json`,
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			configMap := &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      constants.InferenceServiceConfigMapName,
					Namespace: constants.OMENamespace,
				},
				Data: tt.configMapData,
			}
			_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
			require.NoError(t, err)

			cfg, err := NewLifecycleConfig(clientset)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.expectNil {
				assert.Nil(t, cfg)
				return
			}
			require.NotNil(t, cfg)
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestLifecycleConfig_ToScaleDownPodBatchSize(t *testing.T) {
	t.Run("nil config is unconfigured", func(t *testing.T) {
		var cfg *LifecycleConfig
		size, err := cfg.ToScaleDownPodBatchSize()
		require.NoError(t, err)
		assert.Nil(t, size)
	})

	t.Run("absent field is unconfigured", func(t *testing.T) {
		cfg := &LifecycleConfig{}
		size, err := cfg.ToScaleDownPodBatchSize()
		require.NoError(t, err)
		assert.Nil(t, size)
	})

	t.Run("positive value passes through as a copy", func(t *testing.T) {
		configured := int32(100)
		cfg := &LifecycleConfig{ScaleDownPodBatchSize: &configured}
		size, err := cfg.ToScaleDownPodBatchSize()
		require.NoError(t, err)
		require.NotNil(t, size)
		assert.Equal(t, int32(100), *size)

		configured = 200
		assert.Equal(t, int32(100), *size)
	})

	for _, configured := range []int32{0, -1} {
		configured := configured
		t.Run(fmt.Sprintf("rejects %d", configured), func(t *testing.T) {
			cfg := &LifecycleConfig{ScaleDownPodBatchSize: &configured}
			size, err := cfg.ToScaleDownPodBatchSize()
			require.ErrorContains(t, err, "lifecycle.scaleDownPodBatchSize")
			assert.Nil(t, size)
		})
	}
}

func TestLifecycleConfig_ToScaleDownRequeueInterval(t *testing.T) {
	t.Run("nil config disables periodic polling", func(t *testing.T) {
		var cfg *LifecycleConfig
		interval, err := cfg.ToScaleDownRequeueInterval()
		require.NoError(t, err)
		assert.Zero(t, interval)
	})

	t.Run("absent field disables periodic polling", func(t *testing.T) {
		cfg := &LifecycleConfig{}
		interval, err := cfg.ToScaleDownRequeueInterval()
		require.NoError(t, err)
		assert.Zero(t, interval)
	})

	t.Run("positive duration passes through", func(t *testing.T) {
		cfg := &LifecycleConfig{ScaleDownRequeueInterval: stringPointer("37s")}
		interval, err := cfg.ToScaleDownRequeueInterval()
		require.NoError(t, err)
		assert.Equal(t, 37*time.Second, interval)
	})

	for _, value := range []string{"", "many", "0s", "-1s"} {
		value := value
		t.Run("rejects "+value, func(t *testing.T) {
			cfg := &LifecycleConfig{ScaleDownRequeueInterval: &value}
			interval, err := cfg.ToScaleDownRequeueInterval()
			require.ErrorContains(t, err, "lifecycle.scaleDownRequeueInterval")
			assert.Zero(t, interval)
		})
	}
}

func TestLoadPodBatchSizes(t *testing.T) {
	tests := []struct {
		name          string
		lifecycle     *string
		omitConfigMap bool
		wantScaleUp   *int32
		wantScaleDown *int32
		wantInterval  time.Duration
		wantError     string
	}{
		{
			name:          "scale settings come from one snapshot",
			lifecycle:     stringPointer(`{"scaleUpPodBatchSize":37,"scaleDownPodBatchSize":41,"scaleDownRequeueInterval":"7s"}`),
			wantScaleUp:   int32Pointer(37),
			wantScaleDown: int32Pointer(41),
			wantInterval:  7 * time.Second,
		},
		{
			name:        "scale-up alone leaves scale-down unbounded",
			lifecycle:   stringPointer(`{"scaleUpPodBatchSize":37}`),
			wantScaleUp: int32Pointer(37),
		},
		{
			name:          "scale-down alone leaves scale-up unbounded",
			lifecycle:     stringPointer(`{"scaleDownPodBatchSize":41}`),
			wantScaleDown: int32Pointer(41),
		},
		{
			name:         "requeue interval alone leaves both directions unbounded",
			lifecycle:    stringPointer(`{"scaleDownRequeueInterval":"11s"}`),
			wantInterval: 11 * time.Second,
		},
		{
			name: "absent lifecycle key leaves both directions unbounded",
		},
		{
			name:      "empty lifecycle object leaves both directions unbounded",
			lifecycle: stringPointer(`{}`),
		},
		{
			name:          "missing ConfigMap is rejected",
			omitConfigMap: true,
			wantError:     `configmaps "inferenceservice-config" not found`,
		},
		{
			name:      "malformed lifecycle JSON is rejected",
			lifecycle: stringPointer(`{not-json`),
			wantError: "unable to parse lifecycle config json",
		},
		{
			name:      "malformed scale-down field type is rejected",
			lifecycle: stringPointer(`{"scaleDownPodBatchSize":"many"}`),
			wantError: "cannot unmarshal string",
		},
		{
			name:      "malformed requeue interval is rejected",
			lifecycle: stringPointer(`{"scaleDownRequeueInterval":"many"}`),
			wantError: "lifecycle.scaleDownRequeueInterval",
		},
		{
			name:      "explicit empty requeue interval is rejected",
			lifecycle: stringPointer(`{"scaleDownRequeueInterval":""}`),
			wantError: "lifecycle.scaleDownRequeueInterval",
		},
		{
			name:      "zero requeue interval is rejected",
			lifecycle: stringPointer(`{"scaleDownRequeueInterval":"0s"}`),
			wantError: "must be > 0, got 0s",
		},
		{
			name:      "negative requeue interval is rejected",
			lifecycle: stringPointer(`{"scaleDownRequeueInterval":"-1s"}`),
			wantError: "must be > 0, got -1s",
		},
		{
			name:      "zero scale-up is rejected",
			lifecycle: stringPointer(`{"scaleUpPodBatchSize":0,"scaleDownPodBatchSize":41}`),
			wantError: "lifecycle.scaleUpPodBatchSize: must be > 0, got 0",
		},
		{
			name:      "negative scale-up is rejected",
			lifecycle: stringPointer(`{"scaleUpPodBatchSize":-1,"scaleDownPodBatchSize":41}`),
			wantError: "lifecycle.scaleUpPodBatchSize: must be > 0, got -1",
		},
		{
			name:      "zero scale-down is rejected",
			lifecycle: stringPointer(`{"scaleUpPodBatchSize":37,"scaleDownPodBatchSize":0}`),
			wantError: "lifecycle.scaleDownPodBatchSize: must be > 0, got 0",
		},
		{
			name:      "negative scale-down is rejected",
			lifecycle: stringPointer(`{"scaleUpPodBatchSize":37,"scaleDownPodBatchSize":-1}`),
			wantError: "lifecycle.scaleDownPodBatchSize: must be > 0, got -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			if !tt.omitConfigMap {
				data := map[string]string{}
				if tt.lifecycle != nil {
					data[LifecycleConfigName] = *tt.lifecycle
				}
				_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(
					context.Background(),
					&v1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:            constants.InferenceServiceConfigMapName,
							Namespace:       constants.OMENamespace,
							ResourceVersion: "one-snapshot",
						},
						Data: data,
					},
					metav1.CreateOptions{},
				)
				require.NoError(t, err)
			}

			before := len(clientset.Actions())
			got, err := LoadPodBatchSizes(clientset)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				assert.Equal(t, PodBatchSizes{}, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantScaleUp, got.ScaleUp)
				assert.Equal(t, tt.wantScaleDown, got.ScaleDown)
				assert.Equal(t, tt.wantInterval, got.ScaleDownRequeueInterval)
			}

			getCount := 0
			for _, action := range clientset.Actions()[before:] {
				if action.GetVerb() == "get" && action.GetResource().Resource == "configmaps" {
					getCount++
				}
			}
			assert.Equal(t, 1, getCount, "scale settings must share one ConfigMap GET")
		})
	}
}

func TestLoadScaleUpPodBatchSize_IgnoresScaleDownValidation(t *testing.T) {
	clientset := fake.NewSimpleClientset(&v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: map[string]string{
			LifecycleConfigName: `{"scaleUpPodBatchSize":37,"scaleDownPodBatchSize":0}`,
		},
	})

	got, err := LoadScaleUpPodBatchSize(clientset)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int32(37), *got)
}

func int32Pointer(value int32) *int32 {
	return &value
}

func stringPointer(value string) *string {
	return &value
}

// TestLifecycleConfig_ToScaleUpPodBatchSize pins the field-specific contract:
// absent configuration preserves this field's unbounded compatibility path, a
// positive missing-Pod budget passes through exactly, and an explicitly
// non-positive value is rejected during manager startup.
func TestLifecycleConfig_ToScaleUpPodBatchSize(t *testing.T) {
	t.Run("nil config is unconfigured", func(t *testing.T) {
		var cfg *LifecycleConfig
		size, err := cfg.ToScaleUpPodBatchSize()
		require.NoError(t, err)
		assert.Nil(t, size)
	})

	t.Run("absent field is unconfigured", func(t *testing.T) {
		cfg := &LifecycleConfig{}
		size, err := cfg.ToScaleUpPodBatchSize()
		require.NoError(t, err)
		assert.Nil(t, size)
	})

	t.Run("positive value passes through", func(t *testing.T) {
		configured := int32(100)
		cfg := &LifecycleConfig{ScaleUpPodBatchSize: &configured}
		size, err := cfg.ToScaleUpPodBatchSize()
		require.NoError(t, err)
		require.NotNil(t, size)
		assert.Equal(t, int32(100), *size)

		// The conversion returns a value copy, not an alias into the parsed
		// ConfigMap object that a cached reader may share with other callers.
		configured = 200
		assert.Equal(t, int32(100), *size)
	})

	for _, configured := range []int32{0, -1} {
		configured := configured
		t.Run(fmt.Sprintf("rejects %d", configured), func(t *testing.T) {
			cfg := &LifecycleConfig{ScaleUpPodBatchSize: &configured}
			size, err := cfg.ToScaleUpPodBatchSize()
			assert.Error(t, err)
			assert.Nil(t, size)
		})
	}
}

// TestLifecycleConfig_ToRevisionHistoryLimit pins the field-specific
// contract: absent configuration is unconfigured (the retention sweep
// prunes nothing rather than fabricating a default), a positive cap
// passes through as a value copy, and an explicitly non-positive value
// is invalid (the caller treats it as unconfigured).
func TestLifecycleConfig_ToRevisionHistoryLimit(t *testing.T) {
	t.Run("nil config is unconfigured", func(t *testing.T) {
		var cfg *LifecycleConfig
		limit, err := cfg.ToRevisionHistoryLimit()
		require.NoError(t, err)
		assert.Nil(t, limit)
	})

	t.Run("absent field is unconfigured", func(t *testing.T) {
		cfg := &LifecycleConfig{}
		limit, err := cfg.ToRevisionHistoryLimit()
		require.NoError(t, err)
		assert.Nil(t, limit)
	})

	t.Run("positive value passes through as a copy", func(t *testing.T) {
		configured := int32(10)
		cfg := &LifecycleConfig{RevisionHistoryLimit: &configured}
		limit, err := cfg.ToRevisionHistoryLimit()
		require.NoError(t, err)
		require.NotNil(t, limit)
		assert.Equal(t, int32(10), *limit)

		// The conversion returns a value copy, not an alias into the parsed
		// ConfigMap object that a cached reader may share with other callers.
		configured = 42
		assert.Equal(t, int32(10), *limit)
	})

	for _, configured := range []int32{0, -1} {
		configured := configured
		t.Run(fmt.Sprintf("rejects %d", configured), func(t *testing.T) {
			cfg := &LifecycleConfig{RevisionHistoryLimit: &configured}
			limit, err := cfg.ToRevisionHistoryLimit()
			assert.Error(t, err)
			assert.Nil(t, limit)
		})
	}
}

// TestUpdateRetryConfig_ToPolicy pins the validation contract: the chart
// defaults convert 1:1, and every violation is an error (the caller
// treats an invalid policy as unconfigured — fail-safe Held — rather
// than silently patching it with fallback numbers).
func TestUpdateRetryConfig_ToPolicy(t *testing.T) {
	t.Run("happy path matches chart defaults", func(t *testing.T) {
		cfg := &UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "30m", Multiplier: 2.0}
		policy, err := cfg.ToPolicy()
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, int32(3), policy.MaxAttempts)
		assert.Equal(t, time.Minute, policy.InitialDelay)
		assert.Equal(t, 30*time.Minute, policy.MaxDelay)
		assert.Equal(t, 2.0, policy.Multiplier)
	})

	invalid := []struct {
		name string
		cfg  UpdateRetryConfig
	}{
		{"maxAttempts zero", UpdateRetryConfig{MaxAttempts: 0, InitialDelay: "1m", MaxDelay: "30m", Multiplier: 2.0}},
		{"invalid initialDelay duration", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "not-a-duration", MaxDelay: "30m", Multiplier: 2.0}},
		{"invalid maxDelay duration", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "30 minutes", Multiplier: 2.0}},
		{"multiplier below one", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "30m", Multiplier: 0.5}},
		{"zero initialDelay", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "0s", MaxDelay: "30m", Multiplier: 2.0}},
		{"zero maxDelay", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "0s", Multiplier: 2.0}},
		{"maxDelay below initialDelay", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "10m", MaxDelay: "1m", Multiplier: 2.0}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := tt.cfg.ToPolicy()
			assert.Error(t, err)
			assert.Nil(t, policy)
		})
	}
}

// TestLifecycleConfig_ToGracePeriod pins the validation contract:
// chart default 60s parses cleanly, absent key yields zero (not an
// error), invalid duration is an error.
func TestLifecycleConfig_ToGracePeriod(t *testing.T) {
	t.Run("happy path matches chart default", func(t *testing.T) {
		cfg := &LifecycleConfig{StuckPodGracePeriod: "60s"}
		grace, err := cfg.ToGracePeriod()
		require.NoError(t, err)
		assert.Equal(t, 60*time.Second, grace)
	})

	t.Run("absent key yields zero, no error", func(t *testing.T) {
		cfg := &LifecycleConfig{StuckPodGracePeriod: ""}
		grace, err := cfg.ToGracePeriod()
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), grace)
	})

	invalid := []struct {
		name  string
		value string
	}{
		{"invalid duration", "not-a-duration"},
		{"zero duration", "0s"},
		{"negative duration", "-5s"},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &LifecycleConfig{StuckPodGracePeriod: tt.value}
			grace, err := cfg.ToGracePeriod()
			assert.Error(t, err)
			assert.Equal(t, time.Duration(0), grace)
		})
	}
}

// TestLifecycleConfig_ToInstanceReadyTimeout pins the validation contract
// for the operator-wide readiness backstop: a configured duration parses,
// an absent key yields zero (unconfigured, not an error, so operations
// open with no deadline), and any non-positive or unparsable value is an
// error the caller treats as unconfigured.
func TestLifecycleConfig_ToInstanceReadyTimeout(t *testing.T) {
	t.Run("configured value parses", func(t *testing.T) {
		cfg := &LifecycleConfig{InstanceReadyTimeout: "30m"}
		timeout, err := cfg.ToInstanceReadyTimeout()
		require.NoError(t, err)
		assert.Equal(t, 30*time.Minute, timeout)
	})

	t.Run("nil receiver yields zero, no error", func(t *testing.T) {
		var cfg *LifecycleConfig
		timeout, err := cfg.ToInstanceReadyTimeout()
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), timeout)
	})

	t.Run("absent key yields zero, no error", func(t *testing.T) {
		cfg := &LifecycleConfig{InstanceReadyTimeout: ""}
		timeout, err := cfg.ToInstanceReadyTimeout()
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), timeout)
	})

	invalidTimeouts := []struct {
		name  string
		value string
	}{
		{"invalid duration", "not-a-duration"},
		{"zero duration", "0s"},
		{"negative duration", "-5m"},
	}
	for _, tt := range invalidTimeouts {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &LifecycleConfig{InstanceReadyTimeout: tt.value}
			timeout, err := cfg.ToInstanceReadyTimeout()
			assert.Error(t, err)
			assert.Equal(t, time.Duration(0), timeout)
		})
	}
}

// TestNewLifecycleConfig_InstanceReadyTimeout pins the load contract for
// the instanceReadyTimeout key: present + valid parses, absent leaves the
// field empty (no error).
func TestNewLifecycleConfig_InstanceReadyTimeout(t *testing.T) {
	load := func(t *testing.T, lifecycle string) *LifecycleConfig {
		t.Helper()
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{LifecycleConfigName: lifecycle},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)
		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		return cfg
	}

	t.Run("valid instanceReadyTimeout key", func(t *testing.T) {
		assert.Equal(t, "30m", load(t, `{"instanceReadyTimeout":"30m"}`).InstanceReadyTimeout)
	})

	t.Run("lifecycle key without instanceReadyTimeout yields empty field", func(t *testing.T) {
		assert.Equal(t, "", load(t, `{}`).InstanceReadyTimeout)
	})
}

// TestLifecycleConfig_ToUnschedulableGracePeriod pins the validation
// contract for the scheduler-hold escalation window: a configured
// duration parses, an absent key yields zero (unconfigured, not an
// error), and any non-positive or unparsable value is an error the
// caller treats as unconfigured.
func TestLifecycleConfig_ToUnschedulableGracePeriod(t *testing.T) {
	t.Run("configured value parses", func(t *testing.T) {
		cfg := &LifecycleConfig{UnschedulableGracePeriod: "10m"}
		grace, err := cfg.ToUnschedulableGracePeriod()
		require.NoError(t, err)
		assert.Equal(t, 10*time.Minute, grace)
	})

	t.Run("nil receiver yields zero, no error", func(t *testing.T) {
		var cfg *LifecycleConfig
		grace, err := cfg.ToUnschedulableGracePeriod()
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), grace)
	})

	t.Run("absent key yields zero, no error", func(t *testing.T) {
		cfg := &LifecycleConfig{UnschedulableGracePeriod: ""}
		grace, err := cfg.ToUnschedulableGracePeriod()
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), grace)
	})

	invalid := []struct {
		name  string
		value string
	}{
		{"invalid duration", "not-a-duration"},
		{"zero duration", "0s"},
		{"negative duration", "-5s"},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &LifecycleConfig{UnschedulableGracePeriod: tt.value}
			grace, err := cfg.ToUnschedulableGracePeriod()
			assert.Error(t, err)
			assert.Equal(t, time.Duration(0), grace)
		})
	}
}

// TestGangScheduleTimeoutConfig_ToClamp pins the validation contract for
// the gang admission bounds: a configured pair parses, an absent block
// yields a nil clamp (the derived timeout passes through), and any
// missing, unparsable, non-positive, or inverted bound is an error the
// caller treats as unconfigured.
func TestGangScheduleTimeoutConfig_ToClamp(t *testing.T) {
	t.Run("configured pair parses", func(t *testing.T) {
		clamp, err := (&GangScheduleTimeoutConfig{Min: "60s", Max: "10m"}).ToClamp()
		require.NoError(t, err)
		require.NotNil(t, clamp)
		assert.Equal(t, time.Minute, clamp.Min)
		assert.Equal(t, 10*time.Minute, clamp.Max)
	})

	t.Run("absent block yields nil, no error", func(t *testing.T) {
		var cfg *GangScheduleTimeoutConfig
		clamp, err := cfg.ToClamp()
		require.NoError(t, err)
		assert.Nil(t, clamp)
	})

	invalid := []struct {
		name string
		cfg  GangScheduleTimeoutConfig
	}{
		{"missing min", GangScheduleTimeoutConfig{Max: "10m"}},
		{"missing max", GangScheduleTimeoutConfig{Min: "60s"}},
		{"unparsable min", GangScheduleTimeoutConfig{Min: "soon", Max: "10m"}},
		{"unparsable max", GangScheduleTimeoutConfig{Min: "60s", Max: "later"}},
		{"zero min", GangScheduleTimeoutConfig{Min: "0s", Max: "10m"}},
		{"negative max", GangScheduleTimeoutConfig{Min: "60s", Max: "-1m"}},
		{"max below min", GangScheduleTimeoutConfig{Min: "10m", Max: "60s"}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			clamp, err := tt.cfg.ToClamp()
			assert.Error(t, err)
			assert.Nil(t, clamp)
		})
	}
}

// TestAuditConfig_ToPolicy pins the validation contract for the
// migration capacity caps: a configured block parses, an absent block
// yields a nil policy (admission rejects), and any non-positive cap or
// bad window is an error the caller treats as unconfigured.
func TestAuditConfig_ToPolicy(t *testing.T) {
	t.Run("configured block parses", func(t *testing.T) {
		policy, err := (&AuditConfig{MaxInFlightMigrations: 3, MaxMigrationsPerWindow: 10, Window: "1h"}).ToPolicy()
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, int32(3), policy.MaxInFlight)
		assert.Equal(t, int32(10), policy.MaxPerWindow)
		assert.Equal(t, time.Hour, policy.Window)
	})

	t.Run("absent block yields nil, no error", func(t *testing.T) {
		var cfg *AuditConfig
		policy, err := cfg.ToPolicy()
		require.NoError(t, err)
		assert.Nil(t, policy)
	})

	invalid := []struct {
		name string
		cfg  AuditConfig
	}{
		{"zero in-flight cap", AuditConfig{MaxMigrationsPerWindow: 10, Window: "1h"}},
		{"negative in-flight cap", AuditConfig{MaxInFlightMigrations: -1, MaxMigrationsPerWindow: 10, Window: "1h"}},
		{"zero per-window cap", AuditConfig{MaxInFlightMigrations: 3, Window: "1h"}},
		{"missing window", AuditConfig{MaxInFlightMigrations: 3, MaxMigrationsPerWindow: 10}},
		{"unparsable window", AuditConfig{MaxInFlightMigrations: 3, MaxMigrationsPerWindow: 10, Window: "hourly"}},
		{"non-positive window", AuditConfig{MaxInFlightMigrations: 3, MaxMigrationsPerWindow: 10, Window: "-1h"}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := tt.cfg.ToPolicy()
			assert.Error(t, err)
			assert.Nil(t, policy)
		})
	}
}

// TestRequeueConfig_ToIntervals pins the validation contract for the
// dispatcher cadence: a configured pair parses, an absent block yields
// the zero value (passes fall back to rate-limited backoff), and any
// missing, unparsable, or non-positive interval is an error the caller
// treats as unconfigured.
func TestRequeueConfig_ToIntervals(t *testing.T) {
	t.Run("configured pair parses", func(t *testing.T) {
		intervals, err := (&RequeueConfig{Operation: "5s", Gate: "3s"}).ToIntervals()
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, intervals.Operation)
		assert.Equal(t, 3*time.Second, intervals.Gate)
	})

	t.Run("absent block yields the zero value, no error", func(t *testing.T) {
		var cfg *RequeueConfig
		intervals, err := cfg.ToIntervals()
		require.NoError(t, err)
		assert.Zero(t, intervals.Operation)
		assert.Zero(t, intervals.Gate)
	})

	invalid := []struct {
		name string
		cfg  RequeueConfig
	}{
		{"missing operation", RequeueConfig{Gate: "3s"}},
		{"missing gate", RequeueConfig{Operation: "5s"}},
		{"unparsable operation", RequeueConfig{Operation: "soon", Gate: "3s"}},
		{"unparsable gate", RequeueConfig{Operation: "5s", Gate: "later"}},
		{"zero operation", RequeueConfig{Operation: "0s", Gate: "3s"}},
		{"negative gate", RequeueConfig{Operation: "5s", Gate: "-3s"}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			intervals, err := tt.cfg.ToIntervals()
			assert.Error(t, err)
			assert.Zero(t, intervals.Operation)
			assert.Zero(t, intervals.Gate)
		})
	}
}

// TestLifecycleConfig_ToRetryBlockHistoryLimit pins the validation
// contract for the historical RetryBlock cap: a configured value
// parses, an absent field yields nil (every block kept), and a
// non-positive value is an error the caller treats as unconfigured.
func TestLifecycleConfig_ToRetryBlockHistoryLimit(t *testing.T) {
	t.Run("configured value parses", func(t *testing.T) {
		limit := int32(3)
		got, err := (&LifecycleConfig{RetryBlockHistoryLimit: &limit}).ToRetryBlockHistoryLimit()
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, int32(3), *got)
	})

	t.Run("nil receiver yields nil, no error", func(t *testing.T) {
		var cfg *LifecycleConfig
		got, err := cfg.ToRetryBlockHistoryLimit()
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("absent field yields nil, no error", func(t *testing.T) {
		got, err := (&LifecycleConfig{}).ToRetryBlockHistoryLimit()
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	for _, value := range []int32{0, -1} {
		t.Run(fmt.Sprintf("non-positive %d is invalid", value), func(t *testing.T) {
			v := value
			got, err := (&LifecycleConfig{RetryBlockHistoryLimit: &v}).ToRetryBlockHistoryLimit()
			assert.Error(t, err)
			assert.Nil(t, got)
		})
	}
}

// TestNewLifecycleConfig_UnschedulableGracePeriod pins the load
// contract for the unschedulableGracePeriod key: present + valid
// parses, absent leaves the field empty (no error).
func TestNewLifecycleConfig_UnschedulableGracePeriod(t *testing.T) {
	load := func(t *testing.T, lifecycle string) *LifecycleConfig {
		t.Helper()
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{LifecycleConfigName: lifecycle},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)
		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		return cfg
	}

	t.Run("valid unschedulableGracePeriod key", func(t *testing.T) {
		assert.Equal(t, "10m", load(t, `{"unschedulableGracePeriod":"10m"}`).UnschedulableGracePeriod)
	})

	t.Run("lifecycle key without unschedulableGracePeriod yields empty field", func(t *testing.T) {
		assert.Equal(t, "", load(t, `{}`).UnschedulableGracePeriod)
	})
}

// TestAutoMigrateConfig_Validate pins the validation contract: the
// chart default converts cleanly, zero/negative maxAttempts is an error.
func TestAutoMigrateConfig_Validate(t *testing.T) {
	t.Run("happy path matches chart default", func(t *testing.T) {
		cfg := &AutoMigrateConfig{MaxAttempts: 3}
		err := cfg.Validate()
		require.NoError(t, err)
	})

	invalid := []struct {
		name        string
		maxAttempts int32
	}{
		{"zero maxAttempts", 0},
		{"negative maxAttempts", -1},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &AutoMigrateConfig{MaxAttempts: tt.maxAttempts}
			err := cfg.Validate()
			assert.Error(t, err)
		})
	}
}

// TestNewLifecycleConfig_StuckPodGracePeriod pins the load contract
// for the stuckPodGracePeriod key: present + valid parses, absent
// leaves the field empty (no error).
func TestNewLifecycleConfig_StuckPodGracePeriod(t *testing.T) {
	t.Run("valid stuckPodGracePeriod block", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{"stuckPodGracePeriod":"60s"}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "60s", cfg.StuckPodGracePeriod)
	})

	t.Run("lifecycle key without stuckPodGracePeriod yields config with empty field", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "", cfg.StuckPodGracePeriod)
	})
}

// TestForceDeleteConfig_ToPolicy pins the validation contract: a nil
// receiver (absent block) is unconfigured — nil policy, no error — the
// chart example converts 1:1, and every violation (either field missing,
// unparsable, or non-positive) is an error the caller treats as
// unconfigured (escalation OFF), never patched with fallback numbers.
func TestForceDeleteConfig_ToPolicy(t *testing.T) {
	t.Run("nil receiver yields nil policy, no error", func(t *testing.T) {
		var cfg *ForceDeleteConfig
		policy, err := cfg.ToPolicy()
		require.NoError(t, err)
		assert.Nil(t, policy)
	})

	t.Run("happy path matches chart example", func(t *testing.T) {
		cfg := &ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "5m"}
		policy, err := cfg.ToPolicy()
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, 2*time.Minute, policy.OverdueSlack)
		assert.Equal(t, 5*time.Minute, policy.NodeUnreachableThreshold)
	})

	invalid := []struct {
		name string
		cfg  ForceDeleteConfig
	}{
		{"missing overdueSlack", ForceDeleteConfig{OverdueSlack: "", NodeUnreachableThreshold: "5m"}},
		{"missing nodeUnreachableThreshold", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: ""}},
		{"invalid overdueSlack duration", ForceDeleteConfig{OverdueSlack: "not-a-duration", NodeUnreachableThreshold: "5m"}},
		{"invalid nodeUnreachableThreshold duration", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "5 minutes"}},
		{"zero overdueSlack", ForceDeleteConfig{OverdueSlack: "0s", NodeUnreachableThreshold: "5m"}},
		{"zero nodeUnreachableThreshold", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "0s"}},
		{"negative overdueSlack", ForceDeleteConfig{OverdueSlack: "-2m", NodeUnreachableThreshold: "5m"}},
		{"negative nodeUnreachableThreshold", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "-5m"}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := tt.cfg.ToPolicy()
			assert.Error(t, err)
			assert.Nil(t, policy)
		})
	}
}

// TestNewLifecycleConfig_ForceDelete pins the load contract for the
// forceDelete key: present + valid parses, absent leaves the field nil
// (unconfigured — the escalation does not exist).
func TestNewLifecycleConfig_ForceDelete(t *testing.T) {
	t.Run("valid forceDelete block", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{"forceDelete":{"overdueSlack":"2m","nodeUnreachableThreshold":"5m"}}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.ForceDelete)
		assert.Equal(t, "2m", cfg.ForceDelete.OverdueSlack)
		assert.Equal(t, "5m", cfg.ForceDelete.NodeUnreachableThreshold)
	})

	t.Run("lifecycle key without forceDelete yields config with nil ForceDelete", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Nil(t, cfg.ForceDelete)
	})
}

// TestTeardownConfig_ToDeadline pins the validation contract: a nil
// receiver (absent block) is unconfigured — nil deadline, no error —
// the chart default converts 1:1, and every violation (missing
// deadline, unparsable, or non-positive) is an error the caller
// treats as unconfigured (no deadline), never patched with fallback.
func TestTeardownConfig_ToDeadline(t *testing.T) {
	t.Run("nil receiver yields nil deadline, no error", func(t *testing.T) {
		var cfg *TeardownConfig
		deadline, err := cfg.ToDeadline()
		require.NoError(t, err)
		assert.Nil(t, deadline)
	})

	t.Run("happy path matches chart default", func(t *testing.T) {
		cfg := &TeardownConfig{Deadline: "30m"}
		deadline, err := cfg.ToDeadline()
		require.NoError(t, err)
		require.NotNil(t, deadline)
		assert.Equal(t, 30*time.Minute, *deadline)
	})

	invalid := []struct {
		name string
		cfg  TeardownConfig
	}{
		{"missing deadline", TeardownConfig{Deadline: ""}},
		{"invalid deadline duration", TeardownConfig{Deadline: "not-a-duration"}},
		{"zero deadline", TeardownConfig{Deadline: "0s"}},
		{"negative deadline", TeardownConfig{Deadline: "-30m"}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			deadline, err := tt.cfg.ToDeadline()
			assert.Error(t, err)
			assert.Nil(t, deadline)
		})
	}
}

// TestNewLifecycleConfig_Teardown pins the load contract for the
// teardown key: present + valid parses, absent leaves the field nil.
func TestNewLifecycleConfig_Teardown(t *testing.T) {
	t.Run("valid teardown block", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{"teardown":{"deadline":"30m"}}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.Teardown)
		assert.Equal(t, "30m", cfg.Teardown.Deadline)
	})

	t.Run("lifecycle key without teardown yields config with nil Teardown", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Nil(t, cfg.Teardown)
	})
}

// TestNewLifecycleConfig_AutoMigrate pins the load contract for the
// autoMigrate key: present + valid parses, absent leaves the field nil.
func TestNewLifecycleConfig_AutoMigrate(t *testing.T) {
	t.Run("valid autoMigrate block", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{"autoMigrate":{"maxAttempts":3}}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.AutoMigrate)
		assert.Equal(t, int32(3), cfg.AutoMigrate.MaxAttempts)
	})

	t.Run("lifecycle key without autoMigrate yields config with nil AutoMigrate", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Nil(t, cfg.AutoMigrate)
	})
}
