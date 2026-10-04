package controllerconfig

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	testingclock "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

// TestNewEventRecorderConfig pins the load contract for the "eventRecorder"
// ConfigMap key: present and valid parses; absent or empty yields (nil, nil),
// never an error and never a fabricated default.
func TestNewEventRecorderConfig(t *testing.T) {
	tests := []struct {
		name          string
		configMapData map[string]string
		expectNil     bool
		wantError     string
		validate      func(*testing.T, *EventRecorderConfig)
	}{
		{
			name:          "both fields parse",
			configMapData: map[string]string{EventRecorderConfigName: `{"burstSize":400,"refillInterval":"5s"}`},
			validate: func(t *testing.T, cfg *EventRecorderConfig) {
				require.NotNil(t, cfg.BurstSize)
				assert.Equal(t, int32(400), *cfg.BurstSize)
				require.NotNil(t, cfg.RefillInterval)
				assert.Equal(t, "5s", *cfg.RefillInterval)
			},
		},
		{
			name:          "a lone burst leaves the refill absent",
			configMapData: map[string]string{EventRecorderConfigName: `{"burstSize":400}`},
			validate: func(t *testing.T, cfg *EventRecorderConfig) {
				require.NotNil(t, cfg.BurstSize)
				assert.Nil(t, cfg.RefillInterval)
			},
		},
		{
			name:          "absent key is unconfigured",
			configMapData: map[string]string{},
			expectNil:     true,
		},
		{
			name:          "empty value is unconfigured",
			configMapData: map[string]string{EventRecorderConfigName: "  "},
			expectNil:     true,
		},
		{
			name:          "malformed JSON is rejected",
			configMapData: map[string]string{EventRecorderConfigName: `{not-json`},
			wantError:     "unable to parse eventRecorder config json",
		},
		{
			name:          "a numeric refill interval is rejected",
			configMapData: map[string]string{EventRecorderConfigName: `{"refillInterval":5}`},
			wantError:     "unable to parse eventRecorder config json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset(inferenceServiceConfigMap(tt.configMapData))
			cfg, err := NewEventRecorderConfig(clientset)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			if tt.expectNil {
				assert.Nil(t, cfg)
				return
			}
			require.NotNil(t, cfg)
			tt.validate(t, cfg)
		})
	}
}

func TestEventRecorderConfig_ToBurstSize(t *testing.T) {
	t.Run("nil config is unconfigured", func(t *testing.T) {
		var cfg *EventRecorderConfig
		burst, err := cfg.ToBurstSize()
		require.NoError(t, err)
		assert.Zero(t, burst)
	})

	t.Run("absent field is unconfigured", func(t *testing.T) {
		burst, err := (&EventRecorderConfig{}).ToBurstSize()
		require.NoError(t, err)
		assert.Zero(t, burst)
	})

	t.Run("positive value passes through", func(t *testing.T) {
		burst, err := (&EventRecorderConfig{BurstSize: int32Pointer(400)}).ToBurstSize()
		require.NoError(t, err)
		assert.Equal(t, 400, burst)
	})

	for _, configured := range []int32{0, -1} {
		t.Run(fmt.Sprintf("rejects %d", configured), func(t *testing.T) {
			burst, err := (&EventRecorderConfig{BurstSize: int32Pointer(configured)}).ToBurstSize()
			require.ErrorContains(t, err, "eventRecorder.burstSize")
			assert.Zero(t, burst)
		})
	}
}

func TestEventRecorderConfig_ToRefillInterval(t *testing.T) {
	t.Run("nil config is unconfigured", func(t *testing.T) {
		var cfg *EventRecorderConfig
		interval, err := cfg.ToRefillInterval()
		require.NoError(t, err)
		assert.Zero(t, interval)
	})

	t.Run("absent field is unconfigured", func(t *testing.T) {
		interval, err := (&EventRecorderConfig{}).ToRefillInterval()
		require.NoError(t, err)
		assert.Zero(t, interval)
	})

	t.Run("positive duration passes through", func(t *testing.T) {
		interval, err := (&EventRecorderConfig{RefillInterval: stringPointer("5s")}).ToRefillInterval()
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, interval)
	})

	for _, value := range []string{"", "many", "0s", "-1s"} {
		t.Run("rejects "+value, func(t *testing.T) {
			interval, err := (&EventRecorderConfig{RefillInterval: stringPointer(value)}).ToRefillInterval()
			require.ErrorContains(t, err, "eventRecorder.refillInterval")
			assert.Zero(t, interval)
		})
	}
}

// TestLoadEventRecorderSettings pins the startup contract: every configured
// field is validated from one ConfigMap read, an absent field or key stays
// zero so client-go's own default applies to it, and a bad value is an error
// the manager refuses to start on.
func TestLoadEventRecorderSettings(t *testing.T) {
	tests := []struct {
		name          string
		configMapData map[string]string
		omitConfigMap bool
		want          EventRecorderSettings
		wantError     string
	}{
		{
			name:          "both fields configured",
			configMapData: map[string]string{EventRecorderConfigName: `{"burstSize":400,"refillInterval":"5s"}`},
			want:          EventRecorderSettings{BurstSize: 400, RefillInterval: 5 * time.Second},
		},
		{
			name:          "burst alone leaves the refill to client-go",
			configMapData: map[string]string{EventRecorderConfigName: `{"burstSize":400}`},
			want:          EventRecorderSettings{BurstSize: 400},
		},
		{
			name:          "refill alone leaves the burst to client-go",
			configMapData: map[string]string{EventRecorderConfigName: `{"refillInterval":"5s"}`},
			want:          EventRecorderSettings{RefillInterval: 5 * time.Second},
		},
		{
			name:          "absent key leaves both to client-go",
			configMapData: map[string]string{},
		},
		{
			name:          "empty block leaves both to client-go",
			configMapData: map[string]string{EventRecorderConfigName: `{}`},
		},
		{
			name:          "zero burst is rejected",
			configMapData: map[string]string{EventRecorderConfigName: `{"burstSize":0}`},
			wantError:     "eventRecorder.burstSize: must be > 0, got 0",
		},
		{
			name:          "negative burst is rejected",
			configMapData: map[string]string{EventRecorderConfigName: `{"burstSize":-1}`},
			wantError:     "eventRecorder.burstSize: must be > 0, got -1",
		},
		{
			name:          "zero refill interval is rejected",
			configMapData: map[string]string{EventRecorderConfigName: `{"refillInterval":"0s"}`},
			wantError:     "eventRecorder.refillInterval: must be > 0",
		},
		{
			name:          "malformed refill interval is rejected",
			configMapData: map[string]string{EventRecorderConfigName: `{"refillInterval":"many"}`},
			wantError:     `eventRecorder.refillInterval "many"`,
		},
		{
			name:          "malformed block is rejected",
			configMapData: map[string]string{EventRecorderConfigName: `{not-json`},
			wantError:     "unable to parse eventRecorder config json",
		},
		{
			name:          "missing ConfigMap is rejected",
			omitConfigMap: true,
			wantError:     `configmaps "inferenceservice-config" not found`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			if !tt.omitConfigMap {
				clientset = fake.NewSimpleClientset(inferenceServiceConfigMap(tt.configMapData))
			}

			before := len(clientset.Actions())
			got, err := LoadEventRecorderSettings(clientset)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				assert.Equal(t, EventRecorderSettings{}, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}

			getCount := 0
			for _, action := range clientset.Actions()[before:] {
				if action.GetVerb() == "get" && action.GetResource().Resource == "configmaps" {
					getCount++
				}
			}
			assert.Equal(t, 1, getCount, "the budget must come from one ConfigMap GET")
		})
	}
}

// TestEventRecorderSettings_CorrelatorOptions pins the translation client-go
// receives: the burst passes through and the refill interval becomes a fill
// rate in events per second, while an unconfigured side stays zero so
// client-go substitutes its own default for it.
func TestEventRecorderSettings_CorrelatorOptions(t *testing.T) {
	unconfigured := EventRecorderSettings{}.CorrelatorOptions()
	assert.Zero(t, unconfigured.BurstSize)
	assert.Zero(t, unconfigured.QPS)

	burstOnly := EventRecorderSettings{BurstSize: 400}.CorrelatorOptions()
	assert.Equal(t, 400, burstOnly.BurstSize)
	assert.Zero(t, burstOnly.QPS)

	refillOnly := EventRecorderSettings{RefillInterval: 5 * time.Second}.CorrelatorOptions()
	assert.Zero(t, refillOnly.BurstSize)
	assert.InDelta(t, 0.2, refillOnly.QPS, 1e-6)

	subSecond := EventRecorderSettings{RefillInterval: 250 * time.Millisecond}.CorrelatorOptions()
	assert.InDelta(t, 4.0, subSecond.QPS, 1e-6)
}

// TestEventRecorderSettings_RefillGrantsOneEventPerInterval drives client-go's
// own spam filter with the translated options: an object spends its burst,
// is then dropped, and regains exactly one event per refill interval.
func TestEventRecorderSettings_RefillGrantsOneEventPerInterval(t *testing.T) {
	clock := testingclock.NewFakeClock(time.Now())
	options := EventRecorderSettings{BurstSize: 2, RefillInterval: 5 * time.Second}.CorrelatorOptions()
	options.Clock = clock
	correlator := record.NewEventCorrelatorWithOptions(options)

	sequence := 0
	admitted := func() bool {
		sequence++
		result, err := correlator.EventCorrelate(stateChangeEvent(sequence))
		require.NoError(t, err)
		return !result.Skip
	}

	assert.True(t, admitted(), "first event of the burst")
	assert.True(t, admitted(), "second event of the burst")
	assert.False(t, admitted(), "the burst is spent")

	clock.Step(4 * time.Second)
	assert.False(t, admitted(), "no refill before the interval elapses")

	clock.Step(time.Second)
	assert.True(t, admitted(), "one event regained after the interval")
	assert.False(t, admitted(), "only one event is regained per interval")
}

// stateChangeEvent is a distinct Normal event about one fixed object, so
// the spam filter sees one bucket and the aggregator never merges them.
func stateChangeEvent(sequence int) *v1.Event {
	return &v1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: fmt.Sprintf("fleet-engine.%d", sequence)},
		InvolvedObject: v1.ObjectReference{
			Kind:       "InferenceReplica",
			APIVersion: "ome.io/v1beta1",
			Namespace:  "team-a",
			Name:       "fleet-engine",
			UID:        "fleet-engine-uid",
		},
		Source:  v1.EventSource{Component: "v1beta1Controllers"},
		Type:    v1.EventTypeNormal,
		Reason:  fmt.Sprintf("StateChange%d", sequence),
		Message: fmt.Sprintf("state change %d", sequence),
	}
}

func inferenceServiceConfigMap(data map[string]string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: data,
	}
}
