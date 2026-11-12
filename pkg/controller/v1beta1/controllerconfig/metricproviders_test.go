package controllerconfig

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

// TestParseMetricProvidersConfig_TopLevelAndNestedKeys_TopLevelWinsWhenPresent verifies the precedence invariant of the
// shared provider-binding loader: the top-level "metricProviders" key is
// authoritative whenever present (even as an empty object), and the nested
// autoscalerPolicy.metricProviders alias is honored only when the top-level
// key is absent.
func TestParseMetricProvidersConfig_TopLevelAndNestedKeys_TopLevelWinsWhenPresent(t *testing.T) {
	topLevel := `{
		"cluster-prometheus": {
			"serverAddress": "http://prometheus.monitoring.svc:9090",
			"authSecretRef": {"name": "prom-token", "key": "token"},
			"headers": {"X-Scope-OrgID": "team-a", "X-Extra": "1"}
		}
	}`
	nested := `{
		"metricProviders": {
			"nested-prometheus": {"serverAddress": "http://nested.monitoring.svc:9090"}
		},
		"preflight": {"memberGetTimeoutSeconds": 7}
	}`

	tests := []struct {
		name string
		data map[string]string
		want MetricProvidersConfig
	}{
		{
			name: "top-level key parses bindings including headers and auth",
			data: map[string]string{MetricProvidersConfigName: topLevel},
			want: MetricProvidersConfig{
				"cluster-prometheus": {
					ServerAddress: "http://prometheus.monitoring.svc:9090",
					AuthSecretRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{Name: "prom-token"},
						Key:                  "token",
					},
					Headers: map[string]string{"X-Scope-OrgID": "team-a", "X-Extra": "1"},
				},
			},
		},
		{
			name: "nested alias used when top-level key is absent",
			data: map[string]string{AutoscalerPolicyConfigName: nested},
			want: MetricProvidersConfig{
				"nested-prometheus": {ServerAddress: "http://nested.monitoring.svc:9090"},
			},
		},
		{
			name: "top-level wins when both keys are present",
			data: map[string]string{
				MetricProvidersConfigName:  topLevel,
				AutoscalerPolicyConfigName: nested,
			},
			want: MetricProvidersConfig{
				"cluster-prometheus": {
					ServerAddress: "http://prometheus.monitoring.svc:9090",
					AuthSecretRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{Name: "prom-token"},
						Key:                  "token",
					},
					Headers: map[string]string{"X-Scope-OrgID": "team-a", "X-Extra": "1"},
				},
			},
		},
		{
			name: "top-level presence is authoritative even as an empty object",
			data: map[string]string{
				MetricProvidersConfigName:  `{}`,
				AutoscalerPolicyConfigName: nested,
			},
			want: MetricProvidersConfig{},
		},
		{
			name: "absent everywhere yields no bindings",
			data: map[string]string{},
			want: MetricProvidersConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMetricProvidersConfig(&v1.ConfigMap{Data: tt.data})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestParseMetricProvidersConfig_NoServerAddressOrBadJson_LoadError verifies that a binding with no
// serverAddress is a load error from either location (provider addresses have
// no in-code default), and that malformed JSON is a load error.
func TestParseMetricProvidersConfig_NoServerAddressOrBadJson_LoadError(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string]string
		errPart string
	}{
		{
			name:    "top-level binding without serverAddress",
			data:    map[string]string{MetricProvidersConfigName: `{"p": {"headers": {"X-Scope-OrgID": "team-a"}}}`},
			errPart: `metric provider "p" has no serverAddress`,
		},
		{
			name:    "nested alias binding without serverAddress",
			data:    map[string]string{AutoscalerPolicyConfigName: `{"metricProviders": {"p": {}}}`},
			errPart: `metric provider "p" has no serverAddress`,
		},
		{
			name:    "malformed top-level json",
			data:    map[string]string{MetricProvidersConfigName: `{not-json`},
			errPart: "unable to parse metricProviders config json",
		},
		{
			name:    "malformed nested json",
			data:    map[string]string{AutoscalerPolicyConfigName: `{not-json`},
			errPart: "autoscalerPolicy",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseMetricProvidersConfig(&v1.ConfigMap{Data: tt.data})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errPart)
		})
	}
}

// TestParseAutoscalerPolicyConfig_ProviderKeys_TopLevelWinsNestedAloneResolves verifies the autoscaler
// block resolves its MetricProviders through the shared loader: the top-level
// key is visible to autoscaler consumers and wins over the nested alias, and
// the preflight operational fallbacks are unaffected.
func TestParseAutoscalerPolicyConfig_ProviderKeys_TopLevelWinsNestedAloneResolves(t *testing.T) {
	t.Run("top-level bindings visible without a nested block", func(t *testing.T) {
		cfg, err := parseAutoscalerPolicyConfig(&v1.ConfigMap{Data: map[string]string{
			MetricProvidersConfigName: `{"p": {"serverAddress": "http://prometheus.monitoring.svc:9090"}}`,
		}})
		require.NoError(t, err)
		assert.Equal(t, "http://prometheus.monitoring.svc:9090", cfg.MetricProviders["p"].ServerAddress)
		assert.Equal(t, DefaultPolicyMemberGetTimeoutSeconds, cfg.Preflight.MemberGetTimeoutSeconds)
		assert.Equal(t, DefaultPolicySkewDeadlineSeconds, cfg.Preflight.SkewDeadlineSeconds)
	})

	t.Run("top-level wins over nested for autoscaler consumers too", func(t *testing.T) {
		cfg, err := parseAutoscalerPolicyConfig(&v1.ConfigMap{Data: map[string]string{
			MetricProvidersConfigName: `{"top": {"serverAddress": "http://top.monitoring.svc:9090"}}`,
			AutoscalerPolicyConfigName: `{
				"metricProviders": {"nested": {"serverAddress": "http://nested.monitoring.svc:9090"}},
				"preflight": {"memberGetTimeoutSeconds": 7}
			}`,
		}})
		require.NoError(t, err)
		assert.Equal(t, MetricProvidersConfig{
			"top": {ServerAddress: "http://top.monitoring.svc:9090"},
		}, cfg.MetricProviders)
		// Non-provider fields of the nested block still parse normally.
		assert.Equal(t, int32(7), cfg.Preflight.MemberGetTimeoutSeconds)
	})

	t.Run("nested-only bindings still resolve", func(t *testing.T) {
		cfg, err := parseAutoscalerPolicyConfig(&v1.ConfigMap{Data: map[string]string{
			AutoscalerPolicyConfigName: `{"metricProviders": {"nested": {"serverAddress": "http://nested.monitoring.svc:9090"}}}`,
		}})
		require.NoError(t, err)
		assert.Equal(t, "http://nested.monitoring.svc:9090", cfg.MetricProviders["nested"].ServerAddress)
	})
}

// TestNewMetricProvidersConfigCached_DisabledCacheReadsTheConfigMap verifies the clientset-backed loader
// resolves through the shared ConfigMap fetch when caching is disabled.
func TestNewMetricProvidersConfigCached_DisabledCacheReadsTheConfigMap(t *testing.T) {
	clientset, gets := countingClientset(t, map[string]string{
		MetricProvidersConfigName: `{"p": {"serverAddress": "http://prometheus.monitoring.svc:9090"}}`,
	})

	cache := NewConfigCache(0) // disabled cache falls through to live GETs
	got, err := NewMetricProvidersConfigCached(cache, clientset)
	require.NoError(t, err)
	assert.Equal(t, "http://prometheus.monitoring.svc:9090", got["p"].ServerAddress)
	assert.Positive(t, atomic.LoadInt64(gets))
}
