package controllerconfig

import (
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// MetricProvidersConfigName is the top-level inferenceservice-config ConfigMap
// key holding the cluster's metric-provider bindings, shared by every consumer
// that resolves a logical provider name (autoscaler policy triggers, canary
// analysis).
const MetricProvidersConfigName = "metricProviders"

// +kubebuilder:object:generate=false
// MetricProviderBinding is one cluster-local binding of a logical
// metric-provider name (referenced by consumers via providerRef) to an
// endpoint and optional credentials. Only cluster admins author bindings:
// consumers name providers, never raw addresses.
type MetricProviderBinding struct {
	// ServerAddress is the provider endpoint injected wherever the binding is
	// consumed (a rendered trigger's serverAddress, the analysis sampler's
	// query target).
	ServerAddress string `json:"serverAddress"`

	// AuthSecretRef optionally names a Secret key holding a bearer token.
	// The autoscaler materializes a KEDA TriggerAuthentication from it in
	// each consumer namespace; the canary sampler reads the token value per
	// sample. The token itself never appears in policies, rendered blocks,
	// status, or digests. The Secret is read from the consumer namespace.
	// +optional
	AuthSecretRef *v1.SecretKeySelector `json:"authSecretRef,omitempty"`

	// Headers are fleet-default HTTP headers attached to provider requests
	// (e.g. tenant-scoping headers). Operator-authored config, not a user
	// injection surface. A consumer CR's own headers overlay these with the
	// CR key winning; that overlay happens in the consumer.
	// +optional
	Headers map[string]string `json:"headers,omitempty"`
}

// MetricProvidersConfig maps logical provider names to their cluster-local
// bindings.
type MetricProvidersConfig map[string]MetricProviderBinding

// NewMetricProvidersConfigCached loads the effective provider bindings through
// the TTL config cache, so binding edits take effect without a controller
// restart.
func NewMetricProvidersConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (MetricProvidersConfig, error) {
	return loadBlockCached(cache, clientset, parseMetricProvidersConfig)
}

// parseMetricProvidersConfig resolves the effective bindings from the
// ConfigMap. Invariant: the top-level "metricProviders" key is authoritative
// whenever it is present — even as an empty object; the nested
// autoscalerPolicy.metricProviders key is a deprecated alias honored only
// when the top-level key is absent. Every binding must carry a serverAddress:
// provider addresses have deliberately NO in-code default, so an address-less
// binding is a load error, never a baked-in endpoint.
func parseMetricProvidersConfig(configMap *v1.ConfigMap) (MetricProvidersConfig, error) {
	providers := MetricProvidersConfig{}
	if data, ok := configMap.Data[MetricProvidersConfigName]; ok {
		if err := json.Unmarshal([]byte(data), &providers); err != nil {
			return nil, fmt.Errorf("unable to parse metricProviders config json: %w", err)
		}
	} else {
		alias := struct {
			MetricProviders MetricProvidersConfig `json:"metricProviders,omitempty"`
		}{}
		if err := getComponentConfig(AutoscalerPolicyConfigName, configMap, &alias); err != nil {
			return nil, fmt.Errorf("unable to parse autoscalerPolicy config json: %w", err)
		}
		if alias.MetricProviders != nil {
			providers = alias.MetricProviders
		}
	}
	for name, provider := range providers {
		if provider.ServerAddress == "" {
			return nil, fmt.Errorf("metric provider %q has no serverAddress", name)
		}
	}
	return providers, nil
}
