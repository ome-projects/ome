package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func TestResolveIngressConfig(t *testing.T) {
	// Base config from ConfigMap
	baseConfig := &controllerconfig.IngressConfig{
		IngressGateway:         "istio-system/ingress-gateway",
		IngressServiceName:     "istio-ingressgateway.istio-system.svc.cluster.local",
		IngressDomain:          "svc.cluster.local",
		DomainTemplate:         "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
		UrlScheme:              "http",
		PathTemplate:           "",
		DisableIngressCreation: false,
	}

	tests := []struct {
		name        string
		annotations map[string]string
		expected    *controllerconfig.IngressConfig
	}{
		{
			name:        "no annotations - returns base config",
			annotations: map[string]string{},
			expected:    baseConfig,
		},
		{
			name: "custom domain template override",
			annotations: map[string]string{
				constants.IngressDomainTemplate: "{{ .Name }}-custom.example.com",
			},
			expected: &controllerconfig.IngressConfig{
				IngressGateway:         "istio-system/ingress-gateway",
				IngressServiceName:     "istio-ingressgateway.istio-system.svc.cluster.local",
				IngressDomain:          "svc.cluster.local",
				DomainTemplate:         "{{ .Name }}-custom.example.com",
				UrlScheme:              "http",
				PathTemplate:           "",
				DisableIngressCreation: false,
			},
		},
		{
			name: "custom domain and URL scheme",
			annotations: map[string]string{
				constants.IngressDomain:    "my-domain.com",
				constants.IngressURLScheme: "https",
			},
			expected: &controllerconfig.IngressConfig{
				IngressGateway:         "istio-system/ingress-gateway",
				IngressServiceName:     "istio-ingressgateway.istio-system.svc.cluster.local",
				IngressDomain:          "my-domain.com",
				DomainTemplate:         "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
				UrlScheme:              "https",
				PathTemplate:           "",
				DisableIngressCreation: false,
			},
		},
		{
			name: "additional domains with comma separation",
			annotations: map[string]string{
				constants.IngressAdditionalDomains: "alt1.com, alt2.com, alt3.com",
			},
			expected: &controllerconfig.IngressConfig{
				IngressGateway:           "istio-system/ingress-gateway",
				IngressServiceName:       "istio-ingressgateway.istio-system.svc.cluster.local",
				IngressDomain:            "svc.cluster.local",
				DomainTemplate:           "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
				UrlScheme:                "http",
				PathTemplate:             "",
				DisableIngressCreation:   false,
				AdditionalIngressDomains: &[]string{"alt1.com", "alt2.com", "alt3.com"},
			},
		},
		{
			name: "boolean overrides",
			annotations: map[string]string{
				constants.IngressDisableIstioVirtualHost: "true",
				constants.IngressDisableCreation:         "true",
			},
			expected: &controllerconfig.IngressConfig{
				IngressGateway:         "istio-system/ingress-gateway",
				IngressServiceName:     "istio-ingressgateway.istio-system.svc.cluster.local",
				IngressDomain:          "svc.cluster.local",
				DomainTemplate:         "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
				UrlScheme:              "http",
				PathTemplate:           "",
				DisableIngressCreation: true,
			},
		},
		{
			name: "path template override",
			annotations: map[string]string{
				constants.IngressPathTemplate: "/api/v1/models/{{ .Name }}",
			},
			expected: &controllerconfig.IngressConfig{
				IngressGateway:         "istio-system/ingress-gateway",
				IngressServiceName:     "istio-ingressgateway.istio-system.svc.cluster.local",
				IngressDomain:          "svc.cluster.local",
				DomainTemplate:         "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
				UrlScheme:              "http",
				PathTemplate:           "/api/v1/models/{{ .Name }}",
				DisableIngressCreation: false,
			},
		},
		{
			name: "comprehensive override",
			annotations: map[string]string{
				constants.IngressDomainTemplate:          "{{ .Name }}-prod.company.com",
				constants.IngressDomain:                  "company.com",
				constants.IngressURLScheme:               "https",
				constants.IngressPathTemplate:            "/ml/{{ .Name }}",
				constants.IngressAdditionalDomains:       "backup.com,mirror.net",
				constants.IngressDisableIstioVirtualHost: "false",
				constants.IngressDisableCreation:         "false",
			},
			expected: &controllerconfig.IngressConfig{
				IngressGateway:           "istio-system/ingress-gateway",
				IngressServiceName:       "istio-ingressgateway.istio-system.svc.cluster.local",
				IngressDomain:            "company.com",
				DomainTemplate:           "{{ .Name }}-prod.company.com",
				UrlScheme:                "https",
				PathTemplate:             "/ml/{{ .Name }}",
				DisableIngressCreation:   false,
				AdditionalIngressDomains: &[]string{"backup.com", "mirror.net"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ResolveIngressConfig(baseConfig, tt.annotations)
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("consistent hash headers are preserved", func(t *testing.T) {
		configWithHeaders := &controllerconfig.IngressConfig{
			IngressDomain:         "svc.cluster.local",
			ConsistentHashHeaders: []string{"x-routing-key", "x-tenant-key"},
		}
		result := ResolveIngressConfig(configWithHeaders, map[string]string{})
		assert.Equal(t, []string{"x-routing-key", "x-tenant-key"}, result.ConsistentHashHeaders)
	})

	// Guards against the resolver silently dropping a field: set every field,
	// then a no-annotation resolve must return an equal config. This would have
	// caught PerISVCSubdomain being omitted from the copy.
	t.Run("every base-config field is carried", func(t *testing.T) {
		className := "nginx"
		full := &controllerconfig.IngressConfig{
			IngressGateway:           "gw-ns/gw",
			IngressServiceName:       "svc.ns.svc.cluster.local",
			OmeIngressGateway:        "gw-ns/ome-gw",
			IngressDomain:            "example.com",
			IngressClassName:         &className,
			AdditionalIngressDomains: &[]string{"alt.example.com"},
			DomainTemplate:           "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
			UrlScheme:                "https",
			PathTemplate:             "/{{ .Namespace }}/{{ .Name }}",
			DisableIngressCreation:   true,
			EnableGatewayAPI:         true,
			ConsistentHashHeaders:    []string{"x-key"},
			PerISVCSubdomain:         true,
		}
		result := ResolveIngressConfig(full, map[string]string{})
		assert.Equal(t, full, result, "ResolveIngressConfig must carry every base-config field")
		assert.True(t, result.PerISVCSubdomain, "perISVCSubdomain must survive ResolveIngressConfig")
	})
}

// TestResolveIngressConfig_GatewayHostOverrides covers the per-ISVC gateway/host
// annotation overrides so a single ISVC can differ from the cluster default.
func TestResolveIngressConfig_GatewayHostOverrides(t *testing.T) {
	base := &controllerconfig.IngressConfig{
		OmeIngressGateway: "envoy-gateway-system/int-gw",
		IngressDomain:     "int.example.com",
		SharedHostPrefix:  "llm",
		PerISVCSubdomain:  false,
	}

	t.Run("gateway + perISVCSubdomain + sharedHostPrefix overrides", func(t *testing.T) {
		got := ResolveIngressConfig(base, map[string]string{
			constants.IngressGatewayOverride:  "envoy-gateway-system/ext-gw",
			constants.IngressPerISVCSubdomain: "true",
			constants.IngressSharedHostPrefix: "serving",
		})
		assert.Equal(t, "envoy-gateway-system/ext-gw", got.OmeIngressGateway)
		assert.True(t, got.PerISVCSubdomain)
		assert.Equal(t, "serving", got.SharedHostPrefix)
		// base must be untouched
		assert.Equal(t, "envoy-gateway-system/int-gw", base.OmeIngressGateway)
		assert.False(t, base.PerISVCSubdomain)
	})

	t.Run("empty sharedHostPrefix annotation means no prefix", func(t *testing.T) {
		got := ResolveIngressConfig(base, map[string]string{
			constants.IngressSharedHostPrefix: "",
		})
		assert.Equal(t, "", got.SharedHostPrefix)
	})

	t.Run("additionalIngressGateways parsed from JSON", func(t *testing.T) {
		got := ResolveIngressConfig(base, map[string]string{
			constants.IngressAdditionalGateways: `[{"omeIngressGateway":"envoy-gateway-system/ext-gw","ingressDomain":"ext.example.com"}]`,
		})
		if assert.Len(t, got.AdditionalIngressGateways, 1) {
			assert.Equal(t, "envoy-gateway-system/ext-gw", got.AdditionalIngressGateways[0].OmeIngressGateway)
			assert.Equal(t, "ext.example.com", got.AdditionalIngressGateways[0].IngressDomain)
		}
	})

	t.Run("malformed additionalIngressGateways is ignored", func(t *testing.T) {
		got := ResolveIngressConfig(base, map[string]string{
			constants.IngressAdditionalGateways: "not-json",
		})
		assert.Nil(t, got.AdditionalIngressGateways)
	})
}

func TestGetDeploymentModeFromAnnotations(t *testing.T) {
	tests := []struct {
		name          string
		annotations   map[string]string
		expectedMode  constants.DeploymentModeType
		expectedFound bool
	}{
		{
			name:          "nil annotations - returns empty and false",
			annotations:   nil,
			expectedMode:  "",
			expectedFound: false,
		},
		{
			name:          "empty annotations - returns empty and false",
			annotations:   map[string]string{},
			expectedMode:  "",
			expectedFound: false,
		},
		{
			name: "valid RawDeployment mode",
			annotations: map[string]string{
				constants.DeploymentMode: string(constants.RawDeployment),
			},
			expectedMode:  constants.RawDeployment,
			expectedFound: true,
		},
		{
			name: "valid MultiNode mode",
			annotations: map[string]string{
				constants.DeploymentMode: string(constants.MultiNode),
			},
			expectedMode:  constants.MultiNode,
			expectedFound: true,
		},
		{
			name: "valid VirtualDeployment mode",
			annotations: map[string]string{
				constants.DeploymentMode: string(constants.VirtualDeployment),
			},
			expectedMode:  constants.VirtualDeployment,
			expectedFound: true,
		},
		{
			name: "valid OMENative mode",
			annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
			expectedMode:  constants.OMENative,
			expectedFound: true,
		},
		{
			name: "invalid deployment mode - returns empty and false",
			annotations: map[string]string{
				constants.DeploymentMode: "InvalidMode",
			},
			expectedMode:  "",
			expectedFound: false,
		},
		{
			name: "empty string deployment mode - returns empty and false",
			annotations: map[string]string{
				constants.DeploymentMode: "",
			},
			expectedMode:  "",
			expectedFound: false,
		},
		{
			name: "other annotations present but no deployment mode",
			annotations: map[string]string{
				"some.other/annotation": "value",
			},
			expectedMode:  "",
			expectedFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, found := GetDeploymentModeFromAnnotations(tt.annotations)
			assert.Equal(t, tt.expectedMode, mode)
			assert.Equal(t, tt.expectedFound, found)
		})
	}
}
