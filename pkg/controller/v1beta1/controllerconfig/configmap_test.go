package controllerconfig

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	DefaultModelLocalMountPath = "/mnt/models"
	DefaultHTTPPort            = 8080
	DefaultGRPCPort            = 9000
	DefaultWorkers             = 1
	DefaultTimeout             = 60
	IngressGateway             = "knative-ingress-gateway.knative-serving"
	IngressService             = "istio-ingressgateway.istio-system.svc.cluster.local"
	LocalGateway               = "knative-local-gateway.knative-serving"
	LocalGatewayService        = "knative-local-gateway.istio-system.svc.cluster.local"
	Domain                     = "example.com"
	IngressClassName           = "nginx"
	AdditionalDomain           = "additional-example.com"
	AdditionalDomainExtra      = "additional-example-extra.com"
)

var (
	IngressConfigData = fmt.Sprintf(`{
		"ingressGateway":"%s",
		"ingressService":"%s",
		"localGateway":"%s",
		"localGatewayService":"%s",
		"ingressDomain":"%s",
		"ingressClassName":"%s",
		"additionalIngressDomains":["%s","%s"]
	}`,
		IngressGateway, IngressService,
		LocalGateway, LocalGatewayService,
		Domain, IngressClassName,
		AdditionalDomain, AdditionalDomainExtra)
)

func TestNewInferenceServicesConfig(t *testing.T) {
	tests := []struct {
		name           string
		configMapData  map[string]string
		expectedError  bool
		validateConfig func(*testing.T, *InferenceServicesConfig)
	}{
		{
			name:          "valid empty config",
			configMapData: map[string]string{},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.NotNil(t, cfg)
			},
		},
		{
			name:          "missing configmap",
			configMapData: nil,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()

			if tt.configMapData != nil {
				configMap := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      constants.InferenceServiceConfigMapName,
						Namespace: constants.OMENamespace,
					},
					Data: tt.configMapData,
				}
				_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			config, err := NewInferenceServicesConfig(clientset)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, config)
			if tt.validateConfig != nil {
				tt.validateConfig(t, config)
			}
		})
	}
}

func TestInferenceServicesConfigFromConfigMapParsesPodDisruptionBudget(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]string
		validate func(*testing.T, *InferenceServicesConfig)
	}{
		{
			name: "missing key",
			data: map[string]string{},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.Nil(t, cfg.PodDisruptionBudget.RawDeployment)
				assert.Nil(t, cfg.PodDisruptionBudget.OMENative)
			},
		},
		{
			name: "empty key",
			data: map[string]string{PodDisruptionBudgetConfigName: " \n\t"},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.Nil(t, cfg.PodDisruptionBudget.RawDeployment)
				assert.Nil(t, cfg.PodDisruptionBudget.OMENative)
			},
		},
		{
			name: "explicitly null modes",
			data: map[string]string{
				PodDisruptionBudgetConfigName: `{
					"rawDeployment": null,
					"omeNative": null
				}`,
			},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.Nil(t, cfg.PodDisruptionBudget.RawDeployment)
				assert.Nil(t, cfg.PodDisruptionBudget.OMENative)
			},
		},
		{
			name: "per mode policies",
			data: map[string]string{
				PodDisruptionBudgetConfigName: `{
					"rawDeployment": {"maxUnavailable": 1},
					"omeNative": {"minAvailable": "50%"}
				}`,
			},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				require.NotNil(t, cfg.PodDisruptionBudget.RawDeployment)
				require.NotNil(t, cfg.PodDisruptionBudget.RawDeployment.MaxUnavailable)
				assert.Equal(t, intstr.FromInt(1), *cfg.PodDisruptionBudget.RawDeployment.MaxUnavailable)
				require.NotNil(t, cfg.PodDisruptionBudget.OMENative)
				require.NotNil(t, cfg.PodDisruptionBudget.OMENative.MinAvailable)
				assert.Equal(t, intstr.FromString("50%"), *cfg.PodDisruptionBudget.OMENative.MinAvailable)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := inferenceServicesConfigFromConfigMap(&v1.ConfigMap{Data: tt.data})
			require.NoError(t, err)
			tt.validate(t, cfg)
		})
	}
}

func TestInferenceServicesConfigFromConfigMapRejectsInvalidPodDisruptionBudget(t *testing.T) {
	tests := []struct {
		name       string
		data       string
		wantErrors []string
	}{
		{
			name:       "malformed json",
			data:       `{not-json`,
			wantErrors: []string{PodDisruptionBudgetConfigName},
		},
		{
			name: "raw deployment sets both fields",
			data: `{
				"rawDeployment": {"minAvailable": 1, "maxUnavailable": 1}
			}`,
			wantErrors: []string{
				"podDisruptionBudget.rawDeployment.minAvailable",
				"podDisruptionBudget.rawDeployment.maxUnavailable",
			},
		},
		{
			name: "OMENative percentage exceeds one hundred",
			data: `{
				"omeNative": {"maxUnavailable": "101%"}
			}`,
			wantErrors: []string{"podDisruptionBudget.omeNative.maxUnavailable"},
		},
		{
			name: "unknown top-level field",
			data: `{
				"rawDeployment": {"maxUnavailable": 1},
				"unexpected": {}
			}`,
			wantErrors: []string{`unknown field "unexpected"`},
		},
		{
			name: "unknown mode field",
			data: `{
				"rawDeployment": {"maxUnavailable": 1, "unexpected": 2}
			}`,
			wantErrors: []string{`unknown field "unexpected"`},
		},
		{
			name: "empty raw deployment policy",
			data: `{
				"rawDeployment": {}
			}`,
			wantErrors: []string{
				"podDisruptionBudget.rawDeployment.minAvailable",
				"podDisruptionBudget.rawDeployment.maxUnavailable",
			},
		},
		{
			name: "explicit null-only OMENative policy",
			data: `{
				"omeNative": {"maxUnavailable": null}
			}`,
			wantErrors: []string{
				"podDisruptionBudget.omeNative.minAvailable",
				"podDisruptionBudget.omeNative.maxUnavailable",
			},
		},
		{
			name: "misspelled budget field",
			data: `{
				"rawDeployment": {"maxUnavailble": 1}
			}`,
			wantErrors: []string{`unknown field "maxUnavailble"`},
		},
		{
			name: "case-variant top-level field",
			data: `{
				"RawDeployment": {"maxUnavailable": 1}
			}`,
			wantErrors: []string{`unknown field "RawDeployment"`},
		},
		{
			name: "case-variant policy field",
			data: `{
				"rawDeployment": {"MaxUnavailable": 1}
			}`,
			wantErrors: []string{`unknown field "MaxUnavailable"`},
		},
		{
			name: "duplicate top-level field",
			data: `{
				"rawDeployment": {"maxUnavailable": 1},
				"rawDeployment": {"maxUnavailable": 2}
			}`,
			wantErrors: []string{`duplicate field "rawDeployment"`},
		},
		{
			name: "duplicate policy field",
			data: `{
				"rawDeployment": {
					"maxUnavailable": 1,
					"maxUnavailable": 2
				}
			}`,
			wantErrors: []string{`duplicate field "maxUnavailable"`},
		},
		{
			name: "trailing JSON value",
			data: `{
				"rawDeployment": {"maxUnavailable": 1}
			} {}`,
			wantErrors: []string{PodDisruptionBudgetConfigName},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := inferenceServicesConfigFromConfigMap(&v1.ConfigMap{Data: map[string]string{
				PodDisruptionBudgetConfigName: tt.data,
			}})
			require.Error(t, err)
			for _, want := range tt.wantErrors {
				assert.ErrorContains(t, err, want)
			}
		})
	}
}

func TestInferenceServicesConfigFromConfigMapParsesAcceleratorResources(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]string
		wantErr  bool
		validate func(*testing.T, *InferenceServicesConfig)
	}{
		{
			name: "missing key",
			data: map[string]string{},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.Nil(t, cfg.AcceleratorResources)
			},
		},
		{
			name: "empty key",
			data: map[string]string{AcceleratorResourcesConfigName: " \n\t"},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.Nil(t, cfg.AcceleratorResources)
			},
		},
		{
			name: "configured multi-vendor list",
			data: map[string]string{
				AcceleratorResourcesConfigName: `["nvidia.com/gpu", "amd.com/gpu", "google.com/tpu"]`,
			},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.Equal(t, []string{"nvidia.com/gpu", "amd.com/gpu", "google.com/tpu"}, cfg.AcceleratorResources)
			},
		},
		{
			name: "explicit empty list",
			data: map[string]string{
				AcceleratorResourcesConfigName: `[]`,
			},
			validate: func(t *testing.T, cfg *InferenceServicesConfig) {
				assert.Empty(t, cfg.AcceleratorResources)
			},
		},
		{
			name:    "malformed json",
			data:    map[string]string{AcceleratorResourcesConfigName: `not-json`},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := inferenceServicesConfigFromConfigMap(&v1.ConfigMap{Data: tt.data})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			tt.validate(t, cfg)
		})
	}
}

func TestAcceleratorResourceNames(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *InferenceServicesConfig
		expected []string
	}{
		{
			name:     "nil receiver falls back to nvidia-only",
			cfg:      nil,
			expected: []string{constants.NvidiaGPUResourceType},
		},
		{
			name:     "unset field falls back to nvidia-only",
			cfg:      &InferenceServicesConfig{},
			expected: []string{constants.NvidiaGPUResourceType},
		},
		{
			name:     "explicit empty slice falls back to nvidia-only",
			cfg:      &InferenceServicesConfig{AcceleratorResources: []string{}},
			expected: []string{constants.NvidiaGPUResourceType},
		},
		{
			name:     "configured list is returned verbatim",
			cfg:      &InferenceServicesConfig{AcceleratorResources: []string{"nvidia.com/gpu", "amd.com/gpu", "google.com/tpu"}},
			expected: []string{"nvidia.com/gpu", "amd.com/gpu", "google.com/tpu"},
		},
		{
			name:     "configured list need not include nvidia",
			cfg:      &InferenceServicesConfig{AcceleratorResources: []string{"amd.com/gpu"}},
			expected: []string{"amd.com/gpu"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cfg.AcceleratorResourceNames())
		})
	}
}

func TestNewOmeAgentConfig(t *testing.T) {
	tests := []struct {
		name          string
		configMapData map[string]string
		expectedError bool
		validate      func(*testing.T, *OmeAgentConfig)
	}{
		{
			name: "valid config",
			configMapData: map[string]string{
				OmeAgentConfigName: `{
					"image": "ghcr.io/example/ome-agent:v1.0.0",
					"serviceAccount": "ome-model-metadata",
					"cpuRequest": "100m",
					"memoryRequest": "256Mi",
					"cpuLimit": "500m",
					"memoryLimit": "512Mi",
					"backoffLimit": 3,
					"ttlSecondsAfterFinished": 600
				}`,
			},
			validate: func(t *testing.T, cfg *OmeAgentConfig) {
				assert.Equal(t, "ghcr.io/example/ome-agent:v1.0.0", cfg.Image)
				assert.Equal(t, "ome-model-metadata", cfg.ServiceAccount)
				assert.Equal(t, "100m", cfg.CPURequest)
				assert.Equal(t, "256Mi", cfg.MemoryRequest)
				assert.Equal(t, int32(3), cfg.BackoffLimit)
				assert.Equal(t, int32(600), cfg.TTLSecondsAfterFinished)
			},
		},
		{
			name:          "missing block returns zero-value config",
			configMapData: map[string]string{},
			validate: func(t *testing.T, cfg *OmeAgentConfig) {
				assert.Empty(t, cfg.Image)
				assert.Empty(t, cfg.ServiceAccount)
			},
		},
		{
			name: "malformed json fails",
			configMapData: map[string]string{
				OmeAgentConfigName: `{not-json`,
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

			cfg, err := NewOmeAgentConfig(clientset)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg)
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestNewIngressConfig(t *testing.T) {
	tests := []struct {
		name           string
		configMapData  map[string]string
		expectedError  bool
		validateConfig func(*testing.T, *IngressConfig)
	}{
		{
			name: "valid config",
			configMapData: map[string]string{
				IngressConfigKeyName: `{
					"ingressGateway": "istio-ingress",
					"ingressService": "istio-ingress",
					"localGateway": "cluster-local-gateway",
					"localGatewayService": "cluster-local-gateway",
					"ingressDomain": "example.com",
					"ingressClassName": "nginx",
					"additionalIngressDomains": ["extra.example.com"],
					"domainTemplate": "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
					"urlScheme": "https",
					"pathTemplate": "/{{ .Namespace }}/{{ .Name }}",
					"perISVCSubdomain": true,
					"sharedHostPrefix": "serving",
					"omeIngressGatewayClass": "internal",
					"additionalIngressGateways": [
						{"omeIngressGateway": "envoy-gateway-system/ext-gw", "ingressDomain": "ext.example.com", "class": "external"}
					]
				}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *IngressConfig) {
				assert.Equal(t, "istio-ingress", cfg.IngressGateway)
				assert.Equal(t, "istio-ingress", cfg.IngressServiceName)
				assert.Equal(t, "example.com", cfg.IngressDomain)
				assert.Equal(t, "nginx", *cfg.IngressClassName)
				assert.Equal(t, "https", cfg.UrlScheme)
				assert.True(t, cfg.PerISVCSubdomain)
				assert.Equal(t, "serving", cfg.SharedHostPrefix)
				assert.Equal(t, "internal", cfg.OmeIngressGatewayClass)
				require.Len(t, cfg.AdditionalIngressGateways, 1)
				assert.Equal(t, "envoy-gateway-system/ext-gw", cfg.AdditionalIngressGateways[0].OmeIngressGateway)
				assert.Equal(t, "ext.example.com", cfg.AdditionalIngressGateways[0].IngressDomain)
				assert.Equal(t, "external", cfg.AdditionalIngressGateways[0].Class)
			},
		},
		{
			name: "invalid primary gateway class rejected",
			configMapData: map[string]string{
				IngressConfigKeyName: `{
					"ingressGateway": "istio-ingress",
					"ingressService": "istio-ingress",
					"omeIngressGatewayClass": "Internal"
				}`,
			},
			expectedError: true,
		},
		{
			name: "invalid additional gateway class rejected",
			configMapData: map[string]string{
				IngressConfigKeyName: `{
					"ingressGateway": "istio-ingress",
					"ingressService": "istio-ingress",
					"additionalIngressGateways": [
						{"omeIngressGateway": "envoy-gateway-system/ext-gw", "ingressDomain": "ext.example.com", "class": "ext_gw"}
					]
				}`,
			},
			expectedError: true,
		},
		{
			name: "invalid per-namespace gateway class rejected",
			configMapData: map[string]string{
				IngressConfigKeyName: `{
					"ingressGateway": "istio-ingress",
					"ingressService": "istio-ingress",
					"namespaceIngressGateways": {
						"prod": {
							"primary": {"omeIngressGateway": "istio-system/prod-int-gw", "ingressDomain": "int.example.com", "class": "prod internal"}
						}
					}
				}`,
			},
			expectedError: true,
		},
		{
			name: "missing required fields",
			configMapData: map[string]string{
				IngressConfigKeyName: `{
					"ingressDomain": "example.com"
				}`,
			},
			expectedError: true,
		},
		{
			name: "invalid path template",
			configMapData: map[string]string{
				IngressConfigKeyName: `{
					"ingressGateway": "istio-ingress",
					"ingressService": "istio-ingress",
					"pathTemplate": "{{ .Invalid }}"
				}`,
			},
			expectedError: true,
		},
		{
			name:          "default values",
			configMapData: map[string]string{},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *IngressConfig) {
				assert.Equal(t, DefaultDomainTemplate, cfg.DomainTemplate)
				assert.Equal(t, DefaultIngressDomain, cfg.IngressDomain)
				assert.Equal(t, DefaultUrlScheme, cfg.UrlScheme)
				assert.False(t, cfg.PerISVCSubdomain, "perISVCSubdomain must default to false")
				assert.Empty(t, cfg.SharedHostPrefix, "sharedHostPrefix must have NO in-code default (supplied via config)")
			},
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

			config, err := NewIngressConfig(clientset)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, config)
			if tt.validateConfig != nil {
				tt.validateConfig(t, config)
			}
		})
	}
}

func TestNewDeployConfig(t *testing.T) {
	tests := []struct {
		name           string
		configMapData  map[string]string
		expectedError  bool
		validateConfig func(*testing.T, *DeployConfig)
	}{
		{
			name: "valid config",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment"
				}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				assert.Equal(t, "RawDeployment", cfg.DefaultDeploymentMode)
			},
		},
		{
			name: "invalid json",
			configMapData: map[string]string{
				DeployConfigName: `invalid json`,
			},
			expectedError: true,
		},
		{
			name:          "empty config",
			configMapData: map[string]string{},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				assert.Empty(t, cfg.DefaultDeploymentMode)
				assert.Nil(t, cfg.Replicas)
				// Nil-receiver accessors report unconfigured, never a number.
				assert.Nil(t, cfg.Replicas.Min())
				assert.Nil(t, cfg.Replicas.EngineMax())
				assert.Nil(t, cfg.Replicas.DecoderMax())
				assert.Nil(t, cfg.Replicas.RouterMax())
			},
		},
		{
			name: "replicas block parsed with per-component max defaults",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"replicas": {
						"defaultMinReplicas": 1,
						"defaultMaxReplicas": {"engine": 3, "decoder": 3, "router": 2}
					}
				}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				require.NotNil(t, cfg.Replicas)
				require.NotNil(t, cfg.Replicas.Min())
				assert.Equal(t, 1, *cfg.Replicas.Min())
				require.NotNil(t, cfg.Replicas.EngineMax())
				assert.Equal(t, 3, *cfg.Replicas.EngineMax())
				require.NotNil(t, cfg.Replicas.DecoderMax())
				assert.Equal(t, 3, *cfg.Replicas.DecoderMax())
				require.NotNil(t, cfg.Replicas.RouterMax())
				assert.Equal(t, 2, *cfg.Replicas.RouterMax())
			},
		},
		{
			name: "partial replicas block leaves omitted fields unconfigured",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"replicas": {"defaultMaxReplicas": {"engine": 3}}
				}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				require.NotNil(t, cfg.Replicas)
				assert.Nil(t, cfg.Replicas.Min())
				require.NotNil(t, cfg.Replicas.EngineMax())
				assert.Equal(t, 3, *cfg.Replicas.EngineMax())
				assert.Nil(t, cfg.Replicas.DecoderMax())
				assert.Nil(t, cfg.Replicas.RouterMax())
			},
		},
		{
			name: "zero replica default rejected at config-load",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"replicas": {"defaultMinReplicas": 0}
				}`,
			},
			expectedError: true,
		},
		{
			name: "negative per-component max default rejected at config-load",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"replicas": {"defaultMaxReplicas": {"router": -2}}
				}`,
			},
			expectedError: true,
		},
		{
			name: "termination grace default parsed",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"terminationGracePeriodSeconds": 600
				}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				require.NotNil(t, cfg.TerminationGracePeriodSeconds)
				assert.Equal(t, int64(600), *cfg.TerminationGracePeriodSeconds)
			},
		},
		{
			name: "omitted termination grace stays unconfigured",
			configMapData: map[string]string{
				DeployConfigName: `{"defaultDeploymentMode": "RawDeployment"}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				assert.Nil(t, cfg.TerminationGracePeriodSeconds)
			},
		},
		{
			name: "zero termination grace rejected at config-load",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"terminationGracePeriodSeconds": 0
				}`,
			},
			expectedError: true,
		},
		{
			name: "negative termination grace rejected at config-load",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"terminationGracePeriodSeconds": -1
				}`,
			},
			expectedError: true,
		},
		{
			name: "minReadySeconds default parsed",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"minReadySeconds": 30
				}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				require.NotNil(t, cfg.MinReadySeconds)
				assert.Equal(t, int32(30), *cfg.MinReadySeconds)
			},
		},
		{
			name: "zero minReadySeconds is a valid explicit value",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"minReadySeconds": 0
				}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				require.NotNil(t, cfg.MinReadySeconds)
				assert.Equal(t, int32(0), *cfg.MinReadySeconds)
			},
		},
		{
			name: "omitted minReadySeconds stays unconfigured",
			configMapData: map[string]string{
				DeployConfigName: `{"defaultDeploymentMode": "RawDeployment"}`,
			},
			expectedError: false,
			validateConfig: func(t *testing.T, cfg *DeployConfig) {
				assert.Nil(t, cfg.MinReadySeconds)
			},
		},
		{
			name: "negative minReadySeconds rejected at config-load",
			configMapData: map[string]string{
				DeployConfigName: `{
					"defaultDeploymentMode": "RawDeployment",
					"minReadySeconds": -1
				}`,
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

			config, err := NewDeployConfig(clientset)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, config)
			if tt.validateConfig != nil {
				tt.validateConfig(t, config)
			}
		})
	}
}

func TestGetComponentConfig(t *testing.T) {
	type testStruct struct {
		Field string `json:"field"`
	}

	tests := []struct {
		name         string
		key          string
		data         map[string]string
		expectedData testStruct
		expectedErr  bool
	}{
		{
			name: "valid json",
			key:  "test",
			data: map[string]string{
				"test": `{"field": "value"}`,
			},
			expectedData: testStruct{Field: "value"},
			expectedErr:  false,
		},
		{
			name: "invalid json",
			key:  "test",
			data: map[string]string{
				"test": `invalid json`,
			},
			expectedErr: true,
		},
		{
			name:        "missing key",
			key:         "missing",
			data:        map[string]string{},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configMap := &v1.ConfigMap{Data: tt.data}
			var result testStruct
			err := getComponentConfig(tt.key, configMap, &result)
			if tt.expectedErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedData, result)
		})
	}
}

// TestCanaryAnalysisDurationAccessors verifies the query-timeout and cache-TTL
// accessors fall back to the documented defaults for empty, malformed, zero, or
// negative values. The zero/negative cases matter: a non-positive query timeout
// would leave a background sampler query unbounded, and a non-positive TTL would
// disable cache eviction.
func TestCanaryAnalysisDurationAccessors(t *testing.T) {
	defQuery, _ := time.ParseDuration(DefaultAnalysisQueryTimeout)
	defTTL, _ := time.ParseDuration(DefaultAnalysisCacheTTL)

	tests := []struct {
		name      string
		query     string
		ttl       string
		wantQuery time.Duration
		wantTTL   time.Duration
	}{
		{"valid values pass through", "9s", "3m", 9 * time.Second, 3 * time.Minute},
		{"empty falls back", "", "", defQuery, defTTL},
		{"malformed falls back", "nope", "huh", defQuery, defTTL},
		{"zero falls back", "0s", "0s", defQuery, defTTL},
		{"negative falls back", "-5s", "-1m", defQuery, defTTL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CanaryAnalysisConfig{QueryTimeout: tt.query, CacheTTL: tt.ttl}
			assert.Equal(t, tt.wantQuery, c.QueryTimeoutDuration())
			assert.Equal(t, tt.wantTTL, c.CacheTTLDuration())
		})
	}
}

// TestCanaryAnalysisDefaultProvider verifies defaultProvider parses from the
// canaryAnalysis block and has no in-code default: empty keeps the
// BundledPrometheusAddress fallback behavior, and setting it does not disturb
// the sampler-tunable fallbacks.
func TestCanaryAnalysisDefaultProvider(t *testing.T) {
	t.Run("absent stays empty", func(t *testing.T) {
		cfg, err := parseCanaryAnalysisConfig(&v1.ConfigMap{Data: map[string]string{
			CanaryAnalysisConfigName: `{"bundledPrometheusAddress": "http://prometheus.monitoring.svc:9090"}`,
		}})
		require.NoError(t, err)
		assert.Empty(t, cfg.DefaultProvider)
		assert.Equal(t, "http://prometheus.monitoring.svc:9090", cfg.BundledPrometheusAddress)
	})

	t.Run("set names a provider binding", func(t *testing.T) {
		cfg, err := parseCanaryAnalysisConfig(&v1.ConfigMap{Data: map[string]string{
			CanaryAnalysisConfigName: `{"defaultProvider": "cluster-prometheus"}`,
		}})
		require.NoError(t, err)
		assert.Equal(t, "cluster-prometheus", cfg.DefaultProvider)
		// Sampler-tunable fallbacks are unaffected.
		assert.Equal(t, DefaultAnalysisQueryTimeout, cfg.QueryTimeout)
		assert.Equal(t, DefaultAnalysisCacheTTL, cfg.CacheTTL)
		assert.Equal(t, int32(DefaultAnalysisMaxConcurrency), cfg.MaxConcurrency)
	})
}

// countingClientset returns a fake clientset plus an atomic counter that is
// incremented on every GET against the inferenceservice-config ConfigMap, so a
// test can assert exactly how many apiserver round-trips a ConfigCache issues.
func countingClientset(t *testing.T, data map[string]string) (*fake.Clientset, *int64) {
	t.Helper()
	clientset := fake.NewSimpleClientset(&v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: data,
	})
	var gets int64
	clientset.PrependReactor("get", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.GetAction).GetName() == constants.InferenceServiceConfigMapName {
			atomic.AddInt64(&gets, 1)
		}
		// Fall through to the default tracker so the object is actually returned.
		return false, nil, nil
	})
	return clientset, &gets
}

// TestConfigCache exercises the hit / expiry / disabled / error behaviors of
// ConfigCache: within the TTL the constructors share one apiserver GET, after
// the TTL the entry is re-fetched (so ConfigMap edits apply without a restart),
// a non-positive TTL disables caching, and a GET error is surfaced (not served
// from a stale entry).
func TestConfigCache(t *testing.T) {
	deployData := map[string]string{
		DeployConfigName: `{"defaultDeploymentMode":"RawDeployment"}`,
		IngressConfigKeyName: `{
			"ingressGateway":"istio-ingress",
			"ingressService":"istio-ingress",
			"ingressDomain":"example.com"
		}`,
	}

	t.Run("hit: four cached loads share one GET within TTL", func(t *testing.T) {
		clientset, gets := countingClientset(t, deployData)
		fakeNow := time.Unix(0, 0)
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return fakeNow }}

		// Mirror the reconcile hot path: four config loads in one pass.
		_, err := NewDeployConfigCached(cache, clientset)
		require.NoError(t, err)
		_, err = NewInferenceServicesConfigCached(cache, clientset)
		require.NoError(t, err)
		ing, err := NewIngressConfigCached(cache, clientset)
		require.NoError(t, err)
		_, err = NewCanaryAnalysisConfigCached(cache, clientset)
		require.NoError(t, err)

		assert.Equal(t, int64(1), atomic.LoadInt64(gets), "all four loads must share one apiserver GET")
		// Parsed values must be identical to the uncached path.
		assert.Equal(t, "example.com", ing.IngressDomain)
	})

	t.Run("expiry: re-fetches after TTL elapses", func(t *testing.T) {
		clientset, gets := countingClientset(t, deployData)
		fakeNow := time.Unix(0, 0)
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return fakeNow }}

		_, err := NewDeployConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int64(1), atomic.LoadInt64(gets))

		// Still within TTL: served from cache.
		fakeNow = fakeNow.Add(29 * time.Second)
		_, err = NewDeployConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int64(1), atomic.LoadInt64(gets))

		// Past TTL: re-fetched.
		fakeNow = fakeNow.Add(2 * time.Second)
		_, err = NewDeployConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int64(2), atomic.LoadInt64(gets))
	})

	t.Run("expiry picks up a ConfigMap edit without restart", func(t *testing.T) {
		clientset, _ := countingClientset(t, map[string]string{
			DeployConfigName: `{"defaultDeploymentMode":"RawDeployment"}`,
		})
		fakeNow := time.Unix(0, 0)
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return fakeNow }}

		cfg, err := NewDeployConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, "RawDeployment", cfg.DefaultDeploymentMode)

		// Edit the ConfigMap to clear the deploy block.
		_, err = clientset.CoreV1().ConfigMaps(constants.OMENamespace).Update(context.TODO(), &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{},
		}, metav1.UpdateOptions{})
		require.NoError(t, err)

		// Within TTL the edit is not yet visible.
		cfg, err = NewDeployConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, "RawDeployment", cfg.DefaultDeploymentMode)

		// After TTL the edit applies.
		fakeNow = fakeNow.Add(31 * time.Second)
		cfg, err = NewDeployConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Empty(t, cfg.DefaultDeploymentMode)
	})

	t.Run("disabled: non-positive TTL reads apiserver every call", func(t *testing.T) {
		clientset, gets := countingClientset(t, deployData)
		cache := NewConfigCache(0)

		for i := 0; i < 3; i++ {
			_, err := NewDeployConfigCached(cache, clientset)
			require.NoError(t, err)
		}
		assert.Equal(t, int64(3), atomic.LoadInt64(gets), "TTL<=0 must not cache")
	})

	t.Run("nil cache reads apiserver every call", func(t *testing.T) {
		clientset, gets := countingClientset(t, deployData)

		for i := 0; i < 2; i++ {
			_, err := NewDeployConfigCached(nil, clientset)
			require.NoError(t, err)
		}
		assert.Equal(t, int64(2), atomic.LoadInt64(gets))
	})

	t.Run("error: GET failure is surfaced, not served stale", func(t *testing.T) {
		clientset := fake.NewSimpleClientset() // no ConfigMap
		cache := NewConfigCache(30 * time.Second)

		_, err := NewDeployConfigCached(cache, clientset)
		assert.Error(t, err, "missing ConfigMap must surface as an error")
	})
}

func TestConfigCacheTTLAccessor(t *testing.T) {
	assert.Equal(t, 30*time.Second, NewConfigCache(30*time.Second).TTL())
	assert.Equal(t, time.Duration(0), NewConfigCache(0).TTL())

	var nilCache *ConfigCache
	assert.Equal(t, time.Duration(0), nilCache.TTL(), "nil cache reads as caching disabled")
}

// TestConfigCacheSingleflight proves that when many reconciles race into the
// same TTL expiry, the cache issues exactly ONE apiserver GET (and never holds
// its lock across that GET). The reactor blocks all in-flight GETs on a gate so
// the goroutines pile up inside get() simultaneously; without singleflight each
// would issue its own GET, so the counter would exceed one.
func TestConfigCacheSingleflight(t *testing.T) {
	const goroutines = 64

	clientset := fake.NewSimpleClientset(&v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: map[string]string{
			DeployConfigName: `{"defaultDeploymentMode":"RawDeployment"}`,
		},
	})

	var gets int64
	release := make(chan struct{})
	var first sync.Once
	firstInFlight := make(chan struct{})
	clientset.PrependReactor("get", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.GetAction).GetName() == constants.InferenceServiceConfigMapName {
			atomic.AddInt64(&gets, 1)
			// Signal that a GET has begun, then block until the test releases
			// it — this widens the window so concurrent callers must dedupe via
			// singleflight rather than each issuing their own GET.
			first.Do(func() { close(firstInFlight) })
			<-release
		}
		return false, nil, nil
	})

	cache := NewConfigCache(30 * time.Second)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			cfg, err := NewDeployConfigCached(cache, clientset)
			assert.NoError(t, err)
			if cfg != nil {
				assert.Equal(t, "RawDeployment", cfg.DefaultDeploymentMode)
			}
		}()
	}

	// Wait until at least one GET is in flight (all callers should now be
	// joined onto the single singleflight call), then release the apiserver.
	<-firstInFlight
	close(release)
	wg.Wait()

	assert.Equal(t, int64(1), atomic.LoadInt64(&gets),
		"concurrent loads across one expiry window must collapse to exactly one apiserver GET")
}

// TestNewCoordinationConfigCached verifies the cached coordination loader shares
// one apiserver GET within the TTL and re-fetches after it elapses so a
// ConfigMap edit applies without a restart — mirroring the other cached loaders
// (the coordination loader runs on every ISVC reconcile).
func TestNewCoordinationConfigCached(t *testing.T) {
	t.Run("hit: repeated loads share one GET within TTL", func(t *testing.T) {
		clientset, gets := countingClientset(t, map[string]string{
			CoordinationConfigName: `{"trafficWeightDeadbandPercent":5}`,
		})
		fakeNow := time.Unix(0, 0)
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return fakeNow }}

		cfg, err := NewCoordinationConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int32(5), cfg.TrafficWeightDeadbandPercent)

		_, err = NewCoordinationConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int64(1), atomic.LoadInt64(gets), "both loads must share one apiserver GET")
	})

	t.Run("expiry picks up a ConfigMap edit without restart", func(t *testing.T) {
		clientset, _ := countingClientset(t, map[string]string{
			CoordinationConfigName: `{"trafficWeightDeadbandPercent":5}`,
		})
		fakeNow := time.Unix(0, 0)
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return fakeNow }}

		cfg, err := NewCoordinationConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int32(5), cfg.TrafficWeightDeadbandPercent)

		// Edit the ConfigMap to bump the deadband.
		_, err = clientset.CoreV1().ConfigMaps(constants.OMENamespace).Update(context.TODO(), &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				CoordinationConfigName: `{"trafficWeightDeadbandPercent":10}`,
			},
		}, metav1.UpdateOptions{})
		require.NoError(t, err)

		// Within TTL the edit is not yet visible.
		cfg, err = NewCoordinationConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int32(5), cfg.TrafficWeightDeadbandPercent)

		// After TTL the edit applies.
		fakeNow = fakeNow.Add(31 * time.Second)
		cfg, err = NewCoordinationConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int32(10), cfg.TrafficWeightDeadbandPercent)
	})
}

// TestCoordinationConfigDefaultRatioTolerance verifies the ratio-tolerance
// default distinguishes configured (pointer set, zero included) from absent
// (nil — a group that omits tolerance rolls with no drift bound).
func TestCoordinationConfigDefaultRatioTolerance(t *testing.T) {
	load := func(t *testing.T, payload string) *CoordinationConfig {
		clientset, _ := countingClientset(t, map[string]string{
			CoordinationConfigName: payload,
		})
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return time.Unix(0, 0) }}
		cfg, err := NewCoordinationConfigCached(cache, clientset)
		require.NoError(t, err)
		return cfg
	}

	t.Run("configured value parses to a pointer", func(t *testing.T) {
		cfg := load(t, `{"trafficWeightDeadbandPercent":0,"defaultRatioTolerancePercent":5}`)
		require.NotNil(t, cfg.DefaultRatioTolerancePercent)
		assert.Equal(t, int32(5), *cfg.DefaultRatioTolerancePercent)
	})

	t.Run("configured zero stays distinguishable from absent", func(t *testing.T) {
		cfg := load(t, `{"defaultRatioTolerancePercent":0}`)
		require.NotNil(t, cfg.DefaultRatioTolerancePercent)
		assert.Equal(t, int32(0), *cfg.DefaultRatioTolerancePercent)
	})

	t.Run("absent key resolves to nil", func(t *testing.T) {
		cfg := load(t, `{"trafficWeightDeadbandPercent":0}`)
		assert.Nil(t, cfg.DefaultRatioTolerancePercent)
	})
}

func TestDeployConfig_UpdateStrategyDefaults(t *testing.T) {
	tests := []struct {
		name          string
		block         string
		expectedError string
		validate      func(*testing.T, *DeployConfig)
	}{
		{
			name: "per-component entries parsed",
			block: `{
				"defaultDeploymentMode": "RawDeployment",
				"updateStrategy": {
					"router":  {"type": "SurgeThenDrain",    "maxSurge": "25%"},
					"engine":  {"type": "InPlaceIfPossible", "maxUnavailable": "10%"},
					"decoder": {"type": "InPlaceIfPossible", "maxUnavailable": 2}
				}
			}`,
			validate: func(t *testing.T, cfg *DeployConfig) {
				router := cfg.UpdateStrategy.ForComponent(workloadtypes.ComponentRouter)
				require.NotNil(t, router)
				assert.Equal(t, "SurgeThenDrain", router.Type)
				assert.Equal(t, intstr.FromString("25%"), *router.MaxSurge)
				assert.Nil(t, router.MaxUnavailable)

				engine := cfg.UpdateStrategy.ForComponent(workloadtypes.ComponentEngine)
				require.NotNil(t, engine)
				assert.Equal(t, "InPlaceIfPossible", engine.Type)
				assert.Equal(t, intstr.FromString("10%"), *engine.MaxUnavailable)

				decoder := cfg.UpdateStrategy.ForComponent(workloadtypes.ComponentDecoder)
				require.NotNil(t, decoder)
				assert.Equal(t, intstr.FromInt(2), *decoder.MaxUnavailable)
			},
		},
		{
			name:  "absent block is unconfigured",
			block: `{"defaultDeploymentMode": "RawDeployment"}`,
			validate: func(t *testing.T, cfg *DeployConfig) {
				assert.Nil(t, cfg.UpdateStrategy)
				// Nil-receiver accessor reports unconfigured, never a strategy.
				assert.Nil(t, cfg.UpdateStrategy.ForComponent(workloadtypes.ComponentEngine))
			},
		},
		{
			name: "an entry may omit every field",
			block: `{
				"defaultDeploymentMode": "RawDeployment",
				"updateStrategy": {"engine": {}}
			}`,
			validate: func(t *testing.T, cfg *DeployConfig) {
				engine := cfg.UpdateStrategy.ForComponent(workloadtypes.ComponentEngine)
				require.NotNil(t, engine)
				assert.Empty(t, engine.Type)
			},
		},
		{
			name: "unsupported strategy rejected",
			block: `{
				"defaultDeploymentMode": "RawDeployment",
				"updateStrategy": {"engine": {"type": "RollingUpdate"}}
			}`,
			expectedError: `updateStrategy.engine.type "RollingUpdate" is not a supported update strategy`,
		},
		{
			name: "zero budget rejected as a rollout deadlock",
			block: `{
				"defaultDeploymentMode": "RawDeployment",
				"updateStrategy": {"engine": {"type": "InPlaceIfPossible", "maxUnavailable": 0}}
			}`,
			expectedError: "updateStrategy.engine.maxUnavailable must be > 0",
		},
		{
			name: "zero-percent budget rejected as a rollout deadlock",
			block: `{
				"defaultDeploymentMode": "RawDeployment",
				"updateStrategy": {"router": {"maxSurge": "0%"}}
			}`,
			expectedError: "updateStrategy.router.maxSurge must be > 0",
		},
		{
			name: "non-numeric budget rejected",
			block: `{
				"defaultDeploymentMode": "RawDeployment",
				"updateStrategy": {"router": {"maxSurge": "many"}}
			}`,
			expectedError: "updateStrategy.router.maxSurge is not an integer or percentage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseDeployConfig(&v1.ConfigMap{Data: map[string]string{DeployConfigName: tt.block}})
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			}
			require.NoError(t, err)
			tt.validate(t, cfg)
		})
	}
}
