// Package shared holds the Backend interface + cross-cutting helpers
// (RetryUpdate, ModelSpecAndStatus, UpdateSpecWithConfig). Lives apart
// from basemodel/ so the backend sub-packages can implement the
// interface without importing basemodel.
package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/modelparser"
)

// ModelStatus represents the status of a model on a node.
type ModelStatus string

const (
	ModelStatusReady    ModelStatus = "Ready"
	ModelStatusUpdating ModelStatus = "Updating"
	ModelStatusFailed   ModelStatus = "Failed"
	ModelStatusDeleted  ModelStatus = "Deleted"
	ModelStatusEvicting ModelStatus = "Evicting"
	ModelStatusEvicted  ModelStatus = "Evicted"
)

// ModelConfig is the structured per-model entry stored in per-node
// ConfigMaps by the model-agent daemon. The controller reads it back to
// fill in BaseModelSpec fields the operator left blank.
type ModelConfig struct {
	ModelType                 string                 `json:"modelType,omitempty"`
	ModelArchitecture         string                 `json:"modelArchitecture,omitempty"`
	ModelFramework            map[string]string      `json:"modelFramework,omitempty"`
	ModelFormat               map[string]string      `json:"modelFormat,omitempty"`
	ModelParameterSize        string                 `json:"modelParameterSize,omitempty"`
	MaxTokens                 int32                  `json:"maxTokens,omitempty"`
	ModelCapabilities         []string               `json:"modelCapabilities,omitempty"`
	ApiCapabilities           []string               `json:"apiCapabilities,omitempty"`
	DecodedModelConfiguration map[string]interface{} `json:"decodedModelConfiguration,omitempty"`
	Quantization              string                 `json:"quantization,omitempty"`
	Artifact                  modelparser.Artifact   `json:"artifact,omitempty"`
}

// DownloadProgress is the in-flight download progress reported by the
// model-agent daemon for a single model.
type DownloadProgress struct {
	Phase            string  `json:"phase"`
	TotalBytes       uint64  `json:"totalBytes"`
	CompletedBytes   uint64  `json:"completedBytes"`
	TotalFiles       uint32  `json:"totalFiles"`
	CompletedFiles   uint32  `json:"completedFiles"`
	SpeedBytesPerSec float64 `json:"speedBytesPerSec"`
	LastUpdated      string  `json:"lastUpdated"`
}

func (p *DownloadProgress) Percentage() float64 {
	if p == nil || p.TotalBytes == 0 {
		return 0
	}
	return float64(p.CompletedBytes) / float64(p.TotalBytes) * 100
}

// ModelEntry is the top-level per-model record in the node ConfigMap.
type ModelEntry struct {
	ModelUID              types.UID         `json:"modelUID,omitempty"`
	ArtifactRehydrationID string            `json:"artifactRehydrationID,omitempty"`
	NodeUID               types.UID         `json:"nodeUID,omitempty"`
	Name                  string            `json:"name"`
	Status                ModelStatus       `json:"status"`
	Config                *ModelConfig      `json:"config,omitempty"`
	Progress              *DownloadProgress `json:"progress,omitempty"`
}

// ConvertMetadataToModelConfig converts parser-produced metadata into
// the on-wire ModelConfig the daemon writes to ConfigMaps.
func ConvertMetadataToModelConfig(metadata modelparser.ModelMetadata) *ModelConfig {
	var modelFramework map[string]string
	if metadata.ModelFramework != nil {
		modelFramework = map[string]string{"name": metadata.ModelFramework.Name}
		if metadata.ModelFramework.Version != nil {
			modelFramework["version"] = *metadata.ModelFramework.Version
		}
	}

	var modelFormat map[string]string
	if metadata.ModelFormat.Name != "" {
		modelFormat = map[string]string{"name": metadata.ModelFormat.Name}
		if metadata.ModelFormat.Version != nil {
			modelFormat["version"] = *metadata.ModelFormat.Version
		}
	}

	var quantization string
	if metadata.Quantization != "" {
		quantization = string(metadata.Quantization)
	}

	decodedConfig := metadata.DecodedModelConfiguration
	if decodedConfig == nil && len(metadata.ModelConfiguration) > 0 {
		var configMap map[string]interface{}
		if err := json.Unmarshal(metadata.ModelConfiguration, &configMap); err == nil {
			decodedConfig = configMap
		}
	}

	var artifact modelparser.Artifact
	if metadata.Artifact.Sha != "" || metadata.Artifact.ParentPath != nil || metadata.Artifact.ChildrenPaths != nil {
		currentArtifact := metadata.Artifact
		var parent map[string]string
		if currentArtifact.ParentPath != nil {
			parent = make(map[string]string, len(currentArtifact.ParentPath))
			for k, v := range currentArtifact.ParentPath {
				parent[k] = v
			}
		}
		var children []string
		if currentArtifact.ChildrenPaths != nil {
			children = make([]string, len(currentArtifact.ChildrenPaths))
			copy(children, currentArtifact.ChildrenPaths)
		}
		artifact = modelparser.Artifact{
			Sha:           currentArtifact.Sha,
			ParentPath:    parent,
			ChildrenPaths: children,
		}
	}

	return &ModelConfig{
		ModelType:                 metadata.ModelType,
		ModelArchitecture:         metadata.ModelArchitecture,
		ModelFramework:            modelFramework,
		ModelFormat:               modelFormat,
		ModelParameterSize:        metadata.ModelParameterSize,
		MaxTokens:                 metadata.MaxTokens,
		ModelCapabilities:         metadata.ModelCapabilities,
		ApiCapabilities:           metadata.ApiCapabilities,
		DecodedModelConfiguration: decodedConfig,
		Quantization:              quantization,
		Artifact:                  artifact,
	}
}

// Backend is one of OME's BaseModel distribution backends. The
// controller dispatches Reconcile / HandleDeletion to the first
// backend whose Matches(spec) returns true; per-node is the default
// fallback and must be registered last.
type Backend interface {
	Name() string
	Matches(spec *v1beta1.BaseModelSpec) bool
	Reconcile(ctx context.Context, args BackendArgs) (ctrl.Result, error)
	HandleDeletion(ctx context.Context, args BackendArgs) (ctrl.Result, error)
}

// BackendArgs bundles per-reconcile state. Construction-time deps
// (ModelCache, OmeAgentConfig) come in through backend constructors
// instead, so this stays free of backend-specific imports.
type BackendArgs struct {
	Client          client.Client
	Scheme          *runtime.Scheme
	Log             logr.Logger
	Obj             client.Object
	Spec            *v1beta1.BaseModelSpec
	Status          *v1beta1.ModelStatusSpec
	Finalizer       string
	IsClusterScoped bool
	Kind            string
}

func ModelSpecAndStatus(obj client.Object) (*v1beta1.BaseModelSpec, *v1beta1.ModelStatusSpec, error) {
	switch model := obj.(type) {
	case *v1beta1.BaseModel:
		return &model.Spec, &model.Status, nil
	case *v1beta1.ClusterBaseModel:
		return &model.Spec, &model.Status, nil
	default:
		return nil, nil, fmt.Errorf("unsupported model type: %T", obj)
	}
}

func StampObservedReconcile(obj client.Object, status *v1beta1.ModelStatusSpec) {
	if status == nil {
		return
	}
	if status.ObservedGeneration != obj.GetGeneration() {
		status.ObservedGeneration = obj.GetGeneration()
	}
	now := metav1.NewTime(time.Now())
	status.LastReconcileTime = &now
}

// RetryUpdate retries updateFunc on conflict up to 3 times, with
// 100ms/200ms/400ms backoff. Each attempt receives a freshly-fetched
// copy of obj.
func RetryUpdate(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, updateType string, updateFunc func(context.Context, client.Client, client.Object) error) error {
	const maxRetries = 3

	for i := 0; i < maxRetries; i++ {
		latest := obj.DeepCopyObject().(client.Object)
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(obj), latest); err != nil {
			return fmt.Errorf("failed to get latest object version: %w", err)
		}

		if err := updateFunc(ctx, kubeClient, latest); err != nil {
			if errors.IsConflict(err) && i < maxRetries-1 {
				backoff := time.Millisecond * time.Duration(100<<uint(i))
				log.V(1).Info("Resource conflict during update, retrying with backoff",
					"updateType", updateType, "retry", i+1, "backoff", backoff, "object", client.ObjectKeyFromObject(obj))
				time.Sleep(backoff)
				continue
			}
			if errors.IsConflict(err) {
				return fmt.Errorf("failed to update %s after %d retries due to conflicts", updateType, maxRetries)
			}
			return fmt.Errorf("failed to update %s: %w", updateType, err)
		}
		return nil
	}
	return fmt.Errorf("failed to update %s after %d retries", updateType, maxRetries)
}

// UpdateSpecWithConfig applies fields from config onto spec when spec
// is empty for that field. Fill-only — never overwrites operator-set
// values. Returns true iff anything changed.
func UpdateSpecWithConfig(spec *v1beta1.BaseModelSpec, config *ModelConfig) bool {
	if spec == nil || config == nil {
		return false
	}

	updated := false

	if spec.ModelType == nil && config.ModelType != "" {
		modelType := config.ModelType
		spec.ModelType = &modelType
		updated = true
	}

	if spec.ModelArchitecture == nil && config.ModelArchitecture != "" {
		architecture := config.ModelArchitecture
		spec.ModelArchitecture = &architecture
		updated = true
	}

	if spec.Quantization == nil && config.Quantization != "" {
		quant := v1beta1.ModelQuantization(config.Quantization)
		spec.Quantization = &quant
		updated = true
	}

	if spec.ModelParameterSize == nil && config.ModelParameterSize != "" {
		paramSize := config.ModelParameterSize
		spec.ModelParameterSize = &paramSize
		updated = true
	}

	if len(spec.ModelCapabilities) == 0 && len(config.ModelCapabilities) > 0 {
		spec.ModelCapabilities = make([]string, len(config.ModelCapabilities))
		copy(spec.ModelCapabilities, config.ModelCapabilities)
		updated = true
	}

	if spec.ModelFramework == nil && config.ModelFramework != nil {
		name := config.ModelFramework["name"]
		version := config.ModelFramework["version"]
		if name != "" {
			framework := &v1beta1.ModelFrameworkSpec{Name: name}
			if version != "" {
				framework.Version = &version
			}
			spec.ModelFramework = framework
			updated = true
		}
	}

	if config.ModelFormat != nil {
		name := config.ModelFormat["name"]
		version := config.ModelFormat["version"]

		if name != "" && spec.ModelFormat.Name == "" {
			spec.ModelFormat.Name = name
			updated = true
		}

		if version != "" && spec.ModelFormat.Version == nil {
			versionValue := version
			spec.ModelFormat.Version = &versionValue
			updated = true
		}
	}

	if spec.MaxTokens == nil && config.MaxTokens > 0 {
		spec.MaxTokens = &config.MaxTokens
		updated = true
	}

	return updated
}
