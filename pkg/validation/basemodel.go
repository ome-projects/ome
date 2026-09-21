package validation

import (
	"fmt"
	"path"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// ValidatePVCStorage enforces the PVC URI shape rules:
//
//   - namespaced BaseModel: pvc:// URIs must NOT carry a namespace prefix
//   - ClusterBaseModel: pvc:// URIs MUST carry a namespace prefix
//   - the URI itself must parse
//   - PVC + distribution=Sharded is rejected
//
// Non-PVC URIs are accepted unchanged.
func ValidatePVCStorage(spec *v1beta1.BaseModelSpec, isClusterScoped bool) error {
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return nil
	}
	uri := *spec.Storage.StorageUri
	storageType, err := storage.GetStorageType(uri)
	if err != nil || storageType != storage.StorageTypePVC {
		return nil
	}

	if spec.Distribution != nil && *spec.Distribution == v1beta1.DistributionSharded {
		return fmt.Errorf("PVC storage URIs are not compatible with distribution=Sharded; use distribution=PerNode (or omit) for pvc:// models")
	}

	components, err := storage.ParsePVCStorageURI(uri)
	if err != nil {
		return fmt.Errorf("invalid PVC storage URI %q: %w", uri, err)
	}
	if isClusterScoped && components.Namespace == "" {
		return fmt.Errorf("ClusterBaseModel PVC URI must specify a namespace (format: pvc://{namespace}:{pvc-name}/{sub-path}), got %q", uri)
	}
	if !isClusterScoped && components.Namespace != "" {
		return fmt.Errorf("namespaced BaseModel PVC URI must not specify a namespace; the BaseModel's own namespace is used, got %q", uri)
	}
	return nil
}

// ValidateStageStorage enforces the stage:// rules for a BaseModel /
// ClusterBaseModel spec:
//
//   - the source URI must parse (absolute path, no ".." segments);
//   - spec.storage.path is required and must be absolute — it names the
//     node-local destination the model is copied to, and there is no sane
//     default for it;
//   - the destination must not sit inside the source, which would make the
//     agent copy the tree into itself.
//
// Rejecting here keeps these mistakes from surfacing only on the node, where a
// model just sits in In_Transit with the reason buried in agent logs.
//
// Non-stage URIs are accepted unchanged.
func ValidateStageStorage(spec *v1beta1.BaseModelSpec) error {
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return nil
	}
	uri := *spec.Storage.StorageUri
	storageType, err := storage.GetStorageType(uri)
	if err != nil || storageType != storage.StorageTypeStage {
		return nil
	}

	components, err := storage.ParseStageStorageURI(uri)
	if err != nil {
		return fmt.Errorf("invalid stage storage URI %q: %w", uri, err)
	}

	if spec.Storage.Path == nil || *spec.Storage.Path == "" {
		return fmt.Errorf("stage:// storage requires spec.storage.path to name the node-local destination")
	}
	destPath := path.Clean(*spec.Storage.Path)
	if !strings.HasPrefix(destPath, "/") {
		return fmt.Errorf("stage:// spec.storage.path must be an absolute path, got %q", *spec.Storage.Path)
	}

	if destPath == components.SourcePath ||
		strings.HasPrefix(destPath, strings.TrimSuffix(components.SourcePath, "/")+"/") {
		return fmt.Errorf("stage:// spec.storage.path %q must not be inside the source %q", destPath, components.SourcePath)
	}

	return nil
}
