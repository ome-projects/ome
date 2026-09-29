package modelagent

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/utils"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

func isDirectOCIRestoreEligible(task *GopherTask) bool {
	spec := taskModelSpec(task).Storage
	if spec == nil || spec.StorageUri == nil || spec.Path == nil || *spec.Path == "" {
		return false
	}
	kind, err := storage.GetStorageType(*spec.StorageUri)
	return err == nil && kind == storage.StorageTypeOCI
}

// Live recovery does not pass through Scout. Reconstruct exactly its OCI
// serving-model subset; an absent shape cannot select every GPU's files.
func ociRestoreShapeFilter(task *GopherTask, node *corev1.Node) (*TensorRTLLMShapeFilter, error) {
	spec := taskModelSpec(task)
	modelType, present := spec.AdditionalMetadata["type"]
	if !present {
		modelType = string(constants.ServingBaseModel)
	}
	if spec.ModelFormat.Name != constants.TensorRTLLM || modelType != string(constants.ServingBaseModel) {
		return nil, nil
	}
	instanceType, present := node.Labels[constants.NodeInstanceShapeLabel]
	if !present {
		instanceType = node.Labels[constants.DeprecatedNodeInstanceShapeLabel]
	}
	alias, err := utils.GetInstanceTypeShortName(instanceType)
	if err != nil {
		return nil, err
	}
	if alias == "" {
		return nil, fmt.Errorf("OCI restoration requires a node shape for TensorRT-LLM")
	}
	return &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ShapeAlias: alias, ModelType: modelType}, nil
}

// Inspect the whole selected listing before removing anything: a later metadata
// failure must preserve even an earlier file known to be invalid.
func (s *Gopher) inspectDirectOCI(ctx context.Context, task *GopherTask, path string, objects []ociobjectstore.ObjectURI, store ociModelStore) (bool, error) {
	if len(objects) == 0 {
		return false, fmt.Errorf("OCI restoration requires a nonempty object listing")
	}
	var invalid []ociobjectstore.ObjectURI
	for _, object := range objects {
		localPath, err := hfArtifactObjectPath(path, object.Prefix, object.ObjectName)
		if err != nil {
			return false, err
		}
		if !s.modelVerificationLimiter.acquire(ctx) {
			return false, ctx.Err()
		}
		valid, err := store.IsLocalCopyValid(object, localPath)
		s.modelVerificationLimiter.release()
		if err != nil {
			return false, err
		}
		if !valid {
			invalid = append(invalid, object)
		}
	}
	if len(invalid) == 0 {
		return true, nil
	}
	if err := s.prepareDirectRestore(ctx, task, path, true); err != nil {
		return false, err
	}
	for _, object := range invalid {
		localPath, err := hfArtifactObjectPath(path, object.Prefix, object.ObjectName)
		if err != nil {
			return false, err
		}
		if err := validateArtifactRestore(ctx); err != nil {
			return false, err
		}
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, validateArtifactRestore(ctx)
}
