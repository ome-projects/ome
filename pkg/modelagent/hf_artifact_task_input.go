package modelagent

import (
	"fmt"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// newHfArtifactTaskInputForOCI plans a complete HF-origin OCI copy without I/O.
// Ineligible sources use the legacy path (false, nil). Eligible sources with
// unsafe local paths return an error instead of falling back to a download.
func newHfArtifactTaskInputForOCI(task *GopherTask, storageSpec *v1beta1.StorageSpec, modelRootDir string) (hfArtifactTaskInput, bool, error) {
	if task == nil || (task.TaskType != Download && task.TaskType != DownloadOverride) ||
		(task.BaseModel == nil) == (task.ClusterBaseModel == nil) ||
		storageSpec == nil || storageSpec.StorageUri == nil || storageSpec.DownloadPolicy == nil ||
		*storageSpec.DownloadPolicy != v1beta1.ReuseIfExists {
		return hfArtifactTaskInput{}, false, nil
	}
	if filter := task.TensorRTLLMShapeFilter; filter != nil && filter.IsTensorrtLLMModel &&
		filter.ModelType == string(constants.ServingBaseModel) {
		return hfArtifactTaskInput{}, false, nil
	}
	storageType, err := storage.GetStorageType(*storageSpec.StorageUri)
	if err != nil || storageType != storage.StorageTypeOCI {
		return hfArtifactTaskInput{}, false, nil
	}
	identity, valid := hfArtifactIdentityFromTask(task)
	if !valid {
		return hfArtifactTaskInput{}, false, nil
	}
	objectURI, err := storage.NewObjectURI(*storageSpec.StorageUri)
	if err != nil || objectURI.BucketName == "" ||
		(strings.HasPrefix(*storageSpec.StorageUri, "oci://n/") && objectURI.Namespace == "") ||
		!hfOCIArtifactPrefixMatches(objectURI.Prefix, identity) {
		return hfArtifactTaskInput{}, false, nil
	}
	if storageSpec.Path == nil {
		return hfArtifactTaskInput{}, false, fmt.Errorf("shared Hugging Face artifact child path is required")
	}
	if *storageSpec.Path == "" {
		// Preserve the legacy default download destination.
		return hfArtifactTaskInput{}, false, nil
	}
	if err := validateHfArtifactInputPath(*storageSpec.Path); err != nil {
		return hfArtifactTaskInput{}, false, fmt.Errorf("invalid shared Hugging Face artifact child path: %w", err)
	}
	childPath := filepath.Clean(*storageSpec.Path)
	for _, component := range strings.Split(childPath, string(filepath.Separator)) {
		if component == constants.ModelArtifactsDirectory {
			return hfArtifactTaskInput{}, false, fmt.Errorf("child path %s overlaps the shared artifact directory", childPath)
		}
	}
	if childPath == string(filepath.Separator) {
		return hfArtifactTaskInput{}, false, fmt.Errorf("shared Hugging Face artifact child path cannot be the filesystem root")
	}

	root := filepath.Dir(childPath)
	if modelRootDir != "" {
		if err := validateHfArtifactInputPath(modelRootDir); err != nil {
			return hfArtifactTaskInput{}, false, fmt.Errorf("invalid shared Hugging Face artifact model root: %w", err)
		}
		configuredRoot := filepath.Clean(modelRootDir)
		if hfArtifactInputPathWithin(childPath, configuredRoot) {
			return hfArtifactTaskInput{}, false, fmt.Errorf("child path %s contains model root %s", childPath, configuredRoot)
		}
		if hfArtifactInputPathWithin(configuredRoot, childPath) {
			root = configuredRoot
		} else if task.ClusterBaseModel != nil {
			return hfArtifactTaskInput{}, false, fmt.Errorf("child path %s is outside cluster model root %s", childPath, configuredRoot)
		}
	}
	parentPath := canonicalHfArtifactPath(childPath, identity)
	if task.ClusterBaseModel != nil {
		parentPath = filepath.Join(root, constants.ModelArtifactsDirectory, filepath.FromSlash(identity.ModelID), identity.CommitSHA)
	}
	if !hfArtifactInputPathWithin(root, parentPath) ||
		hfArtifactInputPathWithin(childPath, parentPath) || hfArtifactInputPathWithin(parentPath, childPath) {
		return hfArtifactTaskInput{}, false, fmt.Errorf("shared Hugging Face parent %s conflicts with child path %s or model root %s", parentPath, childPath, root)
	}
	return hfArtifactTaskInput{
		Parent: HfArtifactEntry{
			Key: hfArtifactConfigMapKey(identity), Identity: identity, LocalPath: parentPath,
		},
		ChildModelKey:  getModelID(task.BaseModel, task.ClusterBaseModel),
		ChildModelUID:  getModelResourceUID(task.BaseModel, task.ClusterBaseModel),
		ChildModelPath: childPath,
		ModelStoreRoot: root,
	}, true, nil
}

func hfOCIArtifactPrefixMatches(prefix string, identity HfArtifactIdentity) bool {
	// OCI object names are literal keys, not filesystem paths. Do not clean or
	// decode them into a different model/revision before checking eligibility.
	prefix = strings.TrimSuffix(prefix, "/")
	if strings.ContainsAny(prefix, "\\\x00") {
		return false
	}
	parts := strings.Split(prefix, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	separator := strings.LastIndexByte(prefix, '/')
	if separator < 0 || !strings.EqualFold(prefix[separator+1:], identity.CommitSHA) {
		return false
	}
	modelPrefix := prefix[:separator]
	return modelPrefix == identity.ModelID || strings.HasSuffix(modelPrefix, "/"+identity.ModelID)
}

func validateHfArtifactInputPath(path string) error {
	if !filepath.IsAbs(path) || strings.TrimSpace(path) != path || strings.ContainsRune(path, '\x00') {
		return fmt.Errorf("expected an explicit absolute path, got %q", path)
	}
	for _, component := range strings.Split(path, string(filepath.Separator)) {
		if component == ".." {
			return fmt.Errorf("path %q contains traversal", path)
		}
	}
	return nil
}

func hfArtifactInputPathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
