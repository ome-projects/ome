package storage

import (
	"fmt"
	"path"
	"strings"

	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

const (
	// OCIStoragePrefix is the prefix for OCI storage URIs
	OCIStoragePrefix = "oci://"
	// PVCStoragePrefix is the prefix for PVC storage URIs
	PVCStoragePrefix = "pvc://"
	// VendorStoragePrefix is the prefix for vendor storage URIs
	VendorStoragePrefix = "vendor://"
	// HuggingFaceStoragePrefix is the prefix for Hugging Face model storage URIs
	HuggingFaceStoragePrefix = "hf://"
	// S3StoragePrefix is the prefix for AWS S3 storage URIs
	S3StoragePrefix = "s3://"
	// AzureStoragePrefix is the prefix for Azure Blob storage URIs
	AzureStoragePrefix = "az://"
	// GCSStoragePrefix is the prefix for Google Cloud Storage URIs
	GCSStoragePrefix = "gs://"
	// GitHubStoragePrefix is the prefix for GitHub Releases storage URIs
	GitHubStoragePrefix = "github://"
	// LocalStoragePrefix is the prefix for local filesystem storage URIs
	LocalStoragePrefix = "local://"
	// StageStoragePrefix is the prefix for staged filesystem storage URIs.
	// A staged model is copied from an already-mounted source path onto the
	// node's local disk, unlike local:// which is served in place.
	StageStoragePrefix = "stage://"
)

// StorageType is a string enum for storage type
type StorageType string

const (
	// StorageTypePVC is the value for PVC storage
	StorageTypePVC StorageType = "PVC"
	// StorageTypeOCI is the value for OCI storage
	StorageTypeOCI StorageType = "OCI"
	// StorageTypeVendor is the value for Vendor storage
	StorageTypeVendor StorageType = "VENDOR"
	// StorageTypeHuggingFace is the value for Hugging Face model storage
	StorageTypeHuggingFace StorageType = "HUGGINGFACE"
	// StorageTypeS3 is the value for AWS S3 storage
	StorageTypeS3 StorageType = "S3"
	// StorageTypeAzure is the value for Azure Blob storage
	StorageTypeAzure StorageType = "AZURE"
	// StorageTypeGCS is the value for Google Cloud Storage
	StorageTypeGCS StorageType = "GCS"
	// StorageTypeGitHub is the value for GitHub Releases storage
	StorageTypeGitHub StorageType = "GITHUB"
	// StorageTypeLocal is the value for local filesystem storage
	StorageTypeLocal StorageType = "LOCAL"
	// StorageTypeStage is the value for staged filesystem storage
	StorageTypeStage StorageType = "STAGE"
)

// OCIStorageComponents represents the components of an OCI storage URI
type OCIStorageComponents struct {
	Namespace  string
	Bucket     string
	Prefix     string
	ObjectName string
}

// PVCStorageComponents represents the components of a PVC storage URI
type PVCStorageComponents struct {
	Namespace string // Only used for ClusterBaseModel
	PVCName   string
	SubPath   string
}

// VendorStorageComponents represents the components of a vendor storage URI
type VendorStorageComponents struct {
	VendorName   string
	ResourceType string
	ResourcePath string
}

// HuggingFaceStorageComponents represents the components of a Hugging Face model URI
type HuggingFaceStorageComponents struct {
	ModelID string
	Branch  string
}

// S3StorageComponents represents the components of an S3 storage URI
type S3StorageComponents struct {
	Bucket string
	Prefix string
	Region string // Optional region
}

// AzureStorageComponents represents the components of an Azure Blob storage URI
type AzureStorageComponents struct {
	AccountName   string
	ContainerName string
	BlobPath      string
}

// GCSStorageComponents represents the components of a Google Cloud Storage URI
type GCSStorageComponents struct {
	Bucket string
	Object string
}

// GitHubStorageComponents represents the components of a GitHub Releases storage URI
type GitHubStorageComponents struct {
	Owner      string
	Repository string
	Tag        string // Optional tag/release name
}

// LocalStorageComponents represents the components of a local filesystem storage URI
type LocalStorageComponents struct {
	Path string // Absolute or relative path to the model files
}

// StageStorageComponents represents the components of a staged storage URI
type StageStorageComponents struct {
	SourcePath string // Absolute path of the source directory, as seen by the model agent
}

// ParseOCIStorageURI parses an OCI storage URI and returns its components
// Format: oci://n/{namespace}/b/{bucket}/o/{object_path}
func ParseOCIStorageURI(uri string) (*OCIStorageComponents, error) {
	if !strings.HasPrefix(uri, OCIStoragePrefix) {
		return nil, fmt.Errorf("invalid OCI storage URI format: missing %s prefix", OCIStoragePrefix)
	}

	parts := strings.Split(strings.TrimPrefix(uri, OCIStoragePrefix), "/")
	if len(parts) < 6 || parts[0] != "n" || parts[2] != "b" || parts[4] != "o" {
		return nil, fmt.Errorf("invalid OCI storage URI format. Expected: oci://n/{namespace}/b/{bucket}/o/{object_path}")
	}

	return &OCIStorageComponents{
		Namespace: parts[1],
		Bucket:    parts[3],
		Prefix:    strings.Join(parts[5:], "/"),
	}, nil
}

// ValidateOCIStorageURI validates if the given URI matches OCI storage format
func ValidateOCIStorageURI(uri string) error {
	_, err := ParseOCIStorageURI(uri)
	return err
}

// ParsePVCStorageURI parses a PVC storage URI and returns its components
// Format: pvc://{pvc-name}/{sub-path} OR pvc://{namespace}:{pvc-name}/{sub-path}
// When namespace is not specified, it should be inferred from the BaseModel's namespace
func ParsePVCStorageURI(uri string) (*PVCStorageComponents, error) {
	if !strings.HasPrefix(uri, PVCStoragePrefix) {
		return nil, fmt.Errorf("invalid PVC storage URI format: missing %s prefix", PVCStoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, PVCStoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid PVC storage URI format: missing content after prefix")
	}

	// Check if namespace is specified with colon separator
	var namespace, pvcName, subPath string

	// First, check if we have namespace:pvc-name format
	firstSlashIdx := strings.Index(path, "/")
	if firstSlashIdx == -1 {
		return nil, fmt.Errorf("invalid PVC storage URI format: missing subpath")
	}

	firstPart := path[:firstSlashIdx]
	remainingPath := path[firstSlashIdx+1:]

	if colonIdx := strings.Index(firstPart, ":"); colonIdx != -1 {
		// Format: namespace:pvc-name/sub-path
		namespace = firstPart[:colonIdx]
		pvcName = firstPart[colonIdx+1:]

		if namespace == "" {
			return nil, fmt.Errorf("invalid PVC storage URI format: empty namespace before colon")
		}
		if pvcName == "" {
			return nil, fmt.Errorf("invalid PVC storage URI format: empty PVC name after colon")
		}

		// Check for multiple colons - not allowed
		if strings.Contains(pvcName, ":") {
			return nil, fmt.Errorf("invalid PVC storage URI format: multiple colons not allowed in namespace:pvc-name")
		}

		// Validate namespace format
		if !isValidNamespace(namespace) {
			return nil, fmt.Errorf("invalid PVC storage URI format: invalid namespace %q (must be lowercase alphanumeric with hyphens, max 63 chars)", namespace)
		}
	} else {
		// Format: pvc-name/sub-path
		pvcName = firstPart
		if pvcName == "" {
			return nil, fmt.Errorf("invalid PVC storage URI format: missing PVC name")
		}
	}

	subPath = remainingPath
	if subPath == "" {
		return nil, fmt.Errorf("invalid PVC storage URI format: missing subpath")
	}
	// Defense in depth: K8s already rejects `..` segments at admission for
	// VolumeMount.SubPath, but reject here too so we never construct a
	// pod spec that requires the API server to be the only safety net.
	for _, segment := range strings.Split(subPath, "/") {
		if segment == ".." {
			return nil, fmt.Errorf("invalid PVC storage URI format: subpath must not contain '..' segments")
		}
	}

	return &PVCStorageComponents{
		Namespace: namespace, // Empty string if not specified
		PVCName:   pvcName,
		SubPath:   subPath,
	}, nil
}

// isValidNamespace checks if a string could be a valid Kubernetes namespace
func isValidNamespace(s string) bool {
	// Basic validation - K8s namespaces must be lowercase alphanumeric or hyphens
	// This is a simplified check; actual K8s validation is more complex
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			return false
		}
	}
	// Can't start or end with hyphen
	return s[0] != '-' && s[len(s)-1] != '-'
}

// ValidatePVCStorageURI validates if the given URI matches PVC storage format
func ValidatePVCStorageURI(uri string) error {
	_, err := ParsePVCStorageURI(uri)
	return err
}

// ParseVendorStorageURI parses a vendor storage URI and returns its components
// Format: vendor://{vendor-name}/{resource-type}/{resource-path}
func ParseVendorStorageURI(uri string) (*VendorStorageComponents, error) {
	if !strings.HasPrefix(uri, VendorStoragePrefix) {
		return nil, fmt.Errorf("invalid vendor storage URI format: missing %s prefix", VendorStoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, VendorStoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid vendor storage URI format: missing vendor name")
	}

	// Split into components
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, fmt.Errorf("invalid vendor storage URI format. Expected: vendor://{vendor-name}/{resource-type}/{resource-path}")
	}

	return &VendorStorageComponents{
		VendorName:   parts[0],
		ResourceType: parts[1],
		ResourcePath: parts[2],
	}, nil
}

// ValidateVendorStorageURI validates if the given URI matches vendor storage format
func ValidateVendorStorageURI(uri string) error {
	_, err := ParseVendorStorageURI(uri)
	return err
}

// ParseHuggingFaceStorageURI parses a Hugging Face model URI and returns its components
// Format: hf://{model-id}[@{branch}]
func ParseHuggingFaceStorageURI(uri string) (*HuggingFaceStorageComponents, error) {
	if !strings.HasPrefix(uri, HuggingFaceStoragePrefix) {
		return nil, fmt.Errorf("invalid Hugging Face storage URI format: missing %s prefix", HuggingFaceStoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, HuggingFaceStoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid Hugging Face storage URI format: missing model ID")
	}

	// Split into model ID and branch
	var modelID, branch string
	if strings.Contains(path, "@") {
		parts := strings.SplitN(path, "@", 2)
		modelID = parts[0]
		branch = parts[1]
	} else {
		modelID = path
		branch = "main" // Default to 'main' branch if not specified
	}

	if modelID == "" {
		return nil, fmt.Errorf("invalid Hugging Face storage URI format: model ID cannot be empty")
	}

	return &HuggingFaceStorageComponents{
		ModelID: modelID,
		Branch:  branch,
	}, nil
}

// ValidateHuggingFaceStorageURI validates if the given URI matches Hugging Face model storage format
func ValidateHuggingFaceStorageURI(uri string) error {
	_, err := ParseHuggingFaceStorageURI(uri)
	return err
}

// ParseS3StorageURI parses an S3 storage URI and returns its components
// Format: s3://{bucket}/{prefix} or s3://{bucket}@{region}/{prefix}
func ParseS3StorageURI(uri string) (*S3StorageComponents, error) {
	if !strings.HasPrefix(uri, S3StoragePrefix) {
		return nil, fmt.Errorf("invalid S3 storage URI format: missing %s prefix", S3StoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, S3StoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid S3 storage URI format: missing bucket name")
	}

	var bucket, prefix, region string

	// Check if region is specified with @ symbol
	if strings.Contains(path, "@") {
		parts := strings.SplitN(path, "@", 2)
		bucket = parts[0]

		// Split region and prefix
		remainingParts := strings.SplitN(parts[1], "/", 2)
		region = remainingParts[0]

		if len(remainingParts) > 1 {
			prefix = remainingParts[1]
		}
	} else {
		// Simple format without region
		parts := strings.SplitN(path, "/", 2)
		bucket = parts[0]

		if len(parts) > 1 {
			prefix = parts[1]
		}
	}

	if bucket == "" {
		return nil, fmt.Errorf("invalid S3 storage URI format: bucket name cannot be empty")
	}

	return &S3StorageComponents{
		Bucket: bucket,
		Prefix: prefix,
		Region: region,
	}, nil
}

// ValidateS3StorageURI validates if the given URI matches S3 storage format
func ValidateS3StorageURI(uri string) error {
	_, err := ParseS3StorageURI(uri)
	return err
}

// ParseAzureStorageURI parses an Azure Blob storage URI and returns its components
// Format: az://{account}.blob.core.windows.net/{container}/{blob_path} or az://{account}/{container}/{blob_path}
func ParseAzureStorageURI(uri string) (*AzureStorageComponents, error) {
	if !strings.HasPrefix(uri, AzureStoragePrefix) {
		return nil, fmt.Errorf("invalid Azure storage URI format: missing %s prefix", AzureStoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, AzureStoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid Azure storage URI format: missing account name")
	}

	var accountName, containerName, blobPath string

	// Check if it's the full blob endpoint format
	if strings.Contains(path, ".blob.core.windows.net/") {
		parts := strings.SplitN(path, ".blob.core.windows.net/", 2)
		accountName = parts[0]

		if len(parts) > 1 {
			containerAndPath := strings.SplitN(parts[1], "/", 2)
			containerName = containerAndPath[0]

			if len(containerAndPath) > 1 {
				blobPath = containerAndPath[1]
			}
		}
	} else {
		// Simple format: account/container/path
		parts := strings.SplitN(path, "/", 3)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid Azure storage URI format: missing container name")
		}

		accountName = parts[0]
		containerName = parts[1]

		if len(parts) > 2 {
			blobPath = parts[2]
		}
	}

	if accountName == "" || containerName == "" {
		return nil, fmt.Errorf("invalid Azure storage URI format: account name and container name are required")
	}

	return &AzureStorageComponents{
		AccountName:   accountName,
		ContainerName: containerName,
		BlobPath:      blobPath,
	}, nil
}

// ValidateAzureStorageURI validates if the given URI matches Azure storage format
func ValidateAzureStorageURI(uri string) error {
	_, err := ParseAzureStorageURI(uri)
	return err
}

// ParseGCSStorageURI parses a Google Cloud Storage URI and returns its components
// Format: gs://{bucket}/{object_path}
func ParseGCSStorageURI(uri string) (*GCSStorageComponents, error) {
	if !strings.HasPrefix(uri, GCSStoragePrefix) {
		return nil, fmt.Errorf("invalid GCS storage URI format: missing %s prefix", GCSStoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, GCSStoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid GCS storage URI format: missing bucket name")
	}

	// Split into bucket and object path
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]

	var object string
	if len(parts) > 1 {
		object = parts[1]
	}

	if bucket == "" {
		return nil, fmt.Errorf("invalid GCS storage URI format: bucket name cannot be empty")
	}

	return &GCSStorageComponents{
		Bucket: bucket,
		Object: object,
	}, nil
}

// ValidateGCSStorageURI validates if the given URI matches GCS storage format
func ValidateGCSStorageURI(uri string) error {
	_, err := ParseGCSStorageURI(uri)
	return err
}

// ParseGitHubStorageURI parses a GitHub Releases storage URI and returns its components
// Format: github://{owner}/{repository}[@{tag}]
func ParseGitHubStorageURI(uri string) (*GitHubStorageComponents, error) {
	if !strings.HasPrefix(uri, GitHubStoragePrefix) {
		return nil, fmt.Errorf("invalid GitHub storage URI format: missing %s prefix", GitHubStoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, GitHubStoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid GitHub storage URI format: missing owner/repository")
	}

	var owner, repository, tag string

	// Check if tag is specified
	if strings.Contains(path, "@") {
		parts := strings.SplitN(path, "@", 2)
		ownerRepo := parts[0]
		tag = parts[1]

		repoParts := strings.SplitN(ownerRepo, "/", 2)
		if len(repoParts) != 2 {
			return nil, fmt.Errorf("invalid GitHub storage URI format: expected owner/repository")
		}
		owner = repoParts[0]
		repository = repoParts[1]
	} else {
		// No tag specified
		parts := strings.SplitN(path, "/", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid GitHub storage URI format: expected owner/repository")
		}
		owner = parts[0]
		repository = parts[1]
		tag = "latest" // Default to latest release
	}

	if owner == "" || repository == "" {
		return nil, fmt.Errorf("invalid GitHub storage URI format: owner and repository are required")
	}

	return &GitHubStorageComponents{
		Owner:      owner,
		Repository: repository,
		Tag:        tag,
	}, nil
}

// ValidateGitHubStorageURI validates if the given URI matches GitHub storage format
func ValidateGitHubStorageURI(uri string) error {
	_, err := ParseGitHubStorageURI(uri)
	return err
}

// ParseLocalStorageURI parses a local filesystem storage URI and returns its components
// Format: local://{absolute-path} or local://./{relative-path}
func ParseLocalStorageURI(uri string) (*LocalStorageComponents, error) {
	if !strings.HasPrefix(uri, LocalStoragePrefix) {
		return nil, fmt.Errorf("invalid local storage URI format: missing %s prefix", LocalStoragePrefix)
	}

	// Remove prefix
	path := strings.TrimPrefix(uri, LocalStoragePrefix)
	if path == "" {
		return nil, fmt.Errorf("invalid local storage URI format: missing path")
	}

	// The path should be an absolute path or a relative path
	// We don't validate if the path exists here, that's done at runtime
	return &LocalStorageComponents{
		Path: path,
	}, nil
}

// ValidateLocalStorageURI validates if the given URI matches local storage format
func ValidateLocalStorageURI(uri string) error {
	_, err := ParseLocalStorageURI(uri)
	return err
}

// ParseStageStorageURI parses a staged storage URI and returns its components
// Format: stage:///{absolute-source-path}
//
// The source path is a path already mounted into the model agent, not a network
// address: mounting the underlying share (NFS, CephFS, ...) is the node's job.
func ParseStageStorageURI(uri string) (*StageStorageComponents, error) {
	if !strings.HasPrefix(uri, StageStoragePrefix) {
		return nil, fmt.Errorf("invalid stage storage URI format: missing %s prefix", StageStoragePrefix)
	}

	sourcePath := strings.TrimPrefix(uri, StageStoragePrefix)
	if sourcePath == "" {
		return nil, fmt.Errorf("invalid stage storage URI format: missing source path")
	}

	// Absolute paths only: the same string has to be meaningful both inside the
	// agent container and on the node, so relative and ~-based paths are out.
	if !strings.HasPrefix(sourcePath, "/") {
		return nil, fmt.Errorf("invalid stage storage URI format: source path %q must be an absolute path", sourcePath)
	}

	// Reject traversal before cleaning, otherwise Clean would silently resolve it
	// and a URI could escape the configured source roots.
	for _, segment := range strings.Split(sourcePath, "/") {
		if segment == ".." {
			return nil, fmt.Errorf("invalid stage storage URI format: source path %q must not contain \"..\" segments", sourcePath)
		}
	}

	return &StageStorageComponents{
		SourcePath: path.Clean(sourcePath),
	}, nil
}

// ValidateStageStorageURI validates if the given URI matches staged storage format
func ValidateStageStorageURI(uri string) error {
	_, err := ParseStageStorageURI(uri)
	return err
}

// GetStorageType determines the type of storage URI
func GetStorageType(uri string) (StorageType, error) {
	switch {
	case strings.HasPrefix(uri, OCIStoragePrefix):
		return StorageTypeOCI, nil
	case strings.HasPrefix(uri, PVCStoragePrefix):
		return StorageTypePVC, nil
	case strings.HasPrefix(uri, VendorStoragePrefix):
		return StorageTypeVendor, nil
	case strings.HasPrefix(uri, HuggingFaceStoragePrefix):
		return StorageTypeHuggingFace, nil
	case strings.HasPrefix(uri, S3StoragePrefix):
		return StorageTypeS3, nil
	case strings.HasPrefix(uri, AzureStoragePrefix):
		return StorageTypeAzure, nil
	case strings.HasPrefix(uri, GCSStoragePrefix):
		return StorageTypeGCS, nil
	case strings.HasPrefix(uri, GitHubStoragePrefix):
		return StorageTypeGitHub, nil
	case strings.HasPrefix(uri, LocalStoragePrefix):
		return StorageTypeLocal, nil
	case strings.HasPrefix(uri, StageStoragePrefix):
		return StorageTypeStage, nil
	default:
		return "", fmt.Errorf("unknown storage type for URI: %s", uri)
	}
}

// ValidateStorageURI validates a storage URI based on its type
func ValidateStorageURI(uri string) error {
	storageType, err := GetStorageType(uri)
	if err != nil {
		return err
	}

	switch storageType {
	case StorageTypeOCI:
		return ValidateOCIStorageURI(uri)
	case StorageTypePVC:
		return ValidatePVCStorageURI(uri)
	case StorageTypeVendor:
		return ValidateVendorStorageURI(uri)
	case StorageTypeHuggingFace:
		return ValidateHuggingFaceStorageURI(uri)
	case StorageTypeS3:
		return ValidateS3StorageURI(uri)
	case StorageTypeAzure:
		return ValidateAzureStorageURI(uri)
	case StorageTypeGCS:
		return ValidateGCSStorageURI(uri)
	case StorageTypeGitHub:
		return ValidateGitHubStorageURI(uri)
	case StorageTypeLocal:
		return ValidateLocalStorageURI(uri)
	case StorageTypeStage:
		return ValidateStageStorageURI(uri)
	default:
		return fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// NewObjectURI creates a new ObjectURI from a storage URI string
// Example URI formats:
// - oci://namespace@region/bucket/prefix
// - oci://n/namespace/b/bucket/o/prefix
// - hf://model-id[@branch]
// NewObjectURI creates a new ObjectURI from a storage URI string
func NewObjectURI(uriStr string) (*ociobjectstore.ObjectURI, error) {
	storageType, err := GetStorageType(uriStr)
	if err != nil {
		return nil, err
	}

	switch storageType {
	case StorageTypeOCI:
		return parseOCIObjectURI(uriStr)
	case StorageTypeHuggingFace:
		return parseHuggingFaceObjectURI(uriStr)
	case StorageTypePVC:
		return parsePVCStorageURI(uriStr)
	case StorageTypeLocal:
		return parseLocalStorageObjectURI(uriStr)
	default:
		return nil, fmt.Errorf("unsupported storage type for object URI: %s", storageType)
	}
}

func parsePVCStorageURI(uriStr string) (*ociobjectstore.ObjectURI, error) {
	pvcComponents, err := ParsePVCStorageURI(uriStr)
	if err != nil {
		return nil, err
	}

	// For PVCs:
	// - Use Namespace field to store the namespace (if specified)
	// - Use BucketName field to store the PVC name
	// - Use Prefix field to store the sub-path
	return &ociobjectstore.ObjectURI{
		Namespace:  pvcComponents.Namespace,
		BucketName: pvcComponents.PVCName,
		Prefix:     pvcComponents.SubPath,
	}, nil
}

// parseLocalStorageObjectURI parses a local storage URI into an ObjectURI
func parseLocalStorageObjectURI(uriStr string) (*ociobjectstore.ObjectURI, error) {
	localComponents, err := ParseLocalStorageURI(uriStr)
	if err != nil {
		return nil, err
	}

	// For local storage:
	// - Use Namespace field to identify this as local storage
	// - Use Prefix field to store the path
	// - BucketName is not used
	return &ociobjectstore.ObjectURI{
		Namespace: "local", // Identifies this as local storage
		Prefix:    localComponents.Path,
	}, nil
}

// parseHuggingFaceObjectURI parses a Hugging Face URI into an ObjectURI
func parseHuggingFaceObjectURI(uriStr string) (*ociobjectstore.ObjectURI, error) {
	hfComponents, err := ParseHuggingFaceStorageURI(uriStr)
	if err != nil {
		return nil, err
	}

	// For Hugging Face models:
	// - Use BucketName field to store the model ID
	// - Use Prefix field to store the branch
	// - Use Namespace to identify this as a Hugging Face resource
	return &ociobjectstore.ObjectURI{
		Namespace:  "huggingface", // Identifies this as a Hugging Face model
		BucketName: hfComponents.ModelID,
		Prefix:     hfComponents.Branch,
	}, nil
}

// parseOCIObjectURI parses an OCI URI string into an ObjectURI
func parseOCIObjectURI(uriStr string) (*ociobjectstore.ObjectURI, error) {
	if !strings.HasPrefix(uriStr, OCIStoragePrefix) {
		return nil, fmt.Errorf("URI must start with '%s'", OCIStoragePrefix)
	}

	// Remove the scheme
	uriStr = strings.TrimPrefix(uriStr, "oci://")

	// Check for the OCI specific format: n/namespace/b/bucket/o/prefix
	if strings.HasPrefix(uriStr, "n/") {
		// Format: n/namespace/b/bucket/o/prefix
		parts := strings.Split(uriStr, "/")
		if len(parts) < 5 || parts[0] != "n" || parts[2] != "b" {
			return nil, fmt.Errorf("invalid OCI URI format, expected 'oci://n/namespace/b/bucket/o/prefix': %s", uriStr)
		}

		namespace := parts[1]
		bucketName := parts[3]

		// Validate the marker for the object prefix
		if len(parts) > 4 && parts[4] != "o" {
			return nil, fmt.Errorf("invalid OCI URI format, expected 'oci://n/namespace/b/bucket/o/prefix': %s", uriStr)
		}

		// Extract object prefix (everything after "o/")
		var prefix string
		if len(parts) > 5 && parts[4] == "o" {
			prefix = strings.Join(parts[5:], "/")
		}

		return &ociobjectstore.ObjectURI{
			Namespace:  namespace,
			BucketName: bucketName,
			Prefix:     prefix,
		}, nil
	}

	// Handle different standard formats (from previous fix)
	var namespace, region, bucketName, prefix string

	// Check if we have a namespace@region format
	if strings.Contains(uriStr, "@") {
		parts := strings.SplitN(uriStr, "@", 2)
		namespace = parts[0]

		// Split the rest into region and bucket/prefix
		remainingParts := strings.SplitN(parts[1], "/", 2)
		region = remainingParts[0]

		if len(remainingParts) > 1 {
			// Split remaining into bucket and prefix
			bucketPrefixParts := strings.SplitN(remainingParts[1], "/", 2)
			bucketName = bucketPrefixParts[0]

			if len(bucketPrefixParts) > 1 {
				prefix = bucketPrefixParts[1]
			}
		}
	} else {
		// Handle simpler oci://bucket/prefix format
		parts := strings.SplitN(uriStr, "/", 2)
		bucketName = parts[0]

		if len(parts) > 1 {
			prefix = parts[1]
		}
	}

	// Ensure we have at least a bucket name
	if bucketName == "" {
		return nil, fmt.Errorf("invalid URI format, missing bucket name: %s", uriStr)
	}

	return &ociobjectstore.ObjectURI{
		Namespace:  namespace,
		BucketName: bucketName,
		Prefix:     prefix,
		Region:     region,
	}, nil
}
