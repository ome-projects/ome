package modelagent

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// ArtifactResidencyAnnotation requests local artifact residency independently
// of Model CR lifetime. The requester is responsible for eviction eligibility.
const ArtifactResidencyAnnotation = constants.ModelArtifactResidencyAnnotation

func artifactEvictionRequested(task *GopherTask) bool {
	meta := taskModelMeta(task)
	return meta != nil && meta.Annotations[ArtifactResidencyAnnotation] == string(ModelStatusEvicted)
}

// A completed acknowledgement records history, not continuing path ownership.
// Anything retaining a cleanup receipt or artifact reference remains protected.
func completedArtifactEviction(entry ModelEntry) bool {
	return entry.Status == ModelStatusEvicted && entry.ModelUID != "" &&
		entry.DirectArtifactPendingEviction == nil && entry.HfArtifactPendingDeletion == nil &&
		entry.HfArtifactKey == "" && entry.Config == nil
}

var errArtifactStatusLiveValidation = errors.New("artifact status live validation failed")

// Name-based readiness belongs to the current UID. An old deletion may still
// remove its own UID-derived request key, but never a replacement's Ready key.
func (s *Gopher) ownsArtifactDeletionLabel(ctx context.Context, task *GopherTask) (bool, error) {
	return s.artifactDeletionOwner(ctx, task, false)
}

// Cross-UID bookkeeping handoff needs affirmative live ownership; label
// cleanup may also proceed when its original CR is already absent.
func (s *Gopher) artifactDeletionOwner(ctx context.Context, task *GopherTask, requireLive bool) (bool, error) {
	meta := taskModelMeta(task)
	if requireLive && (meta == nil || meta.UID == "" || s.modelClient == nil) {
		return false, fmt.Errorf("ordinary deletion handoff requires live Model identity")
	}
	if meta == nil {
		return true, ctx.Err()
	}
	if s.modelClient == nil && artifactRestorationRequested(task) {
		return false, fmt.Errorf("artifact label deletion requires a live model client")
	}
	if meta.UID == "" || s.modelClient == nil {
		return !s.isTaskModelReplaced(task), ctx.Err()
	}
	var latest metav1.Object
	var err error
	if task.BaseModel != nil {
		latest, err = s.modelClient.OmeV1beta1().BaseModels(meta.Namespace).Get(ctx, meta.Name, metav1.GetOptions{})
	} else {
		latest, err = s.modelClient.OmeV1beta1().ClusterBaseModels().Get(ctx, meta.Name, metav1.GetOptions{})
	}
	if apierrors.IsNotFound(err) {
		return !requireLive, nil
	}
	if err != nil {
		return false, err
	}
	if latest.GetUID() != meta.UID {
		return false, nil
	}
	if latest.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] != meta.Annotations[constants.ModelArtifactRehydrationIDAnnotation] {
		return false, fmt.Errorf("artifact label deletion request changed")
	}
	return true, ctx.Err()
}

func artifactRestorationRequested(task *GopherTask) bool {
	meta := taskModelMeta(task)
	return meta != nil && meta.Annotations[constants.ModelArtifactRehydrationIDAnnotation] != ""
}

// Validate actual artifact work, not only the strict spelling that grants
// deletion ownership. Live legacy Models also need empty-to-request fencing.
func (s *Gopher) requiresArtifactRequestValidation(ctx context.Context, task *GopherTask) bool {
	if artifactRestorationRequested(task) || s.hasSharedArtifactPublication(ctx, task) {
		return true
	}
	meta := taskModelMeta(task)
	if meta == nil || meta.UID == "" {
		return false
	}
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.StorageUri == nil {
		return false
	}
	source, err := storage.GetStorageType(*spec.Storage.StorageUri)
	if err != nil || source != storage.StorageTypeHuggingFace && source != storage.StorageTypeOCI && source != storage.StorageTypeLocal {
		return false
	}
	if s.modelClient != nil {
		return true
	}
	path, err := s.directArtifactOperationPath(task, source == storage.StorageTypeLocal)
	return err != nil || path != ""
}

func (s *Gopher) validateArtifactDownload(ctx context.Context, task *GopherTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := s.currentArtifactTask(ctx, task)
	if err != nil {
		return err
	}
	if current == nil {
		return fmt.Errorf("artifact task is no longer current")
	}
	if request := taskModelMeta(task).Annotations[constants.ModelArtifactRehydrationIDAnnotation]; request != "" {
		if errors := validation.IsValidLabelValue(request); len(errors) != 0 {
			return fmt.Errorf("invalid artifact rehydration label value: %v", errors)
		}
		if errors := validation.IsQualifiedName(constants.GetModelArtifactRequestLabel(taskModelMeta(task).UID)); len(errors) != 0 {
			return fmt.Errorf("invalid artifact rehydration label key: %v", errors)
		}
	}
	spec := taskModelSpec(current)
	checkPlacement := task.TaskType != Evict && spec.Storage != nil &&
		(len(spec.Storage.NodeSelector) != 0 || spec.Storage.NodeAffinity != nil)
	if s.nodeUID != "" || checkPlacement {
		if s.kubeClient == nil || s.configMapReconciler == nil || s.configMapReconciler.nodeName == "" {
			return fmt.Errorf("artifact operation requires current node eligibility")
		}
		node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if s.nodeUID != "" && node.UID != s.nodeUID {
			return fmt.Errorf("model-agent node identity changed")
		}
		if checkPlacement {
			scout := Scout{nodeInfo: node, logger: s.logger}
			if !scout.shouldDownloadModel(spec.Storage) {
				return fmt.Errorf("artifact task is no longer current")
			}
		}
	}
	return ctx.Err()
}

// Limit writer coordination to supported sources and owned paths.
func (s *Gopher) isBoundedDirectArtifactTask(task *GopherTask) bool {
	if task == nil || taskModelMeta(task) == nil {
		return false
	}
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.StorageUri == nil {
		return false
	}
	source, err := storage.GetStorageType(*spec.Storage.StorageUri)
	if err != nil || (source != storage.StorageTypeOCI && source != storage.StorageTypeHuggingFace) {
		return false
	}
	_, err = s.directArtifactPath(task)
	return err == nil
}

// Return the live task after ownership, input, policy, and intent checks, or nil
// for stale, missing, or deleting Models. Callers validate Node eligibility separately.
func (s *Gopher) currentArtifactTask(ctx context.Context, task *GopherTask) (*GopherTask, error) {
	if taskModelMeta(task) == nil || taskModelMeta(task).UID == "" {
		return nil, fmt.Errorf("artifact operation requires a model UID")
	}
	if s.modelClient == nil {
		return nil, fmt.Errorf("artifact operation requires a live model client")
	}
	latest := *task
	var err error
	if task.BaseModel != nil {
		latest.BaseModel, err = s.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(ctx, task.BaseModel.Name, metav1.GetOptions{})
	} else {
		latest.ClusterBaseModel, err = s.modelClient.OmeV1beta1().ClusterBaseModels().Get(ctx, task.ClusterBaseModel.Name, metav1.GetOptions{})
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	meta := taskModelMeta(&latest)
	if meta.UID != taskModelMeta(task).UID ||
		!reflect.DeepEqual(artifactDownloadInputs(taskModelSpec(task)), artifactDownloadInputs(taskModelSpec(&latest))) ||
		!reflect.DeepEqual(downloadAnnotations(meta.Annotations), downloadAnnotations(taskModelMeta(task).Annotations)) {
		return nil, nil
	}
	if !meta.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	if artifactRestorationRequested(task) &&
		downloadPolicyOrDefault(taskModelSpec(task).Storage) != downloadPolicyOrDefault(taskModelSpec(&latest).Storage) {
		return nil, nil
	}
	if task.TaskType == Evict {
		// Local cleanup applies even after this node loses placement eligibility.
		// Unlike an ordinary OCI refresh, eviction must not outlive a changed
		// shared-reuse policy while retaining an old cleanup snapshot.
		if downloadPolicyOrDefault(taskModelSpec(task).Storage) != downloadPolicyOrDefault(taskModelSpec(&latest).Storage) {
			return nil, nil
		}
		if !artifactEvictionRequested(&latest) {
			return nil, nil
		}
		return &latest, nil
	}
	if artifactEvictionRequested(&latest) {
		return nil, nil
	}
	return &latest, nil
}

// Scout handles placement separately and only refreshes HF downloads for
// effective policy changes. Keep in-flight task validation on those semantics.
func artifactDownloadInputs(spec v1beta1.BaseModelSpec) downloadOverrideInputs {
	inputs := downloadOverrideInputsFromSpec(spec)
	if inputs.Storage != nil {
		storageSpec := *inputs.Storage
		storageSpec.NodeSelector = nil
		storageSpec.NodeAffinity = nil
		storageSpec.DownloadPolicy = nil
		if storageSpec.StorageUri != nil {
			if source, err := storage.GetStorageType(*storageSpec.StorageUri); err == nil && source == storage.StorageTypeHuggingFace {
				policy := downloadPolicyOrDefault(inputs.Storage)
				storageSpec.DownloadPolicy = &policy
			}
		}
		inputs.Storage = &storageSpec
	}
	return inputs
}

func taskModelMeta(task *GopherTask) *metav1.ObjectMeta {
	if task == nil {
		return nil
	}
	if task.BaseModel != nil {
		return &task.BaseModel.ObjectMeta
	}
	if task.ClusterBaseModel != nil {
		return &task.ClusterBaseModel.ObjectMeta
	}
	return nil
}

func downloadAnnotations(annotations map[string]string) map[string]string {
	if len(annotations) == 0 {
		return nil
	}
	return annotations
}
