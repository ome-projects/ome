package pvc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// pvcRequeueAfter is the timer backstop for PVC-backed reconciles. The
// primary triggers are the PVC, Job, and ConfigMap watches in
// SetupWithManager; this requeue exists to re-check progress in case a
// watch event was missed.
//
// Kept short (5s) so an operator looking at "why is my model still
// Importing?" sees a Failed terminal state within a few seconds when
// the underlying Job hits BackoffLimit, not after a 30s pause. Cost:
// idle PVC-backed BaseModels reconcile a few times more frequently;
// each reconcile is one apiserver Get for the Job and one for the
// per-PVC ConfigMap, both cache-backed.
const pvcRequeueAfter = 5 * time.Second

// isPVCStorage reports whether the given BaseModel/ClusterBaseModel spec
// references a PVC-backed storage URI.
func IsPVCStorage(spec *v1beta1.BaseModelSpec) bool {
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return false
	}
	storageType, err := storage.GetStorageType(*spec.Storage.StorageUri)
	if err != nil {
		return false
	}
	return storageType == storage.StorageTypePVC
}

// resolvePVCNamespace returns the namespace the metadata-extraction Job and
// the InferenceService pod must run in to access the PVC.
//
// For namespaced BaseModel: the URI must NOT carry a namespace prefix
// (`pvc://name/sub-path`) and the model's own namespace is used.
//
// For ClusterBaseModel: the URI MUST carry a namespace prefix
// (`pvc://ns:name/sub-path`); that namespace is returned.
func resolvePVCNamespace(modelNamespace string, isClusterScoped bool, components *storage.PVCStorageComponents) (string, error) {
	if components == nil {
		return "", fmt.Errorf("PVC storage components are nil")
	}

	if isClusterScoped {
		if components.Namespace == "" {
			return "", fmt.Errorf("ClusterBaseModel PVC URI must specify a namespace (format: pvc://{namespace}:{pvc-name}/{sub-path})")
		}
		return components.Namespace, nil
	}

	if components.Namespace != "" {
		return "", fmt.Errorf("namespaced BaseModel PVC URI must not specify a namespace; the BaseModel's own namespace is used")
	}
	return modelNamespace, nil
}

// reconcilePVCStorage handles the PVC-specific reconcile flow for a BaseModel
// or ClusterBaseModel:
//
//  1. Parse the URI, resolve the PVC namespace, validate the PVC exists
//     and is Bound (pre-Job).
//  2. Ensure the metadata-extraction Job exists.
//  3. Read the per-PVC status ConfigMap (one ConfigMap per PVC model,
//     looked up by deterministic name — there is no node concept here)
//     and apply its ModelEntry to the BaseModel spec + LifeCycleState.
//
// PVC-backed models do NOT participate in the per-node ConfigMap flow:
// Status.NodesReady stays empty, processModelStatus is never invoked,
// and we never list ConfigMaps to find this model.
//
// State transitions written by this branch:
//
//	URI invalid / ns rules violated   → state=Failed,    SourceReachable=False (terminal)
//	PVC not found                     → state=Failed,    SourceReachable=False
//	PVC pending                       → state=In_Transit,SourceReachable=True  (PVCNotBound)
//	Image config missing              → state=Failed,    SourceReachable=False (terminal)
//	Job in flight, ConfigMap absent   → state=In_Transit,SourceReachable=True  (PVCMetadataExtracting)
//	ConfigMap reports Status=Ready    → state=Ready,     SourceReachable=True + Ready=True
//	ConfigMap reports Status=Failed   → state=Failed,    Ready=False (PVCMetadataExtractionFailed)
func Reconcile(ctx context.Context, kubeClient client.Client, scheme *runtime.Scheme, log logr.Logger, obj client.Object, spec *v1beta1.BaseModelSpec, isClusterScoped bool, cfg MetadataJobConfig) (ctrl.Result, error) {
	uri := *spec.Storage.StorageUri

	components, err := storage.ParsePVCStorageURI(uri)
	if err != nil {
		// Terminal: only a CR edit can fix a malformed URI; CR update
		// re-enqueues the reconcile, no need for a timer requeue.
		return setPVCFailedTerminal(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCInvalid,
			fmt.Sprintf("invalid PVC storage URI %q: %v", uri, err))
	}

	pvcNamespace, err := resolvePVCNamespace(obj.GetNamespace(), isClusterScoped, components)
	if err != nil {
		return setPVCFailedTerminal(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCInvalid,
			err.Error())
	}

	pvc := &corev1.PersistentVolumeClaim{}
	pvcKey := types.NamespacedName{Namespace: pvcNamespace, Name: components.PVCName}
	if err := kubeClient.Get(ctx, pvcKey, pvc); err != nil {
		if errors.IsNotFound(err) {
			return setPVCFailedStatus(ctx, kubeClient, log, obj,
				v1beta1.ModelConditionReasonPVCNotFound,
				fmt.Sprintf("PVC %s/%s not found", pvcNamespace, components.PVCName))
		}
		log.Error(err, "Failed to get PVC", "pvc", pvcKey)
		return ctrl.Result{}, err
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return setPVCInTransitStatus(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCNotBound,
			fmt.Sprintf("PVC %s/%s is not Bound (phase: %s)", pvcNamespace, components.PVCName, pvc.Status.Phase))
	}

	if cfg.Image == "" {
		// Terminal: needs a controller restart with a fixed config to
		// recover; the BaseModel CR itself can't change anything. The
		// configmap watch will re-enqueue once the operator updates it.
		return setPVCFailedTerminal(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCConfigMissing,
			"ome-agent image is not configured (set the omeAgent block in inferenceservice-config)")
	}

	job, result, err := ensureMetadataJob(ctx, kubeClient, scheme, log, obj, isClusterScoped, components, pvcNamespace, cfg)
	if err != nil {
		return result, err
	}
	if job == nil {
		// Job was just created (or build failed and a status was already
		// written). Either way, defer to the next reconcile.
		return result, nil
	}

	// The Job is the only way the per-PVC status ConfigMap gets written;
	// once we've ensured it exists we apply whatever the agent has produced
	// so far. If the ConfigMap doesn't exist yet (Job still running), we
	// stay at the InTransit set by ensureMetadataJob.
	return applyPVCStatusFromConfigMap(ctx, kubeClient, log, obj, isClusterScoped, job)
}

// ensureMetadataJob makes sure the metadata-extraction Job exists and
// returns the live Job (after Get, on the existing-Job path) so the
// caller can inspect its status. Returns (nil, result, nil) on the
// just-created path because the freshly-created Job has no Status yet
// and reading it would race the API server's status subresource init.
//
// State writes inside this function:
//
//	build error            → terminal Failed (PVCConfigMissing)
//	Job just created       → InTransit (PVCMetadataExtracting)
//	otherwise              → no state write — caller drives state from
//	                         the ConfigMap and/or the returned Job.
func ensureMetadataJob(ctx context.Context, kubeClient client.Client, scheme *runtime.Scheme, log logr.Logger, obj client.Object, isClusterScoped bool, components *storage.PVCStorageComponents, pvcNamespace string, cfg MetadataJobConfig) (*batchv1.Job, ctrl.Result, error) {
	want, err := buildMetadataJob(obj, isClusterScoped, components, pvcNamespace, cfg, scheme)
	if err != nil {
		result, statusErr := setPVCFailedTerminal(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCConfigMissing,
			fmt.Sprintf("failed to build metadata extraction Job: %v", err))
		return nil, result, statusErr
	}

	got := &batchv1.Job{}
	jobKey := types.NamespacedName{Namespace: want.Namespace, Name: want.Name}
	switch err := kubeClient.Get(ctx, jobKey, got); {
	case errors.IsNotFound(err):
		// Make sure the SA + cross-ns RoleBinding exist BEFORE we
		// submit the Job. Without this, Jobs in non-OME namespaces
		// sit at 0/1 ready forever ("serviceaccount not found") and
		// the BaseModel never leaves InTransit. Idempotent for the
		// in-OME-namespace case.
		if err := ensureMetadataJobRBAC(ctx, kubeClient, log, want.Namespace, cfg.ServiceAccount, cfg.ServiceAccount); err != nil {
			log.Error(err, "Failed to ensure metadata Job RBAC", "namespace", want.Namespace)
			return nil, ctrl.Result{}, err
		}
		if err := kubeClient.Create(ctx, want); err != nil && !errors.IsAlreadyExists(err) {
			log.Error(err, "Failed to create metadata extraction Job", "job", jobKey)
			return nil, ctrl.Result{}, err
		}
		log.Info("Created metadata extraction Job", "job", jobKey)
		// Job just created; assert InTransit and let the next reconcile
		// (triggered by the Job watch) read the ConfigMap.
		result, statusErr := setPVCInTransitStatus(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCMetadataExtracting,
			fmt.Sprintf("metadata extraction Job %s/%s created", jobKey.Namespace, jobKey.Name))
		return nil, result, statusErr
	case err != nil:
		log.Error(err, "Failed to get metadata extraction Job", "job", jobKey)
		return nil, ctrl.Result{}, err
	}

	return got, ctrl.Result{}, nil
}

// jobFailedConditionMessage returns (true, message) if the Job has
// reached its terminal Failed state (BackoffLimit exhausted or
// DeadlineExceeded). The message is the failure reason kubelet/job
// controller surfaced — useful for operator debugging.
//
// Detects ONLY the terminal Failed condition, not transient pod failures
// that the BackoffLimit will retry. For metadata Jobs the failure
// message is typically "Job has reached the specified backoff limit"
// followed by the agent pod's exit reason.
func jobFailedConditionMessage(job *batchv1.Job) (bool, string) {
	if job == nil {
		return false, ""
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			msg := cond.Message
			if cond.Reason != "" {
				msg = cond.Reason + ": " + msg
			}
			if msg == "" {
				msg = "metadata extraction Job exhausted its retry budget"
			}
			return true, msg
		}
	}
	return false, ""
}

// applyPVCStatusFromConfigMap reads the per-PVC status ConfigMap by exact
// name (not via the per-node List path), parses the ModelEntry the agent
// wrote, and updates the BaseModel spec + LifeCycleState accordingly.
//
// `job` is the live metadata Job (may be nil on the just-created path).
// We use it to detect the BackoffLimit-exhausted case where the agent
// pod was OOM-killed or evicted before its SIGTERM handler could write a
// Failed entry. Without this check the BaseModel would sit at InTransit
// indefinitely waiting on a ConfigMap that will never appear.
//
// Behavior:
//
//	ConfigMap absent + Job Failed → state=Failed (PVCMetadataExtractionFailed)
//	                                with the Job's failure message.
//	ConfigMap absent + Job running → leave state alone (already InTransit
//	                                per ensureMetadataJob).
//	ModelEntry.Status=Ready       → state=Ready, SourceReachable+Ready=True,
//	                                update spec from cfg.
//	ModelEntry.Status=Failed      → state=Failed, Ready=False with reason
//	                                PVCMetadataExtractionFailed and the
//	                                agent's error message.
//	ModelEntry.Status=anything    → state=In_Transit (extraction in progress).
//	bad ConfigMap shape           → log + In_Transit (don't crash on bad data).
//
// Status.NodesReady is intentionally never set for PVC — there is no node.
func applyPVCStatusFromConfigMap(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, isClusterScoped bool, job *batchv1.Job) (ctrl.Result, error) {
	cmName := constants.GetPVCMetadataConfigMapName(obj.GetName(), obj.GetNamespace(), isClusterScoped)
	cm := &corev1.ConfigMap{}
	switch err := kubeClient.Get(ctx, types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm); {
	case errors.IsNotFound(err):
		// No ConfigMap yet. If the Job has already given up
		// (BackoffLimit exhausted, DeadlineExceeded, etc.) the agent
		// will never write one — surface a Failed state instead of
		// looping at InTransit forever.
		if failed, msg := jobFailedConditionMessage(job); failed {
			log.Info("Metadata Job reached terminal Failed without writing ConfigMap; surfacing extraction failure",
				"job", job.Name, "namespace", job.Namespace, "message", msg)
			return setPVCExtractionFailedStatus(ctx, kubeClient, log, obj, msg)
		}
		// Watches handle re-enqueue: the agent's ConfigMap-create event
		// fires the PVC-status ConfigMap predicate; the Job's status-change
		// event fires Owns(&Job{}). Timer requeue is a backstop only.
		return ctrl.Result{RequeueAfter: pvcRequeueAfter}, nil
	case err != nil:
		log.Error(err, "Failed to get PVC metadata ConfigMap", "name", cmName)
		return ctrl.Result{}, err
	}

	modelKey := constants.GetModelConfigMapKey(obj.GetNamespace(), obj.GetName(), isClusterScoped)
	raw, ok := cm.Data[modelKey]
	if !ok {
		log.Info("PVC metadata ConfigMap missing model entry; staying In_Transit", "configMap", cmName, "key", modelKey)
		return setPVCInTransitStatus(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCMetadataExtracting,
			fmt.Sprintf("PVC metadata ConfigMap %s lacks entry %q yet", cmName, modelKey))
	}
	var entry shared.ModelEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		log.Error(err, "Failed to parse PVC ConfigMap model entry", "configMap", cmName, "key", modelKey)
		return setPVCInTransitStatus(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCMetadataExtracting,
			fmt.Sprintf("PVC metadata ConfigMap %s entry is not valid JSON", cmName))
	}

	switch entry.Status {
	case shared.ModelStatusReady:
		// Apply spec from the agent's parsed metadata. Re-read the latest
		// CR to avoid clobbering concurrent edits.
		if entry.Config != nil {
			if err := applyPVCSpecUpdate(ctx, kubeClient, log, obj, entry.Config); err != nil {
				return ctrl.Result{}, err
			}
		}
		return setPVCReadyStatus(ctx, kubeClient, log, obj,
			fmt.Sprintf("PVC metadata extraction succeeded; ConfigMap %s applied", cmName))
	case shared.ModelStatusFailed:
		msg := cm.Annotations[constants.PVCMetadataLastErrorAnnotation]
		if msg == "" {
			msg = "agent reported Failed without an error message"
		}
		return setPVCExtractionFailedStatus(ctx, kubeClient, log, obj, msg)
	default:
		return setPVCInTransitStatus(ctx, kubeClient, log, obj,
			v1beta1.ModelConditionReasonPVCMetadataExtracting,
			fmt.Sprintf("PVC metadata extraction in progress (ConfigMap status=%q)", entry.Status))
	}
}

// applyPVCSpecUpdate applies the agent's ModelConfig to the model spec,
// reading the latest CR via retry-on-conflict to avoid clobbering writes.
func applyPVCSpecUpdate(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, cfg *shared.ModelConfig) error {
	return shared.RetryUpdate(ctx, kubeClient, log, obj, "pvc-spec", func(ctx context.Context, c client.Client, latest client.Object) error {
		spec, _, err := shared.ModelSpecAndStatus(latest)
		if err != nil {
			return err
		}
		if !shared.UpdateSpecWithConfig(spec, cfg) {
			return nil
		}
		return c.Update(ctx, latest)
	})
}

// HandleModelDeletion is the deletion path for PVC-backed BaseModel and
// ClusterBaseModel. A PVC model has no agent to mark Status=Deleted across
// N nodes, so the controller removes its artifacts itself: the Job first, so
// the agent stops re-creating the ConfigMap, then the ConfigMap, then the
// finalizer.
func HandleModelDeletion(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, isClusterScoped bool, finalizer string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(obj, finalizer) {
		return ctrl.Result{}, nil
	}

	if err := deleteMetadataArtifacts(ctx, kubeClient, log, obj, isClusterScoped); err != nil {
		log.Error(err, "Failed to delete PVC metadata artifacts", "model", obj.GetName())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Remove the finalizer. Re-fetch to avoid clobbering concurrent edits.
	if err := shared.RetryUpdate(ctx, kubeClient, log, obj, "pvc-finalizer-remove", func(ctx context.Context, c client.Client, latest client.Object) error {
		controllerutil.RemoveFinalizer(latest, finalizer)
		return c.Update(ctx, latest)
	}); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("PVC-backed model deletion complete", "name", obj.GetName(), "namespace", obj.GetNamespace())
	return ctrl.Result{}, nil
}

// CleanupStaleArtifacts deletes any per-PVC ConfigMap, any
// metadata extraction Jobs, and strips PVC-specific stale conditions
// from a BaseModel/ClusterBaseModel that is no longer PVC-backed.
//
// Called when a model whose previous spec was pvc:// has its
// storage URI changed to a non-PVC backend (oci://, hf://, etc.).
// Without this, the orphaned Job keeps running, the orphaned
// ConfigMap stays in the OME namespace forever, and stale
// SourceReachable/Ready conditions confuse downstream consumers.
//
// Idempotent: safe to call on every non-PVC reconcile (no-op when
// no PVC artifacts exist for this model). The List + per-CR
// condition trim is the cost of the no-op path; both are O(per-model)
// label-selector queries.
func CleanupStaleArtifacts(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, isClusterScoped bool) error {
	if err := deleteMetadataArtifacts(ctx, kubeClient, log, obj, isClusterScoped); err != nil {
		return err
	}

	// Strip PVC-specific conditions from the CR's status. The update re-fetches
	// to avoid clobbering concurrent edits and is a no-op when none are present.
	return shared.RetryUpdate(ctx, kubeClient, log, obj, "pvc-condition-cleanup", func(ctx context.Context, c client.Client, latest client.Object) error {
		_, status, err := shared.ModelSpecAndStatus(latest)
		if err != nil {
			return err
		}
		filtered := make([]metav1.Condition, 0, len(status.Conditions))
		removed := false
		for _, cnd := range status.Conditions {
			if isPVCConditionReason(cnd.Reason) {
				removed = true
				continue
			}
			filtered = append(filtered, cnd)
		}
		if !removed {
			return nil
		}
		status.Conditions = filtered
		return c.Status().Update(ctx, latest)
	})
}

// isPVCConditionReason reports whether the supplied condition Reason
// is part of the PVC reconcile branch's reason set. Used by
// CleanupStaleArtifacts to identify stale conditions left behind
// after a URI swap from pvc:// to non-PVC.
func isPVCConditionReason(reason string) bool {
	switch reason {
	case v1beta1.ModelConditionReasonPVCInvalid,
		v1beta1.ModelConditionReasonPVCNotFound,
		v1beta1.ModelConditionReasonPVCNotBound,
		v1beta1.ModelConditionReasonPVCValidated,
		v1beta1.ModelConditionReasonPVCMetadataPending,
		v1beta1.ModelConditionReasonPVCMetadataExtracting,
		v1beta1.ModelConditionReasonPVCMetadataExtractionFailed,
		v1beta1.ModelConditionReasonPVCMetadataReady,
		v1beta1.ModelConditionReasonPVCConfigMissing:
		return true
	}
	return false
}

// deleteMetadataArtifacts removes what a PVC-backed model leaves behind in the
// cluster: its metadata-extraction Jobs, found by the labels the controller set
// on them, and its status ConfigMap in the operator namespace. A Job is deleted
// in the background so its pods go with it; an object already gone is not an
// error.
func deleteMetadataArtifacts(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, isClusterScoped bool) error {
	jobs := &batchv1.JobList{}
	if err := kubeClient.List(ctx, jobs, client.MatchingLabels(metadataJobLabels(obj, isClusterScoped))); err != nil {
		return fmt.Errorf("list PVC metadata Jobs: %w", err)
	}
	propagation := metav1.DeletePropagationBackground
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if err := kubeClient.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("delete PVC metadata Job %s/%s: %w", job.Namespace, job.Name, err)
		}
		log.Info("Deleted PVC metadata Job", "job", job.Name, "namespace", job.Namespace)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.GetPVCMetadataConfigMapName(obj.GetName(), obj.GetNamespace(), isClusterScoped),
			Namespace: constants.OMENamespace,
		},
	}
	if err := kubeClient.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete PVC metadata ConfigMap %s: %w", cm.Name, err)
	}
	return nil
}

// setPVCFailedStatus marks pre-Job validation failures that are recoverable
// without a CR edit (e.g., PVC not yet created). Caller wakes us up via
// PVC/ConfigMap watches, but we keep the timer requeue as a backstop.
func setPVCFailedStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, reason, message string) (ctrl.Result, error) {
	if err := writePVCFailedStatus(ctx, kubeClient, log, obj, reason, message); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pvcRequeueAfter}, nil
}

// setPVCFailedTerminal marks pre-Job validation failures that can only be
// fixed by a CR edit (malformed URI) or a controller-config change
// (missing ome-agent image). Both paths re-enqueue via watches, so the
// reconciler does not requeue on a timer.
func setPVCFailedTerminal(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, reason, message string) (ctrl.Result, error) {
	if err := writePVCFailedStatus(ctx, kubeClient, log, obj, reason, message); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func writePVCFailedStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, reason, message string) error {
	return updatePVCConditionedStatus(ctx, kubeClient, log, obj, ptr.To(v1beta1.LifeCycleStateFailed), []metav1.Condition{
		{Type: v1beta1.ModelConditionSourceReachable, Status: metav1.ConditionFalse, Reason: reason, Message: message},
		{Type: v1beta1.ModelConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message},
	})
}

// setPVCInTransitStatus marks in-flight states (PVC pending, Job running,
// agent extraction in progress) where SourceReachable is True but the
// model isn't Ready yet.
func setPVCInTransitStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, reason, message string) (ctrl.Result, error) {
	if err := updatePVCConditionedStatus(ctx, kubeClient, log, obj, ptr.To(v1beta1.LifeCycleStateInTransit), []metav1.Condition{
		{Type: v1beta1.ModelConditionSourceReachable, Status: metav1.ConditionTrue, Reason: reason, Message: message},
		{Type: v1beta1.ModelConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message},
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pvcRequeueAfter}, nil
}

// setPVCReadyStatus is the success terminal: state=Ready, both conditions
// True. No timer requeue — watches handle further changes.
func setPVCReadyStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, message string) (ctrl.Result, error) {
	if err := updatePVCConditionedStatus(ctx, kubeClient, log, obj, ptr.To(v1beta1.LifeCycleStateReady), []metav1.Condition{
		{Type: v1beta1.ModelConditionSourceReachable, Status: metav1.ConditionTrue, Reason: v1beta1.ModelConditionReasonPVCValidated, Message: "PVC is reachable"},
		{Type: v1beta1.ModelConditionReady, Status: metav1.ConditionTrue, Reason: v1beta1.ModelConditionReasonPVCMetadataReady, Message: message},
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// setPVCExtractionFailedStatus is the agent-failure terminal: PVC was
// reachable, but the extraction Job reported Failed. State=Failed,
// SourceReachable=True (PVC works), Ready=False (extraction failed).
func setPVCExtractionFailedStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, message string) (ctrl.Result, error) {
	if err := updatePVCConditionedStatus(ctx, kubeClient, log, obj, ptr.To(v1beta1.LifeCycleStateFailed), []metav1.Condition{
		{Type: v1beta1.ModelConditionSourceReachable, Status: metav1.ConditionTrue, Reason: v1beta1.ModelConditionReasonPVCValidated, Message: "PVC is reachable"},
		{Type: v1beta1.ModelConditionReady, Status: metav1.ConditionFalse, Reason: v1beta1.ModelConditionReasonPVCMetadataExtractionFailed, Message: message},
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// updatePVCConditionedStatus is the shared write-path for PVC-driven status
// updates. If `state` is nil only conditions are written; otherwise the
// model's LifeCycleState is also set. Retries on resource conflicts.
func updatePVCConditionedStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, state *v1beta1.LifeCycleState, conditions []metav1.Condition) error {
	return shared.RetryUpdate(ctx, kubeClient, log, obj, "pvc-status", func(ctx context.Context, c client.Client, latest client.Object) error {
		_, status, err := shared.ModelSpecAndStatus(latest)
		if err != nil {
			return err
		}
		if state != nil {
			status.State = *state
		}
		for _, cond := range conditions {
			meta.SetStatusCondition(&status.Conditions, cond)
		}
		shared.StampObservedReconcile(latest, status)
		return c.Status().Update(ctx, latest)
	})
}
