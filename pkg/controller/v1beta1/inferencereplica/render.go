package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// EventReasonRuntimeCompatibilityAdvisory is the Warning emitted when a named
// runtime does not declare support for the referenced model. Rendering
// proceeds because the runtime was named explicitly; the reason is the one
// the InferenceService controller emits for the same situation.
const EventReasonRuntimeCompatibilityAdvisory = "RuntimeCompatibilityAdvisory"

// renderBlocked is a template source this pass cannot turn into pod
// templates: the reason lands on the Ready condition and the workload pass
// is skipped.
type renderBlocked struct {
	Reason  string
	Message string
}

func (b *renderBlocked) Error() string { return b.Reason + ": " + b.Message }

// rendersFromRefs reports whether the replica's pods come from modelRef or
// runtimeRef rather than from stored runners.
func rendersFromRefs(ir *v1beta1.InferenceReplica) bool {
	return ir.Spec.ModelRef != nil || ir.Spec.RuntimeRef != nil
}

// serviceShell is the InferenceService-shaped input the rendering library
// takes, built from a refs replica: its name prefix, namespace, labels and
// annotations, the refs, and an empty role spec for its component so the
// runtime piece is the whole template. The shell names no accelerator class:
// the runtime's pod spec places the pods. The mode is OMENative, the only
// mode a replica runs.
func serviceShell(ir *v1beta1.InferenceReplica) *v1beta1.InferenceService {
	shell := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: ir.NamePrefix(), Namespace: ir.Namespace, UID: ir.UID, Generation: ir.Generation,
			Labels: maps.Clone(ir.Labels), Annotations: maps.Clone(ir.Annotations),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model:          ir.Spec.ModelRef.DeepCopy(),
			Runtime:        ir.Spec.RuntimeRef.DeepCopy(),
			DeploymentMode: ptr.To(constants.OMENative),
		},
	}
	switch ir.Spec.Component {
	case v1beta1.EngineComponent:
		shell.Spec.Engine = &v1beta1.EngineSpec{}
	case v1beta1.DecoderComponent:
		shell.Spec.Decoder = &v1beta1.DecoderSpec{}
	case v1beta1.RouterComponent:
		shell.Spec.Router = &v1beta1.RouterSpec{}
	}
	return shell
}

// renderFromRefs renders a refs replica's runners in memory; the replica's
// stored spec is never changed. It returns nil runners for a runners
// replica, a *renderBlocked for a template source the replica reports on its
// Ready condition, and any other error for a failure the caller retries.
func (r *Reconciler) renderFromRefs(ctx context.Context, ir *v1beta1.InferenceReplica) ([]v1beta1.Runner, error) {
	if !rendersFromRefs(ir) {
		if len(ir.Spec.Runners) == 0 {
			return nil, &renderBlocked{Reason: ReasonTemplateSourceMissing,
				Message: "spec sets neither runners nor modelRef or runtimeRef; one template source is required"}
		}
		return nil, nil
	}
	if r.RuntimeSelector == nil || r.Clientset == nil {
		return nil, &renderBlocked{Reason: ReasonRenderFailed,
			Message: "this controller is not configured to render from modelRef or runtimeRef"}
	}
	cfg, err := controllerconfig.NewInferenceServicesConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return nil, fmt.Errorf("load rendering config: %w", err)
	}
	deploy, err := controllerconfig.NewDeployConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return nil, fmt.Errorf("load deploy config: %w", err)
	}

	log := ctrl.LoggerFrom(ctx)
	shell := serviceShell(ir)
	// The role inputs take an accelerator class selector; over a shell that
	// names no class it picks none and reads no AcceleratorClass.
	in := render.Inputs{Service: shell, Client: r.Client, Runtimes: r.RuntimeSelector, Accelerators: acceleratorclassselector.New(r.Client), Config: cfg, Log: log}
	res, err := render.Resolve(ctx, in)
	if err != nil {
		return nil, classifyResolveError(err)
	}
	if res.CompatibilityAdvisory != nil && r.Recorder != nil {
		r.Recorder.Eventf(ir, corev1.EventTypeWarning, EventReasonRuntimeCompatibilityAdvisory,
			"Runtime %s does not declare support for model %s (%v); proceeding because the runtime was named explicitly",
			res.RuntimeName, ir.Spec.ModelRef.Name, res.CompatibilityAdvisory)
	}
	if !res.UserSpecifiedRuntime {
		log.V(1).Info("Auto-selected runtime", "runtime", res.RuntimeName, "model", ir.Spec.ModelRef.Name)
	}
	if !render.RuntimeDeclaresPiece(res.Runtime, ir.Spec.Component) {
		kind := runtimeselector.KindServingRuntime
		if res.RuntimeIsCluster {
			kind = runtimeselector.KindClusterServingRuntime
		}
		return nil, &renderBlocked{Reason: ReasonRuntimePieceMissing,
			Message: fmt.Sprintf("%s %q has no %s piece; a %s replica cannot render from it", kind, res.RuntimeName, ir.Spec.Component, ir.Spec.Component)}
	}
	if err := render.PrepareRole(res, ir.Spec.Component, shell.Spec.DeploymentMode, deploy); err != nil {
		return nil, &renderBlocked{Reason: ReasonRenderFailed, Message: err.Error()}
	}
	piece, _, err := res.Piece(ctx, in, ir.Spec.Component)
	if err != nil {
		return nil, classifyRenderError(err)
	}
	rendered, err := render.RenderComponent(ctx, piece, shell, res.Specs, ir.Spec.Component)
	if err != nil {
		return nil, classifyRenderError(err)
	}
	return rendered.Runners(), nil
}

// classifyResolveError maps a model or runtime resolution failure onto the
// block the replica reports. A failure the next pass may not see is
// returned unchanged so the caller retries it.
func classifyResolveError(err error) error {
	var notReady *render.ModelNotReadyError
	var merge *render.MergeError
	var disabled *runtimeselector.RuntimeDisabledError
	var compatibility *runtimeselector.RuntimeCompatibilityError
	var noRuntime *runtimeselector.NoRuntimeFoundError
	var invalidModel *runtimeselector.ModelValidationError
	var selectorConfig *runtimeselector.ConfigurationError
	switch {
	case errors.As(err, &notReady), isvcutils.IsModelNotFoundError(err):
		return &renderBlocked{Reason: ReasonModelNotFound, Message: err.Error()}
	case isvcutils.IsModelDisabledError(err):
		return &renderBlocked{Reason: ReasonModelDisabled, Message: err.Error()}
	case runtimeselector.IsRuntimeNotFoundError(err):
		return &renderBlocked{Reason: ReasonRuntimeNotFound, Message: err.Error()}
	case errors.As(err, &disabled):
		return &renderBlocked{Reason: ReasonRuntimeDisabled, Message: err.Error()}
	case errors.As(err, &noRuntime), errors.As(err, &compatibility), errors.As(err, &invalidModel), errors.As(err, &selectorConfig):
		return &renderBlocked{Reason: ReasonRuntimeSelectionFailed, Message: err.Error()}
	case errors.As(err, &merge):
		return &renderBlocked{Reason: ReasonRenderFailed, Message: err.Error()}
	}
	return classifyRenderError(err)
}

// classifyRenderError reports a render failure as a RenderFailed block unless
// it is transient: an API server failure other than NotFound, or a cancelled
// context. A missing object the render names is a configuration the replica
// reports.
func classifyRenderError(err error) error {
	var status apierrors.APIStatus
	if errors.As(err, &status) && !apierrors.IsNotFound(err) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &renderBlocked{Reason: ReasonRenderFailed, Message: err.Error()}
}

// reportRenderBlocked publishes a blocked template source: Ready=False with
// the block's reason and message, observed at the replica's generation, and
// a Warning event when the block is new or changed. The reconcile then
// returns without error: a block is a configuration the runtime and model
// watches re-evaluate when the referenced objects change.
func (r *Reconciler) reportRenderBlocked(ctx context.Context, ir *v1beta1.InferenceReplica, blocked *renderBlocked) (ctrl.Result, error) {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             blocked.Reason,
		Message:            blocked.Message,
		ObservedGeneration: ir.Generation,
	}
	existing := apimeta.FindStatusCondition(ir.Status.Conditions, cond.Type)
	if existing == nil || existing.Status != cond.Status || existing.Reason != cond.Reason ||
		existing.Message != cond.Message || existing.ObservedGeneration != cond.ObservedGeneration {
		ctrl.LoggerFrom(ctx).Info("InferenceReplica has no pod templates to run", "reason", blocked.Reason, "message", blocked.Message)
		if r.Recorder != nil {
			r.Recorder.Event(ir, corev1.EventTypeWarning, blocked.Reason, blocked.Message)
		}
	}
	if err := buildWriteAggregateCondition(r.statusWriter(), r.liveReader(), ir)(ctx, cond); err != nil {
		if errors.Is(err, workloadtypes.ErrStatusOwnerGone) {
			return ctrl.Result{}, nil
		}
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: publish blocked render (ir=%s/%s): %w", ir.Namespace, ir.Name, err)
	}
	return ctrl.Result{}, nil
}
