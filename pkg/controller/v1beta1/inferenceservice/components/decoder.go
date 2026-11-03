package components

import (
	"context"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"sigs.k8s.io/ome/pkg/render"
)

var _ Component = &Decoder{}
var _ ComponentConfig = &Decoder{}

// Decoder reconciles resources for the decoder component
type Decoder struct {
	BaseComponentFields
	decoderSpec          *v1beta1.DecoderSpec
	deploymentReconciler *common.DeploymentReconciler
}

// NewDecoder creates a new Decoder component instance. deps carries
// the process-lifetime wiring; in carries the per-reconcile pipeline
// output.
func NewDecoder(deps *ComponentDeps, in ComponentInputs, decoderSpec *v1beta1.DecoderSpec) Component {
	base := newBaseComponentFields(deps, in, "DecoderReconciler")

	return &Decoder{
		BaseComponentFields: base,
		decoderSpec:         decoderSpec,
		deploymentReconciler: &common.DeploymentReconciler{
			Client:        deps.Client,
			APIReader:     deps.APIReader,
			Clientset:     deps.Clientset,
			Scheme:        deps.Scheme,
			StatusManager: base.StatusManager,
			Log:           base.Log,
		},
	}
}

// Reconcile implements the Component interface for Decoder
func (d *Decoder) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if d.decoderSpec == nil {
		return ctrl.Result{}, errors.New("decoder spec is nil")
	}
	return reconcilePairedRole(ctx, &d.BaseComponentFields, d.deploymentReconciler, isvc, d.role())
}

// role is the decoder as a role paired with the engine.
func (d *Decoder) role() pairedRole {
	spec := d.decoderSpec
	return pairedRole{
		component:         v1beta1.DecoderComponent,
		ext:               &spec.ComponentExtensionSpec,
		leader:            spec.Leader != nil,
		worker:            spec.Worker != nil,
		multiPod:          render.DecoderIsMultiPod(spec),
		topologyKey:       spec.TopologyKey,
		topologySpread:    spec.TopologySpread,
		topologySpreadKey: spec.TopologySpreadKey,
		render: func(ctx context.Context, p *render.Piece, isvc *v1beta1.InferenceService) (render.Rendered, error) {
			return render.RenderDecoder(ctx, p, isvc, spec)
		},
	}
}

// reconcileOMENativeSubresources writes the decoder's stable Service and
// PodMonitor through the shared ReconcileOMENativeSubresources helper.
func (d *Decoder) reconcileOMENativeSubresources(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) error {
	return ReconcileOMENativeSubresources(ctx, &d.BaseComponentFields, isvc, v1beta1.DecoderComponent, &d.decoderSpec.ComponentExtensionSpec, objectMeta, podSpec, render.DecoderIsMultiPod(d.decoderSpec))
}

// reconcileDeployment dispatches the decoder's pod set by deployment mode.
func (d *Decoder) reconcileDeployment(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, workerSize int, workerPodSpec *v1.PodSpec, pdbRequest pdb.Request) (ctrl.Result, error) {
	return reconcilePairedRoleDeployment(ctx, &d.BaseComponentFields, d.deploymentReconciler, isvc, d.role(), objectMeta, podSpec, workerSize, workerPodSpec, pdbRequest)
}

// The render steps below expose the library one step at a time for the tests
// that exercise them in isolation; Reconcile renders through
// render.RenderDecoder. Unlike RenderDecoder, they complete the runner
// containers in place on the decoder spec.

// reconcileObjectMeta builds the decoder's object metadata.
func (d *Decoder) reconcileObjectMeta(_ context.Context, isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	var decoderAnnotations, decoderLabels map[string]string
	if d.decoderSpec != nil {
		decoderAnnotations = d.decoderSpec.Annotations
		decoderLabels = d.decoderSpec.Labels
	}

	return ReconcileComponentObjectMeta(&d.BaseComponentFields, isvc, v1beta1.DecoderComponent, render.ComponentName(isvc, v1beta1.DecoderComponent), decoderAnnotations, decoderLabels)
}

// decoderUsesLeaderTemplate reports whether the decoder sources its primary
// pod template from the Leader block.
func decoderUsesLeaderTemplate(spec *v1beta1.DecoderSpec) bool {
	return render.DecoderUsesLeaderTemplate(spec)
}

// reconcilePodSpec renders the decoder's primary pod.
func (d *Decoder) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	return render.DecoderPodSpec(&d.Piece, isvc, d.decoderSpec, objectMeta)
}

// reconcileWorkerPodSpec renders the decoder's worker pod, nil without a worker.
func (d *Decoder) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	return render.DecoderWorkerPodSpec(&d.Piece, isvc, d.decoderSpec, objectMeta)
}

// GetComponentType implements ComponentConfig interface
func (d *Decoder) GetComponentType() v1beta1.ComponentType {
	return v1beta1.DecoderComponent
}

// GetComponentSpec implements ComponentConfig interface
func (d *Decoder) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if d.decoderSpec == nil {
		return nil
	}
	return &d.decoderSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (d *Decoder) GetServiceSuffix() string {
	return "-decoder"
}

// ValidateSpec implements ComponentConfig interface
func (d *Decoder) ValidateSpec() error {
	if d.decoderSpec == nil {
		return errors.New("decoder spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
