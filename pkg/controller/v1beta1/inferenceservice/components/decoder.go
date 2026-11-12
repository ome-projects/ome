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
