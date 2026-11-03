package components

import (
	"context"

	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"sigs.k8s.io/ome/pkg/render"
)

var _ Component = &Encoder{}
var _ ComponentConfig = &Encoder{}

// Encoder reconciles resources for the encoder component: the pods that turn
// a request's images or audio into embeddings the engine reads. It runs the
// decoder's reconcile path for a single-pod block.
type Encoder struct {
	BaseComponentFields
	encoderSpec          *v1beta1.EncoderSpec
	deploymentReconciler *common.DeploymentReconciler
}

// NewEncoder creates a new Encoder component instance. deps carries the
// process-lifetime wiring; in carries the per-reconcile pipeline output.
func NewEncoder(deps *ComponentDeps, in ComponentInputs, encoderSpec *v1beta1.EncoderSpec) Component {
	base := newBaseComponentFields(deps, in, "EncoderReconciler")

	return &Encoder{
		BaseComponentFields: base,
		encoderSpec:         encoderSpec,
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

// Reconcile implements the Component interface for Encoder
func (e *Encoder) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if e.encoderSpec == nil {
		return ctrl.Result{}, errors.New("encoder spec is nil")
	}
	return reconcilePairedRole(ctx, &e.BaseComponentFields, e.deploymentReconciler, isvc, e.role())
}

// role is the encoder as a role paired with the engine: one pod per replica,
// so no gang and no co-location key.
func (e *Encoder) role() pairedRole {
	spec := e.encoderSpec
	return pairedRole{
		component:         v1beta1.EncoderComponent,
		ext:               &spec.ComponentExtensionSpec,
		topologySpread:    spec.TopologySpread,
		topologySpreadKey: spec.TopologySpreadKey,
		render: func(ctx context.Context, p *render.Piece, isvc *v1beta1.InferenceService) (render.Rendered, error) {
			return render.RenderEncoder(ctx, p, isvc, spec)
		},
	}
}

// GetComponentType implements ComponentConfig interface
func (e *Encoder) GetComponentType() v1beta1.ComponentType {
	return v1beta1.EncoderComponent
}

// GetComponentSpec implements ComponentConfig interface
func (e *Encoder) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if e.encoderSpec == nil {
		return nil
	}
	return &e.encoderSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (e *Encoder) GetServiceSuffix() string {
	return "-" + string(v1beta1.EncoderComponent)
}

// ValidateSpec implements ComponentConfig interface
func (e *Encoder) ValidateSpec() error {
	if e.encoderSpec == nil {
		return errors.New("encoder spec is nil")
	}
	return nil
}
