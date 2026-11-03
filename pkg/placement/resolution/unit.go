package resolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/components"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// ResolvedUnit combines the member's rendering and nominal accelerator demand.
// Check verifies read dependencies, not pod admission or member application.
type ResolvedUnit struct {
	Runtime   *Runtime
	Demand    capacity.UnitDemand
	Engine    components.ReplicaTemplates
	Decoder   *components.ReplicaTemplates
	Rendering map[v1beta1.ComponentType]string
	Modes     map[v1beta1.ComponentType]constants.DeploymentModeType
}

func (u *ResolvedUnit) Check(ctx context.Context) error {
	if u == nil {
		return fmt.Errorf("replica unit has no verified inputs")
	}
	return u.Runtime.Check(ctx)
}

// ResolveUnit renders the intended member service without writing any objects.
// ResourceFlavor attribution and proof of member application remain required
// before the result can authorize a capacity plan.
func (r Resolver) ResolveUnit(ctx context.Context, desired, standing *v1beta1.InferenceService) (*ResolvedUnit, error) {
	if r.OperatorNamespace == "" {
		return nil, fmt.Errorf("replica rendering requires the member operator namespace")
	}
	rt, err := r.Resolve(ctx, desired, standing)
	if err != nil {
		return nil, err
	}
	config, err := r.readRenderingConfig(ctx, rt.reads)
	if err != nil {
		return nil, err
	}
	member, err := newMemberRendering(ctx, rt, desired, config)
	if err != nil {
		return nil, err
	}
	rendered, err := member.renderComponents(ctx)
	if err != nil {
		return nil, err
	}
	classes, err := applyRuntimeClasses(ctx, rt.reads, rendered)
	if err != nil {
		return nil, err
	}
	out := &ResolvedUnit{Runtime: rt, Modes: member.resolved.Modes, Engine: rendered.engine.templates, Rendering: map[v1beta1.ComponentType]string{}}
	unit := capacity.ReplicaUnit{Units: maps.Clone(r.ComponentUnits), Engine: rendered.engine.pods}
	if rendered.decoder != nil {
		templates := rendered.decoder.templates
		out.Decoder = &templates
		unit.Decoder = rendered.decoder.pods
	}
	for _, component := range rendered.components() {
		fingerprint, err := capacity.ComponentFingerprint(ctx, rt.reads, component.name, component.mode, component.pods)
		if err != nil {
			return nil, err
		}
		out.Rendering[component.name] = fingerprint
	}
	unit.InputFingerprint, err = inputFingerprint(rt, config.configMap, rendered.accelerators(), classes)
	if err != nil {
		return nil, err
	}
	out.Demand, err = capacity.MeasureUnit(unit, config.settings.AcceleratorResources)
	if err != nil {
		return nil, err
	}
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// renderingConfig is the operator configuration a member rendering reads once:
// the ConfigMap, whose identity and data enter the input fingerprint, the
// rendering settings and the deploy defaults it carries.
type renderingConfig struct {
	configMap *corev1.ConfigMap
	settings  *controllerconfig.InferenceServicesConfig
	deploy    *controllerconfig.DeployConfig
}

// readRenderingConfig reads the operator configuration from the member; the
// accelerator resource names must be configured for a demand to be measured.
func (r Resolver) readRenderingConfig(ctx context.Context, reads client.Reader) (renderingConfig, error) {
	cm := &corev1.ConfigMap{}
	if err := reads.Get(ctx, client.ObjectKey{Namespace: r.OperatorNamespace, Name: constants.InferenceServiceConfigMapName}, cm); err != nil {
		return renderingConfig{}, err
	}
	settings, deploy, err := controllerconfig.RenderingConfig(cm)
	if err != nil {
		return renderingConfig{}, err
	}
	if len(settings.AcceleratorResources) == 0 {
		return renderingConfig{}, fmt.Errorf("replica rendering requires configured member accelerator resources")
	}
	return renderingConfig{configMap: cm, settings: settings, deploy: deploy}, nil
}

// memberRendering is what every component of the member service renders from:
// the service, the merged specs with the deploy defaults applied, and the
// render library's read-only inputs.
type memberRendering struct {
	service  *v1beta1.InferenceService
	resolved *render.Resolved
	inputs   render.Inputs
}

// newMemberRendering applies the deploy defaults to copies of the merged
// specs, so rt keeps them as the runtime produced them, and reads the model
// overlays.
func newMemberRendering(ctx context.Context, rt *Runtime, desired *v1beta1.InferenceService, config renderingConfig) (memberRendering, error) {
	service := desired.DeepCopy()
	resolved := &render.Resolved{
		Model: rt.Model, ModelMeta: rt.ModelMeta,
		Runtime: rt.Spec, RuntimeName: rt.Name, RuntimeIsCluster: rt.IsCluster,
		UserSpecifiedRuntime: service.Spec.Runtime != nil && service.Spec.Runtime.Name != "",
		Specs:                render.Specs{Engine: rt.Engine.DeepCopy(), Decoder: rt.Decoder.DeepCopy(), Router: rt.Router.DeepCopy()},
	}
	if err := render.Prepare(resolved, service.Spec.DeploymentMode, config.deploy); err != nil {
		return memberRendering{}, err
	}
	inputs := render.Inputs{Service: service, Client: rt.reads, Runtimes: runtimeselector.New(rt.reads), Accelerators: acceleratorclassselector.New(rt.reads), Config: config.settings, Log: logr.Discard()}
	overlays, err := render.ResolveOverlays(ctx, inputs)
	if err != nil {
		return memberRendering{}, err
	}
	resolved.Overlays = overlays
	return memberRendering{service: service, resolved: resolved, inputs: inputs}, nil
}

// renderedComponent is one component as the member would create it: its
// templates, the pod sets of one replica under its deployment mode (they share
// the templates' pod specs), and the identity of the accelerator class it
// rendered with, nil when it has none.
type renderedComponent struct {
	name        v1beta1.ComponentType
	mode        constants.DeploymentModeType
	templates   components.ReplicaTemplates
	pods        []capacity.PodSet
	accelerator *v1beta1.AcceleratorClass
}

// renderedUnit is the member's rendered components: the engine, and the
// decoder when the service declares one.
type renderedUnit struct {
	engine  renderedComponent
	decoder *renderedComponent
}

// components lists the rendered components in the order the runtime classes,
// the fingerprints and the input fingerprint read them.
func (u renderedUnit) components() []renderedComponent {
	if u.decoder == nil {
		return []renderedComponent{u.engine}
	}
	return []renderedComponent{u.engine, *u.decoder}
}

// accelerators lists the accelerator class identities in component order; nil
// when no component rendered with one.
func (u renderedUnit) accelerators() []*v1beta1.AcceleratorClass {
	var out []*v1beta1.AcceleratorClass
	for _, component := range u.components() {
		if component.accelerator != nil {
			out = append(out, component.accelerator)
		}
	}
	return out
}

// renderComponents renders the engine and, when the service declares one, the
// decoder, each through renderComponent.
func (m memberRendering) renderComponents(ctx context.Context) (renderedUnit, error) {
	engine, err := m.renderComponent(ctx, v1beta1.EngineComponent)
	if err != nil {
		return renderedUnit{}, err
	}
	unit := renderedUnit{engine: engine}
	if m.resolved.Specs.Decoder != nil {
		decoder, err := m.renderComponent(ctx, v1beta1.DecoderComponent)
		if err != nil {
			return renderedUnit{}, err
		}
		unit.decoder = &decoder
	}
	return unit, nil
}

// renderComponent renders one declared role: its piece, its templates and the
// pod sets of one replica.
func (m memberRendering) renderComponent(ctx context.Context, name v1beta1.ComponentType) (renderedComponent, error) {
	piece, class, err := m.resolved.Piece(ctx, m.inputs, name)
	if err != nil {
		return renderedComponent{}, err
	}
	out := renderedComponent{name: name, mode: m.resolved.Modes[name]}
	if class != nil {
		out.accelerator = class.DeepCopy()
		out.accelerator.ObjectMeta = renderingIdentity(out.accelerator.ObjectMeta)
		out.accelerator.Status = v1beta1.AcceleratorClassStatus{}
	}
	rendered, err := render.RenderComponent(ctx, piece, m.service, m.resolved.Specs, name)
	if err != nil {
		return renderedComponent{}, err
	}
	out.templates = components.ReplicaTemplatesFrom(rendered.Templates)
	leader, worker := m.leaderAndWorker(name)
	out.pods, err = out.templates.PodSets(out.mode, leader, worker)
	if err != nil {
		return renderedComponent{}, fmt.Errorf("%s demand: %w", name, err)
	}
	return out, nil
}

// leaderAndWorker reports whether the role's prepared spec declares a Leader
// and a Worker block, which decide the pod sets of one replica.
func (m memberRendering) leaderAndWorker(name v1beta1.ComponentType) (leader, worker bool) {
	switch name {
	case v1beta1.EngineComponent:
		return m.resolved.Specs.Engine.Leader != nil, m.resolved.Specs.Engine.Worker != nil
	case v1beta1.DecoderComponent:
		return m.resolved.Specs.Decoder.Leader != nil, m.resolved.Specs.Decoder.Worker != nil
	}
	return false, false
}

// applyRuntimeClasses applies each pod set's RuntimeClass to its pod spec, in
// component order, and returns the identities of the classes it read; nil
// when no pod set names one.
func applyRuntimeClasses(ctx context.Context, reader client.Reader, unit renderedUnit) ([]*nodev1.RuntimeClass, error) {
	var classes []*nodev1.RuntimeClass
	for _, component := range unit.components() {
		for _, set := range component.pods {
			class, err := capacity.ApplyRuntimeClass(ctx, reader, set.Spec)
			if err != nil {
				return nil, err
			}
			if class != nil {
				class.ObjectMeta = renderingIdentity(class.ObjectMeta)
				classes = append(classes, class)
			}
		}
	}
	return classes, nil
}

// inputFingerprint hashes what the rendering read: the runtime, the model and
// its identity, the configuration and its identity, the accelerator classes
// and the RuntimeClasses. The encoded field order is part of the hash.
func inputFingerprint(rt *Runtime, cm *corev1.ConfigMap, accelerators []*v1beta1.AcceleratorClass, classes []*nodev1.RuntimeClass) (string, error) {
	var modelIdentity metav1.ObjectMeta
	if rt.ModelMeta != nil {
		modelIdentity = renderingIdentity(*rt.ModelMeta)
	}
	encoded, err := json.Marshal(struct {
		RuntimeName, RuntimeHash string
		RuntimeIsCluster         bool
		Model                    *v1beta1.BaseModelSpec
		ModelIdentity            metav1.ObjectMeta
		ConfigIdentity           metav1.ObjectMeta
		Config                   map[string]string
		Accelerators             []*v1beta1.AcceleratorClass
		Classes                  []*nodev1.RuntimeClass
	}{rt.Name, rt.Hash, rt.IsCluster, rt.Model, modelIdentity, renderingIdentity(cm.ObjectMeta), cm.Data, accelerators, classes})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func renderingIdentity(meta metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: meta.Name, Namespace: meta.Namespace, UID: meta.UID, Labels: maps.Clone(meta.Labels), Annotations: maps.Clone(meta.Annotations)}
}
