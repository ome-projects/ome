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
	service := desired.DeepCopy()
	cm := &corev1.ConfigMap{}
	if err := rt.reads.Get(ctx, client.ObjectKey{Namespace: r.OperatorNamespace, Name: constants.InferenceServiceConfigMapName}, cm); err != nil {
		return nil, err
	}
	cfg, deploy, err := controllerconfig.RenderingConfig(cm)
	if err != nil {
		return nil, err
	}
	if len(cfg.AcceleratorResources) == 0 {
		return nil, fmt.Errorf("replica rendering requires configured member accelerator resources")
	}
	// Deployment defaults are applied to copies; rt keeps the merged specs
	// as the runtime produced them.
	resolved := &render.Resolved{
		Model: rt.Model, ModelMeta: rt.ModelMeta,
		Runtime: rt.Spec, RuntimeName: rt.Name, RuntimeIsCluster: rt.IsCluster,
		UserSpecifiedRuntime: service.Spec.Runtime != nil && service.Spec.Runtime.Name != "",
		Specs:                render.Specs{Engine: rt.Engine.DeepCopy(), Decoder: rt.Decoder.DeepCopy(), Router: rt.Router.DeepCopy()},
	}
	if err := render.Prepare(resolved, service.Spec.DeploymentMode, deploy); err != nil {
		return nil, err
	}
	in := render.Inputs{Service: service, Client: rt.reads, Runtimes: runtimeselector.New(rt.reads), Accelerators: acceleratorclassselector.New(rt.reads), Config: cfg, Log: logr.Discard()}
	resolved.Overlays, err = render.ResolveOverlays(ctx, in)
	if err != nil {
		return nil, err
	}
	engine, decoder := resolved.Specs.Engine, resolved.Specs.Decoder
	engineMode, decoderMode := resolved.Modes[v1beta1.EngineComponent], resolved.Modes[v1beta1.DecoderComponent]
	var accelerators []*v1beta1.AcceleratorClass
	pieceFor := func(component v1beta1.ComponentType) (*render.Piece, error) {
		p, ac, err := resolved.Piece(ctx, in, component)
		if err != nil {
			return nil, err
		}
		if ac != nil {
			identity := ac.DeepCopy()
			identity.ObjectMeta = renderingIdentity(identity.ObjectMeta)
			identity.Status = v1beta1.AcceleratorClassStatus{}
			accelerators = append(accelerators, identity)
		}
		return p, nil
	}
	out := &ResolvedUnit{Runtime: rt, Modes: resolved.Modes}
	enginePiece, err := pieceFor(v1beta1.EngineComponent)
	if err != nil {
		return nil, err
	}
	engineRendered, err := render.RenderEngine(ctx, enginePiece, service, engine)
	if err != nil {
		return nil, err
	}
	out.Engine = components.ReplicaTemplatesFrom(engineRendered.Templates)
	unit := capacity.ReplicaUnit{}
	unit.Engine, err = out.Engine.PodSets(engineMode, engine.Leader != nil, engine.Worker != nil)
	if err != nil {
		return nil, fmt.Errorf("engine demand: %w", err)
	}
	if decoder != nil {
		decoderPiece, err := pieceFor(v1beta1.DecoderComponent)
		if err != nil {
			return nil, err
		}
		decoderRendered, err := render.RenderDecoder(ctx, decoderPiece, service, decoder)
		if err != nil {
			return nil, err
		}
		templates := components.ReplicaTemplatesFrom(decoderRendered.Templates)
		out.Decoder = &templates
		unit.Decoder, err = templates.PodSets(decoderMode, decoder.Leader != nil, decoder.Worker != nil)
		if err != nil {
			return nil, fmt.Errorf("decoder demand: %w", err)
		}
	}
	var classes []*nodev1.RuntimeClass
	for _, sets := range [][]capacity.PodSet{unit.Engine, unit.Decoder} {
		for _, set := range sets {
			class, err := resolveRuntimeClass(ctx, rt.reads, set.Spec)
			if err != nil {
				return nil, err
			}
			if class != nil {
				class.ObjectMeta = renderingIdentity(class.ObjectMeta)
				classes = append(classes, class)
			}
		}
	}
	var modelIdentity metav1.ObjectMeta
	out.Rendering = map[v1beta1.ComponentType]string{}
	for _, component := range []struct {
		name v1beta1.ComponentType
		mode constants.DeploymentModeType
		pods []capacity.PodSet
	}{{v1beta1.EngineComponent, engineMode, unit.Engine}, {v1beta1.DecoderComponent, decoderMode, unit.Decoder}} {
		if len(component.pods) == 0 {
			continue
		}
		fingerprint, err := capacity.ComponentFingerprint(ctx, rt.reads, component.name, component.mode, component.pods)
		if err != nil {
			return nil, err
		}
		out.Rendering[component.name] = fingerprint
	}
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
		return nil, err
	}
	sum := sha256.Sum256(encoded)
	unit.InputFingerprint = hex.EncodeToString(sum[:])
	out.Demand, err = capacity.MeasureUnit(unit, cfg.AcceleratorResources)
	if err != nil {
		return nil, err
	}
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func renderingIdentity(meta metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: meta.Name, Namespace: meta.Namespace, UID: meta.UID, Labels: maps.Clone(meta.Labels), Annotations: maps.Clone(meta.Annotations)}
}

func resolveRuntimeClass(ctx context.Context, reader client.Reader, pod *corev1.PodSpec) (*nodev1.RuntimeClass, error) {
	return capacity.ApplyRuntimeClass(ctx, reader, pod)
}
