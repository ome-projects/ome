package render

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/specdefaults"
)

// Inputs are the service to render and the readers and selectors that
// resolve its model and runtime. A caller that has already read the model or
// the overlays, or that manages runtime pins, supplies the result in the
// Model*, Overlays* or Runtime* fields; Resolve then skips that step, so the
// caller's own ordering of reads and side effects is kept.
type Inputs struct {
	Service      *v1beta1.InferenceService
	Client       client.Client
	Runtimes     runtimeselector.Selector
	Accelerators acceleratorclassselector.Selector
	Config       *controllerconfig.InferenceServicesConfig
	Log          logr.Logger

	// Model and ModelMeta are authoritative when ModelRead is set,
	// including a nil Model for a service without spec.model.
	Model     *v1beta1.BaseModelSpec
	ModelMeta *metav1.ObjectMeta
	ModelRead bool

	// Overlays are authoritative when OverlaysRead is set.
	Overlays     []isvcutils.ResolvedOverlay
	OverlaysRead bool

	// Runtime is a spec the caller resolved itself, such as a pinned
	// revision, with its name and scope; Resolve then skips the lookup.
	Runtime          *v1beta1.ServingRuntimeSpec
	RuntimeName      string
	RuntimeIsCluster bool
}

// Resolved is what Resolve read and merged.
type Resolved struct {
	Model     *v1beta1.BaseModelSpec
	ModelMeta *metav1.ObjectMeta
	Overlays  []isvcutils.ResolvedOverlay

	Runtime              *v1beta1.ServingRuntimeSpec
	RuntimeName          string
	RuntimeIsCluster     bool
	UserSpecifiedRuntime bool
	// CompatibilityAdvisory is the declared-format mismatch a named runtime
	// reported for a non-sharded model. Rendering proceeds; the caller
	// surfaces it.
	CompatibilityAdvisory error

	Specs Specs
	// Modes are the deployment modes Prepare determined for the declared
	// roles; a role the service does not declare has no entry.
	Modes map[v1beta1.ComponentType]constants.DeploymentModeType
}

// ModelNotReadyError reports a sharded model that has not finished loading;
// the caller waits and retries. Message is the model's own readiness report
// and may be empty.
type ModelNotReadyError struct{ Model, Message string }

func (e *ModelNotReadyError) Error() string {
	if e.Message == "" {
		return "model " + e.Model + " is not ready"
	}
	return "model " + e.Model + " is not ready: " + e.Message
}

// ErrNoTemplateSource is returned when the service names neither a runtime
// nor a model.
var ErrNoTemplateSource = errors.New("a runtime or a model reference is required")

// MergeError reports a failure to merge the runtime with the service's role
// specs. Error returns the cause's message unchanged; the type tells a merge
// failure apart from the runtime lookups, whose errors Resolve returns as
// they are so the runtime selector's error predicates apply to them.
type MergeError struct{ Err error }

func (e *MergeError) Error() string { return e.Err.Error() }
func (e *MergeError) Unwrap() error { return e.Err }

// ResolveModel reads spec.model (BaseModel in the namespace, else
// ClusterBaseModel) and gates a sharded model on readiness. A service without
// spec.model resolves to a nil model.
func ResolveModel(ctx context.Context, in Inputs) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, error) {
	if in.Service == nil {
		return nil, nil, errors.New("model resolution requires a service")
	}
	model, meta, status, err := isvcutils.ReconcileBaseModelWithStatus(in.Client, in.Service)
	if err != nil {
		return nil, nil, err
	}
	if model != nil && isvcutils.IsShardedBaseModel(model) {
		if ready, message := isvcutils.ShardedBaseModelReady(status, meta.Generation); !ready {
			return nil, nil, &ModelNotReadyError{Model: in.Service.Spec.Model.Name, Message: message}
		}
	}
	return model, meta, nil
}

// ResolveOverlays reads spec.model.overlays in order. An overlay that cannot
// be mounted is a skipped entry; only a failed read is an error.
func ResolveOverlays(ctx context.Context, in Inputs) ([]isvcutils.ResolvedOverlay, error) {
	return isvcutils.ResolveOverlays(in.Client, in.Service)
}

// Resolve reads the model unless supplied, reads the overlays unless
// supplied, resolves the runtime unless supplied (named on the service, else
// auto-selected for the model), refuses a disabled runtime, and merges the
// runtime with the service's role specs. Errors from the model gate, the
// runtime selector and the readers are returned unwrapped; a merge failure
// is a *MergeError.
func Resolve(ctx context.Context, in Inputs) (*Resolved, error) {
	service := in.Service
	if service == nil {
		return nil, errors.New("resolution requires a service")
	}
	res := &Resolved{Model: in.Model, ModelMeta: in.ModelMeta, Overlays: in.Overlays}
	if !in.ModelRead {
		model, meta, err := ResolveModel(ctx, in)
		if err != nil {
			return nil, err
		}
		res.Model, res.ModelMeta = model, meta
	}
	if !in.OverlaysRead {
		overlays, err := ResolveOverlays(ctx, in)
		if err != nil {
			return nil, err
		}
		res.Overlays = overlays
	}
	res.UserSpecifiedRuntime = service.Spec.Runtime != nil && service.Spec.Runtime.Name != ""
	if err := resolveRuntime(ctx, in, res); err != nil {
		return nil, err
	}
	if err := validateResolvedRuntimeEnabled(res.Runtime, res.RuntimeName, res.RuntimeIsCluster); err != nil {
		return nil, err
	}
	specs, err := isvcutils.MergeRuntimeRoleSpecs(service, res.Runtime, in.Log)
	if err != nil {
		return nil, &MergeError{Err: err}
	}
	res.Specs = specs
	return res, nil
}

// resolveRuntime fills in the runtime of res: the one supplied with the
// inputs, else the one the service names, else the selector's choice for the
// model. With neither a named runtime nor a model there is nothing to render
// from.
func resolveRuntime(ctx context.Context, in Inputs, res *Resolved) error {
	switch {
	case in.Runtime != nil:
		res.Runtime, res.RuntimeName, res.RuntimeIsCluster = in.Runtime, in.RuntimeName, in.RuntimeIsCluster
		return nil
	case !res.UserSpecifiedRuntime && res.Model == nil:
		return ErrNoTemplateSource
	case in.Runtimes == nil:
		return errors.New("runtime resolution requires a runtime selector")
	case res.UserSpecifiedRuntime:
		return resolveNamedRuntime(ctx, in, res)
	}
	selection, err := in.Runtimes.SelectRuntime(ctx, res.Model, in.Service)
	if err != nil {
		return err
	}
	res.Runtime, res.RuntimeName, res.RuntimeIsCluster = selection.Spec, selection.Name, selection.IsCluster
	return nil
}

// resolveNamedRuntime reads the runtime the service names, after checking it
// against the model when one was read.
func resolveNamedRuntime(ctx context.Context, in Inputs, res *Resolved) error {
	service := in.Service
	name := service.Spec.Runtime.Name
	if err := validateNamedRuntime(ctx, in, res, name); err != nil {
		return err
	}
	spec, isCluster, err := in.Runtimes.GetRuntime(ctx, name, service.Namespace, runtimeselector.RefKind(service.Spec.Runtime))
	if err != nil {
		return err
	}
	res.Runtime, res.RuntimeName, res.RuntimeIsCluster = spec, name, isCluster
	return nil
}

// validateNamedRuntime checks a named runtime against the model. A named
// runtime serves the model even when its declared formats do not list it; the
// mismatch is kept on res as an advisory. A sharded model keeps the check: it
// cannot load without a runtime that supports the configured cache provider.
func validateNamedRuntime(ctx context.Context, in Inputs, res *Resolved, name string) error {
	if res.Model == nil {
		return nil
	}
	err := in.Runtimes.ValidateRuntime(ctx, name, res.Model, in.Service)
	if err == nil {
		return nil
	}
	if !runtimeselector.IsRuntimeCompatibilityError(err) || isvcutils.IsShardedBaseModel(res.Model) {
		return err
	}
	res.CompatibilityAdvisory = err
	return nil
}

func validateResolvedRuntimeEnabled(runtimeSpec *v1beta1.ServingRuntimeSpec, runtimeName string, isCluster bool) error {
	if runtimeSpec != nil && runtimeSpec.IsDisabled() {
		return &runtimeselector.RuntimeDisabledError{
			RuntimeName: runtimeName,
			IsCluster:   isCluster,
		}
	}
	return nil
}

// Prepare determines each declared role's deployment mode and applies the
// operator's deploy defaults to the merged specs, in place.
func Prepare(r *Resolved, specMode *constants.DeploymentModeType, deploy *controllerconfig.DeployConfig) error {
	modes, err := isvcutils.DetermineRoleDeploymentModes(r.Specs, specMode)
	if err != nil {
		return err
	}
	r.Modes = modes
	for _, view := range r.Specs.Declared() {
		specdefaults.Role(&view, modes[view.Type], deploy)
	}
	return nil
}

// PrepareRole is Prepare for one role: it determines the deployment mode of
// role c alone and applies the operator's deploy defaults to its merged
// spec, in place. A caller that renders a single role and declares no engine
// uses it instead of Prepare, which requires an engine. The merged specs
// must declare the role.
func PrepareRole(r *Resolved, c v1beta1.ComponentType, specMode *constants.DeploymentModeType, deploy *controllerconfig.DeployConfig) error {
	if _, ok := v1beta1.RoleOf(c); !ok {
		return fmt.Errorf("component %q is not a role", c)
	}
	view := r.Specs.View(c)
	if view == nil {
		return fmt.Errorf("the merged specs declare no %s role", c)
	}
	if r.Modes == nil {
		r.Modes = make(map[v1beta1.ComponentType]constants.DeploymentModeType, 1)
	}
	r.Modes[c] = isvcutils.RoleDeploymentMode(view, specMode)
	specdefaults.Role(view, r.Modes[c], deploy)
	return nil
}

// Piece assembles the render inputs for one role: the accelerator class the
// selector picks for it and the runtime's supported format for the model.
// The picked AcceleratorClass object is returned beside the piece, nil when
// the role has none, for callers that record its identity. A role that does
// not run on an accelerator class (the router) takes neither a class nor a
// format.
func (r *Resolved) Piece(ctx context.Context, in Inputs, c v1beta1.ComponentType) (*Piece, *v1beta1.AcceleratorClass, error) {
	p := &Piece{
		Client:                 in.Client,
		Log:                    in.Log,
		InferenceServiceConfig: in.Config,
		DeploymentMode:         r.Modes[c],
		BaseModel:              r.Model,
		BaseModelMeta:          r.ModelMeta,
		Runtime:                r.Runtime,
		RuntimeName:            r.RuntimeName,
		Overlays:               r.Overlays,
	}
	if role, _ := v1beta1.RoleOf(c); !role.RunsModel {
		return p, nil, nil
	}
	if in.Accelerators == nil || in.Runtimes == nil {
		return nil, nil, errors.New("role inputs require an accelerator class selector and a runtime selector")
	}
	class, name, err := in.Accelerators.GetAcceleratorClass(ctx, in.Service, r.Runtime, c)
	if err != nil {
		return nil, nil, err
	}
	if class != nil {
		p.AcceleratorClass = &class.Spec
	}
	p.AcceleratorClassName = name
	p.SupportedModelFormat = in.Runtimes.GetSupportedModelFormat(ctx, r.Runtime, r.Model, r.UserSpecifiedRuntime)
	return p, class, nil
}
