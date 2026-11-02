// Package resolution reads the member's runtime inputs without creating workloads.
package resolution

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// Resolver uses the identified member client, including its cached kinds. OperatorNamespace
// locates that member's configuration and runtime revisions; it has no fallback.
// ComponentUnits is the replica count of each component in one measured
// capacity unit; nil measures one replica of every declared component.
type Resolver struct {
	Client            client.Client
	OperatorNamespace string
	ComponentUnits    map[v1beta1.ComponentType]int64
}

// Runtime records selection and merged specs before deployment defaults or
// rendering. Only components declared by the service are included.
// Check revalidates dependencies; it does not prove member workload application.
type Runtime struct {
	Name      string
	IsCluster bool
	Hash      string
	Spec      *v1beta1.ServingRuntimeSpec
	Engine    *v1beta1.EngineSpec
	Decoder   *v1beta1.DecoderSpec
	Router    *v1beta1.RouterSpec
	Model     *v1beta1.BaseModelSpec
	ModelMeta *metav1.ObjectMeta
	reads     *snapshotClient
}

func (r *Runtime) Check(ctx context.Context) error {
	if r == nil || r.reads == nil || len(r.reads.snapshots) == 0 {
		return fmt.Errorf("runtime resolution has no verified inputs")
	}
	return r.reads.check(ctx)
}

func (r *Runtime) Components() []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	if r.Engine != nil {
		out = append(out, v1beta1.EngineComponent)
	}
	if r.Decoder != nil {
		out = append(out, v1beta1.DecoderComponent)
	}
	if r.Router != nil {
		out = append(out, v1beta1.RouterComponent)
	}
	return out
}

// Resolve follows the member's explicit, automatic, or pinned runtime path.
// desired carries the intended member spec. standing identifies the owned member
// service whose pin state may be read; nil requires that service to be absent.
func (r Resolver) Resolve(ctx context.Context, desired, standing *v1beta1.InferenceService) (*Runtime, error) {
	if r.Client == nil || desired == nil || desired.Name == "" || desired.Namespace == "" {
		return nil, fmt.Errorf("runtime resolution requires a member client and service")
	}
	service := desired.DeepCopy()
	service.Status.PinnedRevisionName, service.Status.LastRuntimeSyncToken = "", ""
	reads := &snapshotClient{Client: r.Client, ctx: ctx, snapshots: map[string]snapshot{}}
	if err := readPinState(ctx, reads, service, standing); err != nil {
		return nil, err
	}
	model, err := readModel(ctx, reads, service)
	if err != nil {
		return nil, err
	}
	selector, err := r.runtimeSelector(ctx, reads, model.spec)
	if err != nil {
		return nil, err
	}
	chosen, err := r.chooseRuntime(ctx, reads, selector, service, model.spec)
	if err != nil {
		return nil, err
	}
	if chosen.spec == nil {
		return nil, fmt.Errorf("runtime resolution returned no spec")
	}
	out := &Runtime{Name: chosen.name, IsCluster: chosen.isCluster, Spec: chosen.spec, Model: model.spec, ModelMeta: model.meta, reads: reads}
	if err := renderSpecs(ctx, reads, selector, service, model, out); err != nil {
		return nil, err
	}
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// resolvedModel is the service's base model as the member reads it: the spec,
// its object metadata and its status, all nil when the service names no model.
type resolvedModel struct {
	spec   *v1beta1.BaseModelSpec
	meta   *metav1.ObjectMeta
	status *v1beta1.ModelStatusSpec
}

// readModel reads the service's model through the dependency-recording client.
// A sharded model that is not ready is reported with the reason it waits on.
func readModel(ctx context.Context, reads client.Client, service *v1beta1.InferenceService) (resolvedModel, error) {
	spec, meta, status, err := render.ResolveModel(ctx, render.Inputs{Service: service, Client: reads})
	if err != nil {
		var notReady *render.ModelNotReadyError
		if errors.As(err, &notReady) {
			return resolvedModel{}, fmt.Errorf("sharded model is not ready: %s", notReady.Message)
		}
		return resolvedModel{}, err
	}
	return resolvedModel{spec: spec, meta: meta, status: status}, nil
}

// runtimeSelector builds the member's catalog reader.
func (r Resolver) runtimeSelector(ctx context.Context, reads client.Client, model *v1beta1.BaseModelSpec) (runtimeselector.Selector, error) {
	return runtimeselector.NewWithConfig(runtimeselector.NewConfig(reads)), nil
}

// runtimeChoice is the runtime the member would deploy: its name, scope and spec.
type runtimeChoice struct {
	name      string
	isCluster bool
	spec      *v1beta1.ServingRuntimeSpec
}

// chooseRuntime follows the service's reference when it names a runtime, and
// otherwise takes the first compatible runtime of the ranked catalog, which is
// the choice the member itself makes.
func (r Resolver) chooseRuntime(ctx context.Context, reads client.Client, selector runtimeselector.Selector, service *v1beta1.InferenceService, model *v1beta1.BaseModelSpec) (runtimeChoice, error) {
	ref := service.Spec.Runtime
	switch {
	case ref != nil && ref.Name != "":
		return r.referencedRuntime(ctx, reads, selector, service, model)
	case model != nil:
		matches, err := selector.GetCompatibleRuntimes(ctx, model, service, service.Namespace)
		if err != nil {
			return runtimeChoice{}, err
		}
		if len(matches) == 0 {
			return runtimeChoice{}, fmt.Errorf("no compatible member runtime is available")
		}
		return runtimeChoice{name: matches[0].Name, isCluster: matches[0].IsCluster, spec: matches[0].Spec}, nil
	default:
		return runtimeChoice{}, fmt.Errorf("runtime resolution requires a runtime or model reference")
	}
}

// referencedRuntime resolves the runtime the service names. With autoSync off
// the member serves the revision it recorded; otherwise it serves the live
// runtime, after checking that the runtime may serve the model.
func (r Resolver) referencedRuntime(ctx context.Context, reads client.Client, selector runtimeselector.Selector, service *v1beta1.InferenceService, model *v1beta1.BaseModelSpec) (runtimeChoice, error) {
	ref := service.Spec.Runtime
	choice := runtimeChoice{name: ref.Name}
	var err error
	if ref.AutoSync != nil && !*ref.AutoSync {
		choice.spec, choice.isCluster, err = resolvePin(ctx, reads, selector, service, r.OperatorNamespace)
	} else {
		if err := validateLiveRuntime(ctx, selector, ref.Name, service, model); err != nil {
			return runtimeChoice{}, err
		}
		choice.spec, choice.isCluster, err = selector.GetRuntime(ctx, ref.Name, service.Namespace, runtimeselector.RefKind(ref))
	}
	if err != nil {
		return runtimeChoice{}, err
	}
	return choice, nil
}

// validateLiveRuntime checks a named runtime against the model. A declared
// format mismatch is accepted because the operator chose the runtime, except
// for a sharded model, whose runtime must serve the configured model cache
// provider. Every other validation error stands.
func validateLiveRuntime(ctx context.Context, selector runtimeselector.Selector, name string, service *v1beta1.InferenceService, model *v1beta1.BaseModelSpec) error {
	if model == nil {
		return nil
	}
	err := selector.ValidateRuntime(ctx, name, model, service)
	if err != nil && runtimeselector.IsRuntimeCompatibilityError(err) && !isvcutils.IsShardedBaseModel(model) {
		return nil
	}
	return err
}

// renderSpecs merges the chosen runtime into the service's components the way
// the member renders them and records the runtime's revision hash. The model
// and the runtime are already read; the rendering step reads the overlays, so
// the dependency set is complete afterwards.
func renderSpecs(ctx context.Context, reads client.Client, selector runtimeselector.Selector, service *v1beta1.InferenceService, model resolvedModel, out *Runtime) error {
	resolved, err := render.Resolve(ctx, render.Inputs{
		Service: service, Client: reads, Runtimes: selector, Log: logr.Discard(),
		Model: model.spec, ModelMeta: model.meta, ModelStatus: model.status, ModelRead: true,
		OverlaysRead: true,
		Runtime:      out.Spec, RuntimeName: out.Name, RuntimeIsCluster: out.IsCluster,
	})
	if err != nil {
		return err
	}
	out.Hash, _, err = runtimerevision.Hash(out.Spec)
	if err != nil {
		return err
	}
	out.Engine, out.Decoder, out.Router = resolved.Specs.Engine, resolved.Specs.Decoder, resolved.Specs.Router
	return nil
}

func readPinState(ctx context.Context, reads client.Client, desired, standing *v1beta1.InferenceService) error {
	key := client.ObjectKeyFromObject(desired)
	if standing != nil {
		if client.ObjectKeyFromObject(standing) != key {
			return fmt.Errorf("standing member service has a different name or namespace")
		}
		if _, err := objectIdentity(standing); err != nil {
			return err
		}
	}
	current := &v1beta1.InferenceService{}
	err := reads.Get(ctx, key, current)
	if standing == nil && apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if standing == nil || current.UID != standing.UID {
		return fmt.Errorf("standing member service identity changed")
	}
	// Pin state belongs to the same observed member identity and spec. The
	// dependency check also detects pin changes delivered during resolution.
	expected := standing.DeepCopy()
	expected.Status = current.Status
	want, err := objectIdentity(expected)
	if err != nil {
		return err
	}
	got, err := objectIdentity(current)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("standing member service identity changed")
	}
	desired.Status.PinnedRevisionName = current.Status.PinnedRevisionName
	desired.Status.LastRuntimeSyncToken = current.Status.LastRuntimeSyncToken
	return nil
}

// revisionSource is what a service with autoSync off resolves against: the
// runtime it names and the scope that name is expected to have, the live
// runtime under that name when one still exists, and the revision the service
// asks for, named in its spec (explicit) or recorded in its status.
type revisionSource struct {
	ref          *v1beta1.ServingRuntimeRef
	expectedKind string
	live         *v1beta1.ServingRuntimeSpec
	isCluster    bool
	missing      bool
	liveErr      error
	revisionName string
	explicit     bool
}

// resolvePin returns the spec a member with autoSync off serves. Without a
// recorded revision that is the live runtime, from which the member creates its
// first revision. An explicit revision, or one whose live runtime is gone,
// stands as recorded; otherwise liveOrRecorded settles a changed live runtime.
func resolvePin(ctx context.Context, reads client.Client, selector runtimeselector.Selector, service *v1beta1.InferenceService, namespace string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	source, err := readRevisionSource(ctx, selector, service)
	if err != nil {
		return nil, false, err
	}
	if source.revisionName == "" {
		if source.missing {
			return nil, false, source.liveErr
		}
		return source.live, source.isCluster, nil
	}
	if namespace == "" {
		return nil, false, fmt.Errorf("runtime pin resolution requires the member operator namespace")
	}
	recorded, isCluster, err := readRecordedRevision(ctx, reads, namespace, service.Namespace, source)
	if err != nil {
		return nil, false, err
	}
	if source.explicit || source.missing {
		return recorded, isCluster, nil
	}
	return liveOrRecorded(service, source.live, recorded, isCluster)
}

// readRevisionSource reads the runtime reference of a service with autoSync
// off and the live runtime it names. An undeclared kind means the cluster
// scope, and a live runtime found under another scope is refused.
func readRevisionSource(ctx context.Context, selector runtimeselector.Selector, service *v1beta1.InferenceService) (revisionSource, error) {
	ref := service.Spec.Runtime
	expectedKind := runtimeselector.RefKind(ref)
	if expectedKind == "" {
		expectedKind = runtimeselector.KindClusterServingRuntime
	}
	if expectedKind != runtimeselector.KindServingRuntime && expectedKind != runtimeselector.KindClusterServingRuntime {
		return revisionSource{}, fmt.Errorf("runtime pin has an unverified source kind")
	}
	live, isCluster, err := selector.GetRuntime(ctx, ref.Name, service.Namespace, runtimeselector.RefKind(ref))
	missing := runtimeselector.IsRuntimeNotFoundError(err)
	if err != nil && !missing {
		return revisionSource{}, err
	}
	if !missing && isCluster != (expectedKind == runtimeselector.KindClusterServingRuntime) {
		return revisionSource{}, fmt.Errorf("runtime pin source scope differs from the resolved live runtime")
	}
	source := revisionSource{ref: ref, expectedKind: expectedKind, live: live, isCluster: isCluster, missing: missing, liveErr: err, revisionName: service.Status.PinnedRevisionName}
	if ref.Revision != nil && *ref.Revision != "" {
		source.revisionName, source.explicit = *ref.Revision, true
	}
	return source, nil
}

// readRecordedRevision reads the revision the service asks for and proves it
// was taken from the named runtime: the same runtime name, the expected kind,
// and the service's namespace for a namespaced runtime or none for a cluster one.
func readRecordedRevision(ctx context.Context, reads client.Client, operatorNamespace, serviceNamespace string, source revisionSource) (*v1beta1.ServingRuntimeSpec, bool, error) {
	rev, err := runtimerevision.FetchRevision(ctx, reads, operatorNamespace, source.revisionName)
	if err != nil {
		return nil, false, err
	}
	kind := rev.Labels[constants.RuntimeRevisionOfKindLabelKey]
	if rev.Labels[constants.RuntimeRevisionOfLabelKey] != source.ref.Name || kind != source.expectedKind {
		return nil, false, fmt.Errorf("runtime revision has unverified source identity")
	}
	expectedNamespace := ""
	if kind == runtimeselector.KindServingRuntime {
		expectedNamespace = serviceNamespace
	}
	if rev.Labels[constants.RuntimeRevisionOfNamespaceLabelKey] != expectedNamespace {
		return nil, false, fmt.Errorf("runtime revision belongs to a different namespace")
	}
	recorded, err := runtimerevision.DecodeSpec(rev)
	if err != nil {
		return nil, false, err
	}
	return recorded, kind == runtimeselector.KindClusterServingRuntime, nil
}

// liveOrRecorded decides between the live runtime and the recorded revision
// when both exist. An unchanged runtime keeps the revision. A changed one is
// taken only with a sync acknowledgement the member has not consumed yet.
func liveOrRecorded(service *v1beta1.InferenceService, live, recorded *v1beta1.ServingRuntimeSpec, isCluster bool) (*v1beta1.ServingRuntimeSpec, bool, error) {
	liveHash, _, err := runtimerevision.Hash(live)
	if err != nil {
		return nil, false, err
	}
	recordedHash, _, err := runtimerevision.Hash(recorded)
	if err != nil {
		return nil, false, err
	}
	if liveHash == recordedHash {
		return recorded, isCluster, nil
	}
	token := service.Annotations[constants.RuntimeSyncAnnotationKey]
	if token != "" && token != service.Status.LastRuntimeSyncToken {
		return live, isCluster, nil
	}
	return nil, false, fmt.Errorf("runtime pin differs from its live source without a sync acknowledgement")
}
