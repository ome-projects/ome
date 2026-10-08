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

// Resolver uses a direct client for the identified member. OperatorNamespace
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
	model, modelMeta, modelStatus, err := render.ResolveModel(ctx, render.Inputs{Service: service, Client: reads})
	if err != nil {
		var notReady *render.ModelNotReadyError
		if errors.As(err, &notReady) {
			return nil, fmt.Errorf("sharded model is not ready: %s", notReady.Message)
		}
		return nil, err
	}
	config := runtimeselector.NewConfig(reads)
	selector := runtimeselector.NewWithConfig(config)
	out := &Runtime{Model: model, ModelMeta: modelMeta, reads: reads}
	ref := service.Spec.Runtime
	switch {
	case ref != nil && ref.Name != "":
		out.Name = ref.Name
		if ref.AutoSync != nil && !*ref.AutoSync {
			out.Spec, out.IsCluster, err = resolvePin(ctx, reads, selector, service, r.OperatorNamespace)
		} else {
			if model != nil {
				validationErr := selector.ValidateRuntime(ctx, ref.Name, model, service)
				if validationErr != nil && !(runtimeselector.IsRuntimeCompatibilityError(validationErr) && !isvcutils.IsShardedBaseModel(model)) {
					return nil, validationErr
				}
			}
			out.Spec, out.IsCluster, err = selector.GetRuntime(ctx, ref.Name, service.Namespace, runtimeselector.RefKind(ref))
		}
	case model != nil:
		var matches []runtimeselector.RuntimeMatch
		matches, err = selector.GetCompatibleRuntimes(ctx, model, service, service.Namespace)
		if err == nil && len(matches) == 0 {
			return nil, fmt.Errorf("no compatible member runtime is available")
		}
		if err == nil {
			// The member selects the first result from this same ranked catalog.
			out.Spec, out.Name, out.IsCluster = matches[0].Spec, matches[0].Name, matches[0].IsCluster
		}
	default:
		return nil, fmt.Errorf("runtime resolution requires a runtime or model reference")
	}
	if err != nil {
		return nil, err
	}
	if out.Spec == nil {
		return nil, fmt.Errorf("runtime resolution returned no spec")
	}
	// The model and the runtime are already read; overlays are read by the
	// rendering step, so the preflight's dependency set is the runtime inputs.
	resolved, err := render.Resolve(ctx, render.Inputs{
		Service: service, Client: reads, Runtimes: selector, Log: logr.Discard(),
		Model: model, ModelMeta: modelMeta, ModelStatus: modelStatus, ModelRead: true,
		OverlaysRead: true,
		Runtime:      out.Spec, RuntimeName: out.Name, RuntimeIsCluster: out.IsCluster,
	})
	if err != nil {
		return nil, err
	}
	out.Hash, _, err = runtimerevision.Hash(out.Spec)
	if err != nil {
		return nil, err
	}
	out.Engine, out.Decoder, out.Router = resolved.Specs.Engine, resolved.Specs.Decoder, resolved.Specs.Router
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
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
	// Pin state comes from the live read; the supplied observation fences the
	// member's spec and metadata. The snapshot rechecks the live pin separately.
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

func resolvePin(ctx context.Context, reads client.Client, selector runtimeselector.Selector, service *v1beta1.InferenceService, namespace string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	ref := service.Spec.Runtime
	expectedKind := runtimeselector.RefKind(ref)
	if expectedKind == "" {
		// Member pinning uses the CRD's cluster scope for an undeclared kind.
		expectedKind = runtimeselector.KindClusterServingRuntime
	}
	if expectedKind != runtimeselector.KindServingRuntime && expectedKind != runtimeselector.KindClusterServingRuntime {
		return nil, false, fmt.Errorf("runtime pin has an unverified source kind")
	}
	live, isCluster, err := selector.GetRuntime(ctx, ref.Name, service.Namespace, runtimeselector.RefKind(ref))
	missing := runtimeselector.IsRuntimeNotFoundError(err)
	if err != nil && !missing {
		return nil, false, err
	}
	if !missing && isCluster != (expectedKind == runtimeselector.KindClusterServingRuntime) {
		return nil, false, fmt.Errorf("runtime pin source scope differs from the resolved live runtime")
	}
	pinName := service.Status.PinnedRevisionName
	explicit := ref.Revision != nil && *ref.Revision != ""
	if explicit {
		pinName = *ref.Revision
	}
	if pinName == "" {
		if missing {
			return nil, false, err
		}
		// The member creates its initial revision from this exact live spec.
		return live, isCluster, nil
	}
	if namespace == "" {
		return nil, false, fmt.Errorf("runtime pin resolution requires the member operator namespace")
	}
	rev, err := runtimerevision.FetchRevision(ctx, reads, namespace, pinName)
	if err != nil {
		return nil, false, err
	}
	kind := rev.Labels[constants.RuntimeRevisionOfKindLabelKey]
	if rev.Labels[constants.RuntimeRevisionOfLabelKey] != ref.Name ||
		kind != expectedKind {
		return nil, false, fmt.Errorf("runtime revision has unverified source identity")
	}
	expectedNamespace := ""
	if kind == runtimeselector.KindServingRuntime {
		expectedNamespace = service.Namespace
	}
	if rev.Labels[constants.RuntimeRevisionOfNamespaceLabelKey] != expectedNamespace {
		return nil, false, fmt.Errorf("runtime revision belongs to a different namespace")
	}
	isCluster = kind == runtimeselector.KindClusterServingRuntime
	pinned, err := runtimerevision.DecodeSpec(rev)
	if err != nil {
		return nil, false, err
	}
	if explicit || missing {
		return pinned, isCluster, nil
	}
	liveHash, _, err := runtimerevision.Hash(live)
	if err != nil {
		return nil, false, err
	}
	pinHash, _, err := runtimerevision.Hash(pinned)
	if err != nil {
		return nil, false, err
	}
	if liveHash == pinHash {
		return pinned, isCluster, nil
	}
	token := service.Annotations[constants.RuntimeSyncAnnotationKey]
	if token != "" && token != service.Status.LastRuntimeSyncToken {
		return live, isCluster, nil
	}
	return nil, false, fmt.Errorf("runtime pin differs from its live source without a sync acknowledgement")
}
