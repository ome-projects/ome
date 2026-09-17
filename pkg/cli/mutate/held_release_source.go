package mutate

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var ErrHeldRelease = errors.New("action refused: no exact current valid Held retry block is releasable")
var ErrHeldMailbox = errors.New("action refused: a release-held mailbox is already present; inspect instance retry-blocks")

// HeldReleaseEvidence admits bounded original API objects, not read reports.
// Its private defensive copies never enter a public output schema.
type HeldReleaseEvidence struct {
	parent   *v1beta1.InferenceService
	items    []v1beta1.InferenceReplica
	selected int
	pages    int
}

func (HeldReleaseEvidence) String() string   { return "<HeldReleaseEvidence redacted>" }
func (HeldReleaseEvidence) GoString() string { return "<HeldReleaseEvidence redacted>" }

// CollectHeldReleaseEvidence checks the whole finite membership before making
// defensive copies. Typed clients decode first; these are post-decode budgets,
// not a claim of hard wire allocation bounds on generated clients.
func CollectHeldReleaseEvidence(ctx context.Context, client omeclient.OmeV1beta1Interface, parent *v1beta1.InferenceService, component string, clock reportv1alpha1.Clock, requestTimeout ...time.Duration) (HeldReleaseEvidence, error) {
	if err := ValidateTarget(parent); err != nil {
		return HeldReleaseEvidence{}, err
	}
	if ctx == nil || client == nil || !slices.Contains([]string{"engine", "decoder", "router"}, component) {
		return HeldReleaseEvidence{}, ErrHeldRelease
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	base := metav1.ListOptions{}
	timeout := heldRequestTimeout(requestTimeout...)
	if len(parent.Name) <= 63 {
		base.LabelSelector = labels.Set{constants.InferenceServiceLabel: parent.Name}.AsSelector().String()
	}
	list, err := paging.ListBounded(ctx, base, paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: timeout}, func(call context.Context, opts metav1.ListOptions) (paging.Page[v1beta1.InferenceReplica], error) {
		value, e := client.InferenceReplicas(parent.Namespace).List(call, opts)
		if call.Err() != nil {
			return paging.Page[v1beta1.InferenceReplica]{}, call.Err()
		}
		if e != nil {
			return paging.Page[v1beta1.InferenceReplica]{}, SafeAPIError(e)
		}
		if value == nil || value.Kind != "" && value.Kind != "InferenceReplicaList" || value.APIVersion != "" && value.APIVersion != "ome.io/v1beta1" {
			return paging.Page[v1beta1.InferenceReplica]{}, ErrHeldRelease
		}
		if len(value.Items) > 16 || !boundedPrivatePayload(value) {
			return paging.Page[v1beta1.InferenceReplica]{}, ErrBounds
		}
		return paging.Page[v1beta1.InferenceReplica]{Items: value.Items, Continue: value.Continue}, nil
	})
	if err != nil {
		return HeldReleaseEvidence{}, SafeAPIError(err)
	}
	if list.Truncated || !boundedPrivatePayload(struct {
		Parent *v1beta1.InferenceService
		Items  []v1beta1.InferenceReplica
	}{parent, list.Items}) {
		return HeldReleaseEvidence{}, ErrBounds
	}
	result := HeldReleaseEvidence{selected: -1, pages: list.Pages}
	seenComponents, seenUIDs, seenNames := map[v1beta1.ComponentType]bool{}, map[string]bool{}, map[string]bool{}
	for i := range list.Items {
		ir := &list.Items[i]
		if !boundedPrivatePayload(ir) || len(ir.OwnerReferences) > 16 {
			return HeldReleaseEvidence{}, ErrBounds
		}
		claims := ir.Spec.ParentRef.Name == parent.Name || ir.Labels[constants.InferenceServiceLabel] == parent.Name
		for _, ref := range ir.OwnerReferences {
			if ref.UID == parent.UID && ref.Controller != nil && *ref.Controller {
				claims = true
			}
		}
		if !claims && base.LabelSelector == "" {
			continue
		}
		if base.LabelSelector != "" && ir.Labels[constants.InferenceServiceLabel] != parent.Name {
			return HeldReleaseEvidence{}, ErrHeldRelease
		}
		if err = validateHeldReplicaIdentity(ir, parent); err != nil {
			return HeldReleaseEvidence{}, err
		}
		if seenComponents[ir.Spec.Component] || seenUIDs[string(ir.UID)] || seenNames[ir.Name] {
			return HeldReleaseEvidence{}, ErrHeldRelease
		}
		seenComponents[ir.Spec.Component], seenUIDs[string(ir.UID)], seenNames[ir.Name] = true, true, true
		if string(ir.Spec.Component) == component {
			if err = validateHeldBlocks(ir, parent, clock.Now()); err != nil {
				return HeldReleaseEvidence{}, err
			}
		}
		result.items = append(result.items, *ir.DeepCopy())
	}
	sort.Slice(result.items, func(i, j int) bool { return result.items[i].Name < result.items[j].Name })
	for i := range result.items {
		if string(result.items[i].Spec.Component) == component {
			result.selected = i
		}
	}
	if result.selected < 0 {
		return HeldReleaseEvidence{}, ErrHeldRelease
	}
	// Bind the selected list member to one authoritative exact GET. A changed
	// selected object invalidates this pass instead of replanning it.
	selected := &result.items[result.selected]
	call, cancel := context.WithTimeout(ctx, timeout)
	exact, getErr := client.InferenceReplicas(parent.Namespace).Get(call, selected.Name, metav1.GetOptions{})
	callErr := call.Err()
	cancel()
	if callErr != nil {
		return HeldReleaseEvidence{}, callErr
	}
	if getErr != nil {
		return HeldReleaseEvidence{}, SafeAPIError(getErr)
	}
	if exact == nil || !boundedPrivatePayload(exact) {
		return HeldReleaseEvidence{}, ErrBounds
	}
	if err = validateHeldReplicaIdentity(exact, parent); err != nil {
		return HeldReleaseEvidence{}, err
	}
	if err = validateHeldBlocks(exact, parent, clock.Now()); err != nil {
		return HeldReleaseEvidence{}, err
	}
	// Generated clients may clear GVK independently of list decoding.
	copyExact := exact.DeepCopy()
	copyExact.TypeMeta = selected.TypeMeta
	if !reflect.DeepEqual(copyExact, selected) {
		return HeldReleaseEvidence{}, ErrStale
	}
	if ctx.Err() != nil {
		return HeldReleaseEvidence{}, ctx.Err()
	}
	result.parent = parent.DeepCopy()
	return result, nil
}

func validateHeldReplicaIdentity(ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService) error {
	if ir.Kind != "" && ir.Kind != "InferenceReplica" || ir.APIVersion != "" && ir.APIVersion != "ome.io/v1beta1" || !SafeScalar(ir.Name) || len(validation.IsDNS1123Subdomain(ir.Name)) != 0 || ir.Namespace != parent.Namespace || !SafeScalar(string(ir.UID)) || !SafeScalar(ir.ResourceVersion) || ir.Spec.ParentRef.Name != parent.Name || ir.Generation <= 0 || ir.DeletionTimestamp != nil {
		return ErrHeldRelease
	}
	if len(ir.Annotations) > 256 || len(ir.Labels) > 256 || len(ir.Finalizers) > 64 || len(ir.OwnerReferences) > 16 || len(ir.Status.Conditions) > 64 || len(ir.Status.InstanceStatuses) > 2048 || len(ir.Status.Migrations) > 256 || !replicaPayloadBounded(ir) {
		return ErrBounds
	}
	if _, err := normalizedActionReplica(ir); err != nil {
		return err
	}
	for _, key := range []string{"ome.io/accelerator-requirements", "ome.io/cluster-selector", constants.PlacementOrigin, constants.PlacementOriginUID, constants.PlacementControlPlane} {
		if _, exists := ir.Annotations[key]; exists {
			return ErrPlacement
		}
		if _, exists := ir.Labels[key]; exists {
			return ErrPlacement
		}
	}
	for _, finalizer := range ir.Finalizers {
		if finalizer == "ome.io/placement" {
			return ErrPlacement
		}
	}
	controllers := 0
	for _, ref := range ir.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			controllers++
			if ref.APIVersion != "ome.io/v1beta1" || ref.Kind != "InferenceService" || ref.Name != parent.Name || ref.UID != parent.UID {
				return ErrHeldRelease
			}
		}
	}
	if controllers != 1 || !slices.Contains([]v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}, ir.Spec.Component) || ir.Status.ObservedGeneration != ir.Generation || ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] != strconv.FormatInt(parent.Generation, 10) {
		return ErrHeldRelease
	}
	return nil
}

func validateHeldBlocks(ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, now time.Time) error {
	if ir.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] != "true" {
		return ErrHeldRelease
	}
	if _, present := ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey]; present {
		return ErrHeldMailbox
	}
	seen := map[string]bool{}
	prefix := parent.Name + "-" + string(ir.Spec.Component) + "-"
	for _, block := range ir.Status.RetryBlocks {
		if !SafeScalar(block.TargetRevision) || len(validation.IsDNS1123Subdomain(block.TargetRevision)) != 0 || len(block.TargetRevision) != len(prefix)+8 || block.TargetRevision[:len(prefix)] != prefix || !revisionHash.MatchString(block.TargetRevision[len(prefix):]) || seen[block.TargetRevision] || block.AttemptsStarted <= 0 {
			return ErrHeldRelease
		}
		seen[block.TargetRevision] = true
		switch block.State {
		case v1beta1.RetryBlockHeld:
			if block.NextRetryAt != nil {
				return ErrHeldRelease
			}
		case v1beta1.RetryBlockBackoff:
			if block.NextRetryAt == nil {
				return ErrHeldRelease
			}
		case v1beta1.RetryBlockRetryInProgress:
		default:
			return ErrHeldRelease
		}
		for _, stamp := range []*metav1.Time{block.NextRetryAt, block.FirstFailureAt, block.LastFailureAt} {
			if stamp != nil && stamp.IsZero() {
				return ErrHeldRelease
			}
		}
		for _, stamp := range []*metav1.Time{block.FirstFailureAt, block.LastFailureAt} {
			if stamp != nil && stamp.Time.After(now) {
				return ErrHeldRelease
			}
		}
		if block.FirstFailureAt != nil && block.LastFailureAt != nil && block.LastFailureAt.Before(block.FirstFailureAt) || block.NextRetryAt != nil && block.FirstFailureAt != nil && block.NextRetryAt.Before(block.FirstFailureAt) {
			return ErrHeldRelease
		}
	}
	return nil
}
