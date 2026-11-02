package gang

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// PodGroupControllerUIDIndexField is the controller-runtime cache field index
// keyed by a PodGroup's controller OwnerReference UID. Unlike the mutable
// workload labels, the controller UID is the ownership proof used by cleanup.
// The field name is an internal cache identifier, not a Kubernetes field.
const PodGroupControllerUIDIndexField = "ome.io/podgroup-controller-uid"

// PodGroupControllerUIDIndexExtractor indexes PodGroups by controller UID.
// Objects without a controller OwnerReference are intentionally unindexed.
func PodGroupControllerUIDIndexExtractor(obj client.Object) []string {
	pg, ok := obj.(*schedulingv1alpha1.PodGroup)
	if !ok {
		return nil
	}
	controller := metav1.GetControllerOfNoCopy(pg)
	if controller == nil || controller.UID == "" {
		return nil
	}
	return []string{string(controller.UID)}
}

// RegisterPodGroupControllerUIDIndex installs the ownership index on the
// manager cache. It must run before the manager starts.
func RegisterPodGroupControllerUIDIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &schedulingv1alpha1.PodGroup{},
		PodGroupControllerUIDIndexField, PodGroupControllerUIDIndexExtractor)
}

// CachedOwnerHasPodGroups reports whether the watch-backed cache contains any
// PodGroup controlled by owner. It is a cheap hint only: callers that receive
// true still take an authoritative inventory before topology or delete work.
func CachedOwnerHasPodGroups(ctx context.Context, reader client.Reader, owner client.Object) (bool, error) {
	if reader == nil {
		return false, fmt.Errorf("CachedOwnerHasPodGroups: nil reader")
	}
	if owner == nil {
		return false, fmt.Errorf("CachedOwnerHasPodGroups: nil owner")
	}
	if owner.GetUID() == "" {
		return false, fmt.Errorf("CachedOwnerHasPodGroups: owner %s/%s has no UID", owner.GetNamespace(), owner.GetName())
	}
	list := &schedulingv1alpha1.PodGroupList{}
	if err := reader.List(ctx, list,
		client.InNamespace(owner.GetNamespace()),
		client.MatchingFields{PodGroupControllerUIDIndexField: string(owner.GetUID())}); err != nil {
		if podgroup.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("CachedOwnerHasPodGroups: list owner %s: %w", owner.GetUID(), err)
	}
	return len(list.Items) > 0, nil
}

// PodGroupInventoryEntry identifies one owned PodGroup by optional Instance
// index and object name. Name remains authoritative when a damaged object has
// lost or malformed its controller-owned index label.
type PodGroupInventoryEntry struct {
	Index      int32
	IndexKnown bool
	Name       string
	PodGroup   *schedulingv1alpha1.PodGroup
}

// PodGroupInventory is one authoritative namespace LIST partitioned for one
// controller owner. It retains foreign objects by name so ensure can detect a
// deterministic-name collision without a per-Instance GET.
type PodGroupInventory struct {
	available bool
	ownerUID  types.UID
	byName    map[string]*schedulingv1alpha1.PodGroup
	ownedName map[string]*schedulingv1alpha1.PodGroup
	owned     []PodGroupInventoryEntry
	accepted  map[string]struct{}
}

// ObservePodGroups performs exactly one authoritative namespace LIST and
// builds an owner-scoped inventory. A namespace-wide read is intentional:
// labels are mutable, while controller UID is the teardown ownership proof.
// Runtime removal of the optional PodGroup API yields an unavailable inventory
// rather than turning that optional API into a lifecycle blocker.
func ObservePodGroups(ctx context.Context, reader client.Reader, owner client.Object) (*PodGroupInventory, error) {
	if reader == nil {
		return nil, fmt.Errorf("ObservePodGroups: nil reader")
	}
	if owner == nil {
		return nil, fmt.Errorf("ObservePodGroups: nil owner")
	}
	if owner.GetUID() == "" {
		return nil, fmt.Errorf("ObservePodGroups: owner %s/%s has no UID", owner.GetNamespace(), owner.GetName())
	}
	inv := newPodGroupInventory(owner.GetUID(), true)
	list := &schedulingv1alpha1.PodGroupList{}
	if err := reader.List(ctx, list, client.InNamespace(owner.GetNamespace())); err != nil {
		if podgroup.IsNotFound(err) {
			return newPodGroupInventory(owner.GetUID(), false), nil
		}
		return nil, fmt.Errorf("ObservePodGroups: list namespace %s: %w", owner.GetNamespace(), err)
	}
	for i := range list.Items {
		pg := list.Items[i].DeepCopy()
		inv.byName[pg.Name] = pg
		if !podgroup.ControlledByUID(pg, inv.ownerUID) {
			continue
		}
		inv.ownedName[pg.Name] = pg
		entry := PodGroupInventoryEntry{Name: pg.Name, PodGroup: pg}
		if idx, ok := podGroupIndex(pg); ok {
			entry.Index = idx
			entry.IndexKnown = true
		}
		inv.owned = append(inv.owned, entry)
	}
	sort.Slice(inv.owned, func(i, j int) bool { return inv.owned[i].Name < inv.owned[j].Name })
	return inv, nil
}

func newPodGroupInventory(ownerUID types.UID, available bool) *PodGroupInventory {
	return &PodGroupInventory{
		available: available,
		ownerUID:  ownerUID,
		byName:    make(map[string]*schedulingv1alpha1.PodGroup),
		ownedName: make(map[string]*schedulingv1alpha1.PodGroup),
		accepted:  make(map[string]struct{}),
	}
}

// Available reports whether the optional PodGroup API answered the inventory
// LIST. False means lifecycle code follows the existing soft-fail contract.
func (i *PodGroupInventory) Available() bool {
	return i != nil && i.available
}

// OwnerUID returns the controller identity that partitions this inventory.
func (i *PodGroupInventory) OwnerUID() types.UID {
	if i == nil {
		return ""
	}
	return i.ownerUID
}

// ByName returns any object occupying name, including a foreign object.
func (i *PodGroupInventory) ByName(name string) (*schedulingv1alpha1.PodGroup, bool) {
	if i == nil {
		return nil, false
	}
	pg, found := i.byName[name]
	return pg, found
}

// recordReconciled overlays a successful same-pass create or update onto the
// immutable LIST view. Inline surge ensure then observes the write without a
// second read or a duplicate create.
func (i *PodGroupInventory) recordReconciled(pg *schedulingv1alpha1.PodGroup) {
	if i == nil || pg == nil {
		return
	}
	observed := pg.DeepCopy()
	i.byName[pg.Name] = observed
	if podgroup.ControlledByUID(observed, i.ownerUID) {
		i.ownedName[pg.Name] = observed
	}
}

// OwnedByName returns the object at name only when its controller UID matches
// the inventory owner.
func (i *PodGroupInventory) OwnedByName(name string) (*schedulingv1alpha1.PodGroup, bool) {
	if i == nil {
		return nil, false
	}
	pg, found := i.ownedName[name]
	return pg, found
}

// OwnedEntries returns all owned objects in deterministic name order.
func (i *PodGroupInventory) OwnedEntries() []PodGroupInventoryEntry {
	if i == nil {
		return nil
	}
	return append([]PodGroupInventoryEntry(nil), i.owned...)
}

// DeleteAccepted reports whether this inventory issued or observed deletion
// for name. It suppresses duplicate same-pass DELETEs; authoritative absence
// still requires a later inventory.
func (i *PodGroupInventory) DeleteAccepted(name string) bool {
	if i == nil {
		return false
	}
	_, accepted := i.accepted[name]
	return accepted
}

// DeleteOwnedName accepts deletion of one inventory-owned object. A foreign
// same-named object is intentionally treated as absent from this owner's
// resource set; ensure still reports it as a collision through ByName.
func (i *PodGroupInventory) DeleteOwnedName(ctx context.Context, c client.Client, name string) error {
	if i == nil || !i.available {
		return nil
	}
	if _, done := i.accepted[name]; done {
		return nil
	}
	pg, found := i.byName[name]
	if !found || !podgroup.ControlledByUID(pg, i.ownerUID) {
		return nil
	}
	if err := podgroup.DeleteObservedPodGroup(ctx, c, i.ownerUID, pg); err != nil {
		return err
	}
	i.accepted[name] = struct{}{}
	return nil
}

// FinalizeOwnedName requests deletion of an inventory-owned object and reports
// complete only when the authoritative inventory or a NotFound response proves
// it absent. A foreign occupant is absent from this owner's resource set.
func (i *PodGroupInventory) FinalizeOwnedName(ctx context.Context, c client.Client, name string) (bool, error) {
	if i == nil || !i.available {
		return true, nil
	}
	pg, found := i.byName[name]
	if !found || !podgroup.ControlledByUID(pg, i.ownerUID) {
		return true, nil
	}
	if _, done := i.accepted[name]; done {
		return false, nil
	}
	complete, err := podgroup.FinalizeObservedPodGroup(ctx, c, i.ownerUID, pg)
	if err != nil {
		return false, err
	}
	if !complete {
		i.accepted[name] = struct{}{}
	}
	return complete, nil
}

// BuildFinalizeInstanceResources adapts an inventory to
// ReconcileInput.FinalizeInstanceResources. The caller invokes it only after
// the shared authoritative Pod bucket for idx is empty.
func BuildFinalizeInstanceResources(
	c client.Client,
	reader client.Reader,
	inv *PodGroupInventory,
	owner client.Object,
	ownerName string,
	component workload.ComponentType,
) func(context.Context, int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		if owner == nil {
			return false, fmt.Errorf("FinalizeInstanceResources: nil owner")
		}
		if inv == nil {
			return podgroup.FinalizePodGroup(ctx, c, reader, owner, ownerName, component, idx)
		}
		if !inv.Available() {
			return true, nil
		}
		if owner.GetUID() != inv.OwnerUID() {
			return false, fmt.Errorf("FinalizeInstanceResources: inventory owner UID %q does not match owner UID %q", inv.OwnerUID(), owner.GetUID())
		}
		name := query.PodGroupName(ownerName, component, idx)
		return inv.FinalizeOwnedName(ctx, c, name)
	}
}

func podGroupIndex(pg *schedulingv1alpha1.PodGroup) (int32, bool) {
	if pg == nil {
		return 0, false
	}
	raw, found := pg.Labels[query.LabelInstanceIdx]
	if !found {
		return 0, false
	}
	idx, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || idx < 0 {
		return 0, false
	}
	return int32(idx), true
}
