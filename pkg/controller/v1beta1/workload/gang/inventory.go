package gang

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

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

// PodGroupInventoryEntry identifies one owned PodGroup by optional Instance
// index and object name. Name remains authoritative when a damaged object has
// lost or malformed its controller-owned index label.
type PodGroupInventoryEntry struct {
	Index      int32
	IndexKnown bool
	Name       string
	PodGroup   *schedulingv1alpha1.PodGroup
}

// PodGroupInventory is the PodGroups of one namespace partitioned for one
// controller owner, as one reader listed them. The apiserver's picture is
// one namespace LIST that retains another owner's objects by name, so
// ensure can detect a deterministic-name collision without a per-Instance
// GET. The informer cache's picture holds this owner's groups alone, may
// be behind the apiserver, and settles a gang whose group needs no write;
// a write, reset or delete decides on the apiserver's picture, which
// ReadLive brings.
type PodGroupInventory struct {
	available bool
	ownerUID  types.UID
	namespace string
	// cached is set while the picture is the informer cache's.
	cached    bool
	byName    map[string]*schedulingv1alpha1.PodGroup
	ownedName map[string]*schedulingv1alpha1.PodGroup
	owned     []PodGroupInventoryEntry
	accepted  map[string]struct{}
}

// ErrInventoryCached is returned by a write or a delete asked to decide on
// the informer cache's picture of the PodGroups: ReadLive comes first.
var ErrInventoryCached = errors.New("the PodGroup inventory is the cache's picture; a write, reset or delete decides on the apiserver's")

// ObservePodGroups performs exactly one authoritative namespace LIST and
// builds an owner-scoped inventory. A namespace-wide read is intentional:
// labels are mutable, while controller UID is the teardown ownership proof.
// Runtime removal of the optional PodGroup API yields an unavailable inventory
// rather than turning that optional API into a lifecycle blocker.
func ObservePodGroups(ctx context.Context, reader client.Reader, owner client.Object) (*PodGroupInventory, error) {
	inv, err := newPodGroupInventoryOf(owner, reader, "ObservePodGroups")
	if err != nil {
		return nil, err
	}
	if err := inv.read(ctx, reader, client.InNamespace(inv.namespace)); err != nil {
		return nil, fmt.Errorf("ObservePodGroups: list namespace %s: %w", inv.namespace, err)
	}
	return inv, nil
}

// ObserveCachedPodGroups reads the PodGroups owner controls from the
// informer cache, through the controller-UID index, into an inventory that
// may be behind the apiserver. A group it shows matching the plan settles
// its gang: this controller wrote that group, and a copy behind a write is
// repaired by the pass the write's event brings. A name it shows free and
// a group it shows drifted are writes to decide, and a write decides on
// the apiserver's picture, read by ReadLive. Another owner's object at one
// of this owner's names is not in it, so such a collision is found on the
// live read the free name brings. Runtime removal of the optional PodGroup
// API yields an unavailable inventory.
func ObserveCachedPodGroups(ctx context.Context, cache client.Reader, owner client.Object) (*PodGroupInventory, error) {
	inv, err := newPodGroupInventoryOf(owner, cache, "ObserveCachedPodGroups")
	if err != nil {
		return nil, err
	}
	inv.cached = true
	if err := inv.read(ctx, cache, client.InNamespace(inv.namespace),
		client.MatchingFields{PodGroupControllerUIDIndexField: string(inv.ownerUID)}); err != nil {
		return nil, fmt.Errorf("ObserveCachedPodGroups: list owner %s: %w", inv.ownerUID, err)
	}
	return inv, nil
}

// ReadLive replaces the inventory's picture with the apiserver's: one
// namespace LIST through reader, the reading every write, reset and delete
// decides on. A no-op on an inventory that is the apiserver's already. The
// deletes accepted this pass stay accepted.
func (i *PodGroupInventory) ReadLive(ctx context.Context, reader client.Reader) error {
	if i == nil || !i.cached {
		return nil
	}
	if reader == nil {
		return fmt.Errorf("ReadLive: nil reader")
	}
	if err := i.read(ctx, reader, client.InNamespace(i.namespace)); err != nil {
		return fmt.Errorf("ReadLive: list namespace %s: %w", i.namespace, err)
	}
	i.cached = false
	return nil
}

// Cached reports whether the inventory is the informer cache's picture,
// which no write, reset or delete decides on.
func (i *PodGroupInventory) Cached() bool {
	return i != nil && i.cached
}

// newPodGroupInventoryOf is an empty inventory for owner, available until a
// read says otherwise; caller names the function an error is reported for.
func newPodGroupInventoryOf(owner client.Object, reader client.Reader, caller string) (*PodGroupInventory, error) {
	if reader == nil {
		return nil, fmt.Errorf("%s: nil reader", caller)
	}
	if owner == nil {
		return nil, fmt.Errorf("%s: nil owner", caller)
	}
	if owner.GetUID() == "" {
		return nil, fmt.Errorf("%s: owner %s/%s has no UID", caller, owner.GetNamespace(), owner.GetName())
	}
	inv := newPodGroupInventory(owner.GetUID(), true)
	inv.namespace = owner.GetNamespace()
	return inv, nil
}

// read replaces the picture with what reader lists under opts; the listed
// objects are the inventory's own, so none is copied again, and the maps
// and the entries are sized to the list once, so a read costs the same
// few allocations whatever the group count. An API that answers NotFound
// leaves an unavailable inventory holding nothing.
func (i *PodGroupInventory) read(ctx context.Context, reader client.Reader, opts ...client.ListOption) error {
	list := &schedulingv1alpha1.PodGroupList{}
	err := reader.List(ctx, list, opts...)
	owned := ownedCount(list.Items, i.ownerUID)
	i.available = true
	i.byName = make(map[string]*schedulingv1alpha1.PodGroup, len(list.Items))
	i.ownedName = make(map[string]*schedulingv1alpha1.PodGroup, owned)
	i.owned = nil
	switch {
	case podgroup.IsNotFound(err):
		i.available = false
		return nil
	case err != nil:
		return err
	}
	if owned > 0 {
		i.owned = make([]PodGroupInventoryEntry, 0, owned)
	}
	for idx := range list.Items {
		pg := &list.Items[idx]
		i.byName[pg.Name] = pg
		if !podgroup.ControlledByUID(pg, i.ownerUID) {
			continue
		}
		i.ownedName[pg.Name] = pg
		entry := PodGroupInventoryEntry{Name: pg.Name, PodGroup: pg}
		if index, ok := podGroupIndex(pg); ok {
			entry.Index = index
			entry.IndexKnown = true
		}
		i.owned = append(i.owned, entry)
	}
	slices.SortFunc(i.owned, func(a, b PodGroupInventoryEntry) int { return strings.Compare(a.Name, b.Name) })
	return nil
}

// ownedCount is how many of the listed groups owner controls.
func ownedCount(groups []schedulingv1alpha1.PodGroup, owner types.UID) int {
	n := 0
	for idx := range groups {
		if podgroup.ControlledByUID(&groups[idx], owner) {
			n++
		}
	}
	return n
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
// resource set; ensure still reports it as a collision through ByName. The
// delete decides on the apiserver's picture, never the cache's.
func (i *PodGroupInventory) DeleteOwnedName(ctx context.Context, c client.Client, name string) error {
	if i == nil || !i.available {
		return nil
	}
	if i.cached {
		return fmt.Errorf("delete %s: %w", name, ErrInventoryCached)
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
// it absent. A foreign occupant is absent from this owner's resource set. The
// finalization decides on the apiserver's picture, never the cache's.
func (i *PodGroupInventory) FinalizeOwnedName(ctx context.Context, c client.Client, name string) (bool, error) {
	if i == nil || !i.available {
		return true, nil
	}
	if i.cached {
		return false, fmt.Errorf("finalize %s: %w", name, ErrInventoryCached)
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
// the shared authoritative Pod bucket for idx is empty. An inventory the
// cache served is read live through reader before the first finalization.
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
		if err := inv.ReadLive(ctx, reader); err != nil {
			return false, fmt.Errorf("FinalizeInstanceResources: %w", err)
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
