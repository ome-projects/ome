// Package sliceprovision provisions the TPU slices a workload's pods run on,
// on node pools that grant slices on demand.
//
// One slice serves one slot. The pods of a multi-pod Instance share a slice;
// a single-pod Instance has one slice per pod-naming ordinal, so a surge pod
// gets its own slice while the pod it replaces keeps its own. A slot's pods
// are created only once its slice is ready, and a node selector on the
// slice's name confines them to it. A ready slice that no pod holds, with a
// host its pods cannot be scheduled on, is released and provisioned again.
//
// Slices are cluster-scoped, so no owner reference collects them. The owner
// UID label alone makes a slice an owner's: a slice without it is never
// adopted, modified or deleted, and an owner releases every slice it holds
// before it goes away.
package sliceprovision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/tpuslice/gke"
)

const (
	// LabelOwnerUID holds the UID of the object a slice is provisioned for.
	// It is the only label that decides which slices an owner may release.
	LabelOwnerUID = "ome.io/slice-owner-uid"
	// AnnotationOwner holds the owner as <namespace>/<name>, so a slice event
	// reaches the owner's reconcile.
	AnnotationOwner = "ome.io/slice-owner"
)

// hashLength is how many hex digits of the owner hash end a slice name.
const hashLength = 8

// Slot is what one slice serves.
type Slot struct {
	Instance int32
	Ordinal  int32
}

// SlotFor is the slot of the pod at ordinal in inst. Every pod of a multi-pod
// Instance is in slot (Index, 0).
func SlotFor(inst workload.InstancePlan, ordinal int32) Slot {
	if inst.TotalPods() > 1 {
		return Slot{Instance: inst.Index}
	}
	return Slot{Instance: inst.Index, Ordinal: ordinal}
}

// Owner is the object slices are provisioned for.
type Owner struct {
	Kind      string
	Namespace string
	Name      string
	UID       types.UID
}

// OwnerOf returns the owner a slice annotates, for routing slice events.
func OwnerOf(obj client.Object) (types.NamespacedName, bool) {
	namespace, name, ok := strings.Cut(obj.GetAnnotations()[AnnotationOwner], "/")
	if !ok || namespace == "" || name == "" {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: namespace, Name: name}, true
}

// CheckConfig reports configured slice labels and annotations that would
// overwrite the ones this package sets.
func CheckConfig(cfg *controllerconfig.TPUSliceProvisioningConfig) error {
	if cfg == nil {
		return errors.New("no slice provisioning configuration")
	}
	var errs []error
	reserved := []string{LabelOwnerUID, query.LabelManagedBy, query.LabelInstanceIdx, query.LabelPodOrdinal}
	for _, key := range []string{cfg.Slice.OwnerKindLabel, cfg.Slice.OwnerNameLabel} {
		for _, r := range reserved {
			if key == r {
				errs = append(errs, fmt.Errorf("slice label %s is set by the controller", key))
			}
		}
	}
	if _, ok := cfg.Slice.Annotations[AnnotationOwner]; ok {
		errs = append(errs, fmt.Errorf("slice annotation %s is set by the controller", AnnotationOwner))
	}
	return errors.Join(errs...)
}

// Provisioner provisions and releases one owner's slices.
type Provisioner struct {
	cfg *controllerconfig.TPUSliceProvisioningConfig
	// cached serves reads that are corrected by a later reconcile; live
	// serves reads that decide whether pods may be created or a release is
	// complete.
	cached *gke.Client
	live   *gke.Client
	owner  Owner
	// ownerLabels are the labels that make a slice owner's.
	ownerLabels map[string]string
	hash        string
}

// New returns a Provisioner for owner's slices. It reads through cached,
// which may serve from an informer, and through live, which must not, and
// writes through w.
func New(cfg *controllerconfig.TPUSliceProvisioningConfig, cached, live client.Reader, w client.Writer, owner Owner) (*Provisioner, error) {
	if err := CheckConfig(cfg); err != nil {
		return nil, err
	}
	if errs := validation.IsDNS1123Subdomain(owner.Name); len(errs) > 0 {
		return nil, fmt.Errorf("owner name %q: %s", owner.Name, strings.Join(errs, "; "))
	}
	if owner.Namespace == "" {
		return nil, fmt.Errorf("owner %s: namespace must be set", owner.Name)
	}
	for _, f := range []struct{ field, value string }{{"kind", owner.Kind}, {"UID", string(owner.UID)}} {
		if f.value == "" {
			return nil, fmt.Errorf("owner %s/%s: %s must be set", owner.Namespace, owner.Name, f.field)
		}
		if errs := validation.IsValidLabelValue(f.value); len(errs) > 0 {
			return nil, fmt.Errorf("owner %s/%s: %s %q: %s", owner.Namespace, owner.Name, f.field, f.value, strings.Join(errs, "; "))
		}
	}
	sum := sha256.Sum256([]byte(owner.Namespace + "/" + owner.Name + "/" + string(owner.UID)))
	return &Provisioner{
		cfg:         cfg,
		cached:      gke.NewClient(cached, w),
		live:        gke.NewClient(live, w),
		owner:       owner,
		ownerLabels: map[string]string{LabelOwnerUID: string(owner.UID)},
		hash:        hex.EncodeToString(sum[:])[:hashLength],
	}, nil
}

// Name is the name of slot's slice: the owner name, cut to fit, then the slot
// and a hash of the owner's identity. The hash keeps names distinct across
// namespaces and across owners that reuse a name.
func (p *Provisioner) Name(slot Slot) string {
	suffix := fmt.Sprintf("-%d-%d-%s", slot.Instance, slot.Ordinal, p.hash)
	base := strings.ReplaceAll(p.owner.Name, ".", "-")
	if limit := validation.DNS1123LabelMaxLength - len(suffix); len(base) > limit {
		base = base[:limit]
	}
	return strings.TrimRight(base, "-") + suffix
}

func (p *Provisioner) spec(d Demand, slot Slot) gke.Spec {
	annotations := maps.Clone(p.cfg.Slice.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[AnnotationOwner] = p.owner.Namespace + "/" + p.owner.Name
	return gke.Spec{
		Name:     p.Name(slot),
		Type:     d.SliceType,
		Topology: d.Shape.Topology.String(),
		Owner:    p.ownerLabels,
		Labels: map[string]string{
			query.LabelManagedBy:       query.ManagedByOMENative,
			query.LabelInstanceIdx:     strconv.FormatInt(int64(slot.Instance), 10),
			query.LabelPodOrdinal:      strconv.FormatInt(int64(slot.Ordinal), 10),
			p.cfg.Slice.OwnerKindLabel: p.owner.Kind,
			p.cfg.Slice.OwnerNameLabel: truncateLabelValue(p.owner.Name),
		},
		Annotations: annotations,
	}
}

// truncateLabelValue cuts a DNS-1123 subdomain to a label value.
func truncateLabelValue(s string) string {
	if len(s) > validation.LabelValueMaxLength {
		s = s[:validation.LabelValueMaxLength]
	}
	return strings.TrimRight(s, "-.")
}

// Placement is whether a slot's pods may be created, and where.
type Placement struct {
	// Slice is the slot's slice as observed.
	Slice gke.Slice
	// Ready reports that the slot's pods may be created now.
	Ready bool
	// NodeSelector confines the slot's pods to the slice. Set only when
	// Ready.
	NodeSelector map[string]string
	// Reason is why the slot's pods must wait. Empty when Ready.
	Reason string
}

// Ensure makes sure slot has a slice and reports whether its pods may be
// created. It creates the slice when none exists and never modifies one that
// does: a slice of another shape holds the slot until Sweep releases it. A
// slice at the slot's name without the owner label is never used and returns
// gke.ErrOwnershipConflict.
func (p *Provisioner) Ensure(ctx context.Context, d Demand, slot Slot) (Placement, error) {
	spec := p.spec(d, slot)
	s, found, err := p.live.Get(ctx, spec.Name)
	if err != nil {
		return Placement{}, err
	}
	if !found {
		var created bool
		if s, created, err = p.live.Create(ctx, spec); err != nil {
			sliceCreateFailures.WithLabelValues(spec.Type, spec.Topology, createFailureReason(err)).Inc()
			return Placement{}, err
		}
		if created {
			slicesCreated.WithLabelValues(spec.Type, spec.Topology).Inc()
			provisionStarted(s)
		}
	}
	return p.classify(s, spec)
}

// Observe reports, from a cached read, whether slot's pods may be created. It
// never creates a slice: a slot with none must wait for Ensure.
func (p *Provisioner) Observe(ctx context.Context, d Demand, slot Slot) (Placement, error) {
	spec := p.spec(d, slot)
	s, found, err := p.cached.Get(ctx, spec.Name)
	if err != nil {
		return Placement{}, err
	}
	if !found {
		return Placement{Reason: fmt.Sprintf("slice %s is not created yet", spec.Name)}, nil
	}
	return p.classify(s, spec)
}

// classify is whether s, found at spec's name, lets the slot's pods be
// created.
func (p *Provisioner) classify(s gke.Slice, spec gke.Spec) (Placement, error) {
	if !s.OwnedBy(p.ownerLabels) {
		return Placement{}, fmt.Errorf("%w: %s", gke.ErrOwnershipConflict, s.Name)
	}
	pl := Placement{Slice: s}
	switch {
	case s.Terminating:
		pl.Reason = fmt.Sprintf("slice %s is being released", s.Name)
	case !s.Matches(spec):
		pl.Reason = fmt.Sprintf("slice %s is %s %s, want %s %s; it is replaced once no pod holds it",
			s.Name, s.Type, s.Topology, spec.Type, spec.Topology)
	case !s.Ready(p.cfg.Slice.ReadyStates):
		pl.Reason = notReadyReason(s)
	default:
		pl.Ready = true
		pl.NodeSelector = map[string]string{p.cfg.NodeLabels.Slice: s.Name}
	}
	return pl, nil
}

func notReadyReason(s gke.Slice) string {
	switch {
	case s.State == "" && len(s.PartitionIDs) == 0:
		return fmt.Sprintf("slice %s is waiting for partition assignment", s.Name)
	case s.State == "":
		return fmt.Sprintf("slice %s has partitions assigned and no state yet", s.Name)
	case s.Message == "":
		return fmt.Sprintf("slice %s is %s", s.Name, s.State)
	default:
		return fmt.Sprintf("slice %s is %s: %s", s.Name, s.State, s.Message)
	}
}

// Pinned returns the names of the slices the pods are confined to. A
// terminal pod holds no chips, so it pins nothing.
func (p *Provisioner) Pinned(pods []*corev1.Pod) map[string]struct{} {
	pinned := map[string]struct{}{}
	for _, pod := range pods {
		if pod == nil || query.IsTerminalPod(pod) {
			continue
		}
		if name := pod.Spec.NodeSelector[p.cfg.NodeLabels.Slice]; name != "" {
			pinned[name] = struct{}{}
		}
	}
	return pinned
}

// Held returns the names of the slices that pods not being deleted are
// confined to: the slices a pod still runs, or may yet run, on.
func (p *Provisioner) Held(pods []*corev1.Pod) map[string]struct{} {
	held := map[string]struct{}{}
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		if name := pod.Spec.NodeSelector[p.cfg.NodeLabels.Slice]; name != "" {
			held[name] = struct{}{}
		}
	}
	return held
}

// Fits reports whether s is the slice Ensure places slot's pods on for d.
func (p *Provisioner) Fits(s gke.Slice, d Demand, slot Slot) bool {
	return s.Name == p.Name(slot) && s.Matches(p.spec(d, slot))
}

// Holds reports whether a cached read finds any slice the owner holds.
func (p *Provisioner) Holds(ctx context.Context) (bool, error) {
	owned, err := p.cached.List(ctx, labels.SelectorFromSet(p.ownerLabels))
	if err != nil {
		return false, err
	}
	return len(owned) > 0, nil
}

// Sweep releases every owned slice that keep does not claim and no pod is
// pinned to. keep is asked only about a slice at the name of the slot its
// labels record; a nil keep claims none.
//
// pinned is called only when a slice may be released. It must read the
// owner's pods live, terminating ones included: a pinned slice's pods still
// hold its chips.
func (p *Provisioner) Sweep(ctx context.Context, keep func(Slot, gke.Slice) bool, pinned func(context.Context) (map[string]struct{}, error)) error {
	owned, err := p.cached.List(ctx, labels.SelectorFromSet(p.ownerLabels))
	if err != nil {
		return err
	}
	var candidates []gke.Slice
	for _, s := range owned {
		if s.Terminating {
			continue
		}
		if slot, ok := slotOf(s); ok && s.Name == p.Name(slot) && keep != nil && keep(slot, s) {
			continue
		}
		candidates = append(candidates, s)
	}
	if len(candidates) == 0 {
		return nil
	}
	if pinned == nil {
		return errors.New("sweep slices: no source of pinned slices")
	}
	held, err := pinned(ctx)
	if err != nil {
		return fmt.Errorf("sweep slices: %w", err)
	}
	var errs []error
	for _, s := range candidates {
		if _, ok := held[s.Name]; ok {
			continue
		}
		if _, err := p.cached.Release(ctx, s, p.ownerLabels); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// slotOf reads the slot a slice's labels record.
func slotOf(s gke.Slice) (Slot, bool) {
	idx, err := strconv.ParseInt(s.Labels[query.LabelInstanceIdx], 10, 32)
	if err != nil {
		return Slot{}, false
	}
	ord, err := strconv.ParseInt(s.Labels[query.LabelPodOrdinal], 10, 32)
	if err != nil {
		return Slot{}, false
	}
	return Slot{Instance: int32(idx), Ordinal: int32(ord)}, true
}

// ReleaseInstance releases every slice held for Instance idx. The caller
// must have observed that the Instance has no pods. complete is true only
// when a live read finds none of its slices left.
func (p *Provisioner) ReleaseInstance(ctx context.Context, idx int32) (complete bool, err error) {
	set := maps.Clone(p.ownerLabels)
	set[query.LabelInstanceIdx] = strconv.FormatInt(int64(idx), 10)
	return p.release(ctx, labels.SelectorFromSet(set))
}

// ReleaseAll releases every slice the owner holds. The caller must have
// observed that the owner has no pods. complete is true only when a live read
// finds none left.
func (p *Provisioner) ReleaseAll(ctx context.Context) (complete bool, err error) {
	return p.release(ctx, labels.SelectorFromSet(p.ownerLabels))
}

func (p *Provisioner) release(ctx context.Context, selector labels.Selector) (bool, error) {
	held, err := p.live.List(ctx, selector)
	if err != nil {
		return false, err
	}
	complete := true
	var errs []error
	for _, s := range held {
		gone, err := p.live.Release(ctx, s, p.ownerLabels)
		if err != nil {
			errs = append(errs, err)
		}
		complete = complete && gone
	}
	if len(errs) > 0 {
		return false, errors.Join(errs...)
	}
	return complete, nil
}
