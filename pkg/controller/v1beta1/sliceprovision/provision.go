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
	"slices"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/tpuslice"
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

// EventReasonSliceReleaseDeferred is recorded when a slice is kept because
// pods of another workload hold chips on its hosts.
const EventReasonSliceReleaseDeferred workload.EventReason = "SliceReleaseDeferred"

// EventReasonSliceLost is recorded when a slice is deleted, or moved off the
// nodes its pods are bound to, from outside while pods run on it, and the
// pods are deleted so their Instance is rebuilt.
const EventReasonSliceLost workload.EventReason = "SliceLost"

// podNodeNameField selects pods by the node they are bound to.
const podNodeNameField = "spec.nodeName"

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
	if _, ok := cfg.Slice.Annotations[AnnotationOwner]; ok || slices.Contains(cfg.Slice.PodAnnotations, AnnotationOwner) {
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
	// hosts serves the live node and pod reads that decide whether a
	// release would cut another workload off from its chips, or whether a
	// slice still holds its pods' nodes; cachedHosts serves the node reads
	// a live one confirms.
	hosts       client.Reader
	cachedHosts client.Reader
	owner       Owner
	// ownerLabels are the labels that make a slice owner's.
	ownerLabels map[string]string
	hash        string
	// onDeferred, when set, is told about each release kept back by other
	// workloads' pods.
	onDeferred func(s gke.Slice, holders []string)
	// podAnnotations are the owner's pod template annotations that each
	// slice it creates carries.
	podAnnotations map[string]string
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
		hosts:       live,
		cachedHosts: cached,
		owner:       owner,
		ownerLabels: map[string]string{LabelOwnerUID: string(owner.UID)},
		hash:        hex.EncodeToString(sum[:])[:hashLength],
	}, nil
}

// SetPodAnnotations records the owner's pod template annotations: each slice
// the provisioner creates carries those whose keys slice.podAnnotations lists.
func (p *Provisioner) SetPodAnnotations(annotations map[string]string) {
	p.podAnnotations = map[string]string{}
	for _, key := range p.cfg.Slice.PodAnnotations {
		if value, ok := annotations[key]; ok {
			p.podAnnotations[key] = value
		}
	}
}

// OnReleaseDeferred sets fn to be told about each release kept back because
// pods of another workload hold chips on the slice's hosts.
func (p *Provisioner) OnReleaseDeferred(fn func(s gke.Slice, holders []string)) {
	p.onDeferred = fn
}

// Name is the name of slot's slice: the owner name, cut so the whole name fits
// gke.MaxNameLength, then the slot and a hash of the owner's identity. The hash
// keeps names distinct across namespaces and across owners that reuse a name.
func (p *Provisioner) Name(slot Slot) string {
	suffix := fmt.Sprintf("-%d-%d-%s", slot.Instance, slot.Ordinal, p.hash)
	base := strings.ReplaceAll(p.owner.Name, ".", "-")
	if limit := gke.MaxNameLength - len(suffix); len(base) > limit {
		base = base[:limit]
	}
	return strings.TrimRight(base, "-") + suffix
}

func (p *Provisioner) spec(d Demand, slot Slot) gke.Spec {
	annotations := maps.Clone(p.cfg.Slice.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	maps.Copy(annotations, p.podAnnotations)
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
	case s.ReadyStatus == string(metav1.ConditionUnknown) && s.Message != "":
		return fmt.Sprintf("slice %s is %s, but its readiness is unknown: %s", s.Name, s.State, s.Message)
	case s.ReadyStatus == string(metav1.ConditionUnknown):
		return fmt.Sprintf("slice %s is %s, but its readiness is unknown", s.Name, s.State)
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

// LostSlice is one of the owner's slices lost under its pods.
type LostSlice struct {
	Name string
	// Why is how the slice was lost, worded to follow its name.
	Why string
	// Pods are the pods, not being deleted, that ran on the lost slice.
	Pods []*corev1.Pod
}

// Lost returns, sorted by name, the owner's slices lost under pods not being
// deleted, each with the pods it was lost under:
//   - a slice that is gone or being deleted, under every such pod;
//   - a slice created after a pod, under that pod;
//   - a slice that no longer holds the nodes its pods are bound to, under
//     every pod bound to a node: the slice has no partitions, or one of those
//     nodes no longer carries its label. A pod not yet bound follows the
//     label to the slice's new hosts.
//
// The owner releases a slice only once no pod holds it and creates a pod only
// once its slice is ready, so each case is a change from outside. Pods
// confined to a name the owner does not give its slices are ignored. A cached
// read that finds a pod's slice intact is trusted; any other is confirmed
// live, since the cache may lag.
func (p *Provisioner) Lost(ctx context.Context, pods []*corev1.Pod) ([]LostSlice, error) {
	held := map[string][]*corev1.Pod{}
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		if name := pod.Spec.NodeSelector[p.cfg.NodeLabels.Slice]; p.names(name) {
			held[name] = append(held[name], pod)
		}
	}
	names := make([]string, 0, len(held))
	for name := range held {
		names = append(names, name)
	}
	sort.Strings(names)
	var lost []LostSlice
	for _, name := range names {
		l, err := p.lostUnder(ctx, p.cached, p.cachedHosts, name, held[name])
		if err != nil {
			return nil, err
		}
		if len(l.Pods) == 0 {
			continue
		}
		if l, err = p.lostUnder(ctx, p.live, p.hosts, name, held[name]); err != nil {
			return nil, err
		}
		if len(l.Pods) > 0 {
			lost = append(lost, l)
		}
	}
	return lost, nil
}

// lostUnder is how c and nodes find the slice name lost under pods, all
// confined to it; a slice found intact is lost under no pod. A slice at the
// name the owner does not hold is left alone.
func (p *Provisioner) lostUnder(ctx context.Context, c *gke.Client, nodes client.Reader, name string, pods []*corev1.Pod) (LostSlice, error) {
	l := LostSlice{Name: name}
	s, found, err := c.Get(ctx, name)
	if err != nil {
		return l, err
	}
	if !found || s.Terminating {
		l.Why, l.Pods = "was deleted", pods
		return l, nil
	}
	if !s.OwnedBy(p.ownerLabels) {
		return l, nil
	}
	for _, pod := range pods {
		if s.Created.After(pod.CreationTimestamp.Time) {
			l.Pods = append(l.Pods, pod)
		}
	}
	if len(l.Pods) > 0 {
		l.Why = "was deleted"
		return l, nil
	}
	return p.movedUnder(ctx, nodes, s, pods)
}

// movedUnder is how nodes find s moved off the nodes pods are bound to: s
// lost its partitions, or a node no longer carries its label. Every pod bound
// to a node is lost then, those on a node s still holds too: they started
// with the hosts s had before, and an Instance's pods run together.
func (p *Provisioner) movedUnder(ctx context.Context, nodes client.Reader, s gke.Slice, pods []*corev1.Pod) (LostSlice, error) {
	l := LostSlice{Name: s.Name}
	var bound []*corev1.Pod
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			bound = append(bound, pod)
		}
	}
	if len(bound) == 0 {
		return l, nil
	}
	if len(s.PartitionIDs) == 0 {
		l.Why, l.Pods = "lost its partitions", bound
		return l, nil
	}
	var left []string
	for _, pod := range bound {
		node := &corev1.Node{}
		err := nodes.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node)
		if err != nil && !apierrors.IsNotFound(err) {
			return l, fmt.Errorf("read node %s of slice %s: %w", pod.Spec.NodeName, s.Name, err)
		}
		if err != nil || node.Labels[p.cfg.NodeLabels.Slice] != s.Name {
			left = append(left, pod.Spec.NodeName)
		}
	}
	if len(left) == 0 {
		return l, nil
	}
	sort.Strings(left)
	left = slices.Compact(left)
	noun := "node"
	if len(left) > 1 {
		noun = "nodes"
	}
	l.Why, l.Pods = fmt.Sprintf("moved off %s %s", noun, strings.Join(left, ", ")), bound
	return l, nil
}

// names reports whether name is the name of one of the owner's slots.
func (p *Provisioner) names(name string) bool {
	rest, ok := strings.CutSuffix(name, "-"+p.hash)
	if !ok {
		return false
	}
	rest, ordinal, ok := cutIndex(rest)
	if !ok {
		return false
	}
	_, instance, ok := cutIndex(rest)
	return ok && p.Name(Slot{Instance: instance, Ordinal: ordinal}) == name
}

// cutIndex cuts a trailing "-<n>" from s.
func cutIndex(s string) (string, int32, bool) {
	i := strings.LastIndex(s, "-")
	if i < 0 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(s[i+1:], 10, 32)
	if err != nil || n < 0 {
		return "", 0, false
	}
	return s[:i], int32(n), true
}

// Recovered counts l under the slice type and topology its pods select. The
// caller calls it once it has deleted l's pods.
func (p *Provisioner) Recovered(l LostSlice) {
	if len(l.Pods) == 0 {
		return
	}
	shape, found, err := tpuslice.FromNodeSelector(l.Pods[0].Spec.NodeSelector, p.cfg.ShapeKeys())
	acc, configured := p.cfg.Accelerators[shape.Accelerator]
	if err != nil || !found || !configured {
		return
	}
	slicesLost.WithLabelValues(acc.SliceType, shape.Topology.String()).Inc()
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
		releasable, err := p.releasable(ctx, s)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !releasable {
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
		releasable, err := p.releasable(ctx, s)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !releasable {
			complete = false
			continue
		}
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

// releasable reports whether s may be released: no pod of another workload
// holds chips on its hosts. A slice being deleted needs no check.
func (p *Provisioner) releasable(ctx context.Context, s gke.Slice) (bool, error) {
	if s.Terminating {
		return true, nil
	}
	holders, err := p.foreignHolders(ctx, s)
	if err != nil || len(holders) == 0 {
		return err == nil, err
	}
	sliceReleasesDeferred.WithLabelValues(s.Type, s.Topology).Inc()
	if p.onDeferred != nil {
		p.onDeferred(s, holders)
	}
	return false, nil
}

// foreignHolders returns, sorted as namespace/name, the pods that hold chips
// on the hosts of s without being confined to it. Releasing s deactivates
// the partition under them.
func (p *Provisioner) foreignHolders(ctx context.Context, s gke.Slice) ([]string, error) {
	nodes := &corev1.NodeList{}
	if err := p.hosts.List(ctx, nodes, client.MatchingLabels{p.cfg.NodeLabels.Slice: s.Name}); err != nil {
		return nil, fmt.Errorf("list the hosts of slice %s: %w", s.Name, err)
	}
	var holders []string
	for i := range nodes.Items {
		pods := &corev1.PodList{}
		if err := p.hosts.List(ctx, pods, client.MatchingFields{podNodeNameField: nodes.Items[i].Name}); err != nil {
			return nil, fmt.Errorf("list the pods on node %s of slice %s: %w", nodes.Items[i].Name, s.Name, err)
		}
		for j := range pods.Items {
			pod := &pods.Items[j]
			if query.IsTerminalPod(pod) || pod.Spec.NodeSelector[p.cfg.NodeLabels.Slice] == s.Name {
				continue
			}
			if tpuslice.ContainerChips(pod.Spec.Containers, p.cfg.ChipResource) == 0 {
				continue
			}
			holders = append(holders, pod.Namespace+"/"+pod.Name)
		}
	}
	sort.Strings(holders)
	return holders, nil
}
