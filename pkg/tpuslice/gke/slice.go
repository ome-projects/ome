// Package gke reads and writes GKE Slice objects: the cluster-scoped object
// that binds one TPU partition and, once the provider has provisioned it,
// labels the nodes that carry it.
//
// Slices are handled unstructured so OME does not depend on the provider's Go
// module. The coordinates and field paths here are pinned by the Slice CRD;
// what a deployment may vary (labels, annotations, and which states admit
// pods) is supplied by the caller.
package gke

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Slice CRD coordinates, pinned by the provider's API. A version change is a
// deliberate upgrade.
const (
	Group   = "accelerator.gke.io"
	Version = "v1beta1"
	Kind    = "Slice"
)

// MaxNameLength is the longest Slice name the provider's admission webhook
// accepts, shorter than the DNS-1123 label limit.
const MaxNameLength = 49

// GroupVersion is the Slice API group and version.
var GroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// readyConditionType names the condition whose reason is the Slice's
// provisioning state.
const readyConditionType = "Ready"

// ErrOwnershipConflict reports a Slice that lacks the owner labels of the
// object acting on it. Such a Slice is never adopted, modified or deleted.
var ErrOwnershipConflict = errors.New("slice is owned by another object")

// NewObject returns an empty Slice with its kind set, for reads and watches.
func NewObject() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(GroupVersion.WithKind(Kind))
	return u
}

// Spec is a Slice to create.
type Spec struct {
	// Name is the Slice name. The provider copies it into a node label value,
	// so it must be a DNS-1123 label of at most MaxNameLength characters.
	Name string
	// Type is the accelerator family the partition is carved from.
	Type string
	// Topology is the partition shape in XxY or XxYxZ form.
	Topology string
	// Owner labels identify the object the Slice is provisioned for. They are
	// written as labels, and every operation on an existing Slice first
	// checks that it carries all of them.
	Owner map[string]string
	// Labels and Annotations are written as given alongside Owner.
	Labels      map[string]string
	Annotations map[string]string
}

// Build returns spec as an unstructured Slice. partitionIds is written empty:
// the CRD requires the field, and the provider's scheduler assigns it.
func Build(spec Spec) (*unstructured.Unstructured, error) {
	if errs := validation.IsDNS1123Label(spec.Name); len(errs) > 0 {
		return nil, fmt.Errorf("slice name %q: %s", spec.Name, strings.Join(errs, "; "))
	}
	if len(spec.Name) > MaxNameLength {
		return nil, fmt.Errorf("slice name %q: must be no more than %d characters", spec.Name, MaxNameLength)
	}
	if spec.Type == "" || spec.Topology == "" {
		return nil, fmt.Errorf("slice %s: type and topology must both be set", spec.Name)
	}
	if len(spec.Owner) == 0 {
		return nil, fmt.Errorf("slice %s: owner labels must be set", spec.Name)
	}
	merged := make(map[string]string, len(spec.Labels)+len(spec.Owner))
	maps.Copy(merged, spec.Labels)
	for k, v := range spec.Owner {
		if other, ok := spec.Labels[k]; ok && other != v {
			return nil, fmt.Errorf("slice %s: label %s is %q but owner label is %q", spec.Name, k, other, v)
		}
		merged[k] = v
	}
	u := NewObject()
	u.SetName(spec.Name)
	u.SetLabels(merged)
	if len(spec.Annotations) > 0 {
		u.SetAnnotations(maps.Clone(spec.Annotations))
	}
	u.Object["spec"] = map[string]interface{}{
		"type":         spec.Type,
		"topology":     spec.Topology,
		"partitionIds": []interface{}{},
	}
	return u, nil
}

// Slice is an observed Slice.
type Slice struct {
	Name     string
	UID      types.UID
	Labels   map[string]string
	Type     string
	Topology string
	// Terminating reports a deletion in progress. The provider may hold the
	// partition behind a finalizer until it is released.
	Terminating bool
	// State is the Ready condition's reason, empty until the provider reports
	// one.
	State string
	// ReadyStatus is the Ready condition's status, empty until the provider
	// reports the condition.
	ReadyStatus string
	// StateSince is when the Ready condition last transitioned, zero until the
	// provider reports a time.
	StateSince time.Time
	// Message is the Ready condition's message.
	Message string
	// Created is when the Slice was created.
	Created time.Time
	// PartitionIDs are the partitions the provider's scheduler assigned to
	// the Slice, nil until it assigns any.
	PartitionIDs []string
}

// Parse reads an observed Slice.
func Parse(u *unstructured.Unstructured) (Slice, error) {
	if gk := u.GroupVersionKind().GroupKind(); gk != GroupVersion.WithKind(Kind).GroupKind() {
		return Slice{}, fmt.Errorf("object %s is a %s, not a %s.%s", u.GetName(), gk, Kind, Group)
	}
	sliceType, _, err := unstructured.NestedString(u.Object, "spec", "type")
	if err != nil {
		return Slice{}, fmt.Errorf("slice %s: %w", u.GetName(), err)
	}
	topology, _, err := unstructured.NestedString(u.Object, "spec", "topology")
	if err != nil {
		return Slice{}, fmt.Errorf("slice %s: %w", u.GetName(), err)
	}
	partitionIDs, _, err := unstructured.NestedStringSlice(u.Object, "spec", "partitionIds")
	if err != nil {
		return Slice{}, fmt.Errorf("slice %s: %w", u.GetName(), err)
	}
	conditions, _, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil {
		return Slice{}, fmt.Errorf("slice %s: %w", u.GetName(), err)
	}
	s := Slice{
		Name:        u.GetName(),
		UID:         u.GetUID(),
		Labels:      maps.Clone(u.GetLabels()),
		Type:        sliceType,
		Topology:    topology,
		Terminating: u.GetDeletionTimestamp() != nil,
		Created:     u.GetCreationTimestamp().Time,
	}
	if len(partitionIDs) > 0 {
		s.PartitionIDs = partitionIDs
	}
	for _, raw := range conditions {
		c, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := c["type"].(string); t != readyConditionType {
			continue
		}
		s.State, _ = c["reason"].(string)
		s.Message, _ = c["message"].(string)
		s.ReadyStatus, _ = c["status"].(string)
		if raw, _ := c["lastTransitionTime"].(string); raw != "" {
			if at, err := time.Parse(time.RFC3339, raw); err == nil {
				s.StateSince = at
			}
		}
		break
	}
	return s, nil
}

// OwnedBy reports whether s carries every label in owner with the same value.
// No Slice is owned by an empty owner.
func (s Slice) OwnedBy(owner map[string]string) bool {
	if len(owner) == 0 {
		return false
	}
	for k, v := range owner {
		if got, ok := s.Labels[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// Matches reports whether s has the type and topology spec asks for.
func (s Slice) Matches(spec Spec) bool {
	return s.Type == spec.Type && s.Topology == spec.Topology
}

// Ready reports whether pods may bind to s: it is not being deleted, its
// state is one of readyStates, and the provider does not report its
// readiness as unknown.
func (s Slice) Ready(readyStates []string) bool {
	return !s.Terminating && s.State != "" && slices.Contains(readyStates, s.State) &&
		s.ReadyStatus != string(metav1.ConditionUnknown)
}

// Client creates, observes and deletes Slices.
type Client struct {
	reader client.Reader
	writer client.Writer
}

// NewClient returns a Client that reads through reader and writes through
// writer.
func NewClient(reader client.Reader, writer client.Writer) *Client {
	return &Client{reader: reader, writer: writer}
}

// Get returns the Slice named name. found is false when none exists,
// including when the Slice CRD is not installed.
func (c *Client) Get(ctx context.Context, name string) (s Slice, found bool, err error) {
	u := NewObject()
	if err := c.reader.Get(ctx, client.ObjectKey{Name: name}, u); err != nil {
		if absent(err) {
			return Slice{}, false, nil
		}
		return Slice{}, false, fmt.Errorf("get slice %s: %w", name, err)
	}
	s, err = Parse(u)
	if err != nil {
		return Slice{}, false, err
	}
	return s, true, nil
}

// List returns the Slices whose labels match selector, sorted by name. There
// are none when the Slice CRD is not installed. selector must not be empty:
// Slices are cluster-scoped and shared with other producers, so a caller only
// ever lists its own.
func (c *Client) List(ctx context.Context, selector labels.Selector) ([]Slice, error) {
	if selector == nil || selector.Empty() {
		return nil, fmt.Errorf("list slices: selector must not be empty")
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(GroupVersion.WithKind(Kind + "List"))
	if err := c.reader.List(ctx, list, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		if absent(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list slices: %w", err)
	}
	out := make([]Slice, 0, len(list.Items))
	for i := range list.Items {
		s, err := Parse(&list.Items[i])
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Create creates the Slice spec describes and returns it as stored. When a
// Slice of that name already exists, Create returns it if it carries spec's
// owner labels and ErrOwnershipConflict if it does not. It never modifies an
// existing Slice. created is true only when this call created the Slice.
func (c *Client) Create(ctx context.Context, spec Spec) (s Slice, created bool, err error) {
	u, err := Build(spec)
	if err != nil {
		return Slice{}, false, err
	}
	if err := c.writer.Create(ctx, u); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return Slice{}, false, fmt.Errorf("create slice %s: %w", spec.Name, err)
		}
		existing, found, getErr := c.Get(ctx, spec.Name)
		if getErr != nil {
			return Slice{}, false, getErr
		}
		if !found {
			return Slice{}, false, fmt.Errorf("create slice %s: %w", spec.Name, err)
		}
		if !existing.OwnedBy(spec.Owner) {
			return Slice{}, false, fmt.Errorf("%w: %s", ErrOwnershipConflict, spec.Name)
		}
		return existing, false, nil
	}
	if s, err = Parse(u); err != nil {
		return Slice{}, false, err
	}
	return s, true, nil
}

// Release deletes s if it carries owner. The delete is preconditioned on s's
// UID, so a Slice recreated under the same name is never deleted in its place.
//
// complete is true only when the API server proves s absent. An accepted
// delete returns false: the provider may hold the Slice behind a finalizer
// while it releases the partition, and the caller observes completion once a
// later read no longer finds s.
func (c *Client) Release(ctx context.Context, s Slice, owner map[string]string) (complete bool, err error) {
	if !s.OwnedBy(owner) {
		return false, fmt.Errorf("%w: %s", ErrOwnershipConflict, s.Name)
	}
	if s.UID == "" {
		return false, fmt.Errorf("release slice %s: no UID; only an observed Slice can be released", s.Name)
	}
	if s.Terminating {
		return false, nil
	}
	u := NewObject()
	u.SetName(s.Name)
	uid := s.UID
	if err := c.writer.Delete(ctx, u, client.Preconditions{UID: &uid}); err != nil {
		if absent(err) {
			return true, nil
		}
		return false, fmt.Errorf("delete slice %s: %w", s.Name, err)
	}
	return false, nil
}

// absent reports an error that proves no Slice exists: the object is gone, or
// the Slice CRD is not installed.
func absent(err error) bool {
	return apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err)
}
