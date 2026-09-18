package waitheld

import (
	"context"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/actionbounds"
	"sigs.k8s.io/ome/pkg/cli/safetext"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

const requestTimeout = 10 * time.Second

var (
	ErrInvalidTarget    = errors.New("held-revision wait target is invalid")
	ErrAcquisition      = errors.New("held-revision source acquisition failed")
	ErrUnsafeResponse   = errors.New("held-revision source response is unsafe")
	ErrWatchUnsupported = errors.New("held-revision source requires bounded polling")
)

// Source implements a PollOnly waitengine source. InferenceReplica status
// changes need not trigger a parent InferenceService watch event. It reads
// only the exact IR name carried by release-held ActionResult.
// The caller supplies a bounded exact-name IR reader. The predicate uses
// top-level status and validates, but does not inspect, compact instance rows.
type Source struct {
	client  omeclient.OmeV1beta1Interface
	replica ReplicaReader
	target  Target
}

// ReplicaReader reads one exact InferenceReplica through a bounded transport.
type ReplicaReader interface {
	GetInferenceReplica(context.Context, string, string, metav1.GetOptions) (*v1beta1.InferenceReplica, error)
}

var _ waitengine.Source[Observation] = (*Source)(nil)

func NewSource(client omeclient.OmeV1beta1Interface, replica ReplicaReader, target Target) (*Source, error) {
	if client == nil || replica == nil || !validTarget(target) {
		return nil, ErrInvalidTarget
	}
	return &Source{client: client, replica: replica, target: target}, nil
}

func (s *Source) Get(ctx context.Context) (waitengine.Snapshot[Observation], error) {
	if ctx == nil {
		return waitengine.Snapshot[Observation]{}, ErrInvalidTarget
	}
	parent, err := s.getParent(ctx)
	if err != nil {
		// Only parent NotFound may reach waitengine: child NotFound is unmet.
		return waitengine.Snapshot[Observation]{}, err
	}
	if !validParent(parent, s.target) {
		return waitengine.Snapshot[Observation]{}, ErrUnsafeResponse
	}
	if parent.DeletionTimestamp != nil {
		return deletedParentSnapshot(parent), nil
	}
	evidence := Evidence{Parent: parent, Complete: true}
	replica, err := s.getReplica(ctx)
	switch {
	case apierrors.IsNotFound(err):
		// Absence of the action's exact IR is not parent deletion and cannot
		// establish release even when no block is visible.
	case err != nil:
		return waitengine.Snapshot[Observation]{}, err
	case replica == nil || !actionbounds.PrivatePayload(replica):
		return waitengine.Snapshot[Observation]{}, ErrUnsafeResponse
	default:
		evidence.Replica, evidence.Sources = replica, 1
	}
	// Re-read the parent after the child so a concurrent spec/UID change
	// cannot promote an old parent-generation stamp to current evidence.
	refreshed, err := s.getParent(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return deletedParentSnapshot(parent), nil
		}
		return waitengine.Snapshot[Observation]{}, err
	}
	if !validParent(refreshed, s.target) {
		return waitengine.Snapshot[Observation]{}, ErrUnsafeResponse
	}
	if refreshed.UID == parent.UID && refreshed.DeletionTimestamp != nil {
		return deletedParentSnapshot(parent), nil
	}
	if refreshed.UID != parent.UID || refreshed.ResourceVersion != parent.ResourceVersion ||
		refreshed.Generation != parent.Generation {
		evidence.Complete = false
	}
	return waitengine.Snapshot[Observation]{
		Value: Evaluate(s.target, evidence), UID: parent.UID,
		ResourceVersion: parent.ResourceVersion,
	}, nil
}

func deletedParentSnapshot(parent *v1beta1.InferenceService) waitengine.Snapshot[Observation] {
	return waitengine.Snapshot[Observation]{
		UID: parent.UID, ResourceVersion: parent.ResourceVersion, Deleting: true,
		Value: Observation{Reason: ReasonDeleting, Validity: ValidityUnavailable,
			TargetState: TargetUnknown, MailboxState: MailboxUnknown, Attribution: AttributionUnverifiable},
	}
}

func (s *Source) getParent(ctx context.Context) (*v1beta1.InferenceService, error) {
	call, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	parent, err := s.client.InferenceServices(s.target.Namespace).Get(call, s.target.ParentName, metav1.GetOptions{})
	if call.Err() != nil {
		return nil, call.Err()
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
				safetext.Sanitize(s.target.ParentName, 253))
		}
		return nil, ErrAcquisition
	}
	if parent == nil || !actionbounds.PrivatePayload(parent) {
		return nil, ErrUnsafeResponse
	}
	return parent, nil
}

func (s *Source) getReplica(ctx context.Context) (*v1beta1.InferenceReplica, error) {
	call, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	replica, err := s.replica.GetInferenceReplica(call, s.target.Namespace, s.target.IRName, metav1.GetOptions{})
	if call.Err() != nil {
		return nil, call.Err()
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, ErrAcquisition
	}
	return replica, nil
}

func (*Source) Watch(context.Context, string) (watch.Interface, error) {
	return nil, ErrWatchUnsupported
}

func (*Source) Decode(runtime.Object) (waitengine.Snapshot[Observation], error) {
	return waitengine.Snapshot[Observation]{}, ErrWatchUnsupported
}
