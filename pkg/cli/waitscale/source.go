package waitscale

import (
	"context"
	"errors"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

const requestTimeout = 10 * time.Second

var errSource = errors.New("invalid scale wait source")

// Source polls one exact parent and at most one exact status-selected IR.
// IR status can change without a parent resourceVersion change, so callers
// must use waitengine.Options{PollOnly: true}.
type Source struct {
	client          omeclient.OmeV1beta1Interface
	replica         ReplicaReader
	namespace, name string
	target          Target
}

// ReplicaReader reads one exact InferenceReplica through a bounded transport.
type ReplicaReader interface {
	GetInferenceReplica(context.Context, string, string, metav1.GetOptions) (*v1beta1.InferenceReplica, error)
}

func NewSource(client omeclient.OmeV1beta1Interface, replica ReplicaReader, namespace, name string, target Target) *Source {
	return &Source{client: client, replica: replica, namespace: namespace, name: name, target: target}
}

func (s *Source) Get(ctx context.Context) (waitengine.Snapshot[Evidence], error) {
	zero := waitengine.Snapshot[Evidence]{}
	if s == nil || ctx == nil || s.client == nil || s.replica == nil || !validTarget(s.target) ||
		len(utilvalidation.IsDNS1123Label(s.namespace)) != 0 || len(utilvalidation.IsDNS1123Subdomain(s.name)) != 0 {
		return zero, errSource
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	call, cancel := context.WithTimeout(ctx, requestTimeout)
	parent, err := s.client.InferenceServices(s.namespace).Get(call, s.name, metav1.GetOptions{})
	callErr := call.Err()
	cancel()
	if callErr != nil {
		return zero, callErr
	}
	if err != nil {
		return zero, err
	}
	if parent == nil || parent.Name != s.name || parent.Namespace != s.namespace ||
		!validIdentity(parent.Name, parent.Namespace, string(parent.UID), parent.ResourceVersion) || parent.Generation <= 0 ||
		parent.Kind != "" && parent.Kind != "InferenceService" || parent.APIVersion != "" && parent.APIVersion != "ome.io/v1beta1" {
		return zero, errSource
	}
	snapshot := waitengine.Snapshot[Evidence]{Value: Evidence{Parent: parent}, UID: parent.UID,
		ResourceVersion: parent.ResourceVersion, Deleting: parent.DeletionTimestamp != nil}
	if snapshot.Deleting {
		return snapshot, nil
	}
	selected := s.target.IRName
	status, found := parent.Status.Components[s.target.Component]
	if selected == "" {
		if !found || status.ScaleTargetRef == nil || status.ScaleTargetRef.APIVersion != "ome.io/v1beta1" || status.ScaleTargetRef.Kind != "InferenceReplica" {
			return snapshot, nil
		}
		selected = status.ScaleTargetRef.Name
	} else if found && status.ScaleTargetRef != nil && (status.ScaleTargetRef.APIVersion != "ome.io/v1beta1" ||
		status.ScaleTargetRef.Kind != "InferenceReplica" || status.ScaleTargetRef.Name != selected) {
		return snapshot, nil
	}
	if len(utilvalidation.IsDNS1123Subdomain(selected)) != 0 {
		return snapshot, nil
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	call, cancel = context.WithTimeout(ctx, requestTimeout)
	replica, err := s.replica.GetInferenceReplica(call, s.namespace, selected, metav1.GetOptions{})
	callErr = call.Err()
	cancel()
	if callErr != nil {
		return zero, callErr
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return zero, err
	}
	if err != nil {
		replica = nil
	}
	if err == nil && replica == nil {
		return zero, errSource
	}
	// The selected parent may change while the IR GET is in flight. Re-read
	// that same named parent once; a changed owner, generation, or selected
	// target invalidates the join. Returning the refreshed parent without the
	// IR lets waitengine observe deletion/replacement and otherwise poll again.
	call, cancel = context.WithTimeout(ctx, requestTimeout)
	refreshed, err := s.client.InferenceServices(s.namespace).Get(call, s.name, metav1.GetOptions{})
	callErr = call.Err()
	cancel()
	if callErr != nil {
		return zero, callErr
	}
	if err != nil {
		return zero, err
	}
	if refreshed == nil || refreshed.Name != s.name || refreshed.Namespace != s.namespace ||
		!validIdentity(refreshed.Name, refreshed.Namespace, string(refreshed.UID), refreshed.ResourceVersion) || refreshed.Generation <= 0 ||
		refreshed.Kind != "" && refreshed.Kind != "InferenceService" || refreshed.APIVersion != "" && refreshed.APIVersion != "ome.io/v1beta1" {
		return zero, errSource
	}
	latest := waitengine.Snapshot[Evidence]{Value: Evidence{Parent: refreshed}, UID: refreshed.UID,
		ResourceVersion: refreshed.ResourceVersion, Deleting: refreshed.DeletionTimestamp != nil}
	newStatus, newFound := refreshed.Status.Components[s.target.Component]
	if latest.Deleting || parent.UID != refreshed.UID || parent.Generation != refreshed.Generation ||
		found != newFound || !reflect.DeepEqual(status.ScaleTargetRef, newStatus.ScaleTargetRef) {
		return latest, nil
	}
	latest.Value.Replica = replica
	return latest, nil
}

func (*Source) Watch(context.Context, string) (watch.Interface, error) { return nil, errSource }

func (*Source) Decode(runtime.Object) (waitengine.Snapshot[Evidence], error) {
	return waitengine.Snapshot[Evidence]{}, errSource
}
