package waitir

import (
	"context"
	"errors"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"

	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/instanceprojection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

const (
	requestTimeout = 10 * time.Second
	statusRowLimit = 2048
)

var errSource = errors.New("invalid InferenceReplica ready-count wait source")

// Source obtains an exact parent and bounded related-IR snapshot. IR status
// updates do not necessarily change the parent, so this source is poll-only.
type Source struct {
	client          omeclient.OmeV1beta1Interface
	namespace, name string
	now             func() time.Time
}

func NewSource(client omeclient.OmeV1beta1Interface, namespace, name string, now func() time.Time) *Source {
	if now == nil {
		now = time.Now
	}
	return &Source{client: client, namespace: namespace, name: name, now: now}
}

func (s *Source) Get(ctx context.Context) (waitengine.Snapshot[reportv1alpha1.InstanceListReport], error) {
	zero := waitengine.Snapshot[reportv1alpha1.InstanceListReport]{}
	if s == nil || ctx == nil || s.client == nil || len(utilvalidation.IsDNS1123Label(s.namespace)) != 0 || len(utilvalidation.IsDNS1123Subdomain(s.name)) != 0 {
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
	if parent == nil || parent.Name != s.name || parent.Namespace != s.namespace || parent.UID == "" || parent.ResourceVersion == "" || parent.Generation <= 0 ||
		parent.Kind != "" && parent.Kind != "InferenceService" || parent.APIVersion != "" && parent.APIVersion != "ome.io/v1beta1" {
		return zero, errSource
	}
	collection, err := instancecollection.CollectRelated(ctx, s.client.InferenceReplicas(s.namespace), parent, instancecollection.Limits{
		Paging:        paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: requestTimeout},
		MaxStatusRows: statusRowLimit,
	})
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	projected, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: parent, Collection: collection, MaxInstances: statusRowLimit,
	}, reportv1alpha1.ClockFunc(s.now))
	if err != nil {
		return zero, err
	}
	return waitengine.Snapshot[reportv1alpha1.InstanceListReport]{
		Value: projected, UID: parent.UID, ResourceVersion: parent.ResourceVersion, Deleting: parent.DeletionTimestamp != nil,
	}, nil
}

func (*Source) Watch(context.Context, string) (watch.Interface, error) { return nil, errSource }

func (*Source) Decode(runtime.Object) (waitengine.Snapshot[reportv1alpha1.InstanceListReport], error) {
	return waitengine.Snapshot[reportv1alpha1.InstanceListReport]{}, errSource
}
