package waitmigration

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	"sigs.k8s.io/ome/pkg/cli/migrationcollection"
	"sigs.k8s.io/ome/pkg/cli/migrationprojection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

var errMigrationWatchUnsupported = errors.New("migration source requires bounded polling")

// Source reads the named parent and a bounded, identity-validated snapshot of
// sibling InferenceReplicas. Only PollOnly waitengine options are supported:
// parent watches need not fire when authoritative IR status changes.
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

func (s *Source) Get(ctx context.Context) (waitengine.Snapshot[reportv1alpha1.MigrationStatusReport], error) {
	const requestTimeout = 10 * time.Second
	collected, err := migrationcollection.Collect(ctx, s.client, s.namespace, s.name, paging.Limits{PageSize: 32, MaxItems: 64, MaxPages: 2, RequestTimeout: requestTimeout})
	if err != nil {
		return waitengine.Snapshot[reportv1alpha1.MigrationStatusReport]{}, err
	}
	projected, err := migrationprojection.Project(collected, "", migrationprojection.Limits{MaxRecords: 200, MaxScannedRecords: 800, MaxNodeHints: 8, MaxScannedNodeHints: 64}, reportv1alpha1.ClockFunc(s.now))
	if err != nil {
		return waitengine.Snapshot[reportv1alpha1.MigrationStatusReport]{}, err
	}
	return waitengine.Snapshot[reportv1alpha1.MigrationStatusReport]{Value: projected, UID: collected.InferenceService.UID, ResourceVersion: collected.InferenceService.ResourceVersion, Deleting: collected.InferenceService.DeletionTimestamp != nil}, nil
}

func (*Source) Watch(context.Context, string) (watch.Interface, error) {
	return nil, errMigrationWatchUnsupported
}

func (*Source) Decode(runtime.Object) (waitengine.Snapshot[reportv1alpha1.MigrationStatusReport], error) {
	return waitengine.Snapshot[reportv1alpha1.MigrationStatusReport]{}, errMigrationWatchUnsupported
}
