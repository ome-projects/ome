package placement

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestCapacityEvents(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.AcceleratorQuota)
		want bool
	}{
		{name: "same report"},
		{name: "heartbeat", edit: func(q *v1beta1.AcceleratorQuota) {
			q.ResourceVersion = "2"
			q.Status.Capacity[0].PerCluster[0].ReportResourceVersion = "2"
			q.Status.Capacity[0].PerCluster[0].ObservedAt = &metav1.Time{Time: time.Unix(100, 0)}
		}},
		{name: "high water", edit: func(q *v1beta1.AcceleratorQuota) {
			q.Status.Capacity[0].HighWaterMark = resource.MustParse("99")
			q.Status.Capacity[0].PerCluster[0].HighWaterMark = resource.MustParse("99")
		}},
		{name: "hardware", edit: func(q *v1beta1.AcceleratorQuota) {
			q.Status.Capacity[0].PerCluster[0].Allocatable = resource.MustParse("20")
		}, want: true},
		{name: "availability", edit: func(q *v1beta1.AcceleratorQuota) { q.Status.Capacity[0].PerCluster[0].ReportAvailable = false }, want: true},
		{name: "registration identity", edit: func(q *v1beta1.AcceleratorQuota) { q.Status.Capacity[0].PerCluster[0].ClusterUID = "replacement" }, want: true},
		{name: "root identity", edit: func(q *v1beta1.AcceleratorQuota) { q.UID = "replacement" }, want: true},
		{name: "mapping identity", edit: func(q *v1beta1.AcceleratorQuota) {
			q.Status.Capacity[0].PerCluster[0].Attribution.FlavorSetHash = "replacement"
		}, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCapacityProviderFixture(t)
			root := &v1beta1.AcceleratorQuota{}
			if err := f.r.Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
				t.Fatal(err)
			}
			changed := root.DeepCopy()
			if tt.edit != nil {
				tt.edit(changed)
			}
			if diff := cmp.Diff(tt.want, capacityRelevantChange.Update(event.UpdateEvent{ObjectOld: root, ObjectNew: changed})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestCapacityEventOnlyEnqueuesParticipants(t *testing.T) {
	for _, tt := range []struct {
		name                string
		disabled, otherRoot bool
		want                bool
	}{
		{name: "configured root", want: true},
		{name: "other root", otherRoot: true},
		{name: "disabled", disabled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			capacity := srcISVCSplit("", 3)
			capacity.Name = "capacity"
			capacity.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
			static := srcISVCSplit("", 3)
			static.Name = "static"
			local := srcISVCSplit("", 3)
			local.Name = "local"
			local.Spec.Placement = nil
			r, _ := newPlacer(testScheme(t), nil)
			r.Client = indexedPlacementClient(t, testScheme(t), capacity, static, local)
			r.Capacity = providerConfig()
			if tt.disabled {
				r.Capacity = nil
			}
			root := &v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: providerConfig().RootName}}
			if tt.otherRoot {
				root.Name = "different"
			}
			got := r.isvcsForCapacityChange(t.Context(), root)
			var want []ctrl.Request
			if tt.want {
				want = []ctrl.Request{{NamespacedName: client.ObjectKeyFromObject(capacity)}}
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
