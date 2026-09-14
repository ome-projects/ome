package kueue

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
	"sigs.k8s.io/ome/pkg/quota/usage"
)

// quantities carry unexported state, so they are compared on Cmp rather than
// on their fields — two quantities that differ only in how they were parsed
// are the same number.
var cmpQuantity = cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })

// errListFailed stands in for an apiserver the backend cannot reach.
var errListFailed = errors.New("apiserver unreachable")

// flavorUsage builds one flavor's entry: pairs of (resource, total, borrowed).
func flavorUsage(flavor string, resources ...[3]string) kueuev1beta2.FlavorUsage {
	out := kueuev1beta2.FlavorUsage{Name: kueuev1beta2.ResourceFlavorReference(flavor)}
	for _, r := range resources {
		out.Resources = append(out.Resources, kueuev1beta2.ResourceUsage{
			Name:     corev1.ResourceName(r[0]),
			Total:    resource.MustParse(r[1]),
			Borrowed: resource.MustParse(r[2]),
		})
	}
	return out
}

func clusterQueue(name, node string, status kueuev1beta2.ClusterQueueStatus) *kueuev1beta2.ClusterQueue {
	cq := &kueuev1beta2.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: ownedLabels(node)},
		Status:     status,
	}
	return cq
}

// What Kueue reports and what the tree needs are two different shapes, and the
// gap between them is where a rollup silently becomes wrong: usage and
// reservation are reported as separate lists over the same keys, and a leaf
// with no queue is indistinguishable from an idle one unless it is left out.
func TestReadUsage(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
		aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("google.com/tpu", "tpu7x", "64")),
		aq("team-b", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("google.com/tpu", "tpu7x", "32")),
	}

	tests := []struct {
		name    string
		objects []client.Object
		want    map[string]map[usage.Key]usage.Observed
	}{
		{
			// The ordinary case: both lists present, reservation at or above
			// usage, and the two folded onto one key.
			name: "usage and reservation fold onto one key",
			objects: []client.Object{
				clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{
					FlavorsUsage:       []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "40", "8"})},
					FlavorsReservation: []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "56", "0"})},
				}),
			},
			want: map[string]map[usage.Key]usage.Observed{
				"team-a": {
					{ResourceName: "google.com/tpu", ResourceFlavor: "tpu7x"}: {
						Admitted: resource.MustParse("40"),
						Reserved: resource.MustParse("56"),
						Borrowed: resource.MustParse("8"),
					},
				},
			},
		},
		{
			// A queue exists and holds nothing. It must be present at zero, not
			// absent: absent means the budget was never materialized.
			name: "a materialized queue holding nothing reports zero",
			objects: []client.Object{
				clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{
					FlavorsUsage:       []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "0", "0"})},
					FlavorsReservation: []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "0", "0"})},
				}),
			},
			want: map[string]map[usage.Key]usage.Observed{
				"team-a": {
					{ResourceName: "google.com/tpu", ResourceFlavor: "tpu7x"}: {
						Admitted: resource.MustParse("0"),
						Reserved: resource.MustParse("0"),
						Borrowed: resource.MustParse("0"),
					},
				},
			},
		},
		{
			// Reservation is the wider set, so it can name a key usage does not.
			name: "a reservation with nothing admitted is still reported",
			objects: []client.Object{
				clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{
					FlavorsReservation: []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "16", "0"})},
				}),
			},
			want: map[string]map[usage.Key]usage.Observed{
				"team-a": {
					{ResourceName: "google.com/tpu", ResourceFlavor: "tpu7x"}: {
						Admitted: resource.MustParse("0"),
						Reserved: resource.MustParse("16"),
						Borrowed: resource.MustParse("0"),
					},
				},
			},
		},
		{
			// Admitted work holds a reservation by definition, so a backend
			// reporting usage without reservation must still never report
			// reserved below admitted.
			name: "reservation is never reported below admitted",
			objects: []client.Object{
				clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{
					FlavorsUsage: []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "24", "0"})},
				}),
			},
			want: map[string]map[usage.Key]usage.Observed{
				"team-a": {
					{ResourceName: "google.com/tpu", ResourceFlavor: "tpu7x"}: {
						Admitted: resource.MustParse("24"),
						Reserved: resource.MustParse("24"),
						Borrowed: resource.MustParse("0"),
					},
				},
			},
		},
		{
			// Nothing materialized at all. Absent, not a fleet of zeros.
			name:    "leaves with no queue are absent from the result",
			objects: nil,
			want:    nil,
		},
		{
			// A queue this manager did not write must not be charged to a node,
			// however tempting its name.
			name: "an unowned queue with a colliding name is ignored",
			objects: []client.Object{
				&kueuev1beta2.ClusterQueue{
					ObjectMeta: metav1.ObjectMeta{Name: "team-a"},
					Status: kueuev1beta2.ClusterQueueStatus{
						FlavorsUsage: []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "999", "0"})},
					},
				},
			},
			want: nil,
		},
		{
			// Two leaves are read in one pass, and neither picks up the other's
			// figures.
			name: "each leaf gets only its own queue",
			objects: []client.Object{
				clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{
					FlavorsUsage: []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "10", "0"})},
				}),
				clusterQueue("team-b", "team-b", kueuev1beta2.ClusterQueueStatus{
					FlavorsUsage: []kueuev1beta2.FlavorUsage{flavorUsage("tpu7x", [3]string{"google.com/tpu", "20", "4"})},
				}),
			},
			want: map[string]map[usage.Key]usage.Observed{
				"team-a": {
					{ResourceName: "google.com/tpu", ResourceFlavor: "tpu7x"}: {
						Admitted: resource.MustParse("10"),
						Reserved: resource.MustParse("10"),
						Borrowed: resource.MustParse("0"),
					},
				},
				"team-b": {
					{ResourceName: "google.com/tpu", ResourceFlavor: "tpu7x"}: {
						Admitted: resource.MustParse("20"),
						Reserved: resource.MustParse("20"),
						Borrowed: resource.MustParse("4"),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(backendScheme(t)).
				WithObjects(tc.objects...).Build()
			b := &Backend{Writer: c, Reader: c, Options: testOptions()}

			built, _, err := tree.Build(quotas, tree.Options{RootName: "root"})
			if err != nil {
				t.Fatalf("tree.Build() error = %v", err)
			}

			got, err := b.ReadUsage(context.Background(), built.Leaves())
			if err != nil {
				t.Fatalf("ReadUsage() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, cmpQuantity); diff != "" {
				t.Errorf("ReadUsage() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The rollup is observational, but a failed read must not be mistaken for an
// idle fleet — so the error is surfaced rather than swallowed into an empty
// result, leaving the handling to the caller.
func TestReadUsageSurfacesAFailedList(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
		aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("google.com/tpu", "tpu7x", "64")),
	}

	tests := []struct {
		name    string
		leaves  bool
		wantErr bool
	}{
		{name: "a failed LIST is an error, not an empty reading", leaves: true, wantErr: true},
		{
			// Nothing to read means no call to fail, so an unreachable backend
			// costs a tree with no leaves nothing.
			name:   "no leaves means no read at all",
			leaves: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(backendScheme(t)).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
						return errListFailed
					},
				}).Build()
			b := &Backend{Writer: c, Reader: c, Options: testOptions()}

			var leaves []*tree.Node
			if tc.leaves {
				built, _, err := tree.Build(quotas, tree.Options{RootName: "root"})
				if err != nil {
					t.Fatalf("tree.Build() error = %v", err)
				}
				leaves = built.Leaves()
			}

			_, err := b.ReadUsage(context.Background(), leaves)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReadUsage() error = %v, want error: %v", err, tc.wantErr)
			}
		})
	}
}

// The predicate is what keeps a watch on ClusterQueue from costing a whole-tree
// pass every time Kueue restamps a status. Kueue rewrites these objects for
// pending counts, reserving counts and condition timestamps, none of which move
// a number this backend reads.
func TestWatchUsagePredicate(t *testing.T) {
	b := &Backend{Options: testOptions()}
	obj, pred := b.WatchUsage()

	if _, ok := obj.(*kueuev1beta2.ClusterQueue); !ok {
		t.Fatalf("WatchUsage() object = %T, want *kueuev1beta2.ClusterQueue", obj)
	}

	usage := func(total, borrowed string) kueuev1beta2.ClusterQueueStatus {
		return kueuev1beta2.ClusterQueueStatus{
			FlavorsUsage: []kueuev1beta2.FlavorUsage{
				flavorUsage("tpu7x", [3]string{"google.com/tpu", total, borrowed}),
			},
		}
	}

	tests := []struct {
		name   string
		before *kueuev1beta2.ClusterQueue
		after  *kueuev1beta2.ClusterQueue
		want   bool
	}{
		{
			name:   "admitted quantity moving is worth a pass",
			before: clusterQueue("team-a", "team-a", usage("10", "0")),
			after:  clusterQueue("team-a", "team-a", usage("50", "0")),
			want:   true,
		},
		{
			name:   "borrowed quantity moving is worth a pass",
			before: clusterQueue("team-a", "team-a", usage("50", "0")),
			after:  clusterQueue("team-a", "team-a", usage("50", "8")),
			want:   true,
		},
		{
			// The reservation list is read too, so it has to wake the controller
			// on its own -- a workload reserves before it is admitted.
			name:   "reservation moving on its own is worth a pass",
			before: clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{}),
			after: clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{
				FlavorsReservation: []kueuev1beta2.FlavorUsage{
					flavorUsage("tpu7x", [3]string{"google.com/tpu", "16", "0"}),
				},
			}),
			want: true,
		},
		{
			// The specific noise this predicate exists to absorb.
			name:   "a restamped status holding the same figures is not",
			before: clusterQueue("team-a", "team-a", usage("50", "0")),
			after: func() *kueuev1beta2.ClusterQueue {
				cq := clusterQueue("team-a", "team-a", usage("50", "0"))
				cq.Status.PendingWorkloads = 7
				cq.Status.ReservingWorkloads = 3
				cq.Status.AdmittedWorkloads = 2
				return cq
			}(),
			want: false,
		},
		{
			// A queue this manager does not own must never wake it, however its
			// figures move.
			name:   "an unowned queue is ignored",
			before: &kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}, Status: usage("10", "0")},
			after:  &kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}, Status: usage("99", "0")},
			want:   false,
		},
		{
			// Another manager's queues are its own business.
			name:   "a queue owned by a different manager is ignored",
			before: clusterQueue("team-a", "team-a", usage("10", "0")),
			after: func() *kueuev1beta2.ClusterQueue {
				cq := clusterQueue("team-a", "team-a", usage("99", "0"))
				cq.Labels[v1beta1.AcceleratorQuotaManagedByLabel] = "someone-else"
				return cq
			}(),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pred.Update(event.UpdateEvent{ObjectOld: tc.before, ObjectNew: tc.after})
			if got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Create and Delete carry no before-and-after to compare, and both matter: a
// create is how the informer replays what already exists at startup, and a
// delete is a queue removed underneath us, which nothing else would notice
// because these objects carry no OwnerReference.
func TestWatchUsageCreateAndDelete(t *testing.T) {
	b := &Backend{Options: testOptions()}
	_, pred := b.WatchUsage()

	ours := clusterQueue("team-a", "team-a", kueuev1beta2.ClusterQueueStatus{})
	theirs := &kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}

	tests := []struct {
		name string
		got  bool
		want bool
	}{
		{
			name: "a create of our own queue wakes the controller",
			got:  pred.Create(event.CreateEvent{Object: ours}), want: true,
		},
		{
			name: "a create of a queue we do not own does not",
			got:  pred.Create(event.CreateEvent{Object: theirs}), want: false,
		},
		{
			name: "a delete of our own queue wakes the controller",
			got:  pred.Delete(event.DeleteEvent{Object: ours}), want: true,
		},
		{
			name: "a delete of a queue we do not own does not",
			got:  pred.Delete(event.DeleteEvent{Object: theirs}), want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}
