package kueue

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/backend"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

const manager = "ome-quota"

func backendScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := kueuev1beta2.AddToScheme(s); err != nil {
		t.Fatalf("add kueue scheme: %v", err)
	}
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("add ome scheme: %v", err)
	}
	return s
}

func ownedLabels(node string) map[string]string {
	return map[string]string{
		v1beta1.AcceleratorQuotaManagedByLabel: manager,
		v1beta1.AcceleratorQuotaNodeLabel:      node,
	}
}

func planFor(t *testing.T, quotas []v1beta1.AcceleratorQuota, write ...string) backend.Plan {
	t.Helper()
	built, _, err := tree.Build(quotas, tree.Options{RootName: "root"})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}
	plan := backend.Plan{}
	for _, name := range write {
		n, ok := built.Node(name)
		if !ok {
			t.Fatalf("node %q absent", name)
		}
		plan.Write = append(plan.Write, n)
	}
	return plan
}

func simpleTree() []v1beta1.AcceleratorQuota {
	return []v1beta1.AcceleratorQuota{
		aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
		aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8")),
	}
}

func withFlavor(objs ...client.Object) []client.Object {
	return append([]client.Object{
		&kueuev1beta2.ResourceFlavor{ObjectMeta: metav1.ObjectMeta{Name: "a100"}},
	}, objs...)
}

// Server-side apply adopts an object it did not create rather than refusing it,
// so the ownership label is the only thing separating a controller from
// rewriting a queue somebody else authored. Every row here is a shape that
// would otherwise be silently taken over.
func TestMaterializeRefusesForeignObjects(t *testing.T) {
	tests := []struct {
		name     string
		existing []client.Object
		wantNode string
		// refused names the object that must be left untouched. The zero value
		// means the pass is clean and everything should have been written.
		refused objectRef
	}{
		{
			name:     "nothing exists, so the objects are created",
			existing: withFlavor(),
			wantNode: "",
		},
		{
			name: "a ClusterQueue we already own is updated",
			existing: withFlavor(&kueuev1beta2.ClusterQueue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Labels: ownedLabels("team-a")},
			}),
			wantNode: "",
		},
		{
			name: "an unlabelled ClusterQueue of the same name is refused",
			existing: withFlavor(&kueuev1beta2.ClusterQueue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a"},
			}),
			wantNode: "team-a",
			refused:  objectRef{kind: "ClusterQueue", name: "team-a"},
		},
		{
			name: "a ClusterQueue owned by a different manager is refused",
			existing: withFlavor(&kueuev1beta2.ClusterQueue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Labels: map[string]string{
					v1beta1.AcceleratorQuotaManagedByLabel: "someone-else",
				}},
			}),
			wantNode: "team-a",
			refused:  objectRef{kind: "ClusterQueue", name: "team-a"},
		},
		{
			name: "a hand-authored LocalQueue of the leaf's name is refused",
			existing: withFlavor(&kueuev1beta2.LocalQueue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "ns-a"},
			}),
			wantNode: "team-a",
			refused:  objectRef{kind: "LocalQueue", name: "team-a", namespace: "ns-a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(backendScheme(t)).
				WithObjects(tc.existing...).Build()
			b := &Backend{Writer: c, Reader: c, Options: testOptions()}

			got, err := b.Materialize(context.Background(), planFor(t, simpleTree(), "root", "team-a"))
			if err != nil {
				t.Fatalf("Materialize() error = %v", err)
			}

			outcome, refused := got.Nodes[tc.wantNode]
			if tc.wantNode == "" {
				if len(got.Nodes) != 0 {
					t.Errorf("Materialize() reported %v, want a clean pass", got.Nodes)
				}
			} else {
				if !refused {
					t.Fatalf("Materialize() did not report node %q; got %v", tc.wantNode, got.Nodes)
				}
				if outcome.Reason != v1beta1.AcceleratorQuotaReasonObjectConflict {
					t.Errorf("reason = %q, want %q", outcome.Reason,
						v1beta1.AcceleratorQuotaReasonObjectConflict)
				}
			}

			if tc.refused == (objectRef{}) {
				var cq kueuev1beta2.ClusterQueue
				if err := c.Get(context.Background(), client.ObjectKey{Name: "team-a"}, &cq); err != nil {
					t.Errorf("expected the ClusterQueue to be written, got %v", err)
				}
				return
			}

			// The refused object must be left exactly as it was: no ownership
			// labels grafted on by an apply that should never have happened.
			var obj client.Object
			switch tc.refused.kind {
			case "ClusterQueue":
				obj = &kueuev1beta2.ClusterQueue{}
			case "LocalQueue":
				obj = &kueuev1beta2.LocalQueue{}
			default:
				t.Fatalf("unhandled kind %q", tc.refused.kind)
			}
			key := client.ObjectKey{Name: tc.refused.name, Namespace: tc.refused.namespace}
			if err := c.Get(context.Background(), key, obj); err != nil {
				t.Fatalf("the refused %s vanished: %v", tc.refused, err)
			}
			if obj.GetLabels()[v1beta1.AcceleratorQuotaManagedByLabel] == manager {
				t.Errorf("%s was adopted: labels = %v", tc.refused, obj.GetLabels())
			}
		})
	}
}

// A node that cannot be materialized must not stop the rest of the tree. The
// alternative is one tenant's typo freezing quota fleet-wide.
func TestMaterializeToleratesPerNodeFailure(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
		aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8")),
		aq("team-b", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "4")),
	}

	tests := []struct {
		name       string
		existing   []client.Object
		wantBroken string
		wantOK     string
	}{
		{
			name: "a foreign object on one leaf leaves the sibling alone",
			existing: withFlavor(&kueuev1beta2.ClusterQueue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a"},
			}),
			wantBroken: "team-a",
			wantOK:     "team-b",
		},
		{
			name: "a foreign object on the other leaf, symmetrically",
			existing: withFlavor(&kueuev1beta2.ClusterQueue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-b"},
			}),
			wantBroken: "team-b",
			wantOK:     "team-a",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(backendScheme(t)).
				WithObjects(tc.existing...).Build()
			b := &Backend{Writer: c, Reader: c, Options: testOptions()}

			got, err := b.Materialize(context.Background(),
				planFor(t, quotas, "root", "team-a", "team-b"))
			if err != nil {
				t.Fatalf("Materialize() error = %v", err)
			}

			if _, broken := got.Nodes[tc.wantBroken]; !broken {
				t.Errorf("node %q should have been reported, got %v", tc.wantBroken, got.Nodes)
			}
			if outcome, reported := got.Nodes[tc.wantOK]; reported {
				t.Errorf("node %q should have materialized cleanly, got %+v", tc.wantOK, outcome)
			}

			var cq kueuev1beta2.ClusterQueue
			if err := c.Get(context.Background(), client.ObjectKey{Name: tc.wantOK}, &cq); err != nil {
				t.Errorf("the healthy sibling was not written: %v", err)
			}
		})
	}
}

// A read failure is transient and must be reported as such: conflating it with
// a name collision would tell an operator to go rename something over a blip.
func TestMaterializeSeparatesTransientFailureFromConflict(t *testing.T) {
	boom := errors.New("etcdserver: request timed out")

	tests := []struct {
		name       string
		intercept  interceptor.Funcs
		wantReason string
	}{
		{
			name: "a read failure is transient",
			intercept: interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return boom
				},
			},
			wantReason: v1beta1.AcceleratorQuotaReasonMaterializationFailed,
		},
		{
			name: "an apply failure is transient",
			intercept: interceptor.Funcs{
				Apply: func(context.Context, client.WithWatch, runtime.ApplyConfiguration, ...client.ApplyOption) error {
					return boom
				},
			},
			wantReason: v1beta1.AcceleratorQuotaReasonMaterializationFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(backendScheme(t)).
				WithObjects(withFlavor()...).
				WithInterceptorFuncs(tc.intercept).Build()
			b := &Backend{Writer: c, Reader: c, Options: testOptions()}

			got, err := b.Materialize(context.Background(), planFor(t, simpleTree(), "root", "team-a"))
			if err != nil {
				t.Fatalf("Materialize() error = %v", err)
			}
			outcome, ok := got.Nodes["team-a"]
			if !ok {
				t.Fatalf("no outcome for team-a; got %v", got.Nodes)
			}
			if outcome.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", outcome.Reason, tc.wantReason)
			}
		})
	}
}

// The sweep is what makes a deleted node's quota go away, and the freeze rule is
// what stops it taking a live tenant's quota with it.
func TestSweep(t *testing.T) {
	tests := []struct {
		name        string
		existing    []client.Object
		keep        []string
		wantRemains []string
	}{
		{
			name: "an orphan we own is deleted",
			existing: []client.Object{
				&kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{
					Name: "gone", Labels: ownedLabels("gone")}},
			},
			keep:        nil,
			wantRemains: []string{},
		},
		{
			name: "a node still in the plan is kept",
			existing: []client.Object{
				&kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{
					Name: "team-a", Labels: ownedLabels("team-a")}},
			},
			keep:        []string{"team-a"},
			wantRemains: []string{"team-a"},
		},
		{
			// The freeze rule: a node whose subtree violates an invariant keeps
			// its objects. Sweeping them would turn a misauthored parent into a
			// quota outage for its children.
			name: "a retained (frozen) node is kept even though it was not written",
			existing: []client.Object{
				&kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{
					Name: "frozen", Labels: ownedLabels("frozen")}},
			},
			keep:        []string{"frozen"},
			wantRemains: []string{"frozen"},
		},
		{
			name: "an object we do not own is never touched",
			existing: []client.Object{
				&kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: "foreign"}},
				&kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{
					Name: "theirs", Labels: map[string]string{
						v1beta1.AcceleratorQuotaManagedByLabel: "someone-else",
					}}},
			},
			keep:        nil,
			wantRemains: []string{"foreign", "theirs"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(backendScheme(t)).
				WithObjects(tc.existing...).Build()
			b := &Backend{Writer: c, Reader: c, Options: testOptions()}

			keep := sets.New[string](tc.keep...)
			if _, err := b.Sweep(context.Background(), keep); err != nil {
				t.Fatalf("Sweep() error = %v", err)
			}

			var list kueuev1beta2.ClusterQueueList
			if err := c.List(context.Background(), &list); err != nil {
				t.Fatalf("List() error = %v", err)
			}
			remains := []string{}
			for i := range list.Items {
				remains = append(remains, list.Items[i].Name)
			}
			sort.Strings(remains)

			if diff := cmp.Diff(tc.wantRemains, remains); diff != "" {
				t.Errorf("Sweep() survivors mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
