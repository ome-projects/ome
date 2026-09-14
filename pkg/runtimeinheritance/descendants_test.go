package runtimeinheritance

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Descendants walks an adjacency map and the cache lists it feeds on iterate
// a map too, so the order it returns is unspecified. Compare as a set.
var cmpKeysUnordered = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.SortSlices(func(a, b types.NamespacedName) bool {
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	}),
}

func clusterKey(name string) types.NamespacedName {
	return types.NamespacedName{Name: name}
}

func nsKey(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

func TestDescendants(t *testing.T) {
	tests := []struct {
		name     string
		runtimes []client.Object
		root     types.NamespacedName
		want     []types.NamespacedName
	}{
		{
			// A grandchild inherits the root's spec through the middle
			// runtime, so an edit at the root has to reach it too.
			name: "the walk is transitive, not just direct children",
			runtimes: []client.Object{
				mkClusterRuntime("profile", ""),
				mkClusterRuntime("mid", "profile"),
				mkClusterRuntime("leaf", "mid"),
				mkClusterRuntime("unrelated", ""),
			},
			root: clusterKey("profile"),
			want: []types.NamespacedName{clusterKey("mid"), clusterKey("leaf")},
		},
		{
			name: "a cluster root reaches both scopes in every namespace",
			runtimes: []client.Object{
				mkClusterRuntime("root", ""),
				mkClusterRuntime("c1", "root"),
				mkNamespacedRuntime("team-a", "n1", "root"),
				mkNamespacedRuntime("team-b", "n2", "root"),
			},
			root: clusterKey("root"),
			want: []types.NamespacedName{clusterKey("c1"), nsKey("team-a", "n1"), nsKey("team-b", "n2")},
		},
		{
			// A ServingRuntime cannot be inherited from another namespace, and
			// a ClusterServingRuntime resolves its parent against cluster scope
			// alone — so neither of the two decoys is below this root.
			name: "a namespaced root reaches only its own namespace",
			runtimes: []client.Object{
				mkNamespacedRuntime("team-a", "parent", ""),
				mkNamespacedRuntime("team-a", "child", "parent"),
				mkNamespacedRuntime("team-b", "outsider", "parent"),
				mkClusterRuntime("cluster-child", "parent"),
			},
			root: nsKey("team-a", "parent"),
			want: []types.NamespacedName{nsKey("team-a", "child")},
		},
		{
			// team-b/impostor resolves "child" inside team-b or at cluster
			// scope, never to team-a/child, so descending into a namespaced
			// node confines the rest of the walk to that namespace.
			name: "a namespaced node does not expand across namespaces",
			runtimes: []client.Object{
				mkClusterRuntime("profile", ""),
				mkNamespacedRuntime("team-a", "child", "profile"),
				mkNamespacedRuntime("team-a", "grand", "child"),
				mkNamespacedRuntime("team-b", "impostor", "child"),
			},
			root: clusterKey("profile"),
			want: []types.NamespacedName{nsKey("team-a", "child"), nsKey("team-a", "grand")},
		},
		{
			// Resolve would give team-a/child the namespaced base as its
			// parent, shadowing the cluster one. The walk matches on name
			// alone and returns it from either — deliberate, because honouring
			// the shadowing would drop dependents the moment team-a/base is
			// deleted.
			name: "a shadowed parent still yields its namesake's children",
			runtimes: []client.Object{
				mkClusterRuntime("base", ""),
				mkNamespacedRuntime("team-a", "base", ""),
				mkNamespacedRuntime("team-a", "child", "base"),
			},
			root: clusterKey("base"),
			want: []types.NamespacedName{nsKey("team-a", "child")},
		},
		{
			// The webhook rejects cycles, but a mapper that hangs on one would
			// wedge the whole event handler.
			name: "a cycle terminates without re-emitting the root",
			runtimes: []client.Object{
				mkClusterRuntime("a", "b"),
				mkClusterRuntime("b", "a"),
			},
			root: clusterKey("a"),
			want: []types.NamespacedName{clusterKey("b")},
		},
		{
			name:     "a runtime with no dependents yields nothing",
			runtimes: []client.Object{mkClusterRuntime("solo", "")},
			root:     clusterKey("solo"),
			want:     nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(newScheme(t)).
				WithObjects(tc.runtimes...).
				Build()

			got, err := Descendants(context.Background(), c, tc.root)
			if err != nil {
				t.Fatalf("Descendants() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, cmpKeysUnordered); diff != "" {
				t.Errorf("Descendants() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
