package acceleratorquota

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/quota/capacity"
)

func TestCapacityReportsDistinguishZeroAndIncompleteAttribution(t *testing.T) {
	type observation struct {
		Flavor      string
		Allocatable string
		Complete    bool
	}
	flavor := resourceFlavor("gpu-a", map[string]string{"hardware": "gpu"})
	flavor.UID = "flavor-a-uid"
	duplicate := flavor.DeepCopy()
	duplicate.Name, duplicate.UID = "gpu-b", "flavor-b-uid"
	unidentified := flavor.DeepCopy()
	unidentified.UID = ""
	catchAll := flavor.DeepCopy()
	catchAll.Spec.NodeLabels = map[string]string{}
	node := workerNode("node-a", withLabels(map[string]string{"hardware": "gpu"}), withAllocatable(map[string]string{"example.com/gpu": "8"}))
	cordoned, unready, unmatched := node.DeepCopy(), node.DeepCopy(), node.DeepCopy()
	cordoned.Spec.Unschedulable = true
	unready.Status.Conditions[0].Status = corev1.ConditionFalse
	unmatched.Labels = map[string]string{"hardware": "other"}
	tests := []struct {
		name    string
		objects []client.Object
		want    []observation
	}{
		{name: "empty cluster reports fresh zero", objects: []client.Object{flavor}, want: []observation{{"gpu-a", "0", true}}},
		{name: "ready hardware", objects: []client.Object{flavor, node}, want: []observation{{"gpu-a", "8", true}}},
		{name: "catch-all flavor", objects: []client.Object{catchAll, node}, want: []observation{{"gpu-a", "8", true}}},
		{name: "cordoned hardware", objects: []client.Object{flavor, cordoned}, want: []observation{{"gpu-a", "0", true}}},
		{name: "unready hardware", objects: []client.Object{flavor, unready}, want: []observation{{"gpu-a", "0", true}}},
		{name: "ambiguous mapping", objects: []client.Object{flavor, duplicate, node}, want: []observation{{"gpu-a", "0", false}, {"gpu-b", "0", false}}},
		{name: "unmatched hardware", objects: []client.Object{flavor, unmatched}, want: []observation{{"gpu-a", "0", false}}},
		{name: "unverified flavor identity", objects: []client.Object{unidentified, node}, want: []observation{{"gpu-a", "8", false}}},
		{name: "missing flavors", objects: []client.Object{node}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := cohort(rootName, "")
			objects := append([]client.Object{root}, tt.objects...)
			cl := fake.NewClientBuilder().WithScheme(capacityScheme(t)).WithObjects(objects...).WithStatusSubresource(root).Build()
			r := &Reconciler{Client: cl, APIReader: cl, Capacity: CapacityOptions{Resources: []string{"example.com/gpu"}}}
			require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(root), root))
			require.NoError(t, r.reconcileCapacity(context.Background(), root))
			require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(root), root))
			var got []observation
			for _, report := range root.Status.Capacity {
				require.NotNil(t, report.Attribution)
				require.NotNil(t, report.ObservedAt)
				require.False(t, report.ObservedAt.IsZero())
				require.NotEmpty(t, report.Attribution.FlavorSetHash)
				if diff := cmp.Diff("example.com/gpu", report.ResourceName); diff != "" {
					t.Fatalf("resource (-want +got):\n%s", diff)
				}
				got = append(got, observation{report.ResourceFlavor, report.Allocatable.String(), report.Attribution.Complete})
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("capacity reports (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCapacityAttributionFingerprint(t *testing.T) {
	flavors := []capacity.Flavor{
		{Name: "gpu-a", UID: "a-uid", NodeLabels: map[string]string{"hardware": "a"}},
		{Name: "gpu-b", UID: "b-uid", NodeLabels: map[string]string{"hardware": "b"}},
	}
	resources := []string{"example.com/gpu", "example.com/tpu"}
	_, baseline := attributeCapacity(capacity.Result{}, flavors, resources)
	key := budgetKey(resources[0], flavors[0].Name)
	tests := []struct {
		name       string
		change     func([]capacity.Flavor, []string) ([]capacity.Flavor, []string)
		wantChange bool
	}{
		{name: "ordering is irrelevant", change: func(f []capacity.Flavor, r []string) ([]capacity.Flavor, []string) {
			slices.Reverse(f)
			slices.Reverse(r)
			return f, r
		}},
		{name: "duplicate resource config is irrelevant", change: func(f []capacity.Flavor, r []string) ([]capacity.Flavor, []string) {
			return f, append(r, r[0])
		}},
		{name: "flavor recreation", wantChange: true, change: func(f []capacity.Flavor, r []string) ([]capacity.Flavor, []string) {
			f[1].UID = "replacement-uid"
			return f, r
		}},
		{name: "another flavor changes attribution", wantChange: true, change: func(f []capacity.Flavor, r []string) ([]capacity.Flavor, []string) {
			f[1].NodeLabels["region"] = "west"
			return f, r
		}},
		{name: "resource configuration", wantChange: true, change: func(f []capacity.Flavor, r []string) ([]capacity.Flavor, []string) {
			return f, append(r, "example.com/accelerator")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := slices.Clone(flavors)
			for i := range input {
				input[i].NodeLabels = maps.Clone(input[i].NodeLabels)
			}
			input, resourceInput := tt.change(input, slices.Clone(resources))
			_, got := attributeCapacity(capacity.Result{}, input, resourceInput)
			changed := baseline[key].FlavorSetHash != got[key].FlavorSetHash
			if diff := cmp.Diff(tt.wantChange, changed); diff != "" {
				t.Fatalf("fingerprint change (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(flavors[0].NodeLabels, got[key].NodeLabels); diff != "" {
				t.Fatalf("flavor selector (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCapacityReportUpdatesWhenFlavorIdentityChanges(t *testing.T) {
	ctx := context.Background()
	root := cohort(rootName, "")
	flavor := resourceFlavor("gpu-a", map[string]string{"hardware": "gpu"})
	flavor.UID = "old-uid"
	cl := fake.NewClientBuilder().WithScheme(capacityScheme(t)).WithObjects(root, flavor).WithStatusSubresource(root).Build()
	r := &Reconciler{Client: cl, APIReader: cl, Capacity: CapacityOptions{Resources: []string{"example.com/gpu"}}}
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(root), root))
	require.NoError(t, r.reconcileCapacity(ctx, root))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(root), root))
	before := root.DeepCopy()
	require.NoError(t, cl.Delete(ctx, flavor))
	flavor.UID, flavor.ResourceVersion = "new-uid", ""
	require.NoError(t, cl.Create(ctx, flavor))
	require.NoError(t, r.reconcileCapacity(ctx, root))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(root), root))
	require.Len(t, root.Status.Capacity, 1)
	got := root.Status.Capacity[0].Attribution
	if diff := cmp.Diff(types.UID("new-uid"), got.FlavorUID); diff != "" {
		t.Fatalf("published flavor UID (-want +got):\n%s", diff)
	}
	if cmp.Diff(before.Status.Capacity[0].Attribution.FlavorSetHash, got.FlavorSetHash) == "" {
		t.Fatal("flavor recreation must change the attribution fingerprint")
	}
	if diff := cmp.Diff(types.UID("old-uid"), before.Status.Capacity[0].Attribution.FlavorUID); diff != "" {
		t.Fatalf("snapshot aliased updated attribution (-want +got):\n%s", diff)
	}
	got.NodeLabels["hardware"] = "changed"
	if diff := cmp.Diff("gpu", before.Status.Capacity[0].Attribution.NodeLabels["hardware"]); diff != "" {
		t.Fatalf("snapshot aliased labels (-want +got):\n%s", diff)
	}
}
