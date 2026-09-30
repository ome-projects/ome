package placement

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

type backendFixture struct {
	source      *v1beta1.InferenceService
	clusters    []v1beta1.WorkloadCluster
	connections fakeClusters
	workers     map[string]client.WithWatch
	reconciler  *Reconciler
}

func newBackendFixture(t *testing.T, mode v1beta1.PlacementMode) *backendFixture {
	t.Helper()
	source := srcISVC("")
	source.Name, source.Namespace = "service", "team-a"
	source.Spec.Placement.Mode = mode
	if mode == v1beta1.PlacementModeSplit || mode == v1beta1.PlacementModeSplitByCapacity {
		source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
	}
	f := &backendFixture{source: source, connections: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, workers: map[string]client.WithWatch{}}
	objects := []client.Object{source}
	scheme := testScheme(t)
	for _, name := range []string{"member-a", "member-b"} {
		cluster := readyWC(name, nil)
		f.clusters = append(f.clusters, *cluster)
		objects = append(objects, cluster)
		f.workers[name] = emptyWorker(scheme)
		f.connections.m[name] = workloadcluster.NewNeverCachingClient(f.workers[name])
	}
	f.reconciler, _ = newPlacer(scheme, f.connections, objects...)
	return f
}

func TestBackendPreflight(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tt := range []struct {
				name      string
				edit      func(*testing.T, *backendFixture)
				status    corev1.ConditionStatus
				allFailed bool
			}{
				{name: "native without hardware configuration", status: corev1.ConditionTrue},
				{name: "missing runtime on one member", status: corev1.ConditionUnknown, edit: func(t *testing.T, f *backendFixture) {
					t.Helper()
					if err := f.workers["member-b"].Delete(t.Context(), backendTestRuntime()); err != nil {
						t.Fatal(err)
					}
				}},
				{name: "raw runtime on one member", status: corev1.ConditionFalse, edit: func(t *testing.T, f *backendFixture) {
					t.Helper()
					rt := backendTestRuntime()
					if err := f.workers["member-b"].Get(t.Context(), client.ObjectKeyFromObject(rt), rt); err != nil {
						t.Fatal(err)
					}
					rt.Spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
					if err := f.workers["member-b"].Update(t.Context(), rt); err != nil {
						t.Fatal(err)
					}
				}},
				{name: "local collision", status: corev1.ConditionUnknown, edit: func(t *testing.T, f *backendFixture) {
					t.Helper()
					local := f.source.DeepCopy()
					local.Spec.Placement = nil
					local.ResourceVersion = ""
					if err := f.workers["member-b"].Create(t.Context(), local); err != nil {
						t.Fatal(err)
					}
				}},
				{name: "disconnected member", status: corev1.ConditionUnknown, edit: func(_ *testing.T, f *backendFixture) { delete(f.connections.m, "member-b") }},
				{name: "unidentified transports", status: corev1.ConditionUnknown, allFailed: true, edit: func(_ *testing.T, f *backendFixture) { f.reconciler.Clusters = struct{ ClusterClients }{f.connections} }},
				{name: "registration replaced during resolution", status: corev1.ConditionUnknown, edit: func(t *testing.T, f *backendFixture) {
					t.Helper()
					changed := false
					cl := interceptor.NewClient(f.workers["member-b"], interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if err := c.Get(ctx, key, obj, opts...); err != nil {
							return err
						}
						if _, runtimeRead := obj.(*v1beta1.ClusterServingRuntime); runtimeRead && !changed {
							changed = true
							old := f.clusters[1].DeepCopy()
							if err := f.reconciler.Delete(ctx, old); err != nil {
								return err
							}
							replacement := readyWC(old.Name, nil)
							replacement.UID = "replacement-uid"
							if err := f.reconciler.Create(ctx, replacement); err != nil {
								return err
							}
						}
						return nil
					}})
					f.connections.m["member-b"] = workloadcluster.NewNeverCachingClient(cl)
				}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					f := newBackendFixture(t, mode)
					if tt.edit != nil {
						tt.edit(t, f)
					}
					before := f.source.DeepCopy()
					ctx, err := f.reconciler.preflightBackends(t.Context(), f.source, f.clusters, []string{"member-a", "member-b"})
					state := ctx.Value(backendContextKey{}).(*backendPreflight)
					wantErr := tt.allFailed || tt.status != corev1.ConditionTrue && mode == v1beta1.PlacementModeSplitByCapacity
					if diff := cmp.Diff(wantErr, err != nil); diff != "" {
						t.Fatalf("hold (-want +got):\n%s\n%v", diff, err)
					}
					if diff := cmp.Diff(tt.status, state.condition.cond.Status); diff != "" {
						t.Fatalf("condition (-want +got):\n%s", diff)
					}
					want := map[string]types.UID{"member-a": "member-a-uid", "member-b": "member-b-uid"}
					if tt.status != corev1.ConditionTrue {
						delete(want, "member-b")
					}
					if tt.allFailed {
						delete(want, "member-a")
					}
					if diff := cmp.Diff(want, state.clusters); diff != "" {
						t.Fatalf("verified targets (-want +got):\n%s", diff)
					}
					if diff := cmp.Diff(before, f.source); diff != "" {
						t.Fatalf("source mutated (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestBackendPreflightMatchedAndEligibleSets(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity} {
		t.Run(string(mode), func(t *testing.T) {
			f := newBackendFixture(t, mode)
			f.clusters[1].Status.Conditions = nil
			if err := f.workers["member-b"].Delete(t.Context(), backendTestRuntime()); err != nil {
				t.Fatal(err)
			}
			ctx, err := f.reconciler.preflightBackends(t.Context(), f.source, f.clusters, []string{"member-a"})
			wantErr := mode == v1beta1.PlacementModeSplitByCapacity
			if diff := cmp.Diff(wantErr, err != nil); diff != "" {
				t.Fatalf("hold: %s: %v", diff, err)
			}
			state := ctx.Value(backendContextKey{}).(*backendPreflight)
			wantStatus := corev1.ConditionTrue
			if mode == v1beta1.PlacementModeSplit || mode == v1beta1.PlacementModeSplitByCapacity {
				wantStatus = corev1.ConditionUnknown
			}
			if diff := cmp.Diff(wantStatus, state.condition.cond.Status); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestBackendPreflightDeletionIsNotATarget(t *testing.T) {
	f := newBackendFixture(t, v1beta1.PlacementModeSplitByCapacity)
	f.clusters[1].DeletionTimestamp = ptr.To(metav1.Now())
	delete(f.connections.m, "member-b")
	ctx, err := f.reconciler.preflightBackends(t.Context(), f.source, f.clusters, []string{"member-a", "member-b"})
	if err != nil {
		t.Fatal(err)
	}
	got := ctx.Value(backendContextKey{}).(*backendPreflight).clusters
	if diff := cmp.Diff(map[string]types.UID{"member-a": "member-a-uid"}, got); diff != "" {
		t.Fatal(diff)
	}
}
