package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	quotacapacity "sigs.k8s.io/ome/pkg/quota/capacity"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type capacityProviderFixture struct {
	r        *Reconciler
	source   *v1beta1.InferenceService
	clusters []v1beta1.WorkloadCluster
	workers  map[string]client.WithWatch
	clock    *clocktesting.FakeClock
}

func providerConfig() *CapacityConfig {
	return &CapacityConfig{RootName: "capacity-root", MaxAge: 5 * time.Minute, StabilityWindow: time.Minute, RefreshInterval: 10 * time.Second}
}

func capacityMemberObjects(name string, demand, available int64, now time.Time) []client.Object {
	resources := corev1.ResourceList{"example.com/gpu": *resource.NewQuantity(demand, resource.DecimalSI)}
	flavor := &kueuev1beta2.ResourceFlavor{ObjectMeta: metav1.ObjectMeta{Name: "gpu-a", UID: types.UID(name + "-flavor"), ResourceVersion: "1"}, Spec: kueuev1beta2.ResourceFlavorSpec{NodeLabels: map[string]string{"hardware": "a"}}}
	hash := quotacapacity.MappingFingerprint([]string{"example.com/gpu"}, []quotacapacity.Flavor{{Name: flavor.Name, UID: flavor.UID, NodeLabels: flavor.Spec.NodeLabels}})
	root := &v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: providerConfig().RootName, UID: types.UID(name + "-root"), ResourceVersion: "1"}, Spec: v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort}, Status: v1beta1.AcceleratorQuotaStatus{Capacity: []v1beta1.AcceleratorCapacityStatus{{
		ResourceName: "example.com/gpu", ResourceFlavor: flavor.Name, Allocatable: *resource.NewQuantity(available, resource.DecimalSI), ObservedAt: ptr.To(metav1.NewTime(now)),
		Attribution: &v1beta1.AcceleratorCapacityAttribution{Complete: true, FlavorUID: flavor.UID, NodeLabels: flavor.Spec.NodeLabels, FlavorSetHash: hash},
	}}}}
	runtime := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a", UID: types.UID(name + "-runtime"), ResourceVersion: "1"}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{PodSpec: v1beta1.PodSpec{NodeSelector: map[string]string{"hardware": "a"}}, Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1", Resources: corev1.ResourceRequirements{Requests: resources.DeepCopy(), Limits: resources}}}}}}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: "operator-system", UID: types.UID(name + "-config"), ResourceVersion: "1"}, Data: map[string]string{controllerconfig.AcceleratorResourcesConfigName: `["example.com/gpu"]`}}
	return []client.Object{flavor, root, runtime, cm}
}

func newCapacityProviderFixture(t *testing.T) *capacityProviderFixture {
	t.Helper()
	scheme := testScheme(t)
	if err := kueuev1beta2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0).UTC()
	f := &capacityProviderFixture{source: srcISVCSplit("", 12), workers: map[string]client.WithWatch{}, clock: clocktesting.NewFakeClock(now)}
	f.source.Namespace = "team-a"
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
	f.source.Spec.Engine = &v1beta1.EngineSpec{}
	f.source.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: ptr.To(runtimeselector.KindClusterServingRuntime)}
	f.source.Spec.DeploymentMode = ptr.To(constants.OMENative)
	connections := splitTestClusters{fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}}
	objects := []client.Object{f.source, &v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: providerConfig().RootName, UID: "fleet-root"}, Spec: v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort}}}
	for i, name := range []string{"member-a", "member-b", "member-c"} {
		registration := wc(name, true, nil)
		f.clusters = append(f.clusters, registration)
		objects = append(objects, registration.DeepCopy())
		worker := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(capacityMemberObjects(name, []int64{2, 4, 2}[i], []int64{16, 16, 0}[i], now)...).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*v1beta1.InferenceService); ok {
				obj.SetUID(types.UID("derived-" + name))
				obj.SetGeneration(1)
			}
			return c.Create(ctx, obj, opts...)
		}}).Build()
		f.workers[name] = worker
		connections.m[name] = workloadcluster.NewNeverCachingClient(worker)
	}
	f.r, _ = newPlacer(scheme, connections, objects...)
	f.r.Capacity, f.r.CapacityClock = providerConfig(), f.clock
	f.r.MemberOperatorNamespace = "operator-system"
	f.syncFleet(t)
	return f
}

func (f *capacityProviderFixture) syncFleet(t *testing.T) {
	t.Helper()
	root := &v1beta1.AcceleratorQuota{}
	key := client.ObjectKey{Name: providerConfig().RootName}
	if err := f.r.Get(t.Context(), key, root); err != nil {
		t.Fatal(err)
	}
	root.Status.Capacity = []v1beta1.AcceleratorCapacityStatus{{ResourceName: "example.com/gpu", ResourceFlavor: "gpu-a"}}
	for _, cluster := range f.clusters {
		member := &v1beta1.AcceleratorQuota{}
		if err := f.workers[cluster.Name].Get(t.Context(), key, member); err != nil {
			t.Fatal(err)
		}
		row := member.Status.Capacity[0]
		root.Status.Capacity[0].PerCluster = append(root.Status.Capacity[0].PerCluster, v1beta1.AcceleratorClusterCapacityStatus{Cluster: cluster.Name, ClusterUID: cluster.UID, ReportUID: member.UID, ReportResourceVersion: member.ResourceVersion, ReportAvailable: true, Allocatable: row.Allocatable.DeepCopy(), ObservedAt: row.ObservedAt.DeepCopy(), Attribution: row.Attribution.DeepCopy()})
	}
	if err := f.r.Update(t.Context(), root); err != nil {
		t.Fatal(err)
	}
}

func TestReadSplitCapacity(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*testing.T, *capacityProviderFixture)
		wantErr bool
	}{
		{name: "empty members with heterogeneous demand"},
		{name: "readiness does not change the hardware ratio", edit: func(_ *testing.T, f *capacityProviderFixture) {
			f.clusters[1].Status.Conditions[0].Status = metav1.ConditionFalse
		}},
		{name: "missing configuration", edit: func(_ *testing.T, f *capacityProviderFixture) { f.r.Capacity = nil }, wantErr: true},
		{name: "unknown member registration", edit: func(_ *testing.T, f *capacityProviderFixture) { f.clusters[1].UID = "replacement" }, wantErr: true},
		{name: "duplicate registration", edit: func(_ *testing.T, f *capacityProviderFixture) { f.clusters = append(f.clusters, f.clusters[0]) }, wantErr: true},
		{name: "unsupported member backend", edit: func(_ *testing.T, f *capacityProviderFixture) {
			f.source.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
		}, wantErr: true},
		{name: "stale hardware", edit: func(_ *testing.T, f *capacityProviderFixture) { f.clock.Step(providerConfig().MaxAge) }, wantErr: true},
		{name: "report version mismatch", edit: func(t *testing.T, f *capacityProviderFixture) {
			root := &v1beta1.AcceleratorQuota{}
			if err := f.workers["member-b"].Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
				t.Fatal(err)
			}
			root.Annotations = map[string]string{"example.com/revision": "updated"}
			if err := f.workers["member-b"].Update(t.Context(), root); err != nil {
				t.Fatal(err)
			}
		}, wantErr: true},
		{name: "local collision", edit: func(t *testing.T, f *capacityProviderFixture) {
			if err := f.workers["member-b"].Create(t.Context(), &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: f.source.Name, Namespace: f.source.Namespace}}); err != nil {
				t.Fatal(err)
			}
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCapacityProviderFixture(t)
			if tt.edit != nil {
				tt.edit(t, f)
			}
			before := f.source.DeepCopy()
			got, err := f.r.readSplitCapacity(t.Context(), f.source, f.clusters, f.clock.Now())
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error: %s: %v", diff, err)
			}
			if tt.wantErr {
				if diff := cmp.Diff(map[string]capacity.Sample(nil), got); diff != "" {
					t.Fatal(diff)
				}
			} else {
				weights := map[string]int64{}
				for name, sample := range got {
					weights[name] = sample.Weight
					if err := protocol.ValidateDemandComponents(f.source, sample.DemandContract); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(sample.DemandFingerprint, sample.DemandContract.Fingerprint); diff != "" {
						t.Fatal(diff)
					}
				}
				if diff := cmp.Diff(map[string]int64{"member-a": 8, "member-b": 4, "member-c": 0}, weights); diff != "" {
					t.Fatal(diff)
				}
			}
			if diff := cmp.Diff(before, f.source); diff != "" {
				t.Fatalf("source mutated: %s", diff)
			}
		})
	}
}

func configureContractApplication(t *testing.T, source *v1beta1.InferenceService, worker client.WithWatch) *CapacityConfig {
	t.Helper()
	if err := kueuev1beta2.AddToScheme(worker.Scheme()); err != nil {
		t.Fatal(err)
	}
	source.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: ptr.To(runtimeselector.KindClusterServingRuntime)}
	source.Spec.DeploymentMode = ptr.To(constants.OMENative)
	for _, obj := range capacityMemberObjects("member-a", 2, 8, time.Now()) {
		obj.SetResourceVersion("")
		if err := worker.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	r := &Reconciler{Capacity: providerConfig(), MemberOperatorNamespace: "operator-system"}
	desired := DeriveISVC(source, "", "")
	demand, err := r.resolveCapacityOn(t.Context(), worker, source, desired)
	if err != nil {
		t.Fatal(err)
	}
	assignment := source.Status.Placement.Candidates[0].Allocation
	assignment.Capacity.DemandContract = demand.Contract.DeepCopy()
	assignment.Capacity.DemandFingerprint = demand.Contract.Fingerprint
	root := demand.Hardware[0]
	assignment.Capacity.Pools = []v1beta1.PlacementCapacityPool{{ResourceName: root.ResourceName, ResourceFlavor: root.ResourceFlavor, Demand: 2, Allocatable: 8, ObservedAt: *root.ObservedAt, ReportUID: demand.Report.UID, ReportResourceVersion: demand.Report.ResourceVersion, Attribution: *root.Attribution.DeepCopy()}}
	return providerConfig()
}

func TestCapacityProviderRejectsChangesDuringRead(t *testing.T) {
	for _, change := range []string{"runtime", "registration", "source read error"} {
		t.Run(change, func(t *testing.T) {
			f := newCapacityProviderFixture(t)
			f.r.APIReader = interceptor.NewClient(f.r.Client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := cl.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if _, root := obj.(*v1beta1.AcceleratorQuota); !root {
					return nil
				}
				switch change {
				case "runtime":
					runtime := &v1beta1.ClusterServingRuntime{}
					if err := f.workers["member-a"].Get(ctx, client.ObjectKey{Name: "runtime-a"}, runtime); err != nil {
						return err
					}
					runtime.Spec.EngineConfig.Runner.Image = "example.com/serving:v2"
					return f.workers["member-a"].Update(ctx, runtime)
				case "registration":
					registration := f.clusters[0].DeepCopy()
					if err := cl.Get(ctx, client.ObjectKeyFromObject(registration), registration); err != nil {
						return err
					}
					registration.UID = "replacement"
					return cl.Update(ctx, registration)
				default:
					return errors.New("fleet report unavailable")
				}
			}})
			got, err := f.r.readSplitCapacity(t.Context(), f.source, f.clusters, f.clock.Now())
			if err == nil {
				t.Fatal("unverified inputs returned capacity")
			}
			if diff := cmp.Diff(map[string]capacity.Sample(nil), got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
