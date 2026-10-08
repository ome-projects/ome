package capacity

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var sampleTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func mapping() *v1beta1.AcceleratorCapacityAttribution {
	return &v1beta1.AcceleratorCapacityAttribution{FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", Complete: true, NodeLabels: map[string]string{"accelerator": "gpu-a"}}
}

type fixture struct {
	root     *v1beta1.AcceleratorQuota
	clusters []v1beta1.WorkloadCluster
	demands  map[string]Demand
}

func inputs() fixture {
	return fixture{
		root: &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: "root", UID: "fleet-root"},
			Spec:       v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort},
			Status: v1beta1.AcceleratorQuotaStatus{Capacity: []v1beta1.AcceleratorCapacityStatus{{
				ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a",
				PerCluster: []v1beta1.AcceleratorClusterCapacityStatus{{
					Cluster: "member-a", ClusterUID: "registration-a", ReportUID: "member-root", ReportResourceVersion: "10", ReportAvailable: true,
					Allocatable: resource.MustParse("96"), ObservedAt: &metav1.Time{Time: sampleTime}, Attribution: mapping(),
				}},
			}}},
		},
		clusters: []v1beta1.WorkloadCluster{{ObjectMeta: metav1.ObjectMeta{Name: "member-a", UID: "registration-a"}}},
		demands:  map[string]Demand{"member-a": {Fingerprint: "demand-a", Pools: []Pool{{ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a", Quantity: resource.MustParse("8"), FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", NodeLabels: map[string]string{"accelerator": "gpu-a"}}}}},
	}
}

func read(t *testing.T, f fixture) (map[string]Sample, error) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.root.DeepCopy()).Build()
	return (Reader{Client: cl, RootName: "root", MaxAge: time.Minute}).Read(context.Background(), f.clusters, f.demands, sampleTime.Add(time.Second))
}

func row(f *fixture) *v1beta1.AcceleratorClusterCapacityStatus {
	return &f.root.Status.Capacity[0].PerCluster[0]
}
func pool(f *fixture) *Pool { return &f.demands["member-a"].Pools[0] }

func TestReadNominalCapacity(t *testing.T) {
	for _, tt := range []struct {
		name            string
		mutate          func(*fixture)
		wantWeight      int64
		wantDemand      int64
		wantAllocatable int64
	}{
		{name: "nominal whole replicas", wantWeight: 12, wantDemand: 8, wantAllocatable: 96},
		{name: "whole replica truncation", wantWeight: 11, wantDemand: 8, wantAllocatable: 95, mutate: func(f *fixture) { row(f).Allocatable = resource.MustParse("95") }},
		{name: "explicit zero", wantDemand: 8, mutate: func(f *fixture) { row(f).Allocatable = resource.MustParse("0") }},
		{name: "smaller pool than one replica", wantDemand: 8, wantAllocatable: 4, mutate: func(f *fixture) { row(f).Allocatable = resource.MustParse("4") }},
		{name: "engine and decoder share one pool", wantWeight: 6, wantDemand: 16, wantAllocatable: 96, mutate: func(f *fixture) {
			d := f.demands["member-a"]
			d.Pools = append(d.Pools, d.Pools[0])
			f.demands["member-a"] = d
		}},
		{name: "high water and aggregate totals do not affect weight", wantWeight: 12, wantDemand: 8, wantAllocatable: 96, mutate: func(f *fixture) {
			row(f).HighWaterMark = resource.MustParse("1000")
			f.root.Status.Capacity[0].Allocatable = resource.MustParse("5000")
			f.root.Status.Capacity[0].HighWaterMark = resource.MustParse("10000")
			f.root.Status.Budgets = []v1beta1.AcceleratorBudgetStatus{{Nominal: resource.MustParse("1"), Admitted: resource.MustParse("100"), Reserved: resource.MustParse("200")}}
		}},
		{name: "decimal quantity storage", wantWeight: 12, wantDemand: 8, wantAllocatable: 96, mutate: func(f *fixture) { row(f).Allocatable.ToDec(); pool(f).Quantity.ToDec() }},
		{name: "ratio unit counts primary replicas", wantWeight: 57, wantDemand: 5, wantAllocatable: 288, mutate: func(f *fixture) {
			d := f.demands["member-a"]
			d.PrimaryUnits = 3
			d.Pools[0].Quantity = resource.MustParse("5")
			f.demands["member-a"] = d
		}},
		{name: "single primary unit keeps raw evidence", wantWeight: 12, wantDemand: 8, wantAllocatable: 96, mutate: func(f *fixture) {
			d := f.demands["member-a"]
			d.PrimaryUnits = 1
			f.demands["member-a"] = d
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := inputs()
			if tt.mutate != nil {
				tt.mutate(&f)
			}
			before := f.root.DeepCopy()
			beforeDemand, err := json.Marshal(f.demands)
			if err != nil {
				t.Fatal(err)
			}
			got, err := read(t, f)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]Sample{"member-a": {ClusterUID: "registration-a", DemandFingerprint: "demand-a", Weight: tt.wantWeight, Pools: []Evidence{{
				ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a", Demand: tt.wantDemand, Allocatable: tt.wantAllocatable,
				ObservedAt: metav1.NewTime(sampleTime), ReportUID: "member-root", ReportResourceVersion: "10", Attribution: *mapping(),
			}}}}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("samples (-want +got):\n%s", diff)
			}
			got["member-a"].Pools[0].Attribution.NodeLabels["accelerator"] = "changed"
			afterDemand, err := json.Marshal(f.demands)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(string(beforeDemand), string(afterDemand)); diff != "" {
				t.Fatalf("demand mutation (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, f.root, cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })); diff != "" {
				t.Fatalf("input mutation (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReadRejectsUnknownCapacity(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mutate    func(*fixture)
		wantError string
	}{
		{name: "absent report", wantError: "report is missing", mutate: func(f *fixture) { f.root.Status.Capacity = nil }},
		{name: "recreated cluster", wantError: "report is missing", mutate: func(f *fixture) { row(f).ClusterUID = "old-registration" }},
		{name: "unavailable report", wantError: "report is missing", mutate: func(f *fixture) { row(f).ReportAvailable = false }},
		{name: "source UID missing", wantError: "report is missing", mutate: func(f *fixture) { row(f).ReportUID = "" }},
		{name: "source version missing", wantError: "report is missing", mutate: func(f *fixture) { row(f).ReportResourceVersion = "" }},
		{name: "timestamp missing", wantError: "fresh observation", mutate: func(f *fixture) { row(f).ObservedAt = nil }},
		{name: "timestamp at expiry boundary", wantError: "fresh observation", mutate: func(f *fixture) { row(f).ObservedAt = &metav1.Time{Time: sampleTime.Add(time.Second - time.Minute)} }},
		{name: "future timestamp", wantError: "fresh observation", mutate: func(f *fixture) { row(f).ObservedAt = &metav1.Time{Time: sampleTime.Add(2 * time.Second)} }},
		{name: "mapping missing", wantError: "mapping is unverified", mutate: func(f *fixture) { row(f).Attribution = nil }},
		{name: "mapping incomplete", wantError: "mapping is unverified", mutate: func(f *fixture) { row(f).Attribution.Complete = false }},
		{name: "flavor recreated", wantError: "mapping is unverified", mutate: func(f *fixture) { row(f).Attribution.FlavorUID = "old-flavor" }},
		{name: "mapping changed", wantError: "mapping is unverified", mutate: func(f *fixture) { row(f).Attribution.FlavorSetHash = "old-mapping" }},
		{name: "selector changed", wantError: "mapping is unverified", mutate: func(f *fixture) { row(f).Attribution.NodeLabels["accelerator"] = "gpu-b" }},
		{name: "negative allocatable", wantError: "allocatable", mutate: func(f *fixture) { row(f).Allocatable = resource.MustParse("-1") }},
		{name: "fractional allocatable", wantError: "allocatable", mutate: func(f *fixture) { row(f).Allocatable = resource.MustParse("500m") }},
		{name: "allocatable overflow", wantError: "allocatable", mutate: func(f *fixture) { row(f).Allocatable = resource.MustParse("9223372036854775808") }},
		{name: "demand missing", wantError: "resolved accelerator demand", mutate: func(f *fixture) { f.demands = nil }},
		{name: "fingerprint missing", wantError: "resolved accelerator demand", mutate: func(f *fixture) { d := f.demands["member-a"]; d.Fingerprint = ""; f.demands["member-a"] = d }},
		{name: "pools missing", wantError: "resolved accelerator demand", mutate: func(f *fixture) { d := f.demands["member-a"]; d.Pools = nil; f.demands["member-a"] = d }},
		{name: "resource missing", wantError: "mapping fingerprint", mutate: func(f *fixture) { pool(f).ResourceName = "" }},
		{name: "flavor name missing", wantError: "mapping fingerprint", mutate: func(f *fixture) { pool(f).ResourceFlavor = "" }},
		{name: "flavor UID missing", wantError: "mapping fingerprint", mutate: func(f *fixture) { pool(f).FlavorUID = "" }},
		{name: "flavor set missing", wantError: "mapping fingerprint", mutate: func(f *fixture) { pool(f).FlavorSetHash = "" }},
		{name: "zero demand", wantError: "positive whole units", mutate: func(f *fixture) { pool(f).Quantity = resource.MustParse("0") }},
		{name: "fractional demand", wantError: "positive whole units", mutate: func(f *fixture) { pool(f).Quantity = resource.MustParse("500m") }},
		{name: "demand overflow", wantError: "positive whole units", mutate: func(f *fixture) { pool(f).Quantity = resource.MustParse("9223372036854775808") }},
		{name: "conflicting component mappings", wantError: "components disagree", mutate: func(f *fixture) {
			d := f.demands["member-a"]
			other := d.Pools[0]
			other.FlavorUID = "different-flavor"
			d.Pools = append(d.Pools, other)
			f.demands["member-a"] = d
		}},
		{name: "combined demand overflow", wantError: "combined demand", mutate: func(f *fixture) {
			d := f.demands["member-a"]
			other := d.Pools[0]
			other.Quantity = *resource.NewQuantity(math.MaxInt64, resource.DecimalSI)
			d.Pools = append(d.Pools, other)
			f.demands["member-a"] = d
		}},
		{name: "duplicate report", wantError: "duplicate report", mutate: func(f *fixture) {
			f.root.Status.Capacity = append(f.root.Status.Capacity, *f.root.Status.Capacity[0].DeepCopy())
		}},
		{name: "duplicate registration", wantError: "duplicate matched registration", mutate: func(f *fixture) { f.clusters = append(f.clusters, f.clusters[0]) }},
		{name: "missing member after usable member", wantError: "report is missing", mutate: func(f *fixture) {
			w := f.clusters[0]
			w.Name = "member-b"
			w.UID = "registration-b"
			f.clusters = append(f.clusters, w)
			f.demands["member-b"] = f.demands["member-a"]
		}},
		{name: "registry UID missing", wantError: "identified live registration", mutate: func(f *fixture) { f.clusters[0].UID = "" }},
		{name: "registry name missing", wantError: "identified live registration", mutate: func(f *fixture) { f.clusters[0].Name = "" }},
		{name: "registry terminating", wantError: "identified live registration", mutate: func(f *fixture) { f.clusters[0].DeletionTimestamp = &metav1.Time{Time: sampleTime} }},
		{name: "unidentified root", wantError: "identified live root", mutate: func(f *fixture) { f.root.UID = "" }},
		{name: "root terminating", wantError: "identified live root", mutate: func(f *fixture) {
			f.root.DeletionTimestamp = &metav1.Time{Time: sampleTime}
			f.root.Finalizers = []string{"example.com/cleanup"}
		}},
		{name: "root parented", wantError: "identified live root", mutate: func(f *fixture) { f.root.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: "parent"} }},
		{name: "root is a leaf", wantError: "identified live root", mutate: func(f *fixture) { f.root.Spec.Role = v1beta1.AcceleratorQuotaRoleClusterQueue }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := inputs()
			tt.mutate(&f)
			got, err := read(t, f)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Read() error = %v, want containing %q", err, tt.wantError)
			}
			if diff := cmp.Diff(map[string]Sample(nil), got); diff != "" {
				t.Fatalf("partial results escaped (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReadUsesBottleneckAndCoherentReports(t *testing.T) {
	for _, tt := range []struct {
		name           string
		secondResource string
		secondFlavor   string
		secondQuantity string
		secondCapacity string
		secondVersion  string
		secondHash     string
		wantWeight     int64
		wantError      bool
	}{
		{name: "different accelerator resources", secondResource: "google.com/tpu", secondFlavor: "tpu-a", secondQuantity: "4", secondCapacity: "10", secondVersion: "10", wantWeight: 2},
		{name: "different flavors of one resource", secondResource: "nvidia.com/gpu", secondFlavor: "gpu-b", secondQuantity: "16", secondCapacity: "20", secondVersion: "10", wantWeight: 1},
		{name: "mixed attribution mappings", secondResource: "nvidia.com/gpu", secondFlavor: "gpu-b", secondQuantity: "16", secondCapacity: "20", secondVersion: "10", secondHash: "another-mapping", wantError: true},
		{name: "mixed source versions", secondResource: "nvidia.com/gpu", secondFlavor: "gpu-b", secondQuantity: "16", secondCapacity: "20", secondVersion: "11", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := inputs()
			other := *pool(&f)
			other.ResourceName, other.ResourceFlavor, other.Quantity = tt.secondResource, tt.secondFlavor, resource.MustParse(tt.secondQuantity)
			other.FlavorUID = "flavor-b"
			if tt.secondHash != "" {
				other.FlavorSetHash = tt.secondHash
			}
			d := f.demands["member-a"]
			d.Pools = append(d.Pools, other)
			f.demands["member-a"] = d
			cell := *f.root.Status.Capacity[0].DeepCopy()
			cell.ResourceName, cell.ResourceFlavor = tt.secondResource, tt.secondFlavor
			cell.PerCluster[0].Allocatable = resource.MustParse(tt.secondCapacity)
			cell.PerCluster[0].ReportResourceVersion = tt.secondVersion
			cell.PerCluster[0].Attribution.FlavorUID = other.FlavorUID
			cell.PerCluster[0].Attribution.FlavorSetHash = other.FlavorSetHash
			f.root.Status.Capacity = append(f.root.Status.Capacity, cell)
			got, err := read(t, f)
			if diff := cmp.Diff(tt.wantError, err != nil); diff != "" {
				t.Fatalf("error presence (-want +got):\n%s; error: %v", diff, err)
			}
			if err != nil {
				return
			}
			if diff := cmp.Diff(tt.wantWeight, got["member-a"].Weight); diff != "" {
				t.Fatalf("bottleneck (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(2, len(got["member-a"].Pools)); diff != "" {
				t.Fatalf("pool evidence (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReaderConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name      string
		configure func(*Reader, *time.Time)
		wantError string
	}{
		{name: "empty match needs no report"},
		{name: "missing client", wantError: "requires a client", configure: func(r *Reader, _ *time.Time) { r.Client = nil }},
		{name: "missing root name", wantError: "requires a client", configure: func(r *Reader, _ *time.Time) { r.RootName = "" }},
		{name: "missing freshness policy", wantError: "positive maximum age", configure: func(r *Reader, _ *time.Time) { r.MaxAge = 0 }},
		{name: "negative freshness policy", wantError: "positive maximum age", configure: func(r *Reader, _ *time.Time) { r.MaxAge = -time.Second }},
		{name: "missing current time", wantError: "current time", configure: func(_ *Reader, now *time.Time) { *now = time.Time{} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := Reader{Client: fake.NewClientBuilder().Build(), RootName: "root", MaxAge: time.Minute}
			now := sampleTime
			if tt.configure != nil {
				tt.configure(&r, &now)
			}
			got, err := r.Read(context.Background(), nil, nil, now)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("Read() error = %v, want containing %q", err, tt.wantError)
				}
				if diff := cmp.Diff(map[string]Sample(nil), got); diff != "" {
					t.Fatalf("invalid configuration result (-want +got):\n%s", diff)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]Sample{}, got); diff != "" {
				t.Fatalf("empty match (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReaderMissingRoot(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r := Reader{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), RootName: "root", MaxAge: time.Minute}
	f := inputs()
	got, err := r.Read(context.Background(), f.clusters, f.demands, sampleTime)
	if err == nil || !strings.Contains(err.Error(), "read fleet capacity") {
		t.Fatalf("Read() error = %v, want failed root read", err)
	}
	if diff := cmp.Diff(map[string]Sample(nil), got); diff != "" {
		t.Fatalf("missing root result (-want +got):\n%s", diff)
	}
}

func TestReaderNormalizesEachMembersResolvedShape(t *testing.T) {
	f := inputs()
	second := f.clusters[0]
	second.Name, second.UID = "member-b", "registration-b"
	f.clusters = append(f.clusters, second)
	secondPool := *pool(&f)
	secondPool.Quantity = resource.MustParse("4")
	f.demands[second.Name] = Demand{Fingerprint: "demand-b", Pools: []Pool{secondPool}}
	secondReport := row(&f).DeepCopy()
	secondReport.Cluster, secondReport.ClusterUID, secondReport.ReportUID = second.Name, second.UID, "member-b-root"
	secondReport.Allocatable = resource.MustParse("48")
	f.root.Status.Capacity[0].PerCluster = append(f.root.Status.Capacity[0].PerCluster, *secondReport)
	got, err := read(t, f)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Sample{
		"member-a": {ClusterUID: "registration-a", DemandFingerprint: "demand-a", Weight: 12, Pools: []Evidence{{ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a", Demand: 8, Allocatable: 96, ObservedAt: metav1.NewTime(sampleTime), ReportUID: "member-root", ReportResourceVersion: "10", Attribution: *mapping()}}},
		"member-b": {ClusterUID: "registration-b", DemandFingerprint: "demand-b", Weight: 12, Pools: []Evidence{{ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a", Demand: 4, Allocatable: 48, ObservedAt: metav1.NewTime(sampleTime), ReportUID: "member-b-root", ReportResourceVersion: "10", Attribution: *mapping()}}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("normalized samples (-want +got):\n%s", diff)
	}
}
