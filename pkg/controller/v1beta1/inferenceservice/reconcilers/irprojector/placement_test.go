package irprojector

import (
	"context"
	"testing"

	"k8s.io/utils/ptr"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func TestPlacementExecutionProjection(t *testing.T) {
	for _, tt := range []struct {
		name    string
		derived bool
		edit    func(*v1beta1.PlacementExecutionPolicy)
		remove  bool
		wantErr bool
	}{
		{name: "ordinary local ignores policy"},
		{name: "same policy", derived: true},
		{name: "new policy", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PlanID = "plan-b"; p.Revision++; p.PauseSurge = false }},
		{name: "stale policy", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision-- }, wantErr: true},
		{name: "same revision conflicting plan", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PlanID = "plan-b" }, wantErr: true},
		{name: "same revision conflicting pause", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PauseSurge = false }, wantErr: true},
		{name: "missing policy cannot clear pause", derived: true, remove: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			service := baselineISVC("service-a", "team-a")
			policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 2, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true}
			stamp := func() {
				raw, err := protocol.Encode(policy)
				if err != nil {
					t.Fatal(err)
				}
				service.Annotations = map[string]string{constants.PlacementExecution: raw}
			}
			stamp()
			if tt.derived {
				service.Labels = map[string]string{constants.PlacementOrigin: "source-a"}
			}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
			p := minimalParams(t, service, c)
			created, err := EnsureInferenceReplica(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			var want *v1beta1.PlacementExecutionPolicy
			if tt.derived {
				want = policy.DeepCopy()
			}
			if diff := cmp.Diff(want, created.Spec.PlacementExecution); diff != "" {
				t.Fatalf("created policy (-want +got):\n%s", diff)
			}
			if tt.edit != nil {
				tt.edit(policy)
				stamp()
			}
			if tt.remove {
				delete(service.Annotations, constants.PlacementExecution)
			}
			_, err = EnsureInferenceReplica(ctx, p)
			if (err != nil) != tt.wantErr {
				t.Fatalf("projection error = %v, want error %t", err, tt.wantErr)
			}
			stored := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(created), stored); err != nil {
				t.Fatal(err)
			}
			if tt.wantErr {
				if diff := cmp.Diff(created, stored); diff != "" {
					t.Errorf("rejected projection mutated IR (-want +got):\n%s", diff)
				}
			} else if tt.derived {
				if diff := cmp.Diff(policy, stored.Spec.PlacementExecution); diff != "" {
					t.Errorf("updated policy (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestPlacementProjectionCannotOverwriteRacingAuthority(t *testing.T) {
	ctx := context.Background()
	service := baselineISVC("service-a", "team-a")
	service.Labels = map[string]string{constants.PlacementOrigin: "source-a"}
	policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true}
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	service.Annotations = map[string]string{constants.PlacementExecution: raw}
	base := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	created, err := EnsureInferenceReplica(ctx, minimalParams(t, service, base))
	if err != nil {
		t.Fatal(err)
	}
	policy.PlanID, policy.Revision = "plan-b", 2
	raw, err = protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	service.Annotations[constants.PlacementExecution] = raw
	newer := policy.DeepCopy()
	newer.PlanID, newer.Revision = "plan-c", 3
	injected := false
	c := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		if !injected {
			injected = true
			live := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(created), live); err != nil {
				return err
			}
			live.Spec.PlacementExecution = newer.DeepCopy()
			if err := c.Update(ctx, live); err != nil {
				return err
			}
		}
		return c.Patch(ctx, obj, patch, opts...)
	}})
	if _, err := EnsureInferenceReplica(ctx, minimalParams(t, service, c)); err == nil {
		t.Fatal("expected racing authority rejection")
	}
	stored := &v1beta1.InferenceReplica{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(created), stored); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(newer, stored.Spec.PlacementExecution); diff != "" {
		t.Errorf("authority regressed (-want +got):\n%s", diff)
	}
}

func TestPlacementReplicaLimitPreservesScaleRequests(t *testing.T) {
	for _, tt := range []struct {
		name      string
		requested int32
		floor     int
		release   bool
		want      *int32
	}{
		{name: "existing committed demand", requested: 5, floor: 2, want: ptr.To[int32](5)},
		{name: "autoscaler increase stays deferred", requested: 8, floor: 2, want: ptr.To[int32](5)},
		{name: "autoscaler decrease releases reservation", requested: 3, floor: 2, want: ptr.To[int32](3)},
		{name: "placement floor can increase reservation", requested: 8, floor: 6, want: ptr.To[int32](6)},
		{name: "placement floor protects against stale scaler", requested: 1, floor: 2, want: ptr.To[int32](2)},
		{name: "release clears growth limit", requested: 8, floor: 2, release: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := baselineISVC("service-a", "team-a")
			service.Labels = map[string]string{constants.PlacementOrigin: string(service.UID)}
			policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: service.UID, ClusterUID: "cluster-a"}
			stamp := func() {
				raw, err := protocol.Encode(policy)
				if err != nil {
					t.Fatal(err)
				}
				service.Annotations = map[string]string{constants.PlacementExecution: raw}
			}
			stamp()
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
			p := minimalParams(t, service, c)
			p.ResolvedAutoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
			ir, err := EnsureInferenceReplica(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			ir.Spec.Replicas = ptr.To[int32](5)
			if err := c.Update(t.Context(), ir); err != nil {
				t.Fatal(err)
			}
			policy.PlanID, policy.Revision, policy.PauseSurge = "plan-b", 2, true
			stamp()
			ir, err = EnsureInferenceReplica(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(ptr.To[int32](5), ir.Spec.PlacementReplicaLimit); diff != "" {
				t.Fatal(diff)
			}
			ir.Spec.Replicas = ptr.To(tt.requested)
			if err := c.Update(t.Context(), ir); err != nil {
				t.Fatal(err)
			}
			p.ComponentExt.MinReplicas = ptr.To(tt.floor)
			if tt.release || tt.floor != 2 {
				policy.PlanID, policy.Revision = "plan-c", 3
				policy.PauseSurge = !tt.release
				stamp()
			}
			got, err := EnsureInferenceReplica(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got.Spec.PlacementReplicaLimit); diff != "" {
				t.Errorf("limit (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(ptr.To(tt.requested), got.Spec.Replicas); diff != "" {
				t.Errorf("autoscaler request changed:\n%s", diff)
			}
			again, err := EnsureInferenceReplica(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, again); diff != "" {
				t.Errorf("stable projection changed:\n%s", diff)
			}
		})
	}
}

func TestPlacementPauseRequiresResolvedFloor(t *testing.T) {
	for i, floor := range []*int{nil, ptr.To(0), ptr.To(-1)} {
		t.Run([]string{"omitted", "zero", "negative"}[i], func(t *testing.T) {
			service := baselineISVC("service-a", "team-a")
			service.Labels = map[string]string{constants.PlacementOrigin: string(service.UID)}
			raw, err := protocol.Encode(&v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: service.UID, ClusterUID: "cluster-a", PauseSurge: true})
			if err != nil {
				t.Fatal(err)
			}
			service.Annotations = map[string]string{constants.PlacementExecution: raw}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
			p := minimalParams(t, service, c)
			p.ComponentExt.MinReplicas = floor
			if _, err := EnsureInferenceReplica(t.Context(), p); err == nil {
				t.Fatal("unresolved floor accepted")
			}
			list := &v1beta1.InferenceReplicaList{}
			if err := c.List(t.Context(), list); err != nil {
				t.Fatal(err)
			}
			if len(list.Items) != 0 {
				t.Fatal("unresolved floor created an IR")
			}
		})
	}
}
