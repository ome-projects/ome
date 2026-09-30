package resolution

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestResolveHomeWithMemberStatusUpdates(t *testing.T) {
	for _, phase := range []string{"before resolution", "during resolution", "after resolution"} {
		t.Run(phase, func(t *testing.T) {
			f := newRuntimeFixture()
			f.service.Spec.DeploymentMode = ptr.To(constants.OMENative)
			f.service.Spec.Decoder, f.service.Spec.Router = nil, nil
			f.pin(t, false)
			before := f.standing.DeepCopy()
			calls := 0
			checking := false
			cl := interceptor.NewClient(fixtureClient(t, f), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := c.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if member, ok := obj.(*v1beta1.InferenceService); ok {
						calls++
						if phase == "before resolution" || (phase == "during resolution" && calls > 1) || checking {
							member.ResourceVersion = strconv.Itoa(calls + 1)
							member.Status.ObservedGeneration = int64(calls)
							member.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {}}
							member.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "member-controller", Subresource: "status", Time: ptr.To(metav1.Now())}}
						}
					}
					return nil
				},
			})
			resolved, floors, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).ResolveHome(t.Context(), f.service, f.standing)
			if err != nil {
				t.Fatal(err)
			}
			want := []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}
			if diff := cmp.Diff(want, floors); diff != "" {
				t.Fatal(diff)
			}
			checking = true
			for range 3 {
				if err := resolved.Check(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if diff := cmp.Diff(before, f.standing); diff != "" {
				t.Fatalf("standing member mutated (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMemberRuntimeInputChanges(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.InferenceService)
		wantErr bool
	}{
		{name: "status observation", edit: func(s *v1beta1.InferenceService) { s.Status.ObservedGeneration++ }},
		{name: "managed field bookkeeping", edit: func(s *v1beta1.InferenceService) {
			s.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "status-writer", Time: ptr.To(metav1.Now())}}
		}},
		{name: "runtime pin", edit: func(s *v1beta1.InferenceService) { s.Status.PinnedRevisionName = "replacement-pin" }, wantErr: true},
		{name: "runtime sync token", edit: func(s *v1beta1.InferenceService) { s.Status.LastRuntimeSyncToken = "acknowledged" }, wantErr: true},
		{name: "spec floor", edit: func(s *v1beta1.InferenceService) { s.Spec.Engine.MinReplicas = ptr.To(7) }, wantErr: true},
		{name: "generation", edit: func(s *v1beta1.InferenceService) { s.Generation++ }, wantErr: true},
		{name: "labels", edit: func(s *v1beta1.InferenceService) { s.Labels = map[string]string{"selected": "changed"} }, wantErr: true},
		{name: "annotations", edit: func(s *v1beta1.InferenceService) {
			s.Annotations = map[string]string{constants.RuntimeSyncAnnotationKey: "advance"}
		}, wantErr: true},
		{name: "ownership", edit: func(s *v1beta1.InferenceService) { s.OwnerReferences = []metav1.OwnerReference{{UID: "other-owner"}} }, wantErr: true},
		{name: "replacement", edit: func(s *v1beta1.InferenceService) { s.UID = "replacement" }, wantErr: true},
		{name: "deletion", edit: func(s *v1beta1.InferenceService) { s.DeletionTimestamp = ptr.To(metav1.Now()) }, wantErr: true},
		{name: "missing UID", edit: func(s *v1beta1.InferenceService) { s.UID = "" }, wantErr: true},
		{name: "missing resource version", edit: func(s *v1beta1.InferenceService) { s.ResourceVersion = "" }, wantErr: true},
	} {
		for _, phase := range []string{"during resolution", "after resolution"} {
			t.Run(tt.name+"/"+phase, func(t *testing.T) {
				f := newRuntimeFixture()
				f.pin(t, false)
				calls, checking := 0, false
				cl := interceptor.NewClient(fixtureClient(t, f), interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if err := c.Get(ctx, key, obj, opts...); err != nil {
							return err
						}
						if member, ok := obj.(*v1beta1.InferenceService); ok {
							calls++
							if (phase == "during resolution" && calls > 1) || checking {
								member.ResourceVersion = strconv.Itoa(calls + 1)
								tt.edit(member)
							}
						}
						return nil
					},
				})
				resolved, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).Resolve(t.Context(), f.service, f.standing)
				if phase == "after resolution" {
					if err != nil {
						t.Fatal(err)
					}
					checking = true
					err = resolved.Check(t.Context())
				}
				if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
					t.Fatalf("runtime input check (-want +got):\n%s\n%v", diff, err)
				}
			})
		}
	}
}
