package resolution

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func TestResolveUnitRejectsIncompleteReads(t *testing.T) {
	for _, tt := range []struct {
		name      string
		operation string
	}{
		{name: "config read fails", operation: "config-error"},
		{name: "config changes during rendering", operation: "config-change"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, cm := newUnitFixture()
			unitAccelerator(&f)
			changed := false
			cl := unitClient(t, f, interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
				if _, config := object.(*corev1.ConfigMap); config && tt.operation == "config-error" {
					return errors.New("config read unavailable")
				}
				// The accelerator class is read while the engine's render inputs
				// are built: after the rendering config, before the observation
				// is checked.
				if _, accelerator := object.(*v1beta1.AcceleratorClass); accelerator && tt.operation == "config-change" && !changed {
					fresh := &corev1.ConfigMap{}
					if err := cl.Get(ctx, client.ObjectKeyFromObject(cm), fresh); err != nil {
						return err
					}
					fresh.Data[controllerconfig.AcceleratorResourcesConfigName] = `[]`
					if err := cl.Update(ctx, fresh); err != nil {
						return err
					}
					changed = true
				}
				return cl.Get(ctx, key, object, opts...)
			}})
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).ResolveUnit(t.Context(), f.service, nil)
			if err == nil {
				t.Fatal("incomplete observation was accepted")
			}
			if diff := cmp.Diff(true, got == nil); diff != "" {
				t.Fatalf("no partial result (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolvedUnitDependencyFences(t *testing.T) {
	for _, tt := range []struct {
		name            string
		mutate          func(*testing.T, runtimeFixture, client.Client)
		wantInvalid     bool
		wantFingerprint bool
	}{
		{name: "unchanged"},
		{name: "status-only accelerator update", wantInvalid: true, mutate: func(t *testing.T, _ runtimeFixture, cl client.Client) {
			ac := &v1beta1.AcceleratorClass{}
			if err := cl.Get(t.Context(), client.ObjectKey{Name: "accelerator-a"}, ac); err != nil {
				t.Fatal(err)
			}
			ac.Status.AvailableAccelerators = 7
			if err := cl.Update(t.Context(), ac); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "accelerator resource shape changes", wantInvalid: true, wantFingerprint: true, mutate: func(t *testing.T, _ runtimeFixture, cl client.Client) {
			ac := &v1beta1.AcceleratorClass{}
			if err := cl.Get(t.Context(), client.ObjectKey{Name: "accelerator-a"}, ac); err != nil {
				t.Fatal(err)
			}
			ac.Spec.Resources[0].Quantity = resource.MustParse("4")
			if err := cl.Update(t.Context(), ac); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "configuration replaced", wantInvalid: true, wantFingerprint: true, mutate: func(t *testing.T, f runtimeFixture, cl client.Client) {
			cm := f.extra[0].DeepCopyObject().(*corev1.ConfigMap)
			if err := cl.Delete(t.Context(), cm); err != nil {
				t.Fatal(err)
			}
			cm.UID, cm.ResourceVersion = "replacement-config", ""
			if err := cl.Create(t.Context(), cm); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "runtime class changes", wantInvalid: true, wantFingerprint: true, mutate: func(t *testing.T, _ runtimeFixture, cl client.Client) {
			class := &nodev1.RuntimeClass{}
			if err := cl.Get(t.Context(), client.ObjectKey{Name: "sandbox"}, class); err != nil {
				t.Fatal(err)
			}
			class.Scheduling = &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox": "enabled"}}
			if err := cl.Update(t.Context(), class); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unrelated namespace config", mutate: func(t *testing.T, f runtimeFixture, cl client.Client) {
			cm := f.extra[0].DeepCopyObject().(*corev1.ConfigMap)
			cm.Namespace, cm.UID, cm.ResourceVersion = "other-system", "other-config", ""
			if err := cl.Create(t.Context(), cm); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := newUnitFixture()
			unitAccelerator(&f)
			f.service.Spec.Engine.RuntimeClassName = ptr.To("sandbox")
			f.extra = append(f.extra, &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "sandbox", UID: "class-uid"}, Handler: "sandbox-handler"})
			cl := unitClient(t, f, interceptor.Funcs{})
			resolver := Resolver{Client: cl, OperatorNamespace: f.namespace}
			original, err := resolver.ResolveUnit(t.Context(), f.service, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.mutate != nil {
				tt.mutate(t, f, cl)
			}
			if diff := cmp.Diff(tt.wantInvalid, original.Check(t.Context()) != nil); diff != "" {
				t.Fatalf("invalid observation (-want +got):\n%s", diff)
			}
			fresh, err := resolver.ResolveUnit(t.Context(), f.service, nil)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantFingerprint, original.Demand.Fingerprint != fresh.Demand.Fingerprint); diff != "" {
				t.Fatalf("changed fingerprint (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveUnitExcludesReplicaFloorsFromDemand(t *testing.T) {
	f, _ := newUnitFixture()
	resolver := Resolver{Client: unitClient(t, f, interceptor.Funcs{}), OperatorNamespace: f.namespace}
	original, err := resolver.ResolveUnit(t.Context(), f.service, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.service.Spec.Engine.MinReplicas = ptr.To(9)
	fresh, err := resolver.ResolveUnit(t.Context(), f.service, nil)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(original.Demand.Fingerprint, fresh.Demand.Fingerprint); diff != "" {
		t.Fatalf("per-replica demand fingerprint (-want +got):\n%s", diff)
	}
}

func TestResolvedUnitCheckRequiresObservation(t *testing.T) {
	for _, tt := range []struct {
		name string
		unit *ResolvedUnit
	}{
		{name: "nil"},
		{name: "empty", unit: &ResolvedUnit{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.unit.Check(t.Context()); err == nil {
				t.Fatal("unobserved unit was accepted")
			}
		})
	}
}
