package placement

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPlacementReconcileLeavesNonparticipantsUntouched(t *testing.T) {
	for _, tt := range []struct {
		name      string
		configure func(*v1beta1.InferenceService)
	}{
		{name: "local service"},
		{name: "local finalizer", configure: func(s *v1beta1.InferenceService) { s.Finalizers = []string{"example.com/local-controller"} }},
		{name: "placement finalizer without current intent", configure: func(s *v1beta1.InferenceService) { s.Finalizers = []string{PlacementFinalizer} }},
		{name: "retained placement status without intent", configure: func(s *v1beta1.InferenceService) {
			s.Status.Placement = &v1beta1.PlacementStatus{Cluster: "member-a", Phase: v1beta1.PlacementPhasePlaced}
		}},
		{name: "derived label with copied typed intent", configure: func(s *v1beta1.InferenceService) {
			s.Labels = map[string]string{PlacementOriginLabel: "source-uid"}
			s.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}
		}},
		{name: "derived annotation with copied legacy intent", configure: func(s *v1beta1.InferenceService) {
			s.Annotations = map[string]string{PlacementOriginUIDAnnotation: "source-uid", ClusterSelectorAnnotation: "region=west"}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "model-service", Namespace: "team-a", UID: "local-uid"}}
			source.Status.Conditions = append(source.Status.Conditions, apis.Condition{Type: apis.ConditionReady, Status: corev1.ConditionTrue, Reason: "Serving"})
			if tt.configure != nil {
				tt.configure(source)
			}
			r, cl := newPlacer(testScheme(t), nil, source)
			r.Client = interceptor.NewClient(cl.(client.WithWatch), interceptor.Funcs{
				List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
					return errors.New("nonparticipant listed placement resources")
				},
			})
			key := client.ObjectKeyFromObject(source)
			var want v1beta1.InferenceService
			if err := cl.Get(context.Background(), key, &want); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(ctrl.Result{}, result); diff != "" {
					t.Fatalf("result (-want +got):\n%s", diff)
				}
			}
			var got v1beta1.InferenceService
			if err := cl.Get(context.Background(), key, &got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("nonparticipant changed (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlacementParticipationRequiresSourceIntent(t *testing.T) {
	for _, tt := range []struct {
		name    string
		service *v1beta1.InferenceService
		want    bool
	}{
		{name: "nil service"},
		{name: "local service", service: &v1beta1.InferenceService{}},
		{name: "explicit placement", service: &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}}}, want: true},
		{name: "legacy selector", service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{ClusterSelectorAnnotation: "region=west"}}}, want: true},
		{name: "derived label", service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{PlacementOriginLabel: "source-uid"}, Annotations: map[string]string{ClusterSelectorAnnotation: "region=west"}}}},
		{name: "derived annotation", service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{PlacementOriginUIDAnnotation: "source-uid", ClusterSelectorAnnotation: "region=west"}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var wantIndex []string
			if tt.want {
				wantIndex = []string{placementEligibleIndexValue}
			}
			if diff := cmp.Diff(wantIndex, placementEligibleIndexExtractor(tt.service)); diff != "" {
				t.Fatalf("participation index (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.want, IsPlacementEligible(tt.service)); diff != "" {
				t.Fatalf("participation (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlacementDeletionReleasesFormerParticipant(t *testing.T) {
	source := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "model-service", Namespace: "team-a", UID: "source-uid",
		Finalizers: []string{PlacementFinalizer}, DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
	}}
	r, cl := newPlacer(testScheme(t), fakeClusters{}, source)
	key := client.ObjectKeyFromObject(source)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var got v1beta1.InferenceService
	err := cl.Get(context.Background(), key, &got)
	if diff := cmp.Diff(true, apierrors.IsNotFound(err)); diff != "" {
		t.Fatalf("former participant deleted (-want +got):\n%s; error: %v", diff, err)
	}
}
