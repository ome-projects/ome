package inferenceservice

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func TestPlacementPolicyPreflight(t *testing.T) {
	policy, err := protocol.Encode(&v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a"})
	if err != nil {
		t.Fatal(err)
	}
	demand, err := protocol.Encode(&v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a",
		Demand: &v1beta1.PlacementDemandContract{Fingerprint: strings.Repeat("a", 64), Components: []v1beta1.PlacementComponentDemand{{Component: v1beta1.EngineComponent, RenderingHash: strings.Repeat("b", 64)}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []constants.DeploymentModeType{constants.OMENative, constants.RawDeployment, constants.MultiNode, constants.VirtualDeployment} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tt := range []struct {
				name      string
				origin    string
				policy    string
				deleting  bool
				engine    bool
				wantBlock bool
			}{
				{name: "local service"},
				{name: "local malformed annotation ignored", policy: "{"},
				{name: "local future policy ignored", policy: `{"version":4}`},
				{name: "local ignores demand inventory", policy: demand},
				{name: "derived without policy", origin: "source-a"},
				{name: "supported policy", origin: "source-a", policy: policy},
				{name: "malformed policy", origin: "source-a", policy: "{", wantBlock: true},
				{name: "unknown version", origin: "source-a", policy: `{"version":4}`, wantBlock: true},
				{name: "supported demand", origin: "source-a", policy: demand, engine: true},
				{name: "undeclared demand component", origin: "source-a", policy: demand, wantBlock: true},
				{name: "wrong origin", origin: "source-b", policy: policy, wantBlock: true},
				{name: "deletion permits cleanup", origin: "source-a", policy: "{", deleting: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					scheme := runtime.NewScheme()
					if err := v1beta1.AddToScheme(scheme); err != nil {
						t.Fatal(err)
					}
					service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a", UID: "member-uid", ResourceVersion: "1", Annotations: map[string]string{}},
						Spec: v1beta1.InferenceServiceSpec{DeploymentMode: ptr.To(mode)}}
					if tt.engine {
						service.Spec.Engine = &v1beta1.EngineSpec{}
					}
					if tt.origin != "" {
						service.Annotations[constants.PlacementOriginUID] = tt.origin
					}
					if tt.policy != "" {
						service.Annotations[constants.PlacementExecution] = tt.policy
					}
					if tt.deleting {
						service.DeletionTimestamp = ptr.To(metav1.NewTime(time.Unix(100, 0)))
						service.Finalizers = []string{inferenceServiceFinalizer}
					}
					writes := 0
					reject := func() error { writes++; return errors.New("unexpected object write") }
					cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(service).WithInterceptorFuncs(interceptor.Funcs{
						Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error { return reject() },
						Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error { return reject() },
						Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
							return reject()
						},
						Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return reject() },
					}).Build()
					before := &v1beta1.InferenceService{}
					if err := cl.Get(t.Context(), client.ObjectKeyFromObject(service), before); err != nil {
						t.Fatal(err)
					}
					configReads := 0
					configErr := errors.New("configuration read reached")
					clientset := fake.NewClientset()
					clientset.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
						configReads++
						return true, nil, configErr
					})
					r := InferenceServiceReconciler{Client: cl, Clientset: clientset, Log: logr.Discard()}
					_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(service)})
					wantReads := 1
					if tt.wantBlock {
						wantReads = 0
						if err == nil || !strings.Contains(err.Error(), "placement") {
							t.Fatalf("error = %v, want invalid placement policy", err)
						}
					} else if !errors.Is(err, configErr) {
						t.Fatalf("error = %v, want continuation to configuration", err)
					}
					if diff := cmp.Diff(wantReads, configReads); diff != "" {
						t.Fatalf("configuration reads (-want +got):\n%s", diff)
					}
					if diff := cmp.Diff(0, writes); diff != "" {
						t.Fatalf("object writes (-want +got):\n%s", diff)
					}
					stored := &v1beta1.InferenceService{}
					if err := cl.Get(t.Context(), client.ObjectKeyFromObject(service), stored); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(before, stored); diff != "" {
						t.Fatalf("stored service (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}
