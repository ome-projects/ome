package canary

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// canaryGroupISVC is a service of two replicas per named Component with one
// two-step canary group over those Components.
func canaryGroupISVC(name string, components ...v1beta1.ComponentType) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name}}
	for _, component := range components {
		v1beta1.DeclareRole(&isvc.Spec, component)
		v1beta1.SpecOf(&isvc.Spec, component).Extension.MinReplicas = ptr.To(2)
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: components,
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	return isvc
}

// revisionRoutingPort is the one port of a revision's routing Service.
func revisionRoutingPort(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, hash string) corev1.ServicePort {
	t.Helper()
	svc := &corev1.Service{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: coordination.PerRevisionServiceName(isvc.Name, component, hash)}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Fatalf("%s routing Service for %s missing: %v", component, hash, err)
	}
	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("%s routing Service for %s: got ports %+v want one", component, hash, svc.Spec.Ports)
	}
	return svc.Spec.Ports[0]
}

// A canary owns the per-revision Services of its Components and publishes
// each Component's own declared appProtocol on them, on the canary revision
// and the stable one alike. A member that declares nothing gets none even
// when the other Components declare the same port name.
func TestDispatch_RevisionServicesCarryEachComponentsAppProtocol(t *testing.T) {
	const h2c, ws = "kubernetes.io/h2c", "kubernetes.io/ws"
	type revision struct {
		component v1beta1.ComponentType
		hash      string
		want      *string
	}
	cases := []struct {
		name      string
		isvc      *v1beta1.InferenceService
		declared  map[v1beta1.ComponentType]map[string]string
		revisions []revision
	}{
		{
			name:     "a router canary speaks the router's protocol on the stable and the canary revision",
			isvc:     canaryGroupISVC("rt1", v1beta1.RouterComponent),
			declared: map[v1beta1.ComponentType]map[string]string{v1beta1.RouterComponent: {"http": h2c}},
			revisions: []revision{
				{component: v1beta1.RouterComponent, hash: "stable0", want: ptr.To(h2c)},
				{component: v1beta1.RouterComponent, hash: "newhash", want: ptr.To(h2c)},
			},
		},
		{
			name: "each member of an engine and decoder group gets its own",
			isvc: canaryGroupISVC("pd1", v1beta1.EngineComponent, v1beta1.DecoderComponent),
			declared: map[v1beta1.ComponentType]map[string]string{
				v1beta1.RouterComponent: {"http": h2c},
				v1beta1.EngineComponent: {"http": ws},
			},
			revisions: []revision{
				{component: v1beta1.EngineComponent, hash: "engold", want: ptr.To(ws)},
				{component: v1beta1.EngineComponent, hash: "engnew", want: ptr.To(ws)},
				{component: v1beta1.DecoderComponent, hash: "decold"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{tc.isvc}
			index := map[v1beta1.ComponentType]int{}
			for _, rev := range tc.revisions {
				name := fmt.Sprintf("%s-%s-%d", tc.isvc.Name, rev.component, index[rev.component])
				objs = append(objs, canaryPod(tc.isvc.Namespace, tc.isvc.Name, string(rev.component), rev.hash, name))
				index[rev.component]++
			}
			c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
			if _, err := Dispatch(context.Background(), DispatchDeps{
				Client:                           c,
				Reader:                           c,
				ISVC:                             tc.isvc,
				ComponentRunnerPorts:             canaryRunnerPorts(),
				ComponentServicePortAppProtocols: tc.declared,
				Group:                            rollout.CanaryGroup(tc.isvc, rollout.Policies{}),
			}); err != nil {
				t.Fatalf("Dispatch error: %v", err)
			}
			for _, rev := range tc.revisions {
				got := revisionRoutingPort(t, c, tc.isvc, rev.component, rev.hash).AppProtocol
				if !reflect.DeepEqual(got, rev.want) {
					t.Errorf("%s routing Service for %s AppProtocol: got %q want %q", rev.component, rev.hash, ptr.Deref(got, ""), ptr.Deref(rev.want, ""))
				}
			}
		})
	}
}
