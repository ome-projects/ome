package inferenceservice

import (
	"context"
	"sort"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func rtRefISVC(ns, name, runtimeName string, autoSync *bool) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: runtimeName, AutoSync: autoSync},
		},
	}
}

func reqNames(reqs []reconcile.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Namespace+"/"+r.Name)
	}
	sort.Strings(out)
	return out
}

func inheritingCSR(name, parent string) *v1beta1.ClusterServingRuntime {
	annotations := map[string]string{}
	if parent != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = parent
	}
	return &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations},
	}
}

func inheritingSR(ns, name, parent string) *v1beta1.ServingRuntime {
	annotations := map[string]string{}
	if parent != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = parent
	}
	return &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Annotations: annotations},
	}
}

func runtimeWatchClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return ctrlclientfake.NewClientBuilder().WithScheme(s).
		WithIndex(&v1beta1.InferenceService{}, isvcRuntimeNameIndexField, isvcRuntimeNameIndexExtractor).
		WithIndex(&v1beta1.InferenceService{}, isvcRuntimeUnresolvedIndexField, isvcRuntimeUnresolvedIndexExtractor).
		WithObjects(objs...).Build()
}

func TestIsvcsReferencingRuntime(t *testing.T) {
	tests := []struct {
		name    string
		objects []client.Object
		event   client.Object
		want    []string
	}{
		{
			// autoSync decides what the reconcile does, not whether it runs: a
			// float consumer re-renders from the live runtime, a pinned one
			// runs drift detection and warns. Both need waking.
			name: "every consumer enqueues whatever its autoSync",
			objects: []client.Object{
				rtRefISVC("prod", "float-default", "rt-a", nil),
				rtRefISVC("prod", "float-true", "rt-a", ptr.To(true)),
				rtRefISVC("prod", "pinned", "rt-a", ptr.To(false)),
				rtRefISVC("prod", "other-runtime", "rt-b", ptr.To(true)),
			},
			event: inheritingCSR("rt-a", ""),
			want:  []string{"prod/float-default", "prod/float-true", "prod/pinned"},
		},
		{
			name: "a namespaced runtime bounds the fan-out to its own namespace",
			objects: []client.Object{
				rtRefISVC("prod", "same-ns", "rt-ns", ptr.To(true)),
				rtRefISVC("dev", "other-ns", "rt-ns", ptr.To(true)),
			},
			event: inheritingSR("prod", "rt-ns", ""),
			want:  []string{"prod/same-ns"},
		},
		{
			// A consuming ISVC renders the runtime's chain resolved end to
			// end, so a profile edit changes what every ISVC below it
			// produces — at any depth, not just one hop down.
			name: "a profile edit reaches consumers at every depth of the chain",
			objects: []client.Object{
				inheritingCSR("profile", ""),
				inheritingCSR("mid", "profile"),
				inheritingCSR("leaf", "mid"),
				inheritingCSR("unrelated", ""),
				rtRefISVC("prod", "on-profile", "profile", nil),
				rtRefISVC("prod", "on-mid", "mid", nil),
				rtRefISVC("prod", "on-leaf", "leaf", nil),
				rtRefISVC("prod", "on-unrelated", "unrelated", nil),
			},
			event: inheritingCSR("profile", ""),
			want:  []string{"prod/on-leaf", "prod/on-mid", "prod/on-profile"},
		},
		{
			// team-b's ISVC resolves some other runtime named child, so
			// reaching a namespaced descendant does not widen the fan-out
			// past that descendant's namespace.
			name: "a namespaced descendant still bounds its own consumers",
			objects: []client.Object{
				inheritingCSR("profile", ""),
				inheritingSR("team-a", "child", "profile"),
				rtRefISVC("team-a", "in-scope", "child", nil),
				rtRefISVC("team-b", "out-of-scope", "child", nil),
			},
			event: inheritingCSR("profile", ""),
			want:  []string{"team-a/in-scope"},
		},
		{
			name: "an ISVC reachable through two same-named descendants enqueues once",
			objects: []client.Object{
				inheritingCSR("profile", ""),
				inheritingCSR("dup", "profile"),
				inheritingSR("team-a", "dup", "profile"),
				rtRefISVC("team-a", "consumer", "dup", nil),
			},
			event: inheritingCSR("profile", ""),
			want:  []string{"team-a/consumer"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &InferenceServiceReconciler{
				Client: runtimeWatchClient(t, tc.objects...),
				Log:    logr.Discard(),
			}

			got := reqNames(r.isvcsReferencingRuntime(context.Background(), tc.event))

			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("isvcsReferencingRuntime() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
