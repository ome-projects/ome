package servingruntime

import (
	"context"
	"sort"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func newReconciler(t *testing.T, objs ...client.Object) (*InheritanceReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	c := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.ClusterServingRuntime{}, &v1beta1.ServingRuntime{}).
		WithObjects(objs...).
		Build()
	return &InheritanceReconciler{
		Client:   c,
		Log:      testr.New(t),
		Scheme:   scheme,
		Recorder: &record.FakeRecorder{},
	}, c
}

func runReconcile(t *testing.T, r *InheritanceReconciler, namespace, name string) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	if err != nil {
		t.Fatalf("Reconcile(%s/%s): %v", namespace, name, err)
	}
}

func envSpec(envs ...corev1.EnvVar) v1beta1.ServingRuntimeSpec {
	return v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Env: envs}},
		},
	}
}

func mkCSR(name, inheritFrom string, spec v1beta1.ServingRuntimeSpec) *v1beta1.ClusterServingRuntime {
	annotations := map[string]string{}
	if inheritFrom != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = inheritFrom
	}
	return &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations, Generation: 1},
		Spec:       spec,
	}
}

func mkSR(name, namespace, inheritFrom string, spec v1beta1.ServingRuntimeSpec) *v1beta1.ServingRuntime {
	annotations := map[string]string{}
	if inheritFrom != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = inheritFrom
	}
	return &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Annotations: annotations, Generation: 1},
		Spec:       spec,
	}
}

func mustGetCSR(t *testing.T, c client.Client, name string) *v1beta1.ClusterServingRuntime {
	t.Helper()
	out := &v1beta1.ClusterServingRuntime{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, out); err != nil {
		t.Fatalf("Get(%s): %v", name, err)
	}
	return out
}

func mustGetSR(t *testing.T, c client.Client, ns, name string) *v1beta1.ServingRuntime {
	t.Helper()
	out := &v1beta1.ServingRuntime{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, out); err != nil {
		t.Fatalf("Get(%s/%s): %v", ns, name, err)
	}
	return out
}

// Cluster scope

func TestReconcile_Cluster_NoInheritance(t *testing.T) {
	g := gomega.NewWithT(t)
	solo := mkCSR("solo", "", v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)})
	r, c := newReconciler(t, solo)

	runReconcile(t, r, "", "solo")

	got := mustGetCSR(t, c, "solo")
	g.Expect(got.Status.InheritanceChain).To(gomega.Equal([]string{"solo"}))
	g.Expect(got.Status.Conditions).To(gomega.HaveLen(1))
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
}

func TestReconcile_Cluster_TwoLevelChain(t *testing.T) {
	g := gomega.NewWithT(t)
	profile := mkCSR("profile-infra", "",
		envSpec(corev1.EnvVar{Name: "NCCL_DEBUG", Value: "INFO"}))
	rt := mkCSR("rt-sglang", "profile-infra",
		envSpec(corev1.EnvVar{Name: "FROM_RT", Value: "yes"}))
	r, c := newReconciler(t, profile, rt)

	runReconcile(t, r, "", "rt-sglang")

	got := mustGetCSR(t, c, "rt-sglang")
	g.Expect(got.Status.InheritanceChain).To(gomega.Equal([]string{"profile-infra", "rt-sglang"}))
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
}

func TestReconcile_Cluster_ParentMissing_PreservesPriorChain(t *testing.T) {
	g := gomega.NewWithT(t)
	rt := mkCSR("rt", "ghost-parent", v1beta1.ServingRuntimeSpec{})
	rt.Status = v1beta1.ServingRuntimeStatus{
		InheritanceChain: []string{"some-old-parent", "rt"},
	}
	r, c := newReconciler(t, rt)

	runReconcile(t, r, "", "rt")

	got := mustGetCSR(t, c, "rt")
	g.Expect(got.Status.InheritanceChain).To(gomega.Equal([]string{"some-old-parent", "rt"}))
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(got.Status.Conditions[0].Reason).To(gomega.Equal(ReasonParentNotFound))
}

func TestReconcile_Cluster_Cycle(t *testing.T) {
	g := gomega.NewWithT(t)
	a := mkCSR("a", "b", v1beta1.ServingRuntimeSpec{})
	b := mkCSR("b", "a", v1beta1.ServingRuntimeSpec{})
	r, c := newReconciler(t, a, b)

	runReconcile(t, r, "", "a")

	got := mustGetCSR(t, c, "a")
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(got.Status.Conditions[0].Reason).To(gomega.Equal(ReasonCycle))
}

func TestReconcile_Cluster_Idempotent(t *testing.T) {
	g := gomega.NewWithT(t)
	solo := mkCSR("solo", "", v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)})
	r, c := newReconciler(t, solo)

	runReconcile(t, r, "", "solo")
	firstRV := mustGetCSR(t, c, "solo").ResourceVersion
	runReconcile(t, r, "", "solo")
	g.Expect(mustGetCSR(t, c, "solo").ResourceVersion).To(gomega.Equal(firstRV),
		"second reconcile must be a no-op (no status diff → no API write)")
}

// Namespaced scope

func TestReconcile_Namespaced_NoInheritance(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := mkSR("solo", "team-a", "", v1beta1.ServingRuntimeSpec{Disabled: ptr.To(false)})
	r, c := newReconciler(t, sr)

	runReconcile(t, r, "team-a", "solo")

	got := mustGetSR(t, c, "team-a", "solo")
	g.Expect(got.Status.InheritanceChain).To(gomega.Equal([]string{"solo"}))
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
}

func TestReconcile_Namespaced_SameNamespaceParent(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := mkSR("ns-profile", "team-a", "",
		envSpec(corev1.EnvVar{Name: "FROM_NS_PROFILE", Value: "yes"}))
	child := mkSR("rt", "team-a", "ns-profile",
		envSpec(corev1.EnvVar{Name: "FROM_CHILD", Value: "yes"}))
	r, c := newReconciler(t, parent, child)

	runReconcile(t, r, "team-a", "rt")

	got := mustGetSR(t, c, "team-a", "rt")
	g.Expect(got.Status.InheritanceChain).To(gomega.Equal([]string{"ns-profile", "rt"}))
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
}

func TestReconcile_Namespaced_ClusterFallback(t *testing.T) {
	g := gomega.NewWithT(t)
	clusterProfile := mkCSR("cluster-infra", "",
		envSpec(corev1.EnvVar{Name: "FROM_CLUSTER", Value: "yes"}))
	child := mkSR("rt", "team-a", "cluster-infra", v1beta1.ServingRuntimeSpec{})
	r, c := newReconciler(t, clusterProfile, child)

	runReconcile(t, r, "team-a", "rt")

	got := mustGetSR(t, c, "team-a", "rt")
	g.Expect(got.Status.InheritanceChain).To(gomega.Equal([]string{"cluster-infra", "rt"}))
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
}

func TestReconcile_Namespaced_CrossNamespaceRejected(t *testing.T) {
	g := gomega.NewWithT(t)
	otherNS := mkSR("shared", "other-team", "", v1beta1.ServingRuntimeSpec{})
	child := mkSR("rt", "team-a", "shared", v1beta1.ServingRuntimeSpec{})
	r, c := newReconciler(t, otherNS, child)

	runReconcile(t, r, "team-a", "rt")

	got := mustGetSR(t, c, "team-a", "rt")
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(got.Status.Conditions[0].Reason).To(gomega.Equal(ReasonParentNotFound))
}

func TestReconcile_Namespaced_NamespacedShadowsCluster(t *testing.T) {
	g := gomega.NewWithT(t)
	// Same name in both scopes; namespaced wins — the reconciler's
	// chain reflects the local parent, not the cluster one.
	clusterParent := mkCSR("infra", "", envSpec(corev1.EnvVar{Name: "FROM_CLUSTER", Value: "yes"}))
	nsParent := mkSR("infra", "team-a", "", envSpec(corev1.EnvVar{Name: "FROM_NS", Value: "yes"}))
	child := mkSR("rt", "team-a", "infra", v1beta1.ServingRuntimeSpec{})
	r, c := newReconciler(t, clusterParent, nsParent, child)

	runReconcile(t, r, "team-a", "rt")

	got := mustGetSR(t, c, "team-a", "rt")
	g.Expect(got.Status.InheritanceChain).To(gomega.Equal([]string{"infra", "rt"}))
	g.Expect(got.Status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
}

// Cascade fan-out (handler-level)

// reqKeys flattens requests to sorted "namespace/name", so a cluster-scoped
// dependent reads as "/name". Descendants derives its result from a map, so
// the mapper's order is unspecified and has to be normalised before diffing.
func reqKeys(reqs []reconcile.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, req.Namespace+"/"+req.Name)
	}
	sort.Strings(out)
	return out
}

func TestDependentsFanout(t *testing.T) {
	tests := []struct {
		name     string
		runtimes []client.Object
		event    client.Object
		want     []string
	}{
		{
			name: "a cluster event reaches both scopes, in every namespace",
			runtimes: []client.Object{
				mkCSR("root", "", v1beta1.ServingRuntimeSpec{}),
				mkCSR("c1", "root", v1beta1.ServingRuntimeSpec{}),
				mkSR("n1", "team-a", "root", v1beta1.ServingRuntimeSpec{}),
				mkSR("n2", "team-b", "root", v1beta1.ServingRuntimeSpec{}),
				mkCSR("other", "", v1beta1.ServingRuntimeSpec{}),
			},
			event: mkCSR("root", "", v1beta1.ServingRuntimeSpec{}),
			want:  []string{"/c1", "/root", "team-a/n1", "team-b/n2"},
		},
		{
			// Cross-namespace inheritance is disallowed, so team-b's runtime
			// resolves some other parent named "parent".
			name: "a namespaced event stays inside its namespace",
			runtimes: []client.Object{
				mkSR("parent", "team-a", "", v1beta1.ServingRuntimeSpec{}),
				mkSR("sibling", "team-a", "parent", v1beta1.ServingRuntimeSpec{}),
				mkSR("other-ns-child", "team-b", "parent", v1beta1.ServingRuntimeSpec{}),
				mkSR("unrelated", "team-a", "", v1beta1.ServingRuntimeSpec{}),
			},
			event: mkSR("parent", "team-a", "", v1beta1.ServingRuntimeSpec{}),
			want:  []string{"team-a/parent", "team-a/sibling"},
		},
		{
			// Repointing root rewrites the resolved chain of everything below
			// it, and only each runtime's own reconcile can record that.
			name: "a cluster event reaches the whole subtree, however deep",
			runtimes: []client.Object{
				mkCSR("root", "", v1beta1.ServingRuntimeSpec{}),
				mkCSR("child", "root", v1beta1.ServingRuntimeSpec{}),
				mkCSR("grandchild", "child", v1beta1.ServingRuntimeSpec{}),
				mkSR("ns-grandchild", "team-a", "child", v1beta1.ServingRuntimeSpec{}),
			},
			event: mkCSR("root", "", v1beta1.ServingRuntimeSpec{}),
			want:  []string{"/child", "/grandchild", "/root", "team-a/ns-grandchild"},
		},
		{
			name: "a namespaced event reaches its whole same-namespace subtree",
			runtimes: []client.Object{
				mkSR("parent", "team-a", "", v1beta1.ServingRuntimeSpec{}),
				mkSR("child", "team-a", "parent", v1beta1.ServingRuntimeSpec{}),
				mkSR("grandchild", "team-a", "child", v1beta1.ServingRuntimeSpec{}),
				mkSR("outsider", "team-b", "parent", v1beta1.ServingRuntimeSpec{}),
			},
			event: mkSR("parent", "team-a", "", v1beta1.ServingRuntimeSpec{}),
			want:  []string{"team-a/child", "team-a/grandchild", "team-a/parent"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newReconciler(t, tc.runtimes...)

			// Dispatch on kind the way SetupWithManager wires the two
			// Watches: the CSR source feeds dependentsOfCluster, the SR
			// source feeds dependentsOfNamespaced.
			var got []string
			switch tc.event.(type) {
			case *v1beta1.ClusterServingRuntime:
				got = reqKeys(r.dependentsOfCluster(context.Background(), tc.event))
			case *v1beta1.ServingRuntime:
				got = reqKeys(r.dependentsOfNamespaced(context.Background(), tc.event))
			default:
				t.Fatalf("event is neither runtime kind: %T", tc.event)
			}

			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("dependents mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
