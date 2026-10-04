package irprojector

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestRoleHelpers pins the resolver over role x {projected, referenced,
// absent}: a projected role is named after the service and keeps the service
// as its prefix, a referenced role is the standalone replica itself, whose
// name is also its prefix, and a role the spec does not declare still
// resolves to its projected name, so callers that iterate rollout groups or
// peers read a missing replica as "not found", never as a different object.
func TestRoleHelpers(t *testing.T) {
	inline := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Router: &v1beta1.RouterSpec{}},
	}
	referenced := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"},
		Spec:       v1beta1.InferenceServiceSpec{ReplicaRefs: &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Router: []string{"router-a"}}},
	}
	if ReferencesReplicas(inline) || !ReferencesReplicas(referenced) {
		t.Fatal("ReferencesReplicas must follow spec.replicaRefs.engine")
	}
	if got := ReferencedRoles(referenced); !reflect.DeepEqual(got, []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent}) {
		t.Fatalf("ReferencedRoles = %v", got)
	}
	for _, tc := range []struct {
		isvc         *v1beta1.InferenceService
		c            v1beta1.ComponentType
		declared     bool
		name, prefix string
	}{
		{inline, v1beta1.EngineComponent, true, "svc-engine", "svc"},
		{inline, v1beta1.DecoderComponent, false, "svc-decoder", "svc"},
		{inline, v1beta1.RouterComponent, true, "svc-router", "svc"},
		{referenced, v1beta1.EngineComponent, true, "pool-a", "pool-a"},
		{referenced, v1beta1.DecoderComponent, false, "svc-decoder", "svc"},
		{referenced, v1beta1.RouterComponent, true, "router-a", "router-a"},
	} {
		if got := RoleDeclared(tc.isvc, tc.c); got != tc.declared {
			t.Errorf("RoleDeclared(%s) = %v, want %v", tc.c, got, tc.declared)
		}
		if got := RoleReplicaName(tc.isvc, tc.c); got != tc.name {
			t.Errorf("RoleReplicaName(%s) = %q, want %q", tc.c, got, tc.name)
		}
		if got := RoleReplicaPrefix(tc.isvc, tc.c); got != tc.prefix {
			t.Errorf("RoleReplicaPrefix(%s) = %q, want %q", tc.c, got, tc.prefix)
		}
		if got, want := RoleReplicaKey(tc.isvc, tc.c), (types.NamespacedName{Namespace: "team-a", Name: tc.name}); got != want {
			t.Errorf("RoleReplicaKey(%s) = %v, want %v", tc.c, got, want)
		}
	}
}
