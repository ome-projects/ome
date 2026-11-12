package revision

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

var testIRGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceReplica")

// projectedReplica is a replica the service of the given UID controls, as
// the projector creates it.
func projectedReplica(name string, uid types.UID, service *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: service.Namespace, UID: uid,
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(service, testISVCGVK)},
	}}
}

func ownerUIDs(cr *appsv1.ControllerRevision) (controller types.UID, plain []types.UID) {
	for _, ref := range cr.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			controller = ref.UID
			continue
		}
		plain = append(plain, ref.UID)
	}
	return controller, plain
}

// A revision a projected replica mints names the replica as its controller
// and the service as a plain owner, so the revision stays when the replica
// is deleted and leaves with the service.
func TestEnsureControllerRevision_NamesTheServiceAsAnOwnerOfAProjectedReplicasRevision(t *testing.T) {
	service := minimalISVC("svc", "prod", 1)
	replica := projectedReplica("svc-engine", "replica-1", service)
	scope := ScopeOwner(replica, testISVCGVK.GroupKind())
	if scope == nil || scope.UID != service.UID || scope.Controller != nil {
		t.Fatalf("the scope of a projected replica must be its service as a plain owner, got %+v", scope)
	}
	spec := basicPodSpecForRevision()
	hash, raw, _ := Hash(spec, nil, nil, service.UID)
	c := newFakeClientWithApps(t, service, replica)

	cr, collision, err := EnsureControllerRevisionFromHash(context.Background(), c, c, replica, testIRGVK, testKey(service, workload.ComponentEngine), hash, raw, scope)
	if err != nil || collision {
		t.Fatalf("mint: collision=%v err=%v", collision, err)
	}
	controller, plain := ownerUIDs(cr)
	if controller != replica.UID || len(plain) != 1 || plain[0] != service.UID {
		t.Fatalf("owners: controller=%q plain=%v, want the replica as controller and the service as owner", controller, plain)
	}
	if !RetainedBy(cr, replica.UID, scope) {
		t.Fatal("the replica's own revision must read as retained")
	}
}

// A replica deleted and projected again under the same service finds the
// revisions its predecessor minted: the service owns them, so the new
// replica reuses one instead of reading it as a collision, and the stable
// revision of a rollback is still there to roll back onto.
func TestEnsureControllerRevision_ReusesARevisionTheServiceOwnsAfterItsReplicaWasReplaced(t *testing.T) {
	service := minimalISVC("svc", "prod", 1)
	first := projectedReplica("svc-engine", "replica-1", service)
	scope := ScopeOwner(first, testISVCGVK.GroupKind())
	spec := basicPodSpecForRevision()
	hash, raw, _ := Hash(spec, nil, nil, service.UID)
	c := newFakeClientWithApps(t, service, first)
	minted, _, err := EnsureControllerRevisionFromHash(context.Background(), c, c, first, testIRGVK, testKey(service, workload.ComponentEngine), hash, raw, scope)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	second := projectedReplica("svc-engine", "replica-2", service)
	reused, collision, err := EnsureControllerRevisionFromHash(context.Background(), c, c, second, testIRGVK, testKey(service, workload.ComponentEngine), hash, raw, ScopeOwner(second, testISVCGVK.GroupKind()))
	if err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if collision {
		t.Fatal("a revision the service owns is the new replica's history, not a collision")
	}
	if reused.Name != minted.Name || reused.UID != minted.UID {
		t.Fatalf("the new replica must reuse %s, got %s", minted.Name, reused.Name)
	}
	if !RetainedBy(reused, second.UID, scope) {
		t.Fatal("the predecessor's revision must read as retained for the new replica")
	}
	if RetainedBy(reused, second.UID, nil) {
		t.Fatal("without a scope the predecessor's revision is not the new replica's")
	}
}

// A revision minted before the history had a scope gains the service as an
// owner the first time its replica reads it again, so an upgrade does not
// leave the stable revision of a standing rollback to die with its replica.
func TestEnsureControllerRevision_AddsTheServiceToARevisionMintedWithoutIt(t *testing.T) {
	service := minimalISVC("svc", "prod", 1)
	replica := projectedReplica("svc-engine", "replica-1", service)
	spec := basicPodSpecForRevision()
	hash, raw, _ := Hash(spec, nil, nil, service.UID)
	c := newFakeClientWithApps(t, service, replica)
	key := testKey(service, workload.ComponentEngine)
	if _, _, err := EnsureControllerRevisionFromHash(context.Background(), c, c, replica, testIRGVK, key, hash, raw, nil); err != nil {
		t.Fatalf("mint without a scope: %v", err)
	}

	scope := ScopeOwner(replica, testISVCGVK.GroupKind())
	cr, collision, err := EnsureControllerRevisionFromHash(context.Background(), c, c, replica, testIRGVK, key, hash, raw, scope)
	if err != nil || collision {
		t.Fatalf("reuse with a scope: collision=%v err=%v", collision, err)
	}
	stored := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), stored); err != nil {
		t.Fatal(err)
	}
	controller, plain := ownerUIDs(stored)
	if controller != replica.UID || len(plain) != 1 || plain[0] != service.UID {
		t.Fatalf("after the reuse the stored revision must name the service: controller=%q plain=%v", controller, plain)
	}
	if err := EnsureScopeOwner(context.Background(), c, stored, scope); err != nil {
		t.Fatalf("a second EnsureScopeOwner must write nothing and fail nothing: %v", err)
	}
}

// A revision of another service is still a collision: the scope admits the
// replica's own service and nothing else.
func TestEnsureControllerRevision_AnotherServicesRevisionIsStillACollision(t *testing.T) {
	service := minimalISVC("svc", "prod", 1)
	other := minimalISVC("svc", "prod", 1)
	other.UID = "other-service-uid"
	replica := projectedReplica("svc-engine", "replica-2", service)
	spec := basicPodSpecForRevision()
	hash, raw, _ := Hash(spec, nil, nil, service.UID)
	key := testKey(service, workload.ComponentEngine)
	otherService := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: Name(key, hash), Namespace: service.Namespace, Labels: Labels(key),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(projectedReplica("svc-engine", "replica-1", other), testIRGVK),
				{APIVersion: testISVCGVK.GroupVersion().String(), Kind: testISVCGVK.Kind, Name: other.Name, UID: other.UID},
			},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: 1,
	}
	c := newFakeClientWithApps(t, service, replica, otherService)
	_, collision, err := EnsureControllerRevisionFromHash(context.Background(), c, c, replica, testIRGVK, key, hash, raw, ScopeOwner(replica, testISVCGVK.GroupKind()))
	if err != nil {
		t.Fatal(err)
	}
	if !collision {
		t.Fatal("a revision another service owns must read as a collision")
	}
}

func TestRetainedBy(t *testing.T) {
	controller := func(uid types.UID) metav1.OwnerReference {
		return metav1.OwnerReference{UID: uid, Controller: ptr.To(true)}
	}
	service := &metav1.OwnerReference{Kind: "InferenceService", Name: "svc", UID: "service-uid"}
	for _, tc := range []struct {
		name  string
		refs  []metav1.OwnerReference
		scope *metav1.OwnerReference
		want  bool
	}{
		{"controlled by the replica", []metav1.OwnerReference{controller("replica")}, nil, true},
		{"controlled by the replica, the service beside it", []metav1.OwnerReference{controller("replica"), {UID: "service-uid"}}, service, true},
		{"controlled by a previous replica, owned by the service", []metav1.OwnerReference{controller("previous"), {UID: "service-uid"}}, service, true},
		{"owned by the service alone, the previous replica's reference pruned", []metav1.OwnerReference{{UID: "service-uid"}}, service, true},
		{"controlled by a previous replica, no scope", []metav1.OwnerReference{controller("previous"), {UID: "service-uid"}}, nil, false},
		{"controlled by a previous replica, owned by another service", []metav1.OwnerReference{controller("previous"), {UID: "other-service"}}, service, false},
		{"no owner at all", nil, service, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cr := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{OwnerReferences: tc.refs}}
			if got := RetainedBy(cr, "replica", tc.scope); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	if RetainedBy(nil, "replica", service) {
		t.Fatal("a nil revision is not retained")
	}
}

func TestScopeOwner(t *testing.T) {
	kind := testISVCGVK.GroupKind()
	service := minimalISVC("svc", "prod", 1)
	projected := projectedReplica("svc-engine", "replica-1", service)
	scope := ScopeOwner(projected, kind)
	if scope == nil || scope.UID != service.UID || scope.Name != service.Name || scope.Kind != "InferenceService" || scope.Controller != nil || scope.BlockOwnerDeletion != nil {
		t.Fatalf("a projected replica's scope is its service as a plain owner, got %+v", scope)
	}
	standalone := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "pool-a", Namespace: "prod", UID: "pool-uid"}}
	if got := ScopeOwner(standalone, kind); got != nil {
		t.Fatalf("a standalone replica has no scope, got %+v", got)
	}
	adopted := standalone.DeepCopy()
	adopted.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "operator", UID: "dep-uid", Controller: ptr.To(true)}}
	if got := ScopeOwner(adopted, kind); got != nil {
		t.Fatalf("a controller of another kind is no scope, got %+v", got)
	}
	noUID := projected.DeepCopy()
	noUID.OwnerReferences[0].UID = ""
	if got := ScopeOwner(noUID, kind); got != nil {
		t.Fatalf("a controller reference without a UID is no scope, got %+v", got)
	}
	otherGroup := projected.DeepCopy()
	otherGroup.OwnerReferences[0].APIVersion = "serving.example.com/v1"
	if got := ScopeOwner(otherGroup, schema.GroupKind{Group: kind.Group, Kind: kind.Kind}); got != nil {
		t.Fatalf("an InferenceService of another group is no scope, got %+v", got)
	}
}

// The revision name the replica writes and the name every reader composes
// from a hash agree byte for byte.
func TestName_IsTheQueryComposer(t *testing.T) {
	key := Key{Name: "svc-engine"}
	if got, want := Name(key, "0a1b2c3d"), query.ControllerRevisionName("svc", workload.ComponentEngine, "0a1b2c3d"); got != want {
		t.Fatalf("Name %q, query composer %q", got, want)
	}
}
