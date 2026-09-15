package protocol

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestPendingCompetitorValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1.Pod)
		valid  bool
	}{
		{"same profile", func(*v1.Pod) {}, true},
		{"default scheduler", func(p *v1.Pod) { p.Spec.SchedulerName = "" }, true},
		{"gated", func(p *v1.Pod) { p.Spec.SchedulingGates = []v1.PodSchedulingGate{{Name: "example.com/wait"}} }, true},
		{"mixed profile", func(p *v1.Pod) { p.Spec.SchedulerName = "other-scheduler" }, false},
		{"pending gang", func(p *v1.Pod) { p.Labels = map[string]string{"scheduling.x-k8s.io/pod-group": "gang"} }, false},
		{"deleting", func(p *v1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now }, false},
		{"terminal", func(p *v1.Pod) { p.Status.Phase = v1.PodFailed }, false},
		{"missing dependency", func(p *v1.Pod) {
			yes := true
			p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "replicas", UID: "replicas-uid", Controller: &yes}}
		}, false},
		{"nominated", func(p *v1.Pod) { p.Status.NominatedNodeName = "source-node" }, false},
		{"PVC", func(p *v1.Pod) {
			p.Spec.Volumes = []v1.Volume{{Name: "data", VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}
		}, false},
		{"DRA", func(p *v1.Pod) { p.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "gpu"}} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			r.Profile.SchedulerName = "default-scheduler"
			r.ReplacementPods[0].Spec.SchedulerName = "default-scheduler"
			p := r.ReplacementPods[0].DeepCopy()
			p.Name, p.UID = "competitor", "competitor-uid"
			tc.change(p)
			r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: p})
			_, err := Validate(r)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t: %v", tc.valid, err)
			}
		})
	}
}

func TestPendingCompetitorRequiresMatchingControllerIdentity(t *testing.T) {
	for _, uid := range []string{"replicas-uid", "stale-uid", ""} {
		r := validRequest()
		p := r.ReplacementPods[0].DeepCopy()
		p.Name, p.UID = "competitor", "competitor-uid"
		yes := true
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "replicas", UID: "replicas-uid", Controller: &yes}}
		owner := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: "replicas"}}
		owner.SetUID(types.UID(uid))
		r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: p}, runtime.RawExtension{Object: owner})
		_, err := Validate(r)
		if (err == nil) != (uid == "replicas-uid") {
			t.Fatalf("controller UID %q: %v", uid, err)
		}
	}
}
