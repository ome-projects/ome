package canary

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

const (
	simRetargetStable   = "a"
	simRetargetPartial  = "b"
	simRetargetCanary   = "c"
	simRetargetReplicas = 4
)

// newRetargetRollbackSim is a single-Component [engine] canary group after a
// mid-canary retarget: stable a, the first target b superseded by c while the
// first step held, the run pinned toward c over stable a, two ready pods on
// each of a and c, and the first step's partition projected on the replica.
func newRetargetRollbackSim(t *testing.T, name string) *passSim {
	t.Helper()
	ns := "default"
	n := simRetargetReplicas
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: simRetargetCanary, StableRevision: simRetargetStable},
	}
	closed := metav1.NewTime(isvc.Status.Rollout.ActiveRun.OpenedAt.Add(-time.Minute))
	isvc.Status.Rollout.LastRun = &v1beta1.RolloutRunRecord{
		Outcome:  v1beta1.RolloutRunSuperseded,
		OpenedAt: &closed,
		ClosedAt: &closed,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: simRetargetPartial, StableRevision: simRetargetStable},
		},
	}
	partition := int32(2)
	replica := ir(ns, name, v1beta1.EngineComponent, simRetargetCanary)
	replica.Spec.Component = v1beta1.EngineComponent
	replica.Generation = 1
	replica.Status.ObservedGeneration = 1
	replica.Status.CurrentRevision = name + "-engine-" + simRetargetStable
	replica.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: &partition}
	objs := []runtime.Object{isvc, replica,
		canaryPod(ns, name, "engine", simRetargetStable, name+"-engine-0"),
		canaryPod(ns, name, "engine", simRetargetStable, name+"-engine-1"),
		canaryPod(ns, name, "engine", simRetargetCanary, name+"-engine-2"),
		canaryPod(ns, name, "engine", simRetargetCanary, name+"-engine-3"),
		canaryControllerRevision(ns, name, "engine", simRetargetStable, 1),
		canaryControllerRevision(ns, name, "engine", simRetargetPartial, 2),
		canaryControllerRevision(ns, name, "engine", simRetargetCanary, 3),
	}
	return newPassSimWith(t, ns, name, []v1beta1.ComponentType{v1beta1.EngineComponent}, objs)
}

// pods lists the engine's pods as the executor observes them.
func (s *passSim) pods() []corev1.Pod {
	s.t.Helper()
	list := &corev1.PodList{}
	if err := s.c.List(context.Background(), list, client.InNamespace(s.ns)); err != nil {
		s.t.Fatal(err)
	}
	return list.Items
}

// revertObserved reports whether the replica has observed a rollback pin: the
// state in which its controller drains pods off the stable revision.
func (s *passSim) revertObserved(comp v1beta1.ComponentType) bool {
	r := s.replica(comp)
	return r.Status.ObservedGeneration == r.Generation &&
		r.Spec.Pacing != nil && r.Spec.Pacing.RollbackToRevision != nil && *r.Spec.Pacing.RollbackToRevision != ""
}

// revertTick is one unit of the replica controller's revert under a real
// kubelet: a pod off the stable revision starts terminating, or a terminating
// pod is gone and its stable replacement is ready at the same index. It
// reports whether any pod remains off the stable revision.
func (s *passSim) revertTick(stable string) bool {
	s.t.Helper()
	ctx := context.Background()
	for _, pod := range s.pods() {
		if pod.DeletionTimestamp == nil {
			continue
		}
		pod.Finalizers = nil
		if err := s.c.Update(ctx, &pod); err != nil {
			s.t.Fatal(err)
		}
		idx := pod.Labels[query.LabelInstanceIdx]
		replacement := canaryPod(s.ns, s.name, "engine", stable, s.name+"-engine-"+stable+"-"+idx)
		if err := s.c.Create(ctx, replacement); err != nil {
			s.t.Fatal(err)
		}
		return s.offStable(stable)
	}
	for _, pod := range s.pods() {
		if pod.Labels[query.LabelRevisionHash] == stable {
			continue
		}
		pod.Finalizers = []string{"example.com/draining"}
		if err := s.c.Update(ctx, &pod); err != nil {
			s.t.Fatal(err)
		}
		if err := s.c.Delete(ctx, &pod); err != nil {
			s.t.Fatal(err)
		}
		return true
	}
	return s.offStable(stable)
}

// offStable reports whether any pod, terminating or not, is off the stable
// revision.
func (s *passSim) offStable(stable string) bool {
	for _, pod := range s.pods() {
		if pod.Labels[query.LabelRevisionHash] != stable {
			return true
		}
	}
	return false
}

func (s *passSim) persistedPhase() v1beta1.RolloutPhase {
	return s.persisted().Status.Components[v1beta1.EngineComponent].RolloutPhase
}

// A retargeted single-Component canary that rolls back writes RolledBack as
// soon as a pass reads the current record after its replica has converged
// on the stable revision. Under a real kubelet every pass of the revert is
// re-enqueued by the replica's own events before the pass's status write
// reaches the informer, so every pass reads the version before the last
// write, and the pass right after the rollback runs before the replica
// controller has observed the pin. The replica's spec moves twice, the pin
// and the released partition, so its status catches its generation while
// the revert runs and the terminal write lands when the pods are back.
func TestRetargetRollbackWritesRolledBackAsReplicaConverges(t *testing.T) {
	s := newRetargetRollbackSim(t, "retarget")
	s.pass("serve", false)
	if got := s.persistedPhase(); got != v1beta1.RolloutPhasePaused {
		t.Fatalf("the first split must serve before the rollback, got %q", got)
	}
	s.annotate(constants.RolloutRollbackAnnotation, "true")
	s.pass("roll back", false)
	if cs := rollout.CanaryStatusFor(&s.persisted().Status, v1beta1.EngineComponent); cs == nil ||
		cs.RolledBackRevisionHash != simRetargetCanary || s.persistedPhase() != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rollback must record the rejected revision and hold RollingBack, got %+v phase=%q", cs, s.persistedPhase())
	}
	if !s.pinnedAtStable(v1beta1.EngineComponent, simRetargetStable) {
		t.Fatal("the rollback pass must pin the replica at stable")
	}

	const budget = 24
	converged, landed := -1, -1
	var trace []string
	for i := 0; i < budget; i++ {
		// The replica controller observes the last spec write between passes,
		// except that the pass re-enqueued by the pin write runs first.
		if i > 0 {
			s.catchUp(v1beta1.EngineComponent)
		}
		remaining := true
		if s.revertObserved(v1beta1.EngineComponent) {
			remaining = s.revertTick(simRetargetStable)
		}
		if !remaining && converged < 0 {
			converged = i
		}
		s.pass(fmt.Sprintf("pass %d", i), true)
		r := s.replica(v1beta1.EngineComponent)
		cs := rollout.CanaryStatusFor(&s.persisted().Status, v1beta1.EngineComponent)
		rejected := ""
		if cs != nil {
			rejected = cs.RolledBackRevisionHash
		}
		trace = append(trace, fmt.Sprintf("pass %d: offStable=%v gen=%d observed=%d partition=%d rejected=%q phase=%s",
			i, remaining, r.Generation, r.Status.ObservedGeneration, irprojector.IRPartition(r), rejected, s.persistedPhase()))
		if s.persistedPhase() == v1beta1.RolloutPhaseRolledBack {
			if landed < 0 {
				landed = i
			}
		} else if landed >= 0 {
			t.Fatalf("the rolled-back hold regressed at pass %d:\n%s", i, strings.Join(trace, "\n"))
		}
		// The hold is checked across passes after the terminal write.
		if landed >= 0 && i >= landed+3 {
			break
		}
	}
	if converged < 0 {
		t.Fatalf("the replica never converged on stable within %d passes:\n%s", budget, strings.Join(trace, "\n"))
	}
	if landed < 0 {
		t.Fatalf("RolledBack never persisted within %d passes, the replica converged at pass %d:\n%s",
			budget, converged, strings.Join(trace, "\n"))
	}
	if landed > converged+1 {
		t.Fatalf("RolledBack must land on the first pass that reads the current record after convergence (pass %d), landed at pass %d:\n%s",
			converged, landed, strings.Join(trace, "\n"))
	}
	if cs := rollout.CanaryStatusFor(&s.persisted().Status, v1beta1.EngineComponent); cs.RolledBackRevisionHash != simRetargetCanary {
		t.Fatalf("the hold must keep its rejection, got %+v", cs)
	}
	if !s.pinnedAtStable(v1beta1.EngineComponent, simRetargetStable) {
		t.Fatal("the rolled-back hold must keep the replica pinned at stable")
	}
	if s.specWrites[v1beta1.EngineComponent] != 2 {
		t.Fatalf("the replica's spec moves once per decided change, the pin and the released partition, got %d writes:\n%s",
			s.specWrites[v1beta1.EngineComponent], strings.Join(trace, "\n"))
	}
	t.Logf("converged at pass %d, RolledBack persisted at pass %d\n%s", converged, landed, strings.Join(trace, "\n"))
}
