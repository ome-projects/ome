package inferencereplica

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// managedPod returns a baseline OMENative-managed pod with the routing
// labels and a Running phase the predicate considers reconcile-relevant.
func managedPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "isvc-engine-abc-0",
			Namespace: "ns",
			Labels: map[string]string{
				query.LabelManagedBy:                  query.ManagedByOMENative,
				constants.InferenceServicePodLabelKey: "isvc",
				constants.OMEComponentLabel:           "engine",
				query.LabelInstanceIdx:                "0",
				query.LabelRevisionHash:               "abc",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func withContainersReady(p *corev1.Pod, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:   corev1.ContainersReady,
		Status: status,
	})
	return p
}

// withPodReady stamps the kubelet-owned Ready condition — what kubelet
// writes once every readiness gate is satisfied, and what the promote bar
// reads.
func withPodReady(p *corev1.Pod, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:   corev1.PodReady,
		Status: status,
	})
	return p
}

func withServing(p *corev1.Pod, serving bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if serving {
		status = corev1.ConditionTrue
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:   podreadiness.ConditionType,
		Status: status,
	})
	return p
}

func TestManagedByOMENativePredicate_CreateDeleteGenericGateOnManagedBy(t *testing.T) {
	p := managedByOMENativePredicate()
	managed := managedPod()
	unmanaged := managedPod()
	delete(unmanaged.Labels, query.LabelManagedBy)

	if !p.Create(event.CreateEvent{Object: managed}) {
		t.Error("Create on managed pod must pass")
	}
	if p.Create(event.CreateEvent{Object: unmanaged}) {
		t.Error("Create on unmanaged pod must be dropped")
	}
	if !p.Delete(event.DeleteEvent{Object: managed}) {
		t.Error("Delete on managed pod must pass")
	}
	if p.Delete(event.DeleteEvent{Object: unmanaged}) {
		t.Error("Delete on unmanaged pod must be dropped")
	}
	if !p.Generic(event.GenericEvent{Object: managed}) {
		t.Error("Generic on managed pod must pass")
	}
	if p.Generic(event.GenericEvent{Object: unmanaged}) {
		t.Error("Generic on unmanaged pod must be dropped")
	}
}

func TestManagedByOMENativePredicate_UpdateFieldDiff(t *testing.T) {
	terminal := &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}

	tests := []struct {
		name     string
		old      *corev1.Pod
		new      *corev1.Pod
		wantPass bool
	}{
		{
			name:     "no relevant change (heartbeat) is dropped",
			old:      managedPod(),
			new:      managedPod(),
			wantPass: false,
		},
		{
			name: "only timestamp/podIP churn is dropped",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.PodIP = "10.0.0.5"
				p.Status.StartTime = &metav1.Time{}
				return p
			}(),
			wantPass: false,
		},
		{
			name:     "Status.Phase change passes",
			old:      managedPod(),
			new:      func() *corev1.Pod { p := managedPod(); p.Status.Phase = corev1.PodFailed; return p }(),
			wantPass: true,
		},
		{
			name:     "ContainersReady flip passes",
			old:      withContainersReady(managedPod(), false),
			new:      withContainersReady(managedPod(), true),
			wantPass: true,
		},
		{
			// Kubelet folds the serving gate into Ready in a status write of
			// its own: ContainersReady and the gate are already what they
			// were, so this flip is the only evidence that the pod became
			// eligible for its Service — and the promote bar waits on it.
			name:     "PodReady flip passes",
			old:      withPodReady(withServing(withContainersReady(managedPod(), true), true), false),
			new:      withPodReady(withServing(withContainersReady(managedPod(), true), true), true),
			wantPass: true,
		},
		{
			name:     "serving-gate flip passes",
			old:      withServing(managedPod(), false),
			new:      withServing(managedPod(), true),
			wantPass: true,
		},
		{
			name: "ContainerStatuses terminal-failure change passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{State: corev1.ContainerState{Waiting: terminal}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "Waiting.Reason change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "Terminated exit-code change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "LastTerminationState crash detail change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "container Image flip (in-place convergence) passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v1"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v2"},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "InitContainerStatuses terminal-failure change passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: "init", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: "init", State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerError"}}},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "container added passes (length change)",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v1"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "main", Image: "fake-serving:v1"},
					{Name: "sidecar", Image: "sidecar:v1"},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "ImageID/RestartCount/Resources heartbeat churn is dropped",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name:         "main",
						Image:        "fake-serving:v1",
						ImageID:      "sha256:aaaa",
						RestartCount: 1,
						Ready:        true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
							StartedAt: metav1.Now()}},
					},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name:         "main",
						Image:        "fake-serving:v1",
						ImageID:      "sha256:bbbb", // churn
						RestartCount: 2,             // churn
						Ready:        true,          // per-container Ready (pod condition handled separately)
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
							StartedAt: metav1.Now()}}, // timestamp churn
						AllocatedResources: corev1.ResourceList{}, // resource churn
					},
				}
				return p
			}(),
			wantPass: false,
		},
		{
			name: "SchedulingGates removal (Kueue gate-exit) passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				p.Spec.SchedulingGates = []corev1.PodSchedulingGate{
					{Name: "kueue.x-k8s.io/admission"},
				}
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				return p
			}(),
			wantPass: true,
		},
		{
			name: "NodeName bind passes",
			old: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				return p
			}(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Status.Phase = corev1.PodPending
				p.Spec.NodeName = "node-1"
				return p
			}(),
			wantPass: true,
		},
		{
			name: "DeletionTimestamp set passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				now := metav1.Now()
				p.DeletionTimestamp = &now
				return p
			}(),
			wantPass: true,
		},
		{
			name: "OwnerReferences change passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				ctrl := true
				p.OwnerReferences = []metav1.OwnerReference{
					{Kind: irKind, Name: "isvc-engine", Controller: &ctrl},
				}
				return p
			}(),
			wantPass: true,
		},
		{
			name: "revision-hash label change passes",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Labels[query.LabelRevisionHash] = "def"
				return p
			}(),
			wantPass: true,
		},
		{
			name: "orthogonal label churn is dropped",
			old:  managedPod(),
			new: func() *corev1.Pod {
				p := managedPod()
				p.Labels["unrelated/label"] = "x"
				return p
			}(),
			wantPass: false,
		},
		{
			name:     "both unmanaged is dropped",
			old:      func() *corev1.Pod { p := managedPod(); delete(p.Labels, query.LabelManagedBy); return p }(),
			new:      func() *corev1.Pod { p := managedPod(); delete(p.Labels, query.LabelManagedBy); return p }(),
			wantPass: false,
		},
		{
			name:     "managed-by stripped (old managed) still passes",
			old:      managedPod(),
			new:      func() *corev1.Pod { p := managedPod(); delete(p.Labels, query.LabelManagedBy); return p }(),
			wantPass: true,
		},
	}

	p := managedByOMENativePredicate()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Update(event.UpdateEvent{
				ObjectOld: client.Object(tc.old),
				ObjectNew: client.Object(tc.new),
			})
			if got != tc.wantPass {
				t.Errorf("Update predicate = %v, want %v", got, tc.wantPass)
			}
		})
	}
}

// TestIRNameFromDrainServiceName covers the EndpointSlice -> IR name
// reverse-lookup for both OMENative drain Service shapes plus the reject
// cases the unfiltered watch relies on the mapper to screen out.
func TestIRNameFromDrainServiceName(t *testing.T) {
	tests := []struct {
		name    string
		service string
		wantIR  string
		wantOK  bool
	}{
		{name: "headless engine", service: "my-isvc-engine-headless", wantIR: "my-isvc-engine", wantOK: true},
		{name: "headless decoder", service: "my-isvc-decoder-headless", wantIR: "my-isvc-decoder", wantOK: true},
		{name: "headless router", service: "my-isvc-router-headless", wantIR: "my-isvc-router", wantOK: true},
		{name: "per-revision engine", service: "my-isvc-engine-rev-abcdef", wantIR: "my-isvc-engine", wantOK: true},
		{name: "per-revision hex hash", service: "my-isvc-decoder-rev-5f7c9a", wantIR: "my-isvc-decoder", wantOK: true},
		{name: "isvc name with dashes", service: "a-b-c-engine-headless", wantIR: "a-b-c-engine", wantOK: true},
		{name: "empty", service: "", wantOK: false},
		{name: "unrelated headless (unknown component)", service: "kubernetes-headless", wantOK: false},
		{name: "unrelated service", service: "some-random-service", wantOK: false},
		{name: "headless with empty isvc", service: "engine-headless", wantOK: false},
		{name: "stable service (no suffix, not drain)", service: "my-isvc-engine", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotIR, gotOK := irNameFromDrainServiceName(tc.service)
			if gotOK != tc.wantOK {
				t.Fatalf("irNameFromDrainServiceName(%q) ok = %v, want %v", tc.service, gotOK, tc.wantOK)
			}
			if gotOK && gotIR != tc.wantIR {
				t.Errorf("irNameFromDrainServiceName(%q) = %q, want %q", tc.service, gotIR, tc.wantIR)
			}
		})
	}
}

// TestEndpointSliceToIR verifies the map function emits exactly the
// owning IR request for an OMENative drain slice and nothing for a
// foreign slice (or a non-EndpointSlice object).
func TestEndpointSliceToIR(t *testing.T) {
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "my-isvc-engine-headless-xyz",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "my-isvc-engine-headless"},
		},
	}
	reqs := EndpointSliceToIR(context.Background(), slice)
	if len(reqs) != 1 {
		t.Fatalf("EndpointSliceToIR returned %d requests, want 1", len(reqs))
	}
	if reqs[0].Namespace != "ns" || reqs[0].Name != "my-isvc-engine" {
		t.Errorf("EndpointSliceToIR request = %v, want ns/my-isvc-engine", reqs[0].NamespacedName)
	}

	// Per-revision slice.
	revSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "my-isvc-decoder-rev-deadbeef"},
		},
	}
	revReqs := EndpointSliceToIR(context.Background(), revSlice)
	if len(revReqs) != 1 || revReqs[0].Name != "my-isvc-decoder" {
		t.Errorf("EndpointSliceToIR(per-revision) = %v, want ns/my-isvc-decoder", revReqs)
	}

	// Foreign slice -> no request.
	foreign := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "kube-dns"},
		},
	}
	if got := EndpointSliceToIR(context.Background(), foreign); got != nil {
		t.Errorf("EndpointSliceToIR(foreign) = %v, want nil", got)
	}

	// Non-EndpointSlice object -> nil.
	if got := EndpointSliceToIR(context.Background(), &corev1.Pod{}); got != nil {
		t.Errorf("EndpointSliceToIR(non-slice) = %v, want nil", got)
	}
}

// TestPodGroupPredicate_FiltersOnlyStatusChurn: the watch admits the
// gang scheduler's phase, which is what tells an Instance its group has
// to be rebuilt, alongside metadata, spec and deletion changes — and
// still drops the running member tallies that move continuously while a
// gang converges.
func TestPodGroupPredicate_FiltersOnlyStatusChurn(t *testing.T) {
	p := podGroupPredicate()
	oldObj := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "group", Namespace: "prod", Generation: 1},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
		Status:     schedulingv1alpha1.PodGroupStatus{Phase: schedulingv1alpha1.PodGroupScheduling},
	}
	statusOnly := oldObj.DeepCopy()
	statusOnly.ResourceVersion = "2"
	statusOnly.Status.Running = 1

	if !p.Create(event.CreateEvent{Object: statusOnly}) {
		t.Fatal("create event must enqueue reconciliation")
	}
	if p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: statusOnly}) {
		t.Fatal("member-tally churn must not enqueue reconciliation")
	}
	if p.Update(event.UpdateEvent{ObjectOld: statusOnly, ObjectNew: statusOnly.DeepCopy()}) {
		t.Fatal("an identical status must not enqueue reconciliation")
	}

	for _, tc := range []struct {
		name   string
		mutate func(*schedulingv1alpha1.PodGroup)
	}{
		{"a failed gang", func(pg *schedulingv1alpha1.PodGroup) {
			pg.Status.Phase = schedulingv1alpha1.PodGroupFailed
			pg.Status.Failed = 2
		}},
		{"an admitted gang", func(pg *schedulingv1alpha1.PodGroup) {
			pg.Status.Phase = schedulingv1alpha1.PodGroupRunning
			pg.Status.Running = 2
		}},
	} {
		verdict := statusOnly.DeepCopy()
		tc.mutate(verdict)
		if !p.Update(event.UpdateEvent{ObjectOld: statusOnly, ObjectNew: verdict}) {
			t.Errorf("%s must enqueue reconciliation: the row acts on it", tc.name)
		}
	}

	metadataDrift := statusOnly.DeepCopy()
	metadataDrift.Labels = map[string]string{"example.com/reconcile": "true"}
	if !p.Update(event.UpdateEvent{ObjectOld: statusOnly, ObjectNew: metadataDrift}) {
		t.Fatal("metadata update must enqueue reconciliation")
	}

	specDrift := statusOnly.DeepCopy()
	specDrift.Spec.MinMember = 3
	specDrift.Generation = 2
	if !p.Update(event.UpdateEvent{ObjectOld: statusOnly, ObjectNew: specDrift}) {
		t.Fatal("generation update must enqueue reconciliation")
	}

	terminating := statusOnly.DeepCopy()
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	if !p.Update(event.UpdateEvent{ObjectOld: statusOnly, ObjectNew: terminating}) {
		t.Fatal("deletion transition must enqueue reconciliation")
	}
	if !p.Delete(event.DeleteEvent{Object: statusOnly}) {
		t.Fatal("delete event must enqueue terminal cleanup")
	}
	if p.Generic(event.GenericEvent{Object: statusOnly}) {
		t.Fatal("generic event must not enqueue terminal cleanup")
	}
}
