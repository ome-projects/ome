package inferencereplica

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/sliceprovision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/tpuslice/gke"
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
					{APIVersion: irGVK.GroupVersion().String(), Kind: irGVK.Kind, Name: "isvc-engine", Controller: &ctrl},
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

// TestDrainServiceRole covers the drain Service name -> (name prefix,
// component) parse for both OMENative drain Service shapes plus the reject
// cases the unfiltered watch relies on the mapper to screen out.
func TestDrainServiceRole(t *testing.T) {
	tests := []struct {
		name          string
		service       string
		wantPrefix    string
		wantComponent v1beta1.ComponentType
		wantOK        bool
	}{
		{name: "headless engine", service: "my-isvc-engine-headless", wantPrefix: "my-isvc", wantComponent: v1beta1.EngineComponent, wantOK: true},
		{name: "headless decoder", service: "my-isvc-decoder-headless", wantPrefix: "my-isvc", wantComponent: v1beta1.DecoderComponent, wantOK: true},
		{name: "headless router", service: "my-isvc-router-headless", wantPrefix: "my-isvc", wantComponent: v1beta1.RouterComponent, wantOK: true},
		{name: "per-revision engine", service: "my-isvc-engine-rev-abcdef", wantPrefix: "my-isvc", wantComponent: v1beta1.EngineComponent, wantOK: true},
		{name: "per-revision hex hash", service: "my-isvc-decoder-rev-5f7c9a", wantPrefix: "my-isvc", wantComponent: v1beta1.DecoderComponent, wantOK: true},
		{name: "isvc name with dashes", service: "a-b-c-engine-headless", wantPrefix: "a-b-c", wantComponent: v1beta1.EngineComponent, wantOK: true},
		{name: "isvc name containing the revision marker", service: "llama-rev-2-engine-rev-5f7c9a", wantPrefix: "llama-rev-2", wantComponent: v1beta1.EngineComponent, wantOK: true},
		{name: "empty", service: "", wantOK: false},
		{name: "unrelated headless (unknown component)", service: "kubernetes-headless", wantOK: false},
		{name: "unrelated service", service: "some-random-service", wantOK: false},
		{name: "headless with empty isvc", service: "engine-headless", wantOK: false},
		{name: "stable service (no suffix, not drain)", service: "my-isvc-engine", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPrefix, gotComponent, gotOK := drainServiceRole(tc.service)
			if gotOK != tc.wantOK {
				t.Fatalf("drainServiceRole(%q) ok = %v, want %v", tc.service, gotOK, tc.wantOK)
			}
			if gotOK && (gotPrefix != tc.wantPrefix || gotComponent != tc.wantComponent) {
				t.Errorf("drainServiceRole(%q) = (%q, %s), want (%q, %s)", tc.service, gotPrefix, gotComponent, tc.wantPrefix, tc.wantComponent)
			}
		})
	}
}

// TestEndpointSliceToIR verifies that, with no Service in the cache, the
// map function falls back to the name parse: it emits the projected
// replica's request for an OMENative drain slice and nothing for a
// foreign slice (or a non-EndpointSlice object).
func TestEndpointSliceToIR(t *testing.T) {
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "my-isvc-engine-headless-xyz",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "my-isvc-engine-headless"},
		},
	}
	reqs := r.endpointSliceToIR(context.Background(), slice)
	if len(reqs) != 1 {
		t.Fatalf("endpointSliceToIR returned %d requests, want 1", len(reqs))
	}
	if reqs[0].Namespace != "ns" || reqs[0].Name != "my-isvc-engine" {
		t.Errorf("endpointSliceToIR request = %v, want ns/my-isvc-engine", reqs[0].NamespacedName)
	}

	// Per-revision slice.
	revSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "my-isvc-decoder-rev-deadbeef"},
		},
	}
	revReqs := r.endpointSliceToIR(context.Background(), revSlice)
	if len(revReqs) != 1 || revReqs[0].Name != "my-isvc-decoder" {
		t.Errorf("endpointSliceToIR(per-revision) = %v, want ns/my-isvc-decoder", revReqs)
	}

	// Foreign slice -> no request.
	foreign := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "kube-dns"},
		},
	}
	if got := r.endpointSliceToIR(context.Background(), foreign); got != nil {
		t.Errorf("endpointSliceToIR(foreign) = %v, want nil", got)
	}

	// Non-EndpointSlice object -> nil.
	if got := r.endpointSliceToIR(context.Background(), &corev1.Pod{}); got != nil {
		t.Errorf("endpointSliceToIR(non-slice) = %v, want nil", got)
	}
}

func TestEndpointSliceToIRUsesTheServiceOwnerForAStandaloneReplica(t *testing.T) {
	isController := true
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "pool-a-engine-headless", Namespace: "team-a",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica",
			Name: "pool-a", UID: "ir-uid", Controller: &isController,
		}},
	}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()}

	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Name: "pool-a-engine-headless-x7k2p", Namespace: "team-a",
		Labels: map[string]string{discoveryv1.LabelServiceName: "pool-a-engine-headless"},
	}}
	got := r.endpointSliceToIR(context.Background(), slice)
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "pool-a"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpointSliceToIR = %v, want %v", got, want)
	}
}

// A headless Service name over the DNS label limit is truncated to a form
// the name parse cannot map back, so the controller owner resolves it.
func TestEndpointSliceToIRResolvesATruncatedHeadlessNameThroughTheOwner(t *testing.T) {
	prefix := strings.Repeat("a", 70)
	name := query.HeadlessServiceName(prefix, workloadtypes.ComponentEngine)
	if strings.HasPrefix(name, prefix) {
		t.Fatalf("fixture name %q is not truncated", name)
	}
	isController := true
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "team-a",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica",
			Name: prefix, UID: "ir-uid", Controller: &isController,
		}},
	}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()}

	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Name: name + "-x7k2p", Namespace: "team-a",
		Labels: map[string]string{discoveryv1.LabelServiceName: name},
	}}
	got := r.endpointSliceToIR(context.Background(), slice)
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: prefix}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpointSliceToIR = %v, want %v", got, want)
	}
}

// Only the controller owner is authoritative: a replica named by a
// non-controller owner reference leaves the slice to the name parse.
func TestEndpointSliceToIRParsesTheNameWhenTheReplicaIsNotTheController(t *testing.T) {
	isController := false
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "my-isvc-engine-headless", Namespace: "team-a",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica",
			Name: "pool-a", UID: "ir-uid", Controller: &isController,
		}},
	}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()}

	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Name: "my-isvc-engine-headless-x7k2p", Namespace: "team-a",
		Labels: map[string]string{discoveryv1.LabelServiceName: "my-isvc-engine-headless"},
	}}
	got := r.endpointSliceToIR(context.Background(), slice)
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "my-isvc-engine"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpointSliceToIR = %v, want %v", got, want)
	}
}

// The controller owner is matched by group and kind: an InferenceReplica from
// another API group is not this controller's replica, so the slice falls back
// to the name parse.
func TestEndpointSliceToIRParsesTheNameWhenTheOwnerIsFromAnotherGroup(t *testing.T) {
	isController := true
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "my-isvc-engine-headless", Namespace: "team-a",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "serving.example.com/v1", Kind: "InferenceReplica",
			Name: "pool-a", UID: "other-uid", Controller: &isController,
		}},
	}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()}

	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Name: "my-isvc-engine-headless-x7k2p", Namespace: "team-a",
		Labels: map[string]string{discoveryv1.LabelServiceName: "my-isvc-engine-headless"},
	}}
	got := r.endpointSliceToIR(context.Background(), slice)
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "my-isvc-engine"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpointSliceToIR = %v, want %v", got, want)
	}
}

// The pod handler enqueues the InferenceReplica controller matched by group
// and kind, and skips a same-kind controller from another API group.
func TestPodEventHandlerMatchesTheControllerByGroupAndKind(t *testing.T) {
	isController := true
	for _, tc := range []struct {
		name       string
		apiVersion string
		want       []reconcile.Request
	}{
		{
			name:       "ome.io controller",
			apiVersion: v1beta1.SchemeGroupVersion.String(),
			want:       []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "pool-a"}}},
		},
		{name: "controller from another group", apiVersion: "serving.example.com/v1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := managedPod()
			pod.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: tc.apiVersion, Kind: "InferenceReplica",
				Name: "pool-a", UID: "ir-uid", Controller: &isController,
			}}
			h := newPodEventHandler(workloadtypes.NewExpectations())
			ctx := context.Background()
			type requestQueue = workqueue.TypedRateLimitingInterface[reconcile.Request]
			// One queue per event type: a queue collapses duplicate requests,
			// so a shared queue would pass when only one event enqueued.
			for _, fire := range []struct {
				name string
				send func(requestQueue)
			}{
				{"create", func(q requestQueue) { h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: pod}, q) }},
				{"update", func(q requestQueue) {
					h.Update(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: pod, ObjectNew: pod}, q)
				}},
				{"delete", func(q requestQueue) { h.Delete(ctx, event.TypedDeleteEvent[client.Object]{Object: pod}, q) }},
				{"generic", func(q requestQueue) { h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: pod}, q) }},
			} {
				q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
				fire.send(q)
				var got []reconcile.Request
				for q.Len() > 0 {
					req, _ := q.Get()
					got = append(got, req)
					q.Done(req)
				}
				q.ShutDown()
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("%s enqueued %v, want %v", fire.name, got, tc.want)
				}
			}
		})
	}
}

// A per-revision Service is controlled by the InferenceService, not the
// replica, so its slices map through the name parse to the projected replica.
func TestEndpointSliceToIRParsesTheNameWhenTheInferenceServiceOwnsTheService(t *testing.T) {
	isController := true
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "my-isvc-engine-rev-5f7c9a", Namespace: "team-a",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
			Name: "my-isvc", UID: "isvc-uid", Controller: &isController,
		}},
	}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(svc).Build()}

	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Name: "my-isvc-engine-rev-5f7c9a-q4m8d", Namespace: "team-a",
		Labels: map[string]string{discoveryv1.LabelServiceName: "my-isvc-engine-rev-5f7c9a"},
	}}
	got := r.endpointSliceToIR(context.Background(), slice)
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "my-isvc-engine"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpointSliceToIR = %v, want %v", got, want)
	}
}

// A per-revision Service of a referenced replica is owned by the fronting
// InferenceService and named with the replica's own prefix; the owner
// resolves the role to the replica it names, while a projected role of
// another service still resolves to the projected replica.
func TestEndpointSliceToIRResolvesAReferencedReplicaThroughTheOwningService(t *testing.T) {
	isController := true
	referencing := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a", UID: "svc-uid"},
		Spec: v1beta1.InferenceServiceSpec{
			ReplicaRefs: &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}},
		},
	}
	projecting := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "my-isvc", Namespace: "team-a", UID: "my-isvc-uid"},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	ownedBy := func(name, owner string) *corev1.Service {
		return &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "team-a",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: owner, UID: types.UID(owner + "-uid"), Controller: &isController,
			}},
		}}
	}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		referencing, projecting, ownedBy("pool-d-decoder-rev-5f7c9a", "svc"), ownedBy("my-isvc-engine-rev-5f7c9a", "my-isvc"),
	).Build()}
	slice := func(service string) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
			Name: service + "-q4m8d", Namespace: "team-a",
			Labels: map[string]string{discoveryv1.LabelServiceName: service},
		}}
	}
	got := r.endpointSliceToIR(context.Background(), slice("pool-d-decoder-rev-5f7c9a"))
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "pool-d"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("referenced decoder: endpointSliceToIR = %v, want %v", got, want)
	}
	got = r.endpointSliceToIR(context.Background(), slice("my-isvc-engine-rev-5f7c9a"))
	want = []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "my-isvc-engine"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projected engine: endpointSliceToIR = %v, want %v", got, want)
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

// TestTPUSliceToIR pins the slice event routing: a slice provisioned for
// an InferenceReplica enqueues that replica, and any other slice enqueues
// nothing.
func TestTPUSliceToIR(t *testing.T) {
	slice := func(ownerKind, owner string) client.Object {
		u := gke.NewObject()
		u.SetName("llama-engine-0-0-abc")
		u.SetLabels(map[string]string{sliceOwnerKindLabel: ownerKind})
		if owner != "" {
			u.SetAnnotations(map[string]string{sliceprovision.AnnotationOwner: owner})
		}
		return u
	}
	configured := &Reconciler{TPUSliceProvisioning: sliceTestConfig()}
	for _, tc := range []struct {
		name string
		r    *Reconciler
		obj  client.Object
		want []reconcile.Request
	}{
		{
			name: "provisioned for a replica",
			r:    configured,
			obj:  slice(irKind, "prod/llama-engine"),
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "prod", Name: "llama-engine"}}},
		},
		{name: "provisioning unconfigured", r: &Reconciler{}, obj: slice(irKind, "prod/llama-engine")},
		{name: "another owner kind", r: configured, obj: slice("InferenceService", "prod/llama")},
		{name: "no owner", r: configured, obj: slice(irKind, "")},
		{name: "malformed owner", r: configured, obj: slice(irKind, "llama-engine")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.r.tpuSliceToIR(context.Background(), tc.obj); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("tpuSliceToIR = %v, want %v", got, tc.want)
			}
		})
	}
}

// recordingQueue records Add, the only method the mapped enqueue handler
// calls on a queue that is not a priority queue.
type recordingQueue struct {
	workqueue.TypedRateLimitingInterface[reconcile.Request]
	added []reconcile.Request
}

func (q *recordingQueue) Add(req reconcile.Request) { q.added = append(q.added, req) }

// TestTPUSliceEventHandler pins that every slice event enqueues what
// tpuSliceToIR maps it to, and that only a delete counts a provisioned slice
// as released.
func TestTPUSliceEventHandler(t *testing.T) {
	h := (&Reconciler{TPUSliceProvisioning: sliceTestConfig()}).tpuSliceEventHandler()
	provisioned, err := gke.Build(gke.Spec{
		Name:        "llama-engine-0-0-abc",
		Type:        "type-a",
		Topology:    "2x2x1",
		Owner:       map[string]string{sliceprovision.LabelOwnerUID: "uid-a"},
		Labels:      map[string]string{sliceOwnerKindLabel: irKind},
		Annotations: map[string]string{sliceprovision.AnnotationOwner: "prod/llama-engine"},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	foreign, err := gke.Build(gke.Spec{
		Name:     "training-0",
		Type:     "type-a",
		Topology: "2x2x1",
		Owner:    map[string]string{"example.com/owner-uid": "uid-b"},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	owner := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "prod", Name: "llama-engine"}}}
	releasedLabels := map[string]string{"slice_type": "type-a", "topology": "2x2x1"}
	ctx := context.Background()
	for _, tc := range []struct {
		name         string
		send         func(*recordingQueue)
		wantEnqueued []reconcile.Request
		wantReleased float64
	}{
		{name: "create", send: func(q *recordingQueue) { h.Create(ctx, event.CreateEvent{Object: provisioned}, q) }, wantEnqueued: owner},
		{name: "update", send: func(q *recordingQueue) {
			h.Update(ctx, event.UpdateEvent{ObjectOld: provisioned, ObjectNew: provisioned}, q)
		}, wantEnqueued: owner},
		{name: "delete", send: func(q *recordingQueue) { h.Delete(ctx, event.DeleteEvent{Object: provisioned}, q) }, wantEnqueued: owner, wantReleased: 1},
		{name: "delete of another producer's slice", send: func(q *recordingQueue) { h.Delete(ctx, event.DeleteEvent{Object: foreign}, q) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &recordingQueue{}
			before := irStatusMetric(t, "ome_tpu_slice_released_total", releasedLabels)
			tc.send(q)
			if !reflect.DeepEqual(q.added, tc.wantEnqueued) {
				t.Fatalf("enqueued %v, want %v", q.added, tc.wantEnqueued)
			}
			if got := irStatusMetric(t, "ome_tpu_slice_released_total", releasedLabels) - before; got != tc.wantReleased {
				t.Fatalf("released slices counted = %v, want %v", got, tc.wantReleased)
			}
		})
	}
}

// TestTPUSliceEventHandlerTimesProvisioning pins that the create or update
// event that first shows a slice the controller created as ready times its
// provisioning, and that the events before and after it don't.
func TestTPUSliceEventHandlerTimesProvisioning(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	labels := map[string]string{"slice_type": "type-a", "topology": "2x2x1"}
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		// send sends the handler an event that shows the slice as cur, which
		// the watch last saw as prev.
		send func(h handler.EventHandler, q *recordingQueue, prev, cur client.Object)
	}{
		{name: "update", send: func(h handler.EventHandler, q *recordingQueue, prev, cur client.Object) {
			h.Update(ctx, event.UpdateEvent{ObjectOld: prev, ObjectNew: cur}, q)
		}},
		{name: "create", send: func(h handler.EventHandler, q *recordingQueue, _, cur client.Object) {
			h.Create(ctx, event.CreateEvent{Object: cur}, q)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c := newSliceReconciler(t, ir.DeepCopy())
			name := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 0}, "")
			pending := getSlice(t, c, name)
			setSliceState(t, c, name, sliceStateReady)
			ready := getSlice(t, c, name)
			h, q := r.tpuSliceEventHandler(), &recordingQueue{}
			before := irStatusMetric(t, "ome_tpu_slice_provision_duration_seconds", labels)
			for _, step := range []struct {
				seen      string
				prev, cur client.Object
				want      float64 // provisioning times observed so far
			}{
				{seen: "pending", prev: pending, cur: pending, want: 0},
				{seen: "ready", prev: pending, cur: ready, want: 1},
				{seen: "ready again", prev: ready, cur: ready, want: 1},
			} {
				tc.send(h, q, step.prev, step.cur)
				if got := irStatusMetric(t, "ome_tpu_slice_provision_duration_seconds", labels) - before; got != step.want {
					t.Fatalf("seen %s: provisioning times observed = %v, want %v", step.seen, got, step.want)
				}
			}
		})
	}
}

// refReplica is a standalone replica in namespace ns naming the given
// runtime and model; an empty name leaves that reference unset.
func refReplica(name, ns, runtime, model string) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-uid")},
		Spec:       v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent},
	}
	if runtime != "" {
		ir.Spec.RuntimeRef = &v1beta1.ServingRuntimeRef{Name: runtime}
	}
	if model != "" {
		ir.Spec.ModelRef = &v1beta1.ModelRef{Name: model}
	}
	return ir
}

func TestRefIndexExtractors(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		obj                  client.Object
		runtime, model, auto []string
	}{
		{name: "runtime and model", obj: refReplica("a", "team-a", "runtime-a", "model-a"), runtime: []string{"runtime-a"}, model: []string{"model-a"}},
		{name: "model alone selects a runtime", obj: refReplica("a", "team-a", "", "model-a"), model: []string{"model-a"}, auto: []string{irRuntimeAutoSelected}},
		{name: "runtime alone", obj: refReplica("a", "team-a", "runtime-a", ""), runtime: []string{"runtime-a"}},
		{name: "runners replica", obj: baselineIR("llama-engine", "prod", 1)},
		{name: "not a replica", obj: &corev1.Pod{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := irRuntimeRefIndexExtractor(tc.obj); !reflect.DeepEqual(got, tc.runtime) {
				t.Errorf("runtime index = %v, want %v", got, tc.runtime)
			}
			if got := irModelRefIndexExtractor(tc.obj); !reflect.DeepEqual(got, tc.model) {
				t.Errorf("model index = %v, want %v", got, tc.model)
			}
			if got := irRuntimeAutoSelectIndexExtractor(tc.obj); !reflect.DeepEqual(got, tc.auto) {
				t.Errorf("auto-select index = %v, want %v", got, tc.auto)
			}
		})
	}
}

// refWatchReconciler builds a reconciler over a fake client carrying the
// reference indexes and objs.
func refWatchReconciler(t *testing.T, objs ...client.Object) *Reconciler {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).
		WithIndex(&v1beta1.InferenceReplica{}, irRuntimeRefIndexField, irRuntimeRefIndexExtractor).
		WithIndex(&v1beta1.InferenceReplica{}, irModelRefIndexField, irModelRefIndexExtractor).
		WithIndex(&v1beta1.InferenceReplica{}, irRuntimeAutoSelectIndexField, irRuntimeAutoSelectIndexExtractor).
		Build()
	return &Reconciler{Client: c, Log: logr.Discard()}
}

func requestNames(reqs []reconcile.Request) []string {
	names := make([]string, 0, len(reqs))
	for _, req := range reqs {
		names = append(names, req.Namespace+"/"+req.Name)
	}
	sort.Strings(names)
	return names
}

func TestRuntimeToReplicas(t *testing.T) {
	// child-a inherits from the cluster runtime runtime-a, so a runtime-a
	// event reaches the replica naming child-a as well.
	child := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: "child-a", Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "runtime-a"},
	}}
	r := refWatchReconciler(t,
		child,
		refReplica("named", "team-a", "runtime-a", ""),
		refReplica("named-elsewhere", "team-b", "runtime-a", "model-a"),
		refReplica("selecting", "team-a", "", "model-a"),
		refReplica("selecting-elsewhere", "team-b", "", "model-a"),
		refReplica("other-runtime", "team-a", "runtime-b", ""),
		refReplica("inheriting", "team-a", "child-a", ""),
		baselineIR("llama-engine", "team-a", 1),
	)
	for _, tc := range []struct {
		name string
		obj  client.Object
		want []string
	}{
		{
			name: "cluster runtime reaches every namespace",
			obj:  &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a"}},
			want: []string{"team-a/inheriting", "team-a/named", "team-a/selecting", "team-b/named-elsewhere", "team-b/selecting-elsewhere"},
		},
		{
			name: "namespaced runtime reaches its namespace",
			obj:  &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a", Namespace: "team-b"}},
			want: []string{"team-b/named-elsewhere", "team-b/selecting-elsewhere"},
		},
		{
			name: "runtime nobody names reaches the selecting replicas",
			obj:  &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-c"}},
			want: []string{"team-a/selecting", "team-b/selecting-elsewhere"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := requestNames(r.runtimeToReplicas(context.Background(), tc.obj))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("runtimeToReplicas = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestModelToReplicas(t *testing.T) {
	r := refWatchReconciler(t,
		refReplica("named", "team-a", "runtime-a", "model-a"),
		refReplica("named-elsewhere", "team-b", "", "model-a"),
		refReplica("other-model", "team-a", "", "model-b"),
		refReplica("runtime-only", "team-a", "runtime-a", ""),
		baselineIR("llama-engine", "team-a", 1),
	)
	for _, tc := range []struct {
		name string
		obj  client.Object
		want []string
	}{
		{
			name: "cluster model reaches every namespace",
			obj:  &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-a"}},
			want: []string{"team-a/named", "team-b/named-elsewhere"},
		},
		{
			name: "namespaced model reaches its namespace",
			obj:  &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-a", Namespace: "team-a"}},
			want: []string{"team-a/named"},
		},
		{
			name: "model nobody names reaches nothing",
			obj:  &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-c"}},
			want: []string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := requestNames(r.modelToReplicas(context.Background(), tc.obj))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("modelToReplicas = %v, want %v", got, tc.want)
			}
		})
	}
}
