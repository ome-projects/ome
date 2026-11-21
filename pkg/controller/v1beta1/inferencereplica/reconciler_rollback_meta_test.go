package inferencereplica

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A rollback renders the stored revision's labels and annotations, and keeps
// every key a revision does not record from the current template: the Kueue
// queue labels, the local-queue annotation, the placement provenance label
// and the annotations the replica inherits from its InferenceService. A key a
// newer revision added is dropped, because the stored revision lacks it.
func TestApplyRollbackPayload_KeysNoRevisionRecordsFollowTheCurrentTemplate(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey] = "example.com/inherited"
	desired := workloadtypes.WorkloadDesiredSpec{
		PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:2.0"}}},
		PodTemplateObjectMeta: &metav1.ObjectMeta{
			Labels: map[string]string{
				"app.example.com/name":                       "llama-v2",
				"example.com/added-by-v2":                    "yes",
				constants.KueueQueueLabelKey:                 "team-b",
				constants.KueueWorkloadPriorityClassLabelKey: "high",
				constants.PlacementOrigin:                    "source-uid",
			},
			Annotations: map[string]string{
				"example.com/note":      "v2",
				"example.com/inherited": "from-service",
				constants.LocalQueue:    "team-b",
			},
		},
	}
	payload := &revision.DataPayload{
		PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}}},
		PodMeta: &metav1.ObjectMeta{
			Labels:      map[string]string{"app.example.com/name": "llama-v1"},
			Annotations: map[string]string{"example.com/note": "v1"},
		},
	}

	r := &Reconciler{APIReader: podListFailingReader{}}
	if err := r.applyRollbackPayload(context.Background(), ir, &desired, payload, nil); err != nil {
		t.Fatalf("applyRollbackPayload: %v", err)
	}

	want := &metav1.ObjectMeta{
		Labels: map[string]string{
			"app.example.com/name":                       "llama-v1",
			constants.KueueQueueLabelKey:                 "team-b",
			constants.KueueWorkloadPriorityClassLabelKey: "high",
			constants.PlacementOrigin:                    "source-uid",
		},
		Annotations: map[string]string{
			"example.com/note":      "v1",
			"example.com/inherited": "from-service",
			constants.LocalQueue:    "team-b",
		},
	}
	if diff := cmp.Diff(want, desired.PodTemplateObjectMeta); diff != "" {
		t.Fatalf("rollback template metadata (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"app.example.com/name": "llama-v1"}, payload.PodMeta.Labels); diff != "" {
		t.Fatalf("the stored revision's payload must not be written to (-want +got):\n%s", diff)
	}
}

// A replica rolled back with spec.pacing.rollbackToRevision renders every new
// pod from the stored revision. Kueue admits a pod only if it carries the
// queue label when it is created, so each of those pods must carry the queue
// the replica names now, not the one the revision was recorded under.
func TestReconcile_APodCreatedDuringARollbackCarriesTheReplicaQueue(t *testing.T) {
	cases := []struct {
		name            string
		gang            bool
		queueAtRollback string
	}{
		{name: "single pod, queue unchanged", queueAtRollback: "team-a"},
		{name: "single pod, queue moved after the revision was recorded", queueAtRollback: "team-b"},
		{name: "gang, queue unchanged", gang: true, queueAtRollback: "team-a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRollbackQueueHarness(t, tc.gang)
			first := h.passUntil("bring-up", h.podsPerInstance)
			for i := range first {
				if got := first[i].Labels[constants.KueueQueueLabelKey]; got != "team-a" {
					t.Fatalf("bring-up pod %s carries queue %q, want team-a", first[i].Name, got)
				}
			}

			stable := h.rollBackAndScaleUp(tc.queueAtRollback)
			created := newPods(first, h.passUntil("scale-up", 2*h.podsPerInstance))
			if len(created) != h.podsPerInstance {
				t.Fatalf("scale-up created %d pods, want %d", len(created), h.podsPerInstance)
			}
			for i := range created {
				expectRolledBackPod(t, &created[i], stable, tc.queueAtRollback)
			}
		})
	}
}

// rollbackQueueHarness drives one replica whose template carries the queue
// labels through the reconciler, on a fake client.
type rollbackQueueHarness struct {
	t               *testing.T
	r               *Reconciler
	c               client.Client
	key             client.ObjectKey
	podsPerInstance int
}

// newRollbackQueueHarness is a one-Instance replica on queue team-a: a single
// pod, or a gang of one leader and one worker.
func newRollbackQueueHarness(t *testing.T, gang bool) *rollbackQueueHarness {
	t.Helper()
	ir := baselineIR("llama-engine", "prod", 1)
	if gang {
		ir.Spec.Runners = []v1beta1.Runner{
			{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
			}}},
			{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
			}}},
		}
	}
	setQueue(ir, "team-a")
	r, c := newReconciler(t, ir)
	r.Recorder = record.NewFakeRecorder(256)
	return &rollbackQueueHarness{t: t, r: r, c: c, key: client.ObjectKeyFromObject(ir), podsPerInstance: len(ir.Spec.Runners)}
}

// passUntil reconciles until the namespace holds pods pods, and fails when a
// few passes do not get there.
func (h *rollbackQueueHarness) passUntil(label string, pods int) []corev1.Pod {
	h.t.Helper()
	request := ctrl.Request{NamespacedName: h.key}
	for i := 0; i < 8 && len(podsIn(h.t, h.c, h.key.Namespace)) < pods; i++ {
		h.r.Expectations = workloadtypes.NewExpectations()
		if _, err := h.r.Reconcile(context.Background(), request); err != nil {
			h.t.Fatalf("%s pass %d: %v", label, i, err)
		}
	}
	got := podsIn(h.t, h.c, h.key.Namespace)
	if len(got) != pods {
		h.t.Fatalf("%s: want %d pods, got %d", label, pods, len(got))
	}
	return got
}

// rollBackAndScaleUp moves the spec to a second image while the canary
// holds the replica on the revision it runs, puts the template on queue, and
// adds one Instance. It returns the revision the replica is held on.
func (h *rollbackQueueHarness) rollBackAndScaleUp(queue string) string {
	h.t.Helper()
	live := &v1beta1.InferenceReplica{}
	if err := h.c.Get(context.Background(), h.key, live); err != nil {
		h.t.Fatalf("get IR: %v", err)
	}
	stable := live.Status.UpdateRevision
	for i := range live.Spec.Runners {
		live.Spec.Runners[i].Template.Spec.Containers[0].Image = "sgl:2.0"
	}
	setQueue(live, queue)
	live.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To(stable)}
	live.Spec.Replicas = ptr.To(int32(2))
	live.Generation++
	if err := h.c.Update(context.Background(), live); err != nil {
		h.t.Fatalf("set the rollback target: %v", err)
	}
	return stable
}

// expectRolledBackPod checks that pod was rendered from the stable revision
// and carries the replica's queue, priority and own label.
func expectRolledBackPod(t *testing.T, pod *corev1.Pod, stable, queue string) {
	t.Helper()
	if got, want := pod.Labels[query.LabelRevisionHash], query.RevisionFromName(stable).Hash(); got != want {
		t.Fatalf("pod %s is on revision %q, want the rollback revision %q", pod.Name, got, want)
	}
	if got := pod.Spec.Containers[0].Image; got != "sgl:1.0" {
		t.Fatalf("pod %s runs %s, want the rollback revision's sgl:1.0", pod.Name, got)
	}
	wantLabels := map[string]string{
		constants.KueueQueueLabelKey:                 queue,
		constants.KueueWorkloadPriorityClassLabelKey: "high",
		"app.example.com/name":                       "llama",
	}
	for k, v := range wantLabels {
		if got := pod.Labels[k]; got != v {
			t.Errorf("pod %s label %s = %q, want %q", pod.Name, k, got, v)
		}
	}
	if got := pod.Annotations[constants.LocalQueue]; got != queue {
		t.Errorf("pod %s annotation %s = %q, want %q", pod.Name, constants.LocalQueue, got, queue)
	}
}

// newPods is the pods of after whose names before does not hold.
func newPods(before, after []corev1.Pod) []corev1.Pod {
	existing := make(map[string]bool, len(before))
	for i := range before {
		existing[before[i].Name] = true
	}
	var out []corev1.Pod
	for i := range after {
		if !existing[after[i].Name] {
			out = append(out, after[i])
		}
	}
	return out
}

// setQueue writes the queue labels and the local-queue annotation on the
// template whose metadata every pod of the replica renders, the way the
// projector does, beside one label of the replica's own.
func setQueue(ir *v1beta1.InferenceReplica, queue string) {
	tmpl := &ir.Spec.Runners[0].Template
	tmpl.Labels = map[string]string{
		constants.KueueQueueLabelKey:                 queue,
		constants.KueueWorkloadPriorityClassLabelKey: "high",
		"app.example.com/name":                       "llama",
	}
	tmpl.Annotations = map[string]string{constants.LocalQueue: queue}
}

func podsIn(t *testing.T, c client.Client, namespace string) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}
