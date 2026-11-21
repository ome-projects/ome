package ops

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	ometesting "sigs.k8s.io/ome/pkg/utils/testing"
)

// TestShortenGraceAfterLeaderGone_OnARealAPIServer asks a real API server
// what the step's second delete does to a pod that is already terminating.
//
// A gang of one leader and three workers, each with a grace of 480 s, is
// deleted the way the engine deletes it. Every pod then shows a grace of
// 480 s and a deletion time 480 s ahead. The leader's kubelet ends the
// leader with its last delete, at grace 0, and the leader's pod is gone.
// Two seconds later the step runs with a bound of 30 s.
//
// Each worker then shows a grace of 30 s, and its deletion time is the
// first delete's instant plus 30 s: 450 s earlier than before, not 30 s
// from the second delete. A second run writes nothing. A delete that names
// a longer grace changes nothing. A delete that names 3 s once the first
// delete is 5 s old moves the deletion time to the instant of that request,
// and the pod then shows a grace of 1 s whatever the request named. A grace
// of 0 removes the pod object at once, with no kubelet having reported the
// containers stopped, which is why the step never names 0.
//
// A second gang shows the step itself when the leader goes later than the
// bound: its leader is gone 5 s after the delete and the bound is 3 s. Each
// worker gets its one delete with 3 s, the API server leaves each a grace
// of 1 s, and the step's event says so.
//
// No kubelet runs here, so the test plays the kubelet's one call itself.
// It needs the control plane binaries KUBEBUILDER_ASSETS names and is
// skipped without them.
func TestShortenGraceAfterLeaderGone_OnARealAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: export KUBEBUILDER_ASSETS=\"$(bin/setup-envtest use <kubernetes-version> -p path)\"")
	}
	_, cfg := ometesting.StartOnBusyHost(t, func() *envtest.Environment { return &envtest.Environment{} })
	api, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatalf("build the client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const gang = "gang-0"
	workers, first := deleteGangAndEndItsLeader(ctx, t, api, gang)
	time.Sleep(2 * time.Second)

	// The step reads the wall clock here, as the pods' deletion times are
	// the API server's.
	rec := record.NewFakeRecorder(8)
	deps := workload.Deps{Client: api, Recorder: rec}
	input := leaderGoneInput(leaderGoneBound)
	input.Clock = clock.RealClock{}
	if err := ShortenGraceAfterLeaderGone(ctx, deps, input, map[int32][]*corev1.Pod{0: listGang(ctx, t, api, gang)}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	if events := fdDrainEvents(rec); len(events) != 1 {
		t.Fatalf("events = %v, want one for the Instance", events)
	}
	shortened := readShortenedWorkers(ctx, t, api, workers, first)

	if err := ShortenGraceAfterLeaderGone(ctx, deps, input, map[int32][]*corev1.Pod{0: listGang(ctx, t, api, gang)}); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if events := fdDrainEvents(rec); len(events) != 0 {
		t.Fatalf("a second run must say nothing, got %v", events)
	}
	for _, pod := range workers {
		if shown := readPod(ctx, t, api, pod.Name); shown.ResourceVersion != shortened[pod.Name].ResourceVersion {
			t.Fatalf("%s: a second run wrote the pod again", pod.Name)
		}
	}

	aLongerGraceChangesNothing(ctx, t, api, shortened[workers[0].Name])
	aGraceWhoseEndHasPassedLeavesOneSecond(ctx, t, api, workers[1])
	aGraceOfZeroRemovesThePodAtOnce(ctx, t, api, workers[2])
	theLeaderGoesLaterThanTheBound(ctx, t, api)
}

// deleteGangAndEndItsLeader creates the gang, deletes its four pods the way
// the engine does, and ends the leader with its kubelet's last delete. It
// returns the three workers and what each shows after its first delete: a
// grace of 480 s and a deletion time.
func deleteGangAndEndItsLeader(ctx context.Context, t *testing.T, api client.Client, gang string) ([]*corev1.Pod, map[string]*corev1.Pod) {
	t.Helper()
	leader := createBoundGangMember(ctx, t, api, gang+"-leader-0", workload.RunnerLeader)
	workers := []*corev1.Pod{
		createBoundGangMember(ctx, t, api, gang+"-worker-0", workload.RunnerWorker),
		createBoundGangMember(ctx, t, api, gang+"-worker-1", workload.RunnerWorker),
		createBoundGangMember(ctx, t, api, gang+"-worker-2", workload.RunnerWorker),
	}
	for _, pod := range append([]*corev1.Pod{leader}, workers...) {
		uid := pod.UID
		if err := api.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil {
			t.Fatalf("delete %s as the engine does: %v", pod.Name, err)
		}
	}
	first := map[string]*corev1.Pod{}
	for _, pod := range workers {
		shown := readPod(ctx, t, api, pod.Name)
		if shown.DeletionTimestamp == nil || shown.DeletionGracePeriodSeconds == nil || *shown.DeletionGracePeriodSeconds != 480 {
			t.Fatalf("%s after its first delete: deletion time %v, grace %v, want a deletion time and a grace of 480", pod.Name, shown.DeletionTimestamp, shown.DeletionGracePeriodSeconds)
		}
		first[pod.Name] = shown
	}
	leaderUID := leader.UID
	if err := api.Delete(ctx, leader, client.GracePeriodSeconds(0), client.Preconditions{UID: &leaderUID}); err != nil {
		t.Fatalf("the leader's kubelet ends the leader: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(leader), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the leader's pod is gone after its kubelet's last delete: got %v, want not found", err)
	}
	return workers, first
}

// readShortenedWorkers reads each worker after the step: a grace of 30 s,
// and a deletion time 450 s earlier than after the first delete, which is
// the first delete's instant plus 30 s. It returns what each worker shows.
func readShortenedWorkers(ctx context.Context, t *testing.T, api client.Client, workers []*corev1.Pod, first map[string]*corev1.Pod) map[string]*corev1.Pod {
	t.Helper()
	shortened := map[string]*corev1.Pod{}
	for _, pod := range workers {
		shown := readPod(ctx, t, api, pod.Name)
		before := first[pod.Name]
		if shown.DeletionGracePeriodSeconds == nil || *shown.DeletionGracePeriodSeconds != 30 {
			t.Fatalf("%s: grace = %v after the step, want 30", pod.Name, shown.DeletionGracePeriodSeconds)
		}
		if want := before.DeletionTimestamp.Add(-450 * time.Second); !shown.DeletionTimestamp.Time.Equal(want) {
			t.Fatalf("%s: deletion time = %s after the step, want %s: the first delete's instant plus 30 s", pod.Name, shown.DeletionTimestamp.UTC().Format(time.RFC3339), want.UTC().Format(time.RFC3339))
		}
		t.Logf("%s: deletionGracePeriodSeconds 480 -> 30, deletionTimestamp %s -> %s", pod.Name,
			before.DeletionTimestamp.UTC().Format(time.RFC3339), shown.DeletionTimestamp.UTC().Format(time.RFC3339))
		shortened[pod.Name] = shown
	}
	return shortened
}

// aLongerGraceChangesNothing sends a worker that shows 30 s a delete that
// names 60 s, and reads the same grace and the same deletion time.
func aLongerGraceChangesNothing(ctx context.Context, t *testing.T, api client.Client, worker *corev1.Pod) {
	t.Helper()
	uid := worker.UID
	if err := api.Delete(ctx, worker, client.GracePeriodSeconds(60), client.Preconditions{UID: &uid}); err != nil {
		t.Fatalf("a delete with a longer grace: %v", err)
	}
	if shown := readPod(ctx, t, api, worker.Name); *shown.DeletionGracePeriodSeconds != 30 || !shown.DeletionTimestamp.Equal(worker.DeletionTimestamp) {
		t.Fatalf("%s: a delete with a longer grace must change nothing, got grace %d and deletion time %s", worker.Name, *shown.DeletionGracePeriodSeconds, shown.DeletionTimestamp)
	}
}

// firstDeleteOf is the instant of a terminating pod's first delete as the
// API server holds it: the deletion time less the grace the pod shows.
func firstDeleteOf(pod *corev1.Pod) time.Time {
	return pod.DeletionTimestamp.Add(-time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second)
}

// aGraceWhoseEndHasPassedLeavesOneSecond sends a worker a delete that names
// 3 s once the worker's first delete is 5 s old. The first delete's instant
// plus 3 s has passed, so the API server moves the deletion time to the
// instant of the request and shows a grace of 1 s, whatever the request
// named. The API server shows whole seconds.
func aGraceWhoseEndHasPassedLeavesOneSecond(ctx context.Context, t *testing.T, api client.Client, worker *corev1.Pod) {
	t.Helper()
	first := firstDeleteOf(readPod(ctx, t, api, worker.Name))
	if wait := time.Until(first.Add(5 * time.Second)); wait > 0 {
		time.Sleep(wait)
	}
	uid := worker.UID
	asked := time.Now()
	if err := api.Delete(ctx, worker, client.GracePeriodSeconds(3), client.Preconditions{UID: &uid}); err != nil {
		t.Fatalf("a delete with a grace of 3 s: %v", err)
	}
	shown := readPod(ctx, t, api, worker.Name)
	t.Logf("%s: a grace of 3 s asked %s after the first delete: deletionGracePeriodSeconds %d, deletionTimestamp %s", worker.Name,
		asked.Sub(first).Round(time.Second), *shown.DeletionGracePeriodSeconds, shown.DeletionTimestamp.UTC().Format(time.RFC3339))
	if *shown.DeletionGracePeriodSeconds != 1 {
		t.Fatalf("%s: grace = %d after a delete that names 3 s on a pod whose first delete is 5 s old, want 1", worker.Name, *shown.DeletionGracePeriodSeconds)
	}
	if shown.DeletionTimestamp.Time.Before(asked.Truncate(time.Second)) {
		t.Fatalf("%s: deletion time = %s, before the request at %s: the API server moves a deletion time that has passed to the instant of the request",
			worker.Name, shown.DeletionTimestamp.UTC().Format(time.RFC3339), asked.UTC().Format(time.RFC3339))
	}
}

// aGraceOfZeroRemovesThePodAtOnce sends a worker a delete that names 0 and
// reads that the pod object is gone.
func aGraceOfZeroRemovesThePodAtOnce(ctx context.Context, t *testing.T, api client.Client, worker *corev1.Pod) {
	t.Helper()
	uid := worker.UID
	if err := api.Delete(ctx, worker, client.GracePeriodSeconds(0), client.Preconditions{UID: &uid}); err != nil {
		t.Fatalf("a delete with a grace of 0: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(worker), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("%s: a grace of 0 removes the pod object at once, got %v", worker.Name, err)
	}
}

// theLeaderGoesLaterThanTheBound runs the step on a second gang whose leader
// is gone 5 s after the delete, with a bound of 3 s. The step sends each
// worker its one delete with 3 s. The API server counts them from the first
// delete, so it leaves each worker a grace of 1 s with a deletion time at
// the instant of the request, and the step's event says that.
func theLeaderGoesLaterThanTheBound(ctx context.Context, t *testing.T, api client.Client) {
	t.Helper()
	const gang = "gang-1"
	workers, shownFirst := deleteGangAndEndItsLeader(ctx, t, api, gang)
	first := firstDeleteOf(shownFirst[workers[0].Name])
	if wait := time.Until(first.Add(5 * time.Second)); wait > 0 {
		time.Sleep(wait)
	}
	rec := record.NewFakeRecorder(8)
	input := leaderGoneInput(3 * time.Second)
	input.Clock = clock.RealClock{}
	asked := time.Now()
	if err := ShortenGraceAfterLeaderGone(ctx, workload.Deps{Client: api, Recorder: rec}, input, map[int32][]*corev1.Pod{0: listGang(ctx, t, api, gang)}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone with the leader gone later than the bound: %v", err)
	}
	events := fdDrainEvents(rec)
	if len(events) != 1 {
		t.Fatalf("events = %v, want one for the Instance", events)
	}
	checkEventSays(t, events[0],
		"OME asked for a termination grace of 3s (lifecycle.leaderGoneGracePeriod) in place of 480s",
		"The pods were deleted more than 3s ago, so the API server leaves them a grace of 1s")
	for _, pod := range workers {
		shown := readPod(ctx, t, api, pod.Name)
		if shown.DeletionGracePeriodSeconds == nil || *shown.DeletionGracePeriodSeconds != 1 {
			t.Fatalf("%s: grace = %v after the step, want 1: the first delete is 5 s old and the bound is 3 s", pod.Name, shown.DeletionGracePeriodSeconds)
		}
		if shown.DeletionTimestamp.Time.Before(asked.Truncate(time.Second)) {
			t.Fatalf("%s: deletion time = %s, before the step's request at %s", pod.Name,
				shown.DeletionTimestamp.UTC().Format(time.RFC3339), asked.UTC().Format(time.RFC3339))
		}
		t.Logf("%s: the leader gone %s after the delete, a bound of 3 s: deletionGracePeriodSeconds 480 -> 1, deletionTimestamp %s", pod.Name,
			asked.Sub(first).Round(time.Second), shown.DeletionTimestamp.UTC().Format(time.RFC3339))
	}
}

// createBoundGangMember creates one pod of the gang, bound to a node, with
// a grace of 480 s. No kubelet runs it.
func createBoundGangMember(ctx context.Context, t *testing.T, api client.Client, name, runner string) *corev1.Pod {
	t.Helper()
	pod := memberPod(name, runner, 1)
	pod.Namespace = metav1.NamespaceDefault
	pod.UID = ""
	pod.Status = corev1.PodStatus{}
	if err := api.Create(ctx, pod); err != nil {
		t.Fatalf("create pod %s: %v", name, err)
	}
	return pod
}

func readPod(ctx context.Context, t *testing.T, api client.Client, name string) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	if err := api.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: name}, pod); err != nil {
		t.Fatalf("read pod %s: %v", name, err)
	}
	return pod
}

// listGang lists the pods of one gang as a pass would read them.
func listGang(ctx context.Context, t *testing.T, api client.Client, gang string) []*corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	if err := api.List(ctx, list, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		t.Fatalf("list the gang's pods: %v", err)
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, gang+"-") {
			pods = append(pods, &list.Items[i])
		}
	}
	return pods
}
