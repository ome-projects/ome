package gangpack

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	schedfake "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/fake"
	schedinformers "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
)

// TestNewInformerReaderDoesNotHangWithoutPodGroupAPI: the PodGroup CRD is an
// optional dependency, so building the reader must NOT block forever when the
// PodGroup API is unavailable (CRD absent, or apiserver unreachable). The initial
// cache sync is time-bounded; newInformerReader returns after the timeout so the
// scheduler still boots. Simulated with an unreachable apiserver + a short bound.
func TestNewInformerReaderDoesNotHangWithoutPodGroupAPI(t *testing.T) {
	cfg := &rest.Config{Host: "https://127.0.0.1:1"} // nothing listening -> never syncs

	done := make(chan error, 1)
	go func() {
		_, err := newInformerReader(context.Background(), cfg, topologyKeyAnnotation, placementGroupLabel, time.Minute, 200*time.Millisecond)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("newInformerReader returned an error, want nil (boot regardless): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("newInformerReader hung when the PodGroup API was unavailable; it must time-bound the initial sync")
	}
}

func TestFactsFromPodGroup(t *testing.T) {
	pg := &schedv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{topologyKeyAnnotation: "nvidia.com/gpu.clique"}},
		Spec:       schedv1alpha1.PodGroupSpec{MinMember: 18, ScheduleTimeoutSeconds: ptr.To[int32](120)},
	}
	const fallback = 10 * time.Minute
	if mm, tk, to := factsFromPodGroup(pg, topologyKeyAnnotation, fallback); mm != 18 || tk != "nvidia.com/gpu.clique" || to != 120*time.Second {
		t.Fatalf("factsFromPodGroup = %d,%q,%v want 18,nvidia.com/gpu.clique,2m", mm, tk, to)
	}
	// No annotation -> empty key; no ScheduleTimeoutSeconds -> default timeout.
	if mm, tk, to := factsFromPodGroup(&schedv1alpha1.PodGroup{Spec: schedv1alpha1.PodGroupSpec{MinMember: 4}}, topologyKeyAnnotation, fallback); mm != 4 || tk != "" || to != fallback {
		t.Fatalf("factsFromPodGroup(no annotation) = %d,%q,%v want 4,'',default", mm, tk, to)
	}
	if mm, tk, to := factsFromPodGroup(nil, topologyKeyAnnotation, fallback); mm != 0 || tk != "" || to != fallback {
		t.Fatalf("factsFromPodGroup(nil) = %d,%q,%v want 0,'',default", mm, tk, to)
	}
}

func TestInformerReaderGet(t *testing.T) {
	pg := &schedv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "pf", UID: types.UID("pg-uid"), Annotations: map[string]string{topologyKeyAnnotation: "clique"}},
		Spec:       schedv1alpha1.PodGroupSpec{MinMember: 8},
	}
	r := &informerReader{topologyKeyAnnotation: topologyKeyAnnotation, getPG: func(ns, name string) (*schedv1alpha1.PodGroup, error) {
		if ns == "team" && name == "pf" {
			return pg, nil
		}
		return nil, errors.New("not found")
	}}

	if mm, tk, _, uid, ok := r.get("team", "pf"); !ok || mm != 8 || tk != "clique" || uid != "pg-uid" {
		t.Fatalf("get(team/pf) = %d,%q,%v want 8,clique,true", mm, tk, ok)
	}
	if _, _, _, _, ok := r.get("team", "missing"); ok {
		t.Fatal("get(missing) should be not-found")
	}

	// A getPG that returns (nil, nil) is treated as not found, not a panic.
	rNil := &informerReader{getPG: func(string, string) (*schedv1alpha1.PodGroup, error) { return nil, nil }}
	if _, _, _, _, ok := rNil.get("x", "y"); ok {
		t.Fatal("nil PodGroup should be not-found")
	}
}

// TestPlacementChanged: only edits to the facts placement reads — spec,
// annotations, labels — count as a change. Status and metadata-only writes do
// not, so a controller refreshing status cannot wake parked members through the
// backoff-bypassing activation.
func TestPlacementChanged(t *testing.T) {
	base := func() *schedv1alpha1.PodGroup {
		return &schedv1alpha1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "team", Name: "pf", ResourceVersion: "1",
				Labels:      map[string]string{"tier": "a"},
				Annotations: map[string]string{topologyKeyAnnotation: "clique"},
			},
			Spec: schedv1alpha1.PodGroupSpec{MinMember: 2, ScheduleTimeoutSeconds: ptr.To[int32](30)},
		}
	}
	cases := []struct {
		name   string
		mutate func(pg *schedv1alpha1.PodGroup)
		want   bool
	}{
		{"identical", func(*schedv1alpha1.PodGroup) {}, false},
		{"status only", func(pg *schedv1alpha1.PodGroup) { pg.Status.Running = 1 }, false},
		{"resource version only", func(pg *schedv1alpha1.PodGroup) { pg.ResourceVersion = "2" }, false},
		{"minMember", func(pg *schedv1alpha1.PodGroup) { pg.Spec.MinMember = 1 }, true},
		{"timeout", func(pg *schedv1alpha1.PodGroup) { pg.Spec.ScheduleTimeoutSeconds = ptr.To[int32](60) }, true},
		{"topology annotation", func(pg *schedv1alpha1.PodGroup) { pg.Annotations[topologyKeyAnnotation] = "rack" }, true},
		{"label", func(pg *schedv1alpha1.PodGroup) { pg.Labels["tier"] = "b" }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oldPG, newPG := base(), base()
			tc.mutate(newPG)
			if got := placementChanged(oldPG, newPG); got != tc.want {
				t.Fatalf("placementChanged = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestInformerReaderOnChangeReportsNewAndEditedPodGroups: the reader reports a
// PodGroup when its informer stores it and when a stored one's placement facts
// change, and stays quiet on a status-only write. Each report arrives after the
// store holds the version that triggered it, so a get from the handler returns
// that version.
func TestInformerReaderOnChangeReportsNewAndEditedPodGroups(t *testing.T) {
	// The fake clientset's watch never ends its initial events, so a streaming
	// list would leave the informer unsynced; list then watch instead. The
	// simple fake is used because the field-managed one needs an OpenAPI model
	// this module does not carry for PodGroup.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := schedfake.NewSimpleClientset()
	factory := schedinformers.NewSharedInformerFactory(client, 0)
	podGroups := factory.Scheduling().V1alpha1().PodGroups()
	lister := podGroups.Lister()
	r := &informerReader{
		informer:              podGroups.Informer(),
		topologyKeyAnnotation: topologyKeyAnnotation,
		getPG: func(namespace, name string) (*schedv1alpha1.PodGroup, error) {
			return lister.PodGroups(namespace).Get(name)
		},
	}

	type report struct {
		key       string
		minMember int
	}
	reports := make(chan report, 16)
	if err := r.onChange(func(namespace, name string) {
		minMember, _, _, _, _ := r.get(namespace, name)
		reports <- report{key: namespace + "/" + name, minMember: minMember}
	}); err != nil {
		t.Fatalf("onChange: %v", err)
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	next := func(want report) {
		t.Helper()
		select {
		case got := <-reports:
			if got != want {
				t.Fatalf("report = %+v, want %+v", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("no report within 5s, want %+v", want)
		}
	}

	pg := &schedv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "pf"},
		Spec:       schedv1alpha1.PodGroupSpec{MinMember: 2},
	}
	created, err := client.SchedulingV1alpha1().PodGroups("team").Create(ctx, pg, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	next(report{key: "team/pf", minMember: 2})

	// A status-only write produces no report; the spec edit that follows is the
	// next one, carrying the stored minMember.
	statusOnly := created.DeepCopy()
	statusOnly.Status.Running = 1
	updated, err := client.SchedulingV1alpha1().PodGroups("team").Update(ctx, statusOnly, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("status update: %v", err)
	}
	shrunk := updated.DeepCopy()
	shrunk.Spec.MinMember = 1
	if _, err := client.SchedulingV1alpha1().PodGroups("team").Update(ctx, shrunk, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("spec update: %v", err)
	}
	next(report{key: "team/pf", minMember: 1})

	// A reader without an informer accepts a handler and never calls it.
	if err := (&informerReader{}).onChange(func(string, string) { t.Fatal("handler called without an informer") }); err != nil {
		t.Fatalf("onChange without informer: %v", err)
	}
}
