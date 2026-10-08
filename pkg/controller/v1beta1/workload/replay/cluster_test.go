package replay

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestMapDiffRecordsValues pins the property the in-place lock rests on:
// a changed key is recorded with both of its values, normalized.
func TestMapDiffRecordsValues(t *testing.T) {
	d := &driver{norm: newNormalizer(traceStart)}
	got := d.mapDiff(
		map[string]string{"keep": "same", "drop": "gone", "hash": "882059bb"},
		map[string]string{"keep": "same", "hash": "882059bb", "add": "new"},
	)
	if want := "[add:nil→new drop:gone→nil]"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := d.mapDiff(map[string]string{"a": "1"}, map[string]string{"a": "1"}); got != "" {
		t.Fatalf("an unchanged map must produce no diff, got %q", got)
	}
}

func TestPodWriteDiffCoversEveryWrittenField(t *testing.T) {
	d := &driver{norm: newNormalizer(traceStart)}
	before := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{query.LabelRevisionHash: "old"},
			Annotations: map[string]string{"marker": "a"},
			Finalizers:  []string{"keep"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main", Image: "img:v1",
			Env: []corev1.EnvVar{{Name: "OME_PEER", Value: "one"}},
		}}},
	}
	after := before.DeepCopy()
	after.Labels[query.LabelRevisionHash] = "new"
	after.Annotations["marker"] = "b"
	after.Finalizers = nil
	after.Spec.Containers[0].Image = "img:v2"
	after.Spec.Containers[0].Env[0].Value = "two"
	after.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "queue"}}
	setPodCondition(after, query.ServingConditionType, corev1.ConditionTrue, traceStart)

	got := d.podWriteDiff(before, after)
	for _, want := range []string{
		"images=main=img:v1→main=img:v2",
		"serving=absent→True",
		"labels=[ome.io/revision-hash:old→new]",
		"annotations=[marker:a→b]",
		"env=[main.OME_PEER:one→two]",
		"schedulingGates=nil→[queue]",
		"finalizers=[keep]→nil",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if got := d.podWriteDiff(before, before.DeepCopy()); got != "unchanged-fields" {
		t.Fatalf("an inert write must say so, got %q", got)
	}
}

// TestPodWriteDiffIgnoresServingTransitionTime keeps the one wall-clock
// value the engine writes out of the goldens.
func TestPodWriteDiffIgnoresServingTransitionTime(t *testing.T) {
	d := &driver{norm: newNormalizer(traceStart)}
	before := &corev1.Pod{}
	setPodCondition(before, query.ServingConditionType, corev1.ConditionTrue, traceStart)
	after := before.DeepCopy()
	after.Status.Conditions[0].LastTransitionTime = metav1.NewTime(traceStart.Add(time.Hour))
	if got := d.podWriteDiff(before, after); got != "unchanged-fields" {
		t.Fatalf("a moved transition time must not reach the trace, got %q", got)
	}
}

func TestResourceOfNamesEveryKind(t *testing.T) {
	for _, tc := range []struct {
		obj  client.Object
		want string
	}{
		{&corev1.Pod{}, "pods"},
		{&corev1.Service{}, "services"},
		{&discoveryv1.EndpointSlice{}, "endpointslices"},
	} {
		if got := resourceOf(tc.obj); got != tc.want {
			t.Errorf("got %q want %q", got, tc.want)
		}
	}
}

// TestTakeRejectionIgnoresStagedWrites is the fidelity guard on injection:
// a refusal armed for the engine must survive every write the scenario
// itself makes to set the stage.
func TestTakeRejectionIgnoresStagedWrites(t *testing.T) {
	d := &driver{rejections: []*pendingRejection{{
		verb: "create", resource: "pods", remaining: 1,
		err: errors.New("refused"), label: "api.quotaDenied",
	}}}
	d.quiet = true
	if err, _ := d.takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a staged write consumed the rejection")
	}
	d.quiet = false
	if err, label := d.takeRejection("create", &corev1.Pod{}); err == nil || label != "api.quotaDenied" {
		t.Fatalf("the engine's write did not consume the rejection: %v %q", err, label)
	}
	if err, _ := d.takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a one-shot rejection fired twice")
	}
}

func TestTakeRejectionMatchesVerbAndResource(t *testing.T) {
	arm := func() *driver {
		return &driver{rejections: []*pendingRejection{{
			verb: "patch", resource: "pods", remaining: -1, err: errors.New("refused"), label: "api.invalid",
		}}}
	}
	if err, _ := arm().takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a create consumed a patch rejection")
	}
	if err, _ := arm().takeRejection("patch", &corev1.Service{}); err != nil {
		t.Fatalf("a service write consumed a pod rejection")
	}
	d := arm()
	for i := 0; i < 3; i++ {
		if err, _ := d.takeRejection("patch", &corev1.Pod{}); err == nil {
			t.Fatalf("an unbounded rejection stopped firing at %d", i)
		}
	}
}

func TestInstanceIndexOfRejectsAMissingLabel(t *testing.T) {
	if _, err := instanceIndexOf(&corev1.Pod{}); err == nil {
		t.Fatalf("a pod with no instance label must not read as index 0")
	}
	bad := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "x"}}}
	if _, err := instanceIndexOf(bad); err == nil {
		t.Fatalf("an unparsable instance label must not read as index 0")
	}
	good := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "7"}}}
	idx, err := instanceIndexOf(good)
	if err != nil || idx != 7 {
		t.Fatalf("got %d, %v", idx, err)
	}
}

// TestNotFoundRejectionRemovesThePod pins that a not-found answer is never
// a lie about the cluster: the engine's write is refused and the pod it
// addressed is gone when the next read looks for it.
func TestNotFoundRejectionRemovesThePod(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods: [{index: 0, ready: true, serving: true, routed: true}]
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	if _, err := applyRejection(notFound, "delete")(ctx, d, TimelineEvent{ID: "api.notFound"}); err != nil {
		t.Fatalf("arm: %v", err)
	}
	name := d.podName(PodRef{Index: 0})
	pod, err := d.getPod(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	err = d.cli.Delete(ctx, pod)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("the engine's delete must be answered not found, got %v", err)
	}
	if _, err := d.getPod(ctx, name); !apierrors.IsNotFound(err) {
		t.Fatalf("the pod must be gone behind the answer, got %v", err)
	}
	if !strings.Contains(d.trace.String(), "pod delete-rejected name="+name+" rejection=api.notFound") {
		t.Fatalf("the refusal is traced:\n%s", d.trace.String())
	}
}

// TestAlreadyExistsRejectionLandsThePod pins that an already-exists answer
// is true of the cluster: the engine's create is refused and the pod it
// rendered is there, held like every pod, when the next read looks.
func TestAlreadyExistsRejectionLandsThePod(t *testing.T) {
	ctx := context.Background()
	d := mustDriver(t, minimalScenario)
	if _, err := applyRejection(alreadyExists, "create")(ctx, d, TimelineEvent{ID: "api.alreadyExists"}); err != nil {
		t.Fatalf("arm: %v", err)
	}
	rev, err := d.ensureRevision(ctx, d.spec.Image)
	if err != nil {
		t.Fatal(err)
	}
	pod, err := d.renderPod(PodSpec{Index: 0}, rev)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.cli.Create(ctx, pod); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("the engine's create must be answered already exists, got %v", err)
	}
	landed, err := d.getPod(ctx, pod.Name)
	if err != nil {
		t.Fatalf("the pod must be there behind the answer: %v", err)
	}
	if landed.UID == "" || !contains(landed.Finalizers, podLifecycleFinalizer) {
		t.Fatalf("the landed pod is admitted like every pod: uid=%q finalizers=%v", landed.UID, landed.Finalizers)
	}
	if !strings.Contains(d.trace.String(), "pod create-rejected name="+pod.Name+" rejection=api.alreadyExists") {
		t.Fatalf("the refusal is traced:\n%s", d.trace.String())
	}
}

// TestThrottledRejectionCarriesTheRetryAfter pins the shed answer against
// the engine's classifier: both spellings read as throttled, the
// Retry-After the scenario names reaches the classifier, and one without
// a delay suggests none.
func TestThrottledRejectionCarriesTheRetryAfter(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		variant, retryAfter string
		want                time.Duration
	}{
		{"TooManyRequests", "30s", 30 * time.Second},
		{"ServiceUnavailable", "2m", 2 * time.Minute},
		{"TooManyRequests", "", 0},
	} {
		d := mustDriver(t, minimalScenario)
		detail, err := applyThrottled(ctx, d, TimelineEvent{ID: "api.throttled", Variant: tc.variant, Args: EventArgs{RetryAfter: tc.retryAfter}})
		if err != nil {
			t.Fatalf("%s: arm: %v", tc.variant, err)
		}
		if (tc.want > 0) != strings.Contains(detail, "retryAfter=") {
			t.Fatalf("%s: detail %q", tc.variant, detail)
		}
		rev, err := d.ensureRevision(ctx, d.spec.Image)
		if err != nil {
			t.Fatal(err)
		}
		pod, err := d.renderPod(PodSpec{Index: 0}, rev)
		if err != nil {
			t.Fatal(err)
		}
		err = d.cli.Create(ctx, pod)
		rejection := evidence.ClassifyAPIError(err)
		if rejection.Class != types.APIRejectionThrottled || rejection.RetryAfter != tc.want {
			t.Fatalf("%s: the engine must read the shed create as throttled with the suggested delay: class=%v retryAfter=%s err=%v", tc.variant, rejection.Class, rejection.RetryAfter, err)
		}
		if _, err := d.getPod(ctx, pod.Name); !apierrors.IsNotFound(err) {
			t.Fatalf("%s: a shed create lands nothing: %v", tc.variant, err)
		}
	}
	if _, err := applyThrottled(ctx, mustDriver(t, minimalScenario), TimelineEvent{ID: "api.throttled"}); err == nil {
		t.Fatal("the shed answer names its spelling")
	}
}

// TestNamespaceTerminatingRejectionIsWhatTheClassifierReads pins the
// refusal against the engine's classifier: a Forbidden carrying the
// NamespaceTerminating cause reads as a permanent environment rejection
// with that reason.
func TestNamespaceTerminatingRejectionIsWhatTheClassifierReads(t *testing.T) {
	rejection := evidence.ClassifyAPIError(namespaceTerminating("pods", "svc-a"))
	if rejection.Class != types.APIRejectionPermanentEnvironment || rejection.Reason != types.RejectionReasonNamespaceTerminating {
		t.Fatalf("got class=%v reason=%q", rejection.Class, rejection.Reason)
	}
}
