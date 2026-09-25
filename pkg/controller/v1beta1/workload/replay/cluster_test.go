package replay

import (
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
