package podgroup

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ---------------------------------------------------------------------
// IsMultiPodInstance — threshold (>1 total pods) classifies correctly.
// ---------------------------------------------------------------------

func TestIsMultiPodInstance(t *testing.T) {
	cases := []struct {
		name    string
		runners []core.RunnerPlan
		want    bool
	}{
		{"single-pod default", []core.RunnerPlan{{Name: "default", Size: 1}}, false},
		{"leader + zero workers", []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 0}}, false},
		{"leader + 1 worker", []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}, true},
		{"leader + 3 workers", []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 3}}, true},
		{"zero runners", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inst := core.InstancePlan{Index: 0, Runners: tc.runners}
			if got := IsMultiPodInstance(inst); got != tc.want {
				t.Errorf("IsMultiPodInstance: got %v want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------
// BuildPodGroup — pure-compute shape assertions.
// ---------------------------------------------------------------------

func TestBuildPodGroup_NilISVC(t *testing.T) {
	plan := core.ComponentPlan{Component: workload.ComponentEngine}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	if _, err := BuildPodGroup(nil, testPodGroupOwnerGVK, "", plan, inst); err == nil {
		t.Fatal("expected error for nil owner")
	}
}

func TestBuildPodGroup_SinglePodRejected(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "default", Size: 1}}}
	if _, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err == nil {
		t.Fatal("expected single-pod build to error (caller must guard with IsMultiPodInstance)")
	}
}

// TestBuildPodGroup_NameUsesOwnerNameNotOwnerObject pins the IR-path naming
// fix: the PodGroup NAME must follow ownerName (the workload owner name =
// Key.OwnerName, what the pod renderer uses), NOT owner.GetName(). On the
// IR-managed path the owner OBJECT is the InferenceReplica ("gang-engine"),
// but the pods' pod-group label is "gang-engine-0" (ISVC "gang"). Using
// owner.GetName() would emit "gang-engine-engine-0" → the gang scheduler's
// "PodGroup not found". The OwnerReference must still target the owner object
// (the IR) for GC.
func TestBuildPodGroup_NameUsesOwnerNameNotOwnerObject(t *testing.T) {
	irOwner := newPodGroupISVC("test-ns", "gang-engine") // owner object named like an IR: "<isvc>-<component>"
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 15 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}

	pg, err := BuildPodGroup(irOwner, testPodGroupOwnerGVK, "gang", plan, inst) // ownerName = ISVC name (Key.OwnerName)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	if pg.Name != "gang-engine-0" {
		t.Errorf("Name: got %q want gang-engine-0 — must follow ownerName, not owner.GetName()=%q (which would give gang-engine-engine-0)", pg.Name, irOwner.GetName())
	}
	if len(pg.OwnerReferences) != 1 || pg.OwnerReferences[0].Name != "gang-engine" {
		t.Errorf("OwnerReference must still target the owner object (IR gang-engine) for GC; got %+v", pg.OwnerReferences)
	}
}

// TestBuildPodGroup_Shape pins the canonical multi-pod PodGroup
// (leader + 3 workers). One assertion per surface
// (name, namespace, labels, ownerRefs, spec) keeps diffs readable.
func TestBuildPodGroup_Shape(t *testing.T) {
	isvc := newPodGroupISVC("prod-models", "llama-engine")
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 15 * time.Minute,
		TopologyKey:          "network.example.com/fabric-domain",
	}
	inst := core.InstancePlan{
		Index: 0,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 3},
		},
	}

	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}

	// Name: matches query.PodGroupName.
	if pg.Name != "llama-engine-engine-0" {
		t.Errorf("Name: got %q want llama-engine-engine-0", pg.Name)
	}
	if pg.Namespace != "prod-models" {
		t.Errorf("Namespace: got %q want prod-models", pg.Namespace)
	}

	// MinMember: 1 leader + 3 workers = 4.
	if pg.Spec.MinMember != 4 {
		t.Errorf("MinMember: got %d want 4", pg.Spec.MinMember)
	}

	// ScheduleTimeoutSeconds: 15m = 900s, unclamped (no operator bounds).
	if pg.Spec.ScheduleTimeoutSeconds == nil {
		t.Fatal("ScheduleTimeoutSeconds: nil pointer")
	}
	if *pg.Spec.ScheduleTimeoutSeconds != 900 {
		t.Errorf("ScheduleTimeoutSeconds: got %d want 900", *pg.Spec.ScheduleTimeoutSeconds)
	}

	// Labels: same keys the pod renderer / headless Service selector use,
	// so the Service/pods <-> gang pivot actually matches.
	wantLabels := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-engine",
		constants.OMEComponentLabel:           "engine",
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelInstanceIdx:                "0",
	}
	if diff := cmp.Diff(wantLabels, pg.Labels); diff != "" {
		t.Errorf("Labels (-want +got):\n%s", diff)
	}
	if got := pg.Annotations[query.AnnotationTopologyKey]; got != plan.TopologyKey {
		t.Errorf("%s annotation: got %q want %q", query.AnnotationTopologyKey, got, plan.TopologyKey)
	}

	// OwnerReferences: single Controller-owner pointing at the ISVC.
	if len(pg.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(pg.OwnerReferences))
	}
	ref := pg.OwnerReferences[0]
	if ref.Name != "llama-engine" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-engine", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
	if ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
		t.Errorf("OwnerRef.BlockOwnerDeletion: want true, got %v", ref.BlockOwnerDeletion)
	}
}

// TestBuildPodGroup_TopologyKeyAnnotation verifies the gang topology-domain
// key is stamped when declared and omitted when unset.
func TestBuildPodGroup_TopologyKeyAnnotation(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	inst := core.InstancePlan{
		Index: 0,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 1},
		},
	}

	t.Run("declared topology key is stamped", func(t *testing.T) {
		plan := core.ComponentPlan{
			Component:            workload.ComponentEngine,
			InstanceReadyTimeout: 5 * time.Minute,
			TopologyKey:          "cloud.google.com/gke-tpu-partition-2x2x2-id",
		}
		pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
		if err != nil {
			t.Fatalf("BuildPodGroup: %v", err)
		}
		if got := pg.Annotations[query.AnnotationTopologyKey]; got != plan.TopologyKey {
			t.Errorf("annotation %s: got %q want %q", query.AnnotationTopologyKey, got, plan.TopologyKey)
		}
	})

	t.Run("no topology key means no annotations", func(t *testing.T) {
		plan := core.ComponentPlan{
			Component:            workload.ComponentEngine,
			InstanceReadyTimeout: 5 * time.Minute,
		}
		pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
		if err != nil {
			t.Fatalf("BuildPodGroup: %v", err)
		}
		if pg.Annotations != nil {
			t.Errorf("annotations: got %v want nil", pg.Annotations)
		}
	})
}

func TestBuildPodGroup_TimeoutClamp(t *testing.T) {
	clamp := &workload.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
	cases := []struct {
		name    string
		clamp   *workload.GangScheduleTimeoutClamp
		in      time.Duration
		wantSec int32
		wantSet bool
	}{
		{"zero takes the configured floor", clamp, 0, 60, true},
		{"30s rounded up to the floor", clamp, 30 * time.Second, 60, true},
		{"3 minutes pass through", clamp, 3 * time.Minute, 180, true},
		{"10 minutes at the ceiling", clamp, 10 * time.Minute, 600, true},
		{"30 minutes lowered to the ceiling", clamp, 30 * time.Minute, 600, true},
		{"unconfigured passes the derived value through", nil, 30 * time.Minute, 1800, true},
		{"unconfigured sub-second leaves the field unset", nil, 0, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := newPodGroupISVC("prod", "llama")
			plan := core.ComponentPlan{
				Component:            workload.ComponentEngine,
				InstanceReadyTimeout: tc.in,
				GangScheduleTimeout:  tc.clamp,
			}
			inst := core.InstancePlan{
				Index: 0,
				Runners: []core.RunnerPlan{
					{Name: "leader", Size: 1},
					{Name: "worker", Size: 1},
				},
			}
			pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
			if err != nil {
				t.Fatalf("BuildPodGroup: %v", err)
			}
			if !tc.wantSet {
				if pg.Spec.ScheduleTimeoutSeconds != nil {
					t.Fatalf("ScheduleTimeoutSeconds: got %d want unset (the scheduler default)",
						*pg.Spec.ScheduleTimeoutSeconds)
				}
				return
			}
			if pg.Spec.ScheduleTimeoutSeconds == nil {
				t.Fatal("ScheduleTimeoutSeconds: nil")
			}
			if *pg.Spec.ScheduleTimeoutSeconds != tc.wantSec {
				t.Errorf("ScheduleTimeoutSeconds: got %d want %d (in=%v)",
					*pg.Spec.ScheduleTimeoutSeconds, tc.wantSec, tc.in)
			}
		})
	}
}

// TestBuildPodGroup_HighWorkerCount confirms MinMember scales linearly
// with worker count — guards against an off-by-one in the sum.
func TestBuildPodGroup_HighWorkerCount(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{
		Index: 2,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 7}, // tensor-parallel-8 setup
		},
	}
	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	if pg.Spec.MinMember != 8 {
		t.Errorf("MinMember: got %d want 8 (1 leader + 7 workers)", pg.Spec.MinMember)
	}
	if pg.Name != "llama-engine-2" {
		t.Errorf("Name: got %q want llama-engine-2 (instance-2)", pg.Name)
	}
	if pg.Labels[query.LabelInstanceIdx] != "2" {
		t.Errorf("LabelInstanceIdx: got %q want 2", pg.Labels[query.LabelInstanceIdx])
	}
}

// ---------------------------------------------------------------------
// EnsurePodGroup — controller-side reconcile via fake client.
// ---------------------------------------------------------------------

func TestEnsurePodGroup_SinglePodNoOp(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "default", Size: 1}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}

	// Assert nothing was created.
	pg := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg)
	if err == nil {
		t.Errorf("expected single-pod Instance to NOT create a PodGroup, found %s/%s", pg.Namespace, pg.Name)
	} else if !apierrors.IsNotFound(err) {
		t.Errorf("unexpected error type: %v", err)
	}
}

func TestEnsurePodGroup_CreatesWhenAbsent(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 5 * time.Minute,
		TopologyKey:          "network.example.com/fabric-domain",
	}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 3}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}

	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get PodGroup after ensure: %v", err)
	}
	if got.Spec.MinMember != 4 {
		t.Errorf("MinMember: got %d want 4", got.Spec.MinMember)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != "llama" {
		t.Errorf("OwnerReferences: got %#v want one pointing at the ISVC", got.OwnerReferences)
	}
	if got.Annotations[query.AnnotationTopologyKey] != plan.TopologyKey {
		t.Errorf("%s annotation: got %#v want %q", query.AnnotationTopologyKey, got.Annotations, plan.TopologyKey)
	}
}

func TestEnsurePodGroupForPodsFromObservation_TerminatingBlocksReuse(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	existing, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	now := metav1.Now()
	existing.DeletionTimestamp = &now

	_, _, err = EnsurePodGroupForPodsFromObservation(context.Background(), c, isvc,
		testPodGroupOwnerGVK, isvc.GetName(), plan, inst, nil, existing, true)
	if !errors.Is(err, ErrPodGroupTerminating) {
		t.Fatalf("terminating PodGroup error: got %v want ErrPodGroupTerminating", err)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(existing), got); !apierrors.IsNotFound(err) {
		t.Fatalf("ensure must not create a replacement while observed object terminates: got %v", err)
	}
}

func TestEnsurePodGroupForPodsFromObservation_ForeignCollisionIsNeverAdopted(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	foreign := newPodGroupISVC("prod", "foreign")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	existing, err := BuildPodGroup(foreign, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}

	_, _, err = EnsurePodGroupForPodsFromObservation(context.Background(), c, isvc,
		testPodGroupOwnerGVK, isvc.GetName(), plan, inst, nil, existing, true)
	if !errors.Is(err, ErrPodGroupOwnershipConflict) {
		t.Fatalf("foreign collision error: got %v want ErrPodGroupOwnershipConflict", err)
	}
	if ref := metav1.GetControllerOfNoCopy(existing); ref == nil || ref.UID != foreign.UID {
		t.Fatalf("foreign owner changed: %+v", existing.OwnerReferences)
	}
}

// TestEnsurePodGroup_DriftCorrected — pre-existing PodGroup with stale
// MinMember / ScheduleTimeoutSeconds / Labels gets reconciled to the
// desired shape on the next ensure.
func TestEnsurePodGroup_DriftCorrected(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	stale := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-engine-0",
			Namespace: "prod",
			Labels:    map[string]string{"stale": "true"},
			Annotations: map[string]string{
				query.AnnotationTopologyKey: "stale.example.com/domain",
				"example.com/preserve":      "yes",
			},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:              99, // stale
			ScheduleTimeoutSeconds: int32Ptr(9999),
		},
	}
	c := newPodGroupClient(t, isvc, stale)
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 3 * time.Minute,
		TopologyKey:          "network.example.com/fabric-domain",
	}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}

	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get PodGroup after ensure: %v", err)
	}
	if got.Spec.MinMember != 2 {
		t.Errorf("MinMember: got %d want 2 (drift not corrected)", got.Spec.MinMember)
	}
	if got.Spec.ScheduleTimeoutSeconds == nil || *got.Spec.ScheduleTimeoutSeconds != 180 {
		t.Errorf("ScheduleTimeoutSeconds: want 180 (3m), got %v", got.Spec.ScheduleTimeoutSeconds)
	}
	if got.Labels[constants.OMEComponentLabel] != "engine" {
		t.Errorf("Labels: want engine label set, got %#v", got.Labels)
	}
	if _, hasStale := got.Labels["stale"]; hasStale {
		t.Errorf("Labels: stale label not overwritten, got %#v", got.Labels)
	}
	if got.Annotations[query.AnnotationTopologyKey] != plan.TopologyKey {
		t.Errorf("topology annotation drift not corrected: got %#v want %q", got.Annotations, plan.TopologyKey)
	}
	if got.Annotations["example.com/preserve"] != "yes" {
		t.Errorf("unrelated annotation was not preserved: got %#v", got.Annotations)
	}
}

func TestEnsurePodGroup_UnsetTopologyKeyRemovesOwnedAnnotationOnly(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 3 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	existing, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	existing.Annotations = map[string]string{
		query.AnnotationTopologyKey: "stale.example.com/domain",
		"example.com/preserve":      "yes",
	}
	c := newPodGroupClient(t, isvc, existing)

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(existing), got); err != nil {
		t.Fatalf("get PodGroup: %v", err)
	}
	if _, found := got.Annotations[query.AnnotationTopologyKey]; found {
		t.Errorf("unset topology key must remove owned annotation, got %#v", got.Annotations)
	}
	if got.Annotations["example.com/preserve"] != "yes" {
		t.Errorf("unrelated annotation was not preserved: got %#v", got.Annotations)
	}
}

func TestEnsurePodGroup_TopologyAnnotationDriftTriggersUpdate(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 3 * time.Minute,
		TopologyKey:          "network.example.com/fabric-domain",
	}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	existing, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	existing.Annotations[query.AnnotationTopologyKey] = "stale.example.com/domain"
	existing.Annotations["example.com/preserve"] = "yes"
	cc := newCountingClient(newPodGroupClient(t, isvc, existing))

	if err := EnsurePodGroup(context.Background(), cc, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}
	if cc.updates != 1 {
		t.Fatalf("topology-only drift update count: got %d want 1", cc.updates)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := cc.Get(context.Background(), client.ObjectKeyFromObject(existing), got); err != nil {
		t.Fatalf("get PodGroup: %v", err)
	}
	if got.Annotations[query.AnnotationTopologyKey] != plan.TopologyKey || got.Annotations["example.com/preserve"] != "yes" {
		t.Errorf("annotations after topology drift correction: got %#v", got.Annotations)
	}
}

// TestEnsurePodGroup_HealsLegacyComponentLabelKey — PodGroups stamped
// with the old "ome.io/component" key (which never matched the
// renderer's constants.OMEComponentLabel selector) must be rewritten to
// the canonical keys on the next ensure.
func TestEnsurePodGroup_HealsLegacyComponentLabelKey(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 3 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	legacy, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	legacy.Labels = map[string]string{
		constants.InferenceServicePodLabelKey: "llama",
		"ome.io/component":                    "engine",
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelInstanceIdx:                "0",
	}
	c := newPodGroupClient(t, isvc, legacy)

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(legacy), got); err != nil {
		t.Fatalf("get PodGroup: %v", err)
	}
	if got.Labels[constants.OMEComponentLabel] != "engine" {
		t.Errorf("component label not healed to %q: got %#v", constants.OMEComponentLabel, got.Labels)
	}
	if _, stale := got.Labels["ome.io/component"]; stale {
		t.Errorf("legacy ome.io/component key not removed: got %#v", got.Labels)
	}
}

// TestEnsurePodGroup_TransientGetErrorSurfaces — a non-NotFound Get
// failure must be returned as the read error, not fall through to the
// live-pod topology proof and masquerade as an unprovable-topology
// ("no PodGroup annotation") error.
func TestEnsurePodGroup_TransientGetErrorSurfaces(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	base := newPodGroupClient(t, isvc)
	boom := apierrors.NewServiceUnavailable("apiserver overloaded")
	failing := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isPG := obj.(*schedulingv1alpha1.PodGroup); isPG {
				return boom
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 3 * time.Minute,
		TopologyKey:          "network.example.com/fabric-domain",
	}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	// A live pod with no provable OME-generated topology: before the
	// early return this combination produced the misleading
	// "cannot prove active topology ... no PodGroup annotation" error.
	pods := []*corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelRunner: "worker"}},
	}}

	_, err := EnsurePodGroupForPods(context.Background(), failing, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst, pods)
	if err == nil {
		t.Fatal("expected error from failing Get")
	}
	if !apierrors.IsServiceUnavailable(err) {
		t.Errorf("expected the transient Get error to surface, got: %v", err)
	}
	if strings.Contains(err.Error(), "cannot prove active topology") {
		t.Errorf("Get error masked as a topology-proof error: %v", err)
	}
}

func TestReconcileDesiredTopologyWithLivePods_MissingGroupAmbiguousAffinityFailsClosed(t *testing.T) {
	desired := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{query.AnnotationTopologyKey: "topology.example.com/new"},
	}}
	pods := []*corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelRunner: "leader"}},
	}}

	err := reconcileDesiredTopologyWithLivePods(
		desired, &schedulingv1alpha1.PodGroup{}, false,
		"llama", workload.ComponentEngine, 0, pods)
	if err == nil {
		t.Fatal("ambiguous live topology with nonempty desired key must fail closed")
	}
	if got := desired.Annotations[query.AnnotationTopologyKey]; got != "topology.example.com/new" {
		t.Fatalf("failed reconciliation mutated desired topology: got %q", got)
	}
}

func TestReconcileDesiredTopologyWithLivePods_DoesNotPromoteUserAffinity(t *testing.T) {
	desired := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{query.AnnotationTopologyKey: "topology.example.com/desired"},
	}}
	pods := []*corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelRunner: "worker"}},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey: "kubernetes.io/hostname",
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					"app": "cache",
				}},
			}},
		}}},
	}}

	err := reconcileDesiredTopologyWithLivePods(
		desired, &schedulingv1alpha1.PodGroup{}, false,
		"llama", workload.ComponentEngine, 0, pods)
	if err == nil {
		t.Fatal("unrelated user affinity must not prove the gang topology")
	}
	if got := desired.Annotations[query.AnnotationTopologyKey]; got != "topology.example.com/desired" {
		t.Fatalf("user affinity replaced desired topology with %q", got)
	}
}

func TestGeneratedTopologyKeyFromPods_AcceptsOnlyOMELeaderSelector(t *testing.T) {
	const generatedKey = "topology.example.com/domain"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			query.LabelRunner:      "worker",
			query.LabelInstanceIdx: "0",
		}},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					TopologyKey: "kubernetes.io/hostname",
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						"app": "cache",
					}},
				},
				{
					TopologyKey: generatedKey,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						constants.InferenceServicePodLabelKey: "llama",
						constants.OMEComponentLabel:           string(workload.ComponentEngine),
						query.LabelInstanceIdx:                "0",
						query.LabelRunner:                     "leader",
					}},
				},
			},
		}}},
	}

	key, ok, err := GeneratedTopologyKeyFromPods("llama", workload.ComponentEngine, []*corev1.Pod{pod})
	if err != nil || !ok || key != generatedKey {
		t.Fatalf("generated topology: got %q ok=%v err=%v want %q", key, ok, err, generatedKey)
	}

	pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution =
		pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[:1]
	if key, ok, err := GeneratedTopologyKeyFromPods("llama", workload.ComponentEngine, []*corev1.Pod{pod}); err != nil || ok {
		t.Fatalf("user-only affinity result: key=%q ok=%v err=%v", key, ok, err)
	}
}

func TestGeneratedTopologyKeyFromPods_RejectsConflictingOMEKeys(t *testing.T) {
	makeWorker := func(index, key string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				query.LabelRunner:      "worker",
				query.LabelInstanceIdx: index,
			}},
			Spec: corev1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					TopologyKey: key,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						constants.InferenceServicePodLabelKey: "llama",
						constants.OMEComponentLabel:           string(workload.ComponentEngine),
						query.LabelInstanceIdx:                index,
						query.LabelRunner:                     "leader",
					}},
				}},
			}}},
		}
	}

	_, _, err := GeneratedTopologyKeyFromPods("llama", workload.ComponentEngine, []*corev1.Pod{
		makeWorker("0", "topology.example.com/a"),
		makeWorker("1", "topology.example.com/b"),
	})
	if err == nil {
		t.Fatal("conflicting generated topology keys must fail closed")
	}
}

// TestEnsurePodGroup_Idempotent — two ensures back-to-back leave
// MinMember stable and don't error.
func TestEnsurePodGroup_Idempotent(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 5 * time.Minute,
		TopologyKey:          "network.example.com/fabric-domain",
	}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 2}}}

	for i := 0; i < 2; i++ {
		if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
			t.Fatalf("EnsurePodGroup pass %d: %v", i, err)
		}
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get PodGroup: %v", err)
	}
	if got.Spec.MinMember != 3 {
		t.Errorf("MinMember after idempotent ensure: got %d want 3", got.Spec.MinMember)
	}
	if got.Annotations[query.AnnotationTopologyKey] != plan.TopologyKey {
		t.Errorf("topology annotation after idempotent ensure: got %#v want %q", got.Annotations, plan.TopologyKey)
	}
}

// TestEnsurePodGroup_FastPathReadsBeforeWriteAndSkipsNoDrift pins the
// steady-state fast path: EnsurePodGroup must do its own cache Get up
// front and, on the no-drift re-ensure, return after that Get without
// touching the write path (no Create/Update/Patch).
func TestEnsurePodGroup_FastPathReadsBeforeWriteAndSkipsNoDrift(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	cc := newCountingClient(newPodGroupClient(t, isvc))
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 5 * time.Minute,
		TopologyKey:          "network.example.com/fabric-domain",
	}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 2}}}

	// First ensure: object absent -> must be created. The fast path runs
	// its NotFound probe Get before the direct Create, so we expect one Get
	// and exactly one Create.
	cc.reset()
	if err := EnsurePodGroup(context.Background(), cc, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup (create): %v", err)
	}
	if cc.creates != 1 {
		t.Errorf("create ensure: Create count = %d, want 1", cc.creates)
	}
	if cc.gets != 1 {
		t.Errorf("create ensure: Get count = %d, want 1 fast-path probe", cc.gets)
	}

	// Add an unrelated annotation between passes. It is outside this
	// controller's field ownership, so it must neither trigger a write nor be
	// removed by the next ensure.
	seeded := &schedulingv1alpha1.PodGroup{}
	if err := cc.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, seeded); err != nil {
		t.Fatalf("get before adding unrelated annotation: %v", err)
	}
	seeded.Annotations["example.com/preserve"] = "yes"
	if err := cc.Update(context.Background(), seeded); err != nil {
		t.Fatalf("add unrelated annotation: %v", err)
	}

	// Second ensure: all owned fields match desired -> the fast path must
	// short-circuit after a single Get with NO write.
	cc.reset()
	if err := EnsurePodGroup(context.Background(), cc, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup (no-drift): %v", err)
	}
	if cc.creates != 0 || cc.updates != 0 || cc.patches != 0 {
		t.Errorf("no-drift ensure issued a write: creates=%d updates=%d patches=%d, want all 0",
			cc.creates, cc.updates, cc.patches)
	}
	if cc.gets != 1 {
		t.Errorf("no-drift ensure: Get count = %d, want 1 (fast-path probe only, no write)", cc.gets)
	}

	// Sanity: the PodGroup that survives the no-drift pass still carries
	// the reconciled shape.
	got := &schedulingv1alpha1.PodGroup{}
	if err := cc.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get after no-drift ensure: %v", err)
	}
	if got.Spec.MinMember != 3 {
		t.Errorf("MinMember after no-drift ensure: got %d want 3", got.Spec.MinMember)
	}
	if got.Annotations[query.AnnotationTopologyKey] != plan.TopologyKey || got.Annotations["example.com/preserve"] != "yes" {
		t.Errorf("annotations after no-drift ensure: got %#v", got.Annotations)
	}
}

// TestEnsurePodGroup_DriftStillWrites is the
// counterpart to the fast-path skip: when the cached object drifts from
// desired, podGroupMatches must return false and EnsurePodGroup must
// fall through to the update path and issue exactly one write. Pins that
// the fast path never swallows a real drift.
func TestEnsurePodGroup_DriftStillWrites(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	stale := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-engine-0",
			Namespace: "prod",
			Labels:    map[string]string{"stale": "true"},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:              99, // drift vs. desired (3)
			ScheduleTimeoutSeconds: int32Ptr(9999),
		},
	}
	cc := newCountingClient(newPodGroupClient(t, isvc, stale))
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 2}}}

	cc.reset()
	if err := EnsurePodGroup(context.Background(), cc, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup (drift): %v", err)
	}
	if cc.updates+cc.patches+cc.creates == 0 {
		t.Errorf("drift ensure issued no write (creates=%d updates=%d patches=%d): fast path swallowed a real drift",
			cc.creates, cc.updates, cc.patches)
	}
}

// ---------------------------------------------------------------------
// DeletePodGroup — explicit removal path (scale-down / migration).
// ---------------------------------------------------------------------

func TestDeletePodGroup_Removes(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-0", Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	c := newPodGroupClient(t, isvc, existing)

	if err := DeletePodGroup(context.Background(), c, isvc, isvc.GetName(), workload.ComponentEngine, 0); err != nil {
		t.Fatalf("DeletePodGroup: %v", err)
	}
	got := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound after delete, got err=%v pg=%+v", err, got)
	}
}

func TestDeletePodGroup_AbsentIsNoOp(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	if err := DeletePodGroup(context.Background(), c, isvc, isvc.GetName(), workload.ComponentEngine, 7); err != nil {
		t.Errorf("DeletePodGroup on absent PodGroup should be no-op, got %v", err)
	}
}

func TestDeleteObservedPodGroup_ForeignObjectIsNeverDeleted(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	foreign := newPodGroupISVC("prod", "foreign")
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	pg, err := BuildPodGroup(foreign, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	pg.UID = "foreign-pg-uid"
	c := newPodGroupClient(t, isvc, pg)

	err = DeleteObservedPodGroup(context.Background(), c, isvc.UID, pg)
	if !errors.Is(err, ErrPodGroupOwnershipConflict) {
		t.Fatalf("foreign delete error: got %v want ErrPodGroupOwnershipConflict", err)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pg), got); err != nil {
		t.Fatalf("foreign PodGroup was deleted: %v", err)
	}
}

func TestDeleteObservedPodGroup_TerminatingIsAcceptedWithoutDelete(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	now := metav1.Now()
	pg.DeletionTimestamp = &now
	c := newPodGroupClient(t, isvc)
	if err := DeleteObservedPodGroup(context.Background(), c, isvc.UID, pg); err != nil {
		t.Fatalf("terminating PodGroup must count as accepted deletion: %v", err)
	}
	complete, err := FinalizeObservedPodGroup(context.Background(), c, isvc.UID, pg)
	if err != nil || complete {
		t.Fatalf("terminating PodGroup finalization: complete=%v err=%v", complete, err)
	}
}

func TestDeleteObservedPodGroup_RuntimeAPIRemovalIsAccepted(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	base := newPodGroupClient(t, isvc, pg)
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return &apimeta.NoResourceMatchError{PartialResource: schema.GroupVersionResource{
				Group: "scheduling.x-k8s.io", Version: "v1alpha1", Resource: "podgroups",
			}}
		},
	})
	if err := DeleteObservedPodGroup(context.Background(), c, isvc.UID, pg); err != nil {
		t.Fatalf("runtime PodGroup API removal must be treated as absence: %v", err)
	}
	complete, err := FinalizeObservedPodGroup(context.Background(), c, isvc.UID, pg)
	if err != nil || !complete {
		t.Fatalf("runtime API removal finalization: complete=%v err=%v", complete, err)
	}
}

func TestDeleteObservedPodGroup_UIDPreconditionProtectsNameReuse(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	observed, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	observed.UID = "old-pg-uid"
	replacement := observed.DeepCopy()
	replacement.UID = "replacement-pg-uid"
	base := newPodGroupClient(t, isvc, replacement)
	sawUIDPrecondition := false
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.DeleteOption) error {
			for _, opt := range opts {
				preconditions, ok := opt.(client.Preconditions)
				if ok && preconditions.UID != nil && *preconditions.UID == observed.UID {
					sawUIDPrecondition = true
				}
			}
			return errors.New("simulated UID precondition conflict")
		},
	})

	complete, err := FinalizeObservedPodGroup(context.Background(), c, isvc.UID, observed)
	if err == nil || complete {
		t.Fatal("stale inventory delete unexpectedly passed the UID precondition")
	}
	if !sawUIDPrecondition {
		t.Fatal("delete did not carry the observed PodGroup UID precondition")
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(replacement), got); err != nil {
		t.Fatalf("same-named replacement was deleted: %v", err)
	}
	if got.UID != replacement.UID {
		t.Fatalf("replacement UID changed: got %q want %q", got.UID, replacement.UID)
	}
}

// ---------------------------------------------------------------------
// Helpers — local fixtures (intentionally not shared with the parent
// package; podgroup is leaf-level and shouldn't import test_helpers).
// ---------------------------------------------------------------------

// testPodGroupOwnerGVK is the GroupVersionKind every test passes
// alongside the ISVC fixture so the OwnerReference matches what the
// ISVC adapter populates at the dispatch site. Mirrors the gang
// reconciler's wiring of internalsource.ISVCGVK() without taking on
// that import here.
var testPodGroupOwnerGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

func newPodGroupISVC(ns, name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
	}
}

// countingClient wraps a client.Client (via an interceptor) and tallies
// the Get / Create / Update / Patch calls EnsurePodGroup makes, so a
// test can prove the steady-state fast path reads before writing and
// skips the write path entirely on no drift. reset() zeroes the
// counters between phases of a single test.
type countingClient struct {
	client.Client
	gets, creates, updates, patches int
}

func newCountingClient(base client.Client) *countingClient {
	cc := &countingClient{}
	cc.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			cc.gets++
			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			cc.creates++
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			cc.updates++
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			cc.patches++
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	return cc
}

func (cc *countingClient) reset() { cc.gets, cc.creates, cc.updates, cc.patches = 0, 0, 0, 0 }

func newPodGroupClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	var owner client.Object
	for _, obj := range objs {
		if _, isPodGroup := obj.(*schedulingv1alpha1.PodGroup); !isPodGroup {
			owner = obj
			break
		}
	}
	if owner != nil {
		for _, obj := range objs {
			pg, ok := obj.(*schedulingv1alpha1.PodGroup)
			if !ok || metav1.GetControllerOfNoCopy(pg) != nil {
				continue
			}
			pg.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(owner, testPodGroupOwnerGVK)}
		}
	}
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1 to scheme: %v", err)
	}
	if err := schedulingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheduling/v1alpha1 to scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func int32Ptr(n int32) *int32 { return &n }
