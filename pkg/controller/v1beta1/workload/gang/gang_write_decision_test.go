package gang

import (
	"context"
	"errors"
	"flag"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateGangWriteDecisionGoldens = flag.Bool("update-gang-write-decision", false, "re-record testdata/gang_write_decision_allocations.json from this run")

// decisionPass is a pass over the cache's picture up to the point where it
// decides the gangs: the owner's groups read from the cache, the
// Component's pods grouped under the PodGroup name each carries, and no
// name read yet.
func (f *podGroupPassFixture) decisionPass(tb testing.TB) *podGroupPass {
	tb.Helper()
	ctx := context.Background()
	pass := newPodGroupPass(f.deps, &f.input, f.plan, f.state.Inventory)
	if _, err := pass.readInventory(ctx); err != nil {
		tb.Fatalf("readInventory: %v", err)
	}
	pods, err := pass.listPodsCached(ctx)
	if err != nil {
		tb.Fatalf("listPodsCached: %v", err)
	}
	pass.groupPods(pods)
	return pass
}

// undecidedWrites are the gangs' writes with their names read and nothing
// decided: what gangDecidedOnCache holds the moment before it asks whether
// a write is needed.
func (p *podGroupPass) undecidedWrites(tb testing.TB) []*gangWrite {
	tb.Helper()
	writes := make([]*gangWrite, 0, len(p.gangs))
	for _, inst := range p.gangs {
		write := p.writeOf(inst)
		if err := write.readName(context.Background()); err != nil {
			tb.Fatalf("readName: %v", err)
		}
		write.observe()
		writes = append(writes, write)
	}
	return writes
}

// gangDecision is the pass's decision for one gang on the cache's picture:
// whether its group needs a write and the key the pass would publish, or
// the error the decision ends in.
type gangDecision struct {
	needed bool
	key    string
	err    error
}

// decideGang is the decision for one gang, as gangDecidedOnCache makes it
// over the cache's groups and pods.
func (f *podGroupPassFixture) decideGang(tb testing.TB, index int32) gangDecision {
	tb.Helper()
	pass := f.decisionPass(tb)
	for _, inst := range pass.gangs {
		if inst.Index != index {
			continue
		}
		write := pass.writeOf(inst)
		if err := write.readName(context.Background()); err != nil {
			tb.Fatalf("readName: %v", err)
		}
		write.observe()
		needed, key, err := write.needed()
		return gangDecision{needed: needed, key: key, err: err}
	}
	tb.Fatalf("gang %d is not planned", index)
	return gangDecision{}
}

// gangGroup is the cached group of one gang, for a case to change.
func (f *podGroupPassFixture) gangGroup(tb testing.TB, index int32) *schedulingv1alpha1.PodGroup {
	tb.Helper()
	name := query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, index)
	for i := range f.cached.groups {
		if f.cached.groups[i].Name == name {
			return &f.cached.groups[i]
		}
	}
	tb.Fatalf("the cache holds no group %s", name)
	return nil
}

// changeLiveGroup changes the gang's group as the apiserver holds it, and
// makes the pass decide on the apiserver's inventory: the cache's index
// shows only this owner's groups, so a group that is not this owner's is
// met on the live read alone.
func (f *podGroupPassFixture) changeLiveGroup(tb testing.TB, index int32, mutate func(group *schedulingv1alpha1.PodGroup)) {
	tb.Helper()
	ctx := context.Background()
	name := query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, index)
	group := &schedulingv1alpha1.PodGroup{}
	if err := f.live.Get(ctx, client.ObjectKey{Namespace: f.input.Key.Namespace, Name: name}, group); err != nil {
		tb.Fatalf("get %s: %v", name, err)
	}
	mutate(group)
	if err := f.live.Update(ctx, group); err != nil {
		tb.Fatalf("update %s: %v", name, err)
	}
	inventory, err := ObservePodGroups(ctx, f.live, f.input.OwnerObject)
	if err != nil {
		tb.Fatalf("ObservePodGroups: %v", err)
	}
	f.state.Inventory = inventory
}

// removeGangGroup frees the gang's name in the cache.
func (f *podGroupPassFixture) removeGangGroup(index int32) {
	name := query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, index)
	f.cached.groups = slices.DeleteFunc(f.cached.groups, func(group schedulingv1alpha1.PodGroup) bool {
		return group.Name == name
	})
}

// removeGangPods takes the gang's pods out of the cache: the workers
// alone, or every pod of the gang.
func (f *podGroupPassFixture) removeGangPods(index int32, workersOnly bool) {
	name := query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, index)
	f.cached.pods = slices.DeleteFunc(f.cached.pods, func(pod corev1.Pod) bool {
		return pod.Labels[query.LabelPodGroup] == name &&
			(!workersOnly || pod.Labels[query.LabelRunner] == workloadtypes.RunnerWorker)
	})
}

// setWorkerTermKey puts the gang's worker of one ordinal on another
// topology key, the generated term still naming the gang's leader.
func (f *podGroupPassFixture) setWorkerTermKey(tb testing.TB, index int32, ordinal int, key string) {
	tb.Helper()
	name := query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, index)
	for i := range f.cached.pods {
		pod := &f.cached.pods[i]
		if pod.Labels[query.LabelPodGroup] != name || pod.Labels[query.LabelRunner] != workloadtypes.RunnerWorker ||
			pod.Labels[query.LabelPodOrdinal] != strconv.Itoa(ordinal) {
			continue
		}
		pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey = key
		return
	}
	tb.Fatalf("gang %d has no worker %d in the cache", index, ordinal)
}

func int32Ptr(v int32) *int32 { return &v }

func boolPtr(v bool) *bool { return &v }

// decisionShape is the Component the decision table runs over: two gangs
// of a leader and two workers, the second the one every case changes.
var decisionShape = podGroupPassShape{name: "2x3", gangs: 2, workers: 2}

const decisionGang int32 = 1

// writeDecisionCase is one change to the cache's picture of a gang, and
// the decision the pass makes on it.
type writeDecisionCase struct {
	name   string
	change func(t *testing.T, f *podGroupPassFixture)
	want   gangDecision
	// wantErr is the error value or the message the decision's error must
	// carry; nil and "" for a decision without one.
	wantErr     error
	wantMessage string
}

// settledDecisions are the pictures a gang at rest may present, every one
// of which the pass leaves alone: nothing to write, and the key the group
// records is the key published.
func settledDecisions() []writeDecisionCase {
	settled := gangDecision{needed: false, key: podGroupPassTopologyKey}
	return []writeDecisionCase{
		{name: "a group at rest", change: func(*testing.T, *podGroupPassFixture) {}, want: settled},
		{name: "a group at rest with the placement grace bounded", change: func(t *testing.T, f *podGroupPassFixture) {
			f.input.UnschedulableGrace = 5 * time.Minute
			f.gangGroup(t, decisionGang).Spec.ScheduleTimeoutSeconds = int32Ptr(300)
		}, want: settled},
		{name: "a leader-only gang whose group records a key", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.removeGangPods(decisionGang, true)
		}, want: settled},
		{name: "an empty gang whose group records the plan's key", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.removeGangPods(decisionGang, false)
		}, want: settled},
		{name: "a leader-only gang under no key whose group records none", change: func(t *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = ""
			f.removeGangPods(decisionGang, true)
			f.gangGroup(t, decisionGang).Annotations = nil
		}, want: gangDecision{needed: false, key: ""}},
		{name: "a group carrying an annotation that is not the pass's", change: func(t *testing.T, f *podGroupPassFixture) {
			f.gangGroup(t, decisionGang).Annotations["example.com/note"] = "kept"
		}, want: settled},
		{name: "a group the scheduler has admitted", change: func(t *testing.T, f *podGroupPassFixture) {
			f.gangGroup(t, decisionGang).Status.Phase = schedulingv1alpha1.PodGroupRunning
		}, want: settled},
		{name: "a gang under a key change with its pods still up", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = "topology.example.com/zone"
		}, want: settled},
	}
}

// writtenDecisions are the pictures in which one owned field of the group
// is not the plan's, or the name is free: the pass writes, and publishes
// the key the written group carries, the pods' key while they stand.
func writtenDecisions() []writeDecisionCase {
	written := gangDecision{needed: true, key: podGroupPassTopologyKey}
	group := func(mutate func(group *schedulingv1alpha1.PodGroup)) func(t *testing.T, f *podGroupPassFixture) {
		return func(t *testing.T, f *podGroupPassFixture) { mutate(f.gangGroup(t, decisionGang)) }
	}
	return []writeDecisionCase{
		{name: "a label with another value", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.Labels[query.LabelManagedBy] = "someone-else"
		}), want: written},
		{name: "a label missing", change: group(func(g *schedulingv1alpha1.PodGroup) {
			delete(g.Labels, query.LabelManagedBy)
		}), want: written},
		{name: "a label the pass does not write", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.Labels["example.com/extra"] = "x"
		}), want: written},
		{name: "the index label spelled with a leading zero", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.Labels[query.LabelInstanceIdx] = "01"
		}), want: written},
		{name: "the owner reference naming another object of this owner's UID", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences[0].Name = "renamed"
		}), want: written},
		{name: "the owner reference with another kind", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences[0].Kind = "Other"
		}), want: written},
		{name: "the owner reference with another API version", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences[0].APIVersion = "ome.io/v1"
		}), want: written},
		{name: "the owner reference without the block-owner-deletion flag", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences[0].BlockOwnerDeletion = nil
		}), want: written},
		{name: "the owner reference with the block-owner-deletion flag false", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences[0].BlockOwnerDeletion = boolPtr(false)
		}), want: written},
		{name: "a second owner reference", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences = append(g.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "x", UID: "x-uid"})
		}), want: written},
		{name: "a member count off by one", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.Spec.MinMember++
		}), want: written},
		{name: "a timeout while the grace is zero", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.Spec.ScheduleTimeoutSeconds = int32Ptr(300)
		}), want: written},
		{name: "a timeout that is not the bounded grace", change: func(t *testing.T, f *podGroupPassFixture) {
			f.input.UnschedulableGrace = 5 * time.Minute
			f.gangGroup(t, decisionGang).Spec.ScheduleTimeoutSeconds = int32Ptr(299)
		}, want: written},
		{name: "no timeout while the grace is bounded", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.input.UnschedulableGrace = 5 * time.Minute
		}, want: written},
		{name: "no key recorded while the pods prove one", change: group(func(g *schedulingv1alpha1.PodGroup) {
			delete(g.Annotations, query.AnnotationTopologyKey)
		}), want: written},
		{name: "another key recorded than the pods prove", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.Annotations[query.AnnotationTopologyKey] = "topology.example.com/zone"
		}), want: written},
		{name: "an empty key recorded while the pods prove one", change: group(func(g *schedulingv1alpha1.PodGroup) {
			g.Annotations[query.AnnotationTopologyKey] = ""
		}), want: written},
		{name: "a leader-only gang under no key whose group records an empty key", change: func(t *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = ""
			f.removeGangPods(decisionGang, true)
			f.gangGroup(t, decisionGang).Annotations[query.AnnotationTopologyKey] = ""
		}, want: gangDecision{needed: true, key: ""}},
		{name: "an empty gang whose group records another key than the plan's", change: func(t *testing.T, f *podGroupPassFixture) {
			f.removeGangPods(decisionGang, false)
			f.gangGroup(t, decisionGang).Annotations[query.AnnotationTopologyKey] = "topology.example.com/zone"
		}, want: written},
		{name: "an empty gang under no key whose group records one", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = ""
			f.removeGangPods(decisionGang, false)
		}, want: gangDecision{needed: true, key: ""}},
		{name: "a free name", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.removeGangGroup(decisionGang)
		}, want: written},
		{name: "another owner's group, which the cache's index does not show", change: func(t *testing.T, f *podGroupPassFixture) {
			f.gangGroup(t, decisionGang).OwnerReferences[0].UID = "someone-else-uid"
		}, want: written},
	}
}

// refusedDecisions are the pictures the pass cannot decide a write on: a
// name this owner cannot write, and pods whose topology no group can
// describe.
func refusedDecisions() []writeDecisionCase {
	live := func(mutate func(group *schedulingv1alpha1.PodGroup)) func(t *testing.T, f *podGroupPassFixture) {
		return func(t *testing.T, f *podGroupPassFixture) { f.changeLiveGroup(t, decisionGang, mutate) }
	}
	return []writeDecisionCase{
		{name: "a terminating group", change: func(t *testing.T, f *podGroupPassFixture) {
			now := metav1.Now()
			group := f.gangGroup(t, decisionGang)
			group.DeletionTimestamp = &now
			group.Finalizers = []string{"example.com/hold"}
		}, wantErr: podgroup.ErrPodGroupTerminating},
		{name: "another owner's group on the live read", change: live(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences[0].UID = "someone-else-uid"
		}), wantErr: podgroup.ErrPodGroupOwnershipConflict},
		{name: "a group with no owner reference on the live read", change: live(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences = nil
		}), wantErr: podgroup.ErrPodGroupOwnershipConflict},
		{name: "a group whose owner reference is not the controller on the live read", change: live(func(g *schedulingv1alpha1.PodGroup) {
			g.OwnerReferences[0].Controller = boolPtr(false)
		}), wantErr: podgroup.ErrPodGroupOwnershipConflict},
		{name: "the first worker on another key", change: func(t *testing.T, f *podGroupPassFixture) {
			f.setWorkerTermKey(t, decisionGang, 0, "topology.example.com/zone")
		}, wantMessage: "conflicting live topology keys"},
		{name: "the last worker on another key", change: func(t *testing.T, f *podGroupPassFixture) {
			f.setWorkerTermKey(t, decisionGang, 1, "topology.example.com/zone")
		}, wantMessage: "conflicting live topology keys"},
		{name: "a leader-only gang under a key whose group records none", change: func(t *testing.T, f *podGroupPassFixture) {
			f.removeGangPods(decisionGang, true)
			f.gangGroup(t, decisionGang).Annotations = nil
		}, wantMessage: "cannot prove active topology"},
	}
}

// TestGangWriteDecision_OnTheCachesPicture holds the decision the pass
// makes for one gang on the cache's groups and pods: a group at rest,
// whatever else it carries, needs no write and keeps its key; a group with
// one owned field off the plan's value, or a free name, is written, with
// the pods' key while they stand; a name this owner cannot write and pods
// on two keys refuse the decision with the error the build reports.
func TestGangWriteDecision_OnTheCachesPicture(t *testing.T) {
	var cases []writeDecisionCase
	cases = append(cases, settledDecisions()...)
	cases = append(cases, writtenDecisions()...)
	cases = append(cases, refusedDecisions()...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPodGroupPassFixture(t, decisionShape, true)
			tc.change(t, f)
			got := f.decideGang(t, decisionGang)
			switch {
			case tc.wantErr != nil:
				if !errors.Is(got.err, tc.wantErr) {
					t.Fatalf("decision error %v, want %v", got.err, tc.wantErr)
				}
			case tc.wantMessage != "":
				if got.err == nil || !strings.Contains(got.err.Error(), tc.wantMessage) {
					t.Fatalf("decision error %v, want one saying %q", got.err, tc.wantMessage)
				}
			default:
				if got.err != nil {
					t.Fatalf("decision error %v, want none", got.err)
				}
				if got.needed != tc.want.needed || got.key != tc.want.key {
					t.Fatalf("decision needed=%t key=%q, want needed=%t key=%q", got.needed, got.key, tc.want.needed, tc.want.key)
				}
			}
			if n := f.writes.Count(); n != 0 {
				t.Fatalf("the decision wrote %d times; want none", n)
			}
		})
	}
}

// TestGangWrite_AtRestWritesNothingAndKeepsTheGroup: the write of a gang at
// rest sends nothing, publishes the key the group records, and leaves the
// inventory's entry for the name as it was.
func TestGangWrite_AtRestWritesNothingAndKeepsTheGroup(t *testing.T) {
	f := newPodGroupPassFixture(t, decisionShape, true)
	pass := f.decisionPass(t)
	for _, write := range pass.undecidedWrites(t) {
		before, found := pass.inventory.ByName(write.name)
		if !found {
			t.Fatalf("gang %d: the inventory holds no %s", write.inst.Index, write.name)
		}
		key, err := write.write(context.Background())
		if err != nil {
			t.Fatalf("gang %d: write: %v", write.inst.Index, err)
		}
		if key != podGroupPassTopologyKey {
			t.Fatalf("gang %d: published %q, want the group's %q", write.inst.Index, key, podGroupPassTopologyKey)
		}
		after, found := pass.inventory.ByName(write.name)
		if !found || after.UID != before.UID || !equality(after, before) {
			t.Fatalf("gang %d: the inventory's entry changed under a write at rest", write.inst.Index)
		}
	}
	if n := f.writes.Count(); n != 0 {
		t.Fatalf("writes at rest sent %d times; want none", n)
	}
}

// equality reads two groups as the apiserver's semantic compare does.
func equality(a, b *schedulingv1alpha1.PodGroup) bool {
	return a.Name == b.Name && a.Namespace == b.Namespace &&
		podgroup.WriteNeeded(a, b, true) == false && podgroup.TopologyKeyOf(a) == podgroup.TopologyKeyOf(b)
}

// TestGangWriteDecisionAtRestStaysWithinBudget holds the cost of deciding
// a gang at rest to its golden: the allocations of asking every gang of a
// Component at rest whether its group needs a write, on the cache's
// groups and pods, and of the write that follows and sends nothing.
func TestGangWriteDecisionAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "gang_write_decision_allocations.json"), *updateGangWriteDecisionGoldens)
	defer golden.Close()
	for _, shape := range podGroupPassShapes() {
		f := newPodGroupPassFixture(t, shape, true)
		writes := f.decisionPass(t).undecidedWrites(t)
		golden.Check(shape.name+"/decision/allocations", func() {
			for _, write := range writes {
				undecided := *write
				if _, _, err := undecided.needed(); err != nil {
					t.Fatalf("needed: %v", err)
				}
			}
		})
		for _, write := range writes {
			needed, key, err := write.needed()
			if err != nil || needed || key != podGroupPassTopologyKey {
				t.Fatalf("gang %d at rest: needed=%t key=%q err=%v; want no write on %q", write.inst.Index, needed, key, err, podGroupPassTopologyKey)
			}
		}
		golden.Check(shape.name+"/write-at-rest/allocations", func() {
			for _, write := range writes {
				decided := *write
				if _, err := decided.write(context.Background()); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
		})
		if n := f.writes.Count(); n != 0 {
			t.Fatalf("%s: writes at rest sent %d times; want none", shape.name, n)
		}
	}
}
