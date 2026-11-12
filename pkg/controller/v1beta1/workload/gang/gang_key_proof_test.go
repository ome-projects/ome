package gang

import (
	"context"
	"flag"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateGangKeyProofGoldens = flag.Bool("update-gang-key-proof", false, "re-record testdata/gang_key_proof_allocations.json from this run")

// keyProofPass is a pass without the PodGroup API over the cache's pods,
// at the point where it decides the gangs: the Component's pods grouped
// under the PodGroup name each carries, and every gang's write built over
// them, so what runs after it is the proof of each gang's key alone.
func (f *podGroupPassFixture) keyProofPass(tb testing.TB) *podGroupPass {
	tb.Helper()
	pass := newPodGroupPass(f.deps, &f.input, f.plan, f.state.Inventory)
	pods, err := pass.listPodsCached(context.Background())
	if err != nil {
		tb.Fatalf("listPodsCached: %v", err)
	}
	pass.groupPods(pods)
	for _, inst := range pass.gangs {
		pass.writeOf(inst)
	}
	return pass
}

// keyProofReading is one of the three readings a pass without the PodGroup
// API makes of a gang's cached pods, run over every gang of the Component.
type keyProofReading struct {
	name string
	run  func()
}

// keyProofReadings are, in the order the pass makes them: the proof of the
// key the gang's pods carry, the decision that the cached pods settle the
// gang (gangDecidedOnCache), and the key the pass publishes for it
// (topologyWithoutGroups). Each is held to the Component's key at rest.
func keyProofReadings(tb testing.TB, pass *podGroupPass) []keyProofReading {
	tb.Helper()
	ctx := context.Background()
	return []keyProofReading{
		{name: "proof", run: func() {
			for _, inst := range pass.gangs {
				key, proven, err := podgroup.TopologyKeyProvenByPods(pass.input.Key.OwnerName, pass.plan.Component, inst.Index, pass.writeOf(inst).pods)
				if err != nil || !proven || key != podGroupPassTopologyKey {
					tb.Fatalf("gang %d: proof key=%q proven=%t err=%v, want the pods' %q", inst.Index, key, proven, err, podGroupPassTopologyKey)
				}
			}
		}},
		{name: "decision", run: func() {
			for _, inst := range pass.gangs {
				if !pass.gangDecidedOnCache(ctx, inst, false) {
					tb.Fatalf("gang %d at rest is not settled by its cached pods", inst.Index)
				}
			}
		}},
		{name: "published-keys", run: func() {
			topology, err := pass.topologyWithoutGroups()
			if err != nil {
				tb.Fatalf("topologyWithoutGroups: %v", err)
			}
			for _, inst := range pass.gangs {
				if key := topology[inst.Index]; key != podGroupPassTopologyKey {
					tb.Fatalf("gang %d: published %q, want the pods' %q", inst.Index, key, podGroupPassTopologyKey)
				}
			}
		}},
	}
}

// TestGangKeyProofAtRestStaysWithinBudget holds the cost of proving every
// gang's key at rest without the PodGroup API to its golden: per shape, the
// allocations and the bytes of the proof over every gang's cached pods, of
// the decision that those pods settle the gang, and of the keys the pass
// publishes; beside the golden, every gang on its pods' key and no write.
func TestGangKeyProofAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "gang_key_proof_allocations.json"), *updateGangKeyProofGoldens)
	defer golden.Close()
	for _, shape := range podGroupPassShapes() {
		f := newPodGroupPassFixture(t, shape, false)
		pass := f.keyProofPass(t)
		for _, reading := range keyProofReadings(t, pass) {
			golden.Check(shape.name+"/"+reading.name+"/allocations", reading.run)
			golden.CheckBytes(shape.name+"/"+reading.name+"/bytes", reading.run)
		}
		if n := f.writes.Count(); n != 0 {
			t.Fatalf("%s: the proofs wrote %d times; want none", shape.name, n)
		}
	}
}

// cachedWorker is the gang's worker of one ordinal as the cache holds it,
// for a case to change.
func (f *podGroupPassFixture) cachedWorker(tb testing.TB, index int32, ordinal int) *corev1.Pod {
	tb.Helper()
	name := query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, index)
	for i := range f.cached.pods {
		pod := &f.cached.pods[i]
		if pod.Labels[query.LabelPodGroup] == name && pod.Labels[query.LabelRunner] == workloadtypes.RunnerWorker &&
			pod.Labels[query.LabelPodOrdinal] == strconv.Itoa(ordinal) {
			return pod
		}
	}
	tb.Fatalf("gang %d has no worker %d in the cache", index, ordinal)
	return nil
}

// removeCachedWorker takes one worker of the gang out of the cache.
func (f *podGroupPassFixture) removeCachedWorker(tb testing.TB, index int32, ordinal int) {
	tb.Helper()
	gone := f.cachedWorker(tb, index, ordinal).Name
	for i := range f.cached.pods {
		if f.cached.pods[i].Name == gone {
			f.cached.pods = append(f.cached.pods[:i], f.cached.pods[i+1:]...)
			return
		}
	}
}

// keyProofCase is one picture of a gang's cached pods without the PodGroup
// API and what the pass decides on it: whether the cached pods settle the
// gang, so the apiserver is not read, and the key the pass publishes for
// the gang from those pods, or the error that holds its members.
type keyProofCase struct {
	name    string
	change  func(t *testing.T, f *podGroupPassFixture)
	settled bool
	key     string
	wantErr string
}

// keyProofCases are the pictures. Without the API nothing but the pods
// records a gang's key: the cached pods settle the gang when they prove the
// plan's key, or when the plan asks for none and every pod is among them
// proving none; a gang short of the workers that would prove its key, one
// whose pods prove a key the plan has left, and workers on two keys are
// read from the apiserver.
func keyProofCases() []keyProofCase {
	const otherKey = "topology.example.com/zone"
	return []keyProofCase{
		{name: "every pod cached, the workers following the leader on the plan's key", change: func(*testing.T, *podGroupPassFixture) {},
			settled: true, key: podGroupPassTopologyKey},
		{name: "a worker missing from the cache, the other following the leader on the plan's key", change: func(t *testing.T, f *podGroupPassFixture) {
			f.removeCachedWorker(t, decisionGang, 1)
		}, settled: true, key: podGroupPassTopologyKey},
		{name: "the same worker twice in the cache", change: func(t *testing.T, f *podGroupPassFixture) {
			f.cached.pods = append(f.cached.pods, *f.cachedWorker(t, decisionGang, 0))
		}, settled: true, key: podGroupPassTopologyKey},
		{name: "a worker with no node yet", change: func(t *testing.T, f *podGroupPassFixture) {
			f.cachedWorker(t, decisionGang, 0).Spec.NodeName = ""
		}, settled: true, key: podGroupPassTopologyKey},
		{name: "every pod cached under no key, no worker following the leader", change: func(t *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = ""
			for ordinal := range 2 {
				f.cachedWorker(t, decisionGang, ordinal).Spec.Affinity = nil
			}
		}, settled: true, key: ""},
		{name: "the workers following the leader on a key the plan has left", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = otherKey
		}, settled: false, key: podGroupPassTopologyKey},
		{name: "every pod cached under no key, the workers following the leader on a key", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = ""
		}, settled: false, key: podGroupPassTopologyKey},
		{name: "a leader alone under the plan's key", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.removeGangPods(decisionGang, true)
		}, settled: false, wantErr: "cannot prove active topology"},
		{name: "a leader alone under no key", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = ""
			f.removeGangPods(decisionGang, true)
		}, settled: false, key: ""},
		{name: "an empty gang under the plan's key", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.removeGangPods(decisionGang, false)
		}, settled: false, key: podGroupPassTopologyKey},
		{name: "an empty gang under no key", change: func(_ *testing.T, f *podGroupPassFixture) {
			f.plan.TopologyKey = ""
			f.removeGangPods(decisionGang, false)
		}, settled: false, key: ""},
		{name: "the first worker on another key", change: func(t *testing.T, f *podGroupPassFixture) {
			f.setWorkerTermKey(t, decisionGang, 0, otherKey)
		}, settled: false, wantErr: "conflicting live topology keys"},
		{name: "the last worker on another key", change: func(t *testing.T, f *podGroupPassFixture) {
			f.setWorkerTermKey(t, decisionGang, 1, otherKey)
		}, settled: false, wantErr: "conflicting live topology keys"},
		{name: "a worker whose term follows another Instance's leader", change: func(t *testing.T, f *podGroupPassFixture) {
			term := &f.cachedWorker(t, decisionGang, 1).Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
			term.LabelSelector.MatchLabels[query.LabelInstanceIdx] = "7"
		}, settled: true, key: podGroupPassTopologyKey},
		{name: "both workers' terms following another Instance's leader", change: func(t *testing.T, f *podGroupPassFixture) {
			for ordinal := range 2 {
				term := &f.cachedWorker(t, decisionGang, ordinal).Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
				term.LabelSelector.MatchLabels[query.LabelInstanceIdx] = "7"
			}
		}, settled: false, wantErr: "cannot prove active topology"},
	}
}

// TestGangDecisionWithoutTheAPI_OnTheCachesPicture holds, for each picture
// of a gang's cached pods without the PodGroup API, whether the pass
// settles the gang on them and the key it publishes from them.
func TestGangDecisionWithoutTheAPI_OnTheCachesPicture(t *testing.T) {
	for _, tc := range keyProofCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newPodGroupPassFixture(t, decisionShape, false)
			tc.change(t, f)
			pass := f.keyProofPass(t)
			var gang *workloadtypes.InstancePlan
			for _, inst := range pass.gangs {
				if inst.Index == decisionGang {
					gang = inst
				}
			}
			if gang == nil {
				t.Fatalf("gang %d is not planned", decisionGang)
			}
			if settled := pass.gangDecidedOnCache(context.Background(), gang, false); settled != tc.settled {
				t.Fatalf("settled on the cached pods: %t, want %t", settled, tc.settled)
			}
			key, err := pass.writeOf(gang).topologyFromPods()
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("published key %q, %v; want an error saying %q", key, err, tc.wantErr)
				}
			case err != nil:
				t.Fatalf("published key: %v, want none", err)
			case key != tc.key:
				t.Fatalf("published key %q, want %q", key, tc.key)
			}
			if n := f.writes.Count(); n != 0 {
				t.Fatalf("the decision wrote %d times; want none", n)
			}
		})
	}
}
