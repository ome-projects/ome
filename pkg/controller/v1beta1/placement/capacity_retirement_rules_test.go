package placement

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// retirementFixture is a member whose capacity minimum retires: the capacity
// provider fixture's first member, created on its worker with an engine
// minimum of two, under a source whose plan allocates it zero replicas while
// one still runs.
type retirementFixture struct {
	r      *Reconciler
	worker client.WithWatch
	source *v1beta1.InferenceService
	member *v1beta1.InferenceService
}

func newRetirementFixture(t *testing.T) *retirementFixture {
	t.Helper()
	f := newCapacityProviderFixture(t)
	samples, err := f.r.readSplitCapacity(t.Context(), f.source, f.clusters, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sample, err := recordedCapacitySample(samples["member-a"])
	if err != nil {
		t.Fatal(err)
	}
	member, err := f.r.derivedFor(f.source)
	if err != nil {
		t.Fatal(err)
	}
	setPlannedReplicas(member, map[v1beta1.ComponentType]plannedBound{v1beta1.EngineComponent: {Floor: 2}})
	member.Annotations["example.com/owner"] = "team-a"
	worker := f.workers["member-a"]
	if err := worker.Create(t.Context(), member); err != nil {
		t.Fatal(err)
	}
	source := f.source.DeepCopy()
	source.Status.Placement = &v1beta1.PlacementStatus{
		Plan:       &v1beta1.PlacementPlanStatus{ID: "plan-a", Revision: 1, SourceUID: source.UID, ObservedGeneration: source.Generation},
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: f.clusters[0].UID, OriginalReplicas: 2, CurrentReplicas: 1, Capacity: sample, CurrentHome: &v1beta1.PlacementHomePolicy{InputDigest: "digest-a", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}}}}},
	}
	r, _ := newPlacer(f.r.Scheme(), f.r.Clusters, source, f.clusters[0].DeepCopy())
	r.Capacity, r.CapacityClock = f.r.Capacity, f.clock
	return &retirementFixture{r: r, worker: worker, source: source, member: member}
}

// setCurrentReplicas edits the allocation on the source and on its stored
// copy, as the plan store leaves them.
func (f *retirementFixture) setCurrentReplicas(t *testing.T, replicas int32) {
	t.Helper()
	f.source.Status.Placement.Candidates[0].Allocation.CurrentReplicas = replicas
	stored := &v1beta1.InferenceService{}
	if err := f.r.Get(t.Context(), client.ObjectKeyFromObject(f.source), stored); err != nil {
		t.Fatal(err)
	}
	stored.Status.Placement = f.source.Status.Placement.DeepCopy()
	if err := f.r.Status().Update(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
}

func (f *retirementFixture) retire(t *testing.T) error {
	t.Helper()
	return f.r.retireCapacityFloor(t.Context(), f.worker, f.source, f.source.Status.Placement.Candidates[0])
}

func (f *retirementFixture) live(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	got := &v1beta1.InferenceService{}
	if err := f.worker.Get(t.Context(), client.ObjectKeyFromObject(f.member), got); err != nil {
		t.Fatal(err)
	}
	return got
}

func (f *retirementFixture) editMember(t *testing.T, edit func(*v1beta1.InferenceService)) {
	t.Helper()
	got := f.live(t)
	edit(got)
	if err := f.worker.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
}

// withPolicy writes an execution policy derived from the source's and edited
// onto the member.
func (f *retirementFixture) withPolicy(t *testing.T, edit func(*v1beta1.PlacementExecutionPolicy)) {
	t.Helper()
	policy := executionPolicy(f.source, f.source.Status.Placement.Candidates[0].Allocation)
	edit(policy)
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	f.editMember(t, func(m *v1beta1.InferenceService) { m.Annotations[constants.PlacementExecution] = raw })
}

// wantRetirementRefusal checks a refusal of the retirement itself: it opens
// with "retiring capacity" and ends with the rule it names.
func wantRetirementRefusal(t *testing.T, err error, tail string) {
	t.Helper()
	if err == nil || !strings.HasPrefix(err.Error(), "retiring capacity ") || !strings.HasSuffix(err.Error(), tail) {
		t.Fatalf("retirement error = %v, want the retirement refusal ending with %q", err, tail)
	}
}

func TestCapacityRetirementRefusalsInOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*testing.T, *retirementFixture)
		// want is the protocol's refusal, or, with retirement set, the tail
		// of the retirement's own refusal.
		want       string
		retirement bool
	}{
		{name: "a member of another source", want: "requires an identified live derived service", retirement: true, edit: func(t *testing.T, f *retirementFixture) {
			f.editMember(t, func(m *v1beta1.InferenceService) {
				delete(m.Labels, constants.PlacementOrigin)
				delete(m.Annotations, constants.PlacementOriginUID)
			})
		}},
		{name: "a member being deleted", want: "requires an identified live derived service", retirement: true, edit: func(t *testing.T, f *retirementFixture) {
			f.editMember(t, func(m *v1beta1.InferenceService) { m.Finalizers = []string{"example.com/cleanup"} })
			if err := f.worker.Delete(t.Context(), f.live(t)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "a member the API did not identify", want: "requires an identified live derived service", retirement: true, edit: func(t *testing.T, f *retirementFixture) {
			f.editMember(t, func(m *v1beta1.InferenceService) { m.UID = "" })
		}},
		{name: "an unidentified member is refused before its minimum is read", want: "requires an identified live derived service", retirement: true, edit: func(t *testing.T, f *retirementFixture) {
			f.editMember(t, func(m *v1beta1.InferenceService) {
				delete(m.Labels, constants.PlacementOrigin)
				delete(m.Annotations, constants.PlacementOriginUID)
				m.Spec.Engine.MinReplicas = nil
			})
		}},
		{name: "one replica above the standing minimum", want: "cannot grow member replicas", retirement: true, edit: func(t *testing.T, f *retirementFixture) {
			f.setCurrentReplicas(t, 3)
		}},
		{name: "growth is refused before the member's policy is read", want: "cannot grow member replicas", retirement: true, edit: func(t *testing.T, f *retirementFixture) {
			f.withPolicy(t, func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++ })
			f.setCurrentReplicas(t, 3)
		}},
		{name: "a member that declares a component the demand does not name", want: `placement demand is missing component "decoder"`, edit: func(t *testing.T, f *retirementFixture) {
			f.editMember(t, func(m *v1beta1.InferenceService) {
				m.Spec.Decoder = &v1beta1.DecoderSpec{}
				m.Spec.Decoder.MinReplicas = ptr.To(1)
			})
		}},
		{name: "a newer member policy", want: "placement execution revision is stale or conflicting", edit: func(t *testing.T, f *retirementFixture) {
			f.withPolicy(t, func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++ })
		}},
		{name: "a member policy of another registration", want: "placement execution identity changed", edit: func(t *testing.T, f *retirementFixture) {
			f.withPolicy(t, func(p *v1beta1.PlacementExecutionPolicy) { p.ClusterUID = "registration-b" })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetirementFixture(t)
			before := f.live(t).ResourceVersion
			tc.edit(t, f)
			err := f.retire(t)
			if tc.retirement {
				wantRetirementRefusal(t, err, tc.want)
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("retirement error = %v, want %q", err, tc.want)
			}
			if got := f.live(t); got.Annotations[constants.PlacementExecution] != f.member.Annotations[constants.PlacementExecution] && before == got.ResourceVersion {
				t.Fatalf("a refused retirement wrote the member: %+v", got.Annotations)
			}
		})
	}
}

func TestCapacityRetirementAtTheStandingMinimumIsAllowed(t *testing.T) {
	f := newRetirementFixture(t)
	f.setCurrentReplicas(t, 2)
	if err := f.retire(t); err != nil {
		t.Fatalf("retirement at the standing minimum = %v, want no refusal", err)
	}
}

func TestCapacityRetirementWritesOnceAndSetsThePolicy(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*testing.T, *retirementFixture)
	}{
		{name: "a member without a policy"},
		{name: "a member policy one revision behind the plan's is replaced", edit: func(t *testing.T, f *retirementFixture) {
			f.withPolicy(t, func(p *v1beta1.PlacementExecutionPolicy) { p.Revision = 1 })
			f.source.Status.Placement.Plan.Revision = 2
			f.setCurrentReplicas(t, 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetirementFixture(t)
			if tc.edit != nil {
				tc.edit(t, f)
			}
			want, err := protocol.Encode(executionPolicy(f.source, f.source.Status.Placement.Candidates[0].Allocation))
			if err != nil {
				t.Fatal(err)
			}
			before := f.live(t)
			if before.Annotations[constants.PlacementExecution] == want {
				t.Fatalf("the member already carries the plan's policy %q", want)
			}
			if err := f.retire(t); err != nil {
				t.Fatal(err)
			}
			after := f.live(t)
			if after.ResourceVersion == before.ResourceVersion {
				t.Fatal("the first retirement wrote nothing")
			}
			if after.Annotations[constants.PlacementExecution] != want {
				t.Fatalf("execution policy = %q, want the plan's %q", after.Annotations[constants.PlacementExecution], want)
			}
			if after.Annotations["example.com/owner"] != "team-a" || after.Labels[constants.PlacementOrigin] != before.Labels[constants.PlacementOrigin] {
				t.Fatalf("the retirement dropped the member's other metadata: %v %v", after.Annotations, after.Labels)
			}
			if err := f.retire(t); err != nil {
				t.Fatal(err)
			}
			if again := f.live(t); again.ResourceVersion != after.ResourceVersion {
				t.Fatalf("a retirement with nothing to change wrote the member (%s -> %s)", after.ResourceVersion, again.ResourceVersion)
			}
		})
	}
}
