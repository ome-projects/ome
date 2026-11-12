package placement

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
)

// singleMoveProposal plans one pass of a bounded replacement: the source
// keeps its chosen cluster and its original minimum while a replacement is
// probed on a matching cluster, and the replacement is selected only once
// it is admitted. One row per rule: what the pass refuses (registrations,
// the standing plan, a zero minimum, a drained replacement with resources
// left), which cluster is selected, and the target each registered cluster
// is given or denied.

// singleMoveFixture is the standing picture: the source placed on member-a
// with a minimum of 3 engines, member-b an active replacement with its copy
// up, member-c a candidate with nothing yet, member-b and member-c matching
// the selector and eligible.
type singleMoveFixture struct {
	source      *v1beta1.InferenceService
	clusters    []v1beta1.WorkloadCluster
	eligible    []string
	standing    *placementObservations
	connections fakeClusters
	scheme      *runtime.Scheme
	objects     []client.Object
}

func newSingleMoveFixture(t *testing.T) *singleMoveFixture {
	t.Helper()
	source, proposal, observations := replacementRaceFixture()
	source.Spec.Engine.MinReplicas = ptr.To(3)
	activeReplacement(&proposal, observations, "member-b", false)
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
		Mode: v1beta1.PlacementModeSingle, Winner: proposal.Winner, SourceUID: source.UID, SingleMove: proposal.SingleMove.DeepCopy(),
	}}
	f := &singleMoveFixture{source: source, scheme: testScheme(t), connections: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}}
	for _, name := range []string{"member-a", "member-b", "member-c"} {
		allocation := proposal.Assignments[name]
		source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: allocation.DeepCopy()})
		f.register(name)
	}
	f.eligible = []string{"member-b", "member-c"}
	f.standing = &placementObservations{matches: map[string]bool{"member-b": true, "member-c": true}}
	return f
}

// register adds a Ready cluster with an empty worker behind it.
func (f *singleMoveFixture) register(name string) {
	cluster := readyWC(name, nil)
	f.clusters = append(f.clusters, *cluster)
	f.objects = append(f.objects, cluster)
	f.connections.m[name] = workloadcluster.NewNeverCachingClient(emptyWorker(f.scheme))
}

// allocation is the standing allocation of the named candidate.
func (f *singleMoveFixture) allocation(name string) *v1beta1.CandidateAllocationStatus {
	for i := range f.source.Status.Placement.Candidates {
		if f.source.Status.Placement.Candidates[i].Cluster == name {
			return f.source.Status.Placement.Candidates[i].Allocation
		}
	}
	return nil
}

// standingMember puts a serving copy of the source on the named cluster.
func (f *singleMoveFixture) standingMember(t *testing.T, name string) {
	t.Helper()
	f.connections.m[name] = workloadcluster.NewNeverCachingClient(workerWithAdmittedDerived(t, f.scheme))
}

func (f *singleMoveFixture) propose(t *testing.T) (plan.Proposal, error) {
	t.Helper()
	r, _ := newPlacer(f.scheme, f.connections, f.objects...)
	return r.singleMoveProposal(t.Context(), f.source, f.clusters, f.eligible, f.standing)
}

// minimumOf is a placement policy asking for the given engines under the digest.
func minimumOf(digest string, engines int32) *v1beta1.PlacementHomePolicy {
	return &v1beta1.PlacementHomePolicy{InputDigest: digest, ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: engines}}}
}

// wantSingleMoveProposal is the proposal the standing picture yields: the
// chosen cluster keeps its placement and is given no target, the
// replacement and the candidate are given the target resolved from the
// source under the pass's digest.
func wantSingleMoveProposal(digest string) plan.Proposal {
	return plan.Proposal{
		Mode: v1beta1.PlacementModeSingle, Winner: "member-a", PauseSurge: true, InputDigest: digest,
		SingleMove: &v1beta1.PlacementSingleMoveStatus{},
		Assignments: map[string]v1beta1.CandidateAllocationStatus{
			"member-a": {ClusterUID: "member-a-uid", OriginalReplicas: 3, CurrentReplicas: 3, CurrentHome: minimumOf("intent", 3)},
			"member-b": {ClusterUID: "member-b-uid", CurrentReplicas: 3, CurrentHome: minimumOf("intent", 3), DesiredReplicas: 3, DesiredHome: minimumOf(digest, 3), RaceCandidate: true, Matched: true},
			"member-c": {ClusterUID: "member-c-uid", DesiredReplicas: 3, DesiredHome: minimumOf(digest, 3), RaceCandidate: true, Matched: true},
		},
	}
}

// member edits one cluster's assignment in the wanted proposal.
func member(want *plan.Proposal, name string, edit func(a *v1beta1.CandidateAllocationStatus)) {
	a := want.Assignments[name]
	edit(&a)
	want.Assignments[name] = a
}

// noTarget denies the cluster a target.
func noTarget(a *v1beta1.CandidateAllocationStatus) { a.DesiredHome, a.DesiredReplicas = nil, 0 }

// keepsPlacement gives the cluster its current placement as its target.
func keepsPlacement(a *v1beta1.CandidateAllocationStatus) {
	a.DesiredHome, a.DesiredReplicas = a.CurrentHome.DeepCopy(), a.CurrentReplicas
}

// singleMoveRow is one shape of source, clusters and observations.
type singleMoveRow struct {
	name string
	edit func(t *testing.T, f *singleMoveFixture)
	// wantErr is the named error the answer wraps; wantErrText the fragments
	// an error message carries, all of them.
	wantErr     error
	wantErrText []string
	// want edits the proposal the standing picture yields; nil keeps it.
	want func(f *singleMoveFixture, want *plan.Proposal)
}

var singleMoveRows = []singleMoveRow{
	{
		name: "the replacement and the candidate are given the target resolved from the source",
	},
	{
		name: "a cluster outside the eligible set keeps its accepted target and stays pending",
		edit: func(_ *testing.T, f *singleMoveFixture) { f.eligible = nil },
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			for _, name := range []string{"member-b", "member-c"} {
				member(want, name, func(a *v1beta1.CandidateAllocationStatus) {
					a.HomeInputsPending, a.DesiredHome = true, minimumOf("intent", 3)
				})
			}
		},
	},
	{
		name: "the chosen cluster matching the selector again cancels the replacement",
		edit: func(_ *testing.T, f *singleMoveFixture) { f.standing.matches["member-a"] = true },
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			want.SingleMove.Selected = "member-a"
			member(want, "member-a", func(a *v1beta1.CandidateAllocationStatus) { a.Matched = true; keepsPlacement(a) })
			member(want, "member-b", noTarget)
			member(want, "member-c", noTarget)
		},
	},
	{
		name: "a selected cluster that stopped matching gives the selection back to the chosen cluster",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.source.Status.Placement.Plan.SingleMove.Selected = "member-b"
			f.standing.matches["member-b"] = false
		},
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			want.SingleMove.Selected = "member-a"
			member(want, "member-a", keepsPlacement)
			member(want, "member-b", func(a *v1beta1.CandidateAllocationStatus) { a.Matched = false; noTarget(a) })
			member(want, "member-c", noTarget)
		},
	},
	{
		name: "an accepted replacement keeps its selection and its placement",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.source.Status.Placement.Plan.Winner = "member-b"
			f.source.Status.Placement.Plan.SingleMove.Selected = "member-b"
			f.allocation("member-b").RaceCandidate = false
		},
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			want.Winner, want.SingleMove.Selected = "member-b", "member-b"
			member(want, "member-a", noTarget)
			member(want, "member-b", func(a *v1beta1.CandidateAllocationStatus) { a.RaceCandidate = false; keepsPlacement(a) })
			member(want, "member-c", noTarget)
		},
	},
	{
		name: "a deleting cluster is not matched and is given no target",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.clusters[1].DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
		},
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			member(want, "member-b", func(a *v1beta1.CandidateAllocationStatus) { a.Matched = false; noTarget(a) })
		},
	},
	{
		name: "a new registration joins as a candidate",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.register("member-d")
			f.standing.matches["member-d"] = true
			f.eligible = append(f.eligible, "member-d")
		},
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			want.Assignments["member-d"] = v1beta1.CandidateAllocationStatus{
				ClusterUID: "member-d-uid", RaceCandidate: true, Matched: true, DesiredReplicas: 3, DesiredHome: minimumOf(want.InputDigest, 3),
			}
		},
	},
	{
		name: "a standing candidate whose cluster is not registered keeps its record without a target",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.source.Status.Placement.Candidates = append(f.source.Status.Placement.Candidates, v1beta1.CandidatePlacement{
				Cluster:    "member-d",
				Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "member-d-uid", RaceCandidate: true, DesiredReplicas: 3, DesiredHome: minimumOf("intent", 3)},
			})
		},
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			want.Assignments["member-d"] = v1beta1.CandidateAllocationStatus{ClusterUID: "member-d-uid", RaceCandidate: true}
		},
	},
	{
		name: "a drained replacement with no resources left drops its placement record",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			a := f.allocation("member-c")
			a.CurrentReplicas, a.DrainRequested, a.CurrentHome = 0, true, minimumOf("intent", 0)
		},
		want: func(_ *singleMoveFixture, want *plan.Proposal) {
			member(want, "member-c", func(a *v1beta1.CandidateAllocationStatus) { a.DrainRequested = true; noTarget(a) })
		},
	},
	{
		name: "a drained replacement that still has resources is cleaned up first",
		edit: func(t *testing.T, f *singleMoveFixture) {
			a := f.allocation("member-c")
			a.CurrentReplicas, a.DrainRequested, a.CurrentHome = 0, true, minimumOf("intent", 0)
			f.standingMember(t, "member-c")
		},
		wantErrText: []string{`"member-c" still requires cleanup`},
	},
	{
		name:        "no retained chosen cluster is refused",
		edit:        func(_ *testing.T, f *singleMoveFixture) { f.source.Status.Placement.Plan.Winner = "" },
		wantErrText: []string{"bounded replacement requires a retained"},
	},
	{
		name:        "a registration without a UID is refused",
		edit:        func(_ *testing.T, f *singleMoveFixture) { f.clusters[1].UID = "" },
		wantErrText: []string{"requires distinct identified registrations"},
	},
	{
		name:        "two registrations of one name are refused",
		edit:        func(_ *testing.T, f *singleMoveFixture) { f.clusters = append(f.clusters, f.clusters[1]) },
		wantErrText: []string{"requires distinct identified registrations"},
	},
	{
		name: "a plan of another mode is refused",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.source.Status.Placement.Cluster = "member-a"
			f.source.Status.Placement.Plan.Mode = v1beta1.PlacementModeAll
		},
		wantErrText: []string{"requires accepted Single authority"},
	},
	{
		name:        "a plan of another source is refused",
		edit:        func(_ *testing.T, f *singleMoveFixture) { f.source.Status.Placement.Plan.SourceUID = "older-source" },
		wantErrText: []string{"requires accepted Single authority"},
	},
	{
		name:        "a standing candidate without an allocation is refused",
		edit:        func(_ *testing.T, f *singleMoveFixture) { f.source.Status.Placement.Candidates[1].Allocation = nil },
		wantErrText: []string{"lacks allocation authority"},
	},
	{
		name:        "a registration whose UID changed is refused",
		edit:        func(_ *testing.T, f *singleMoveFixture) { f.clusters[1].UID = "replacement-uid" },
		wantErrText: []string{`member "member-b"`, "changed"},
	},
	{
		name: "a zero minimum on the standing placement is refused",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.allocation("member-a").CurrentHome.ReplicaFloors[0].Replicas = 0
		},
		wantErr: errPositiveMovementFloor,
	},
	{
		name:    "a zero minimum on a resolved target is refused",
		edit:    func(_ *testing.T, f *singleMoveFixture) { f.source.Spec.Engine.MinReplicas = ptr.To(0) },
		wantErr: errPositiveMovementFloor,
	},
	{
		name: "a chosen cluster without a plan and without a standing copy is refused",
		edit: func(_ *testing.T, f *singleMoveFixture) {
			f.source.Status.Placement = &v1beta1.PlacementStatus{Cluster: "member-a"}
		},
		wantErrText: []string{"has no verified full"},
	},
	{
		name: "a chosen cluster without a plan adopts its standing copy",
		edit: func(t *testing.T, f *singleMoveFixture) {
			f.source.Status.Placement = &v1beta1.PlacementStatus{Cluster: "member-a"}
			f.standingMember(t, "member-a")
		},
		want: func(f *singleMoveFixture, want *plan.Proposal) {
			served := &v1beta1.InferenceService{}
			if err := f.connections.m["member-a"].Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, served); err != nil {
				panic(err)
			}
			adopted := minimumOf(string(served.UID)+"/"+served.ResourceVersion, 1)
			want.Assignments["member-a"] = v1beta1.CandidateAllocationStatus{ClusterUID: "member-a-uid", OriginalReplicas: 1, CurrentReplicas: 1, CurrentHome: adopted}
			member(want, "member-b", func(a *v1beta1.CandidateAllocationStatus) { a.CurrentReplicas, a.CurrentHome = 0, nil })
		},
	},
}

var digestShape = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestSingleMoveProposalRules(t *testing.T) {
	for _, row := range singleMoveRows {
		t.Run(row.name, func(t *testing.T) {
			f := newSingleMoveFixture(t)
			if row.edit != nil {
				row.edit(t, f)
			}
			before := f.source.DeepCopy()

			got, err := f.propose(t)

			if diff := cmp.Diff(before, f.source); diff != "" {
				t.Errorf("the source changed (-before +after):\n%s", diff)
			}
			if row.wantErr != nil || len(row.wantErrText) > 0 {
				requireRefusal(t, row, err)
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			requireProposal(t, f, row, before, got)
		})
	}
}

// requireRefusal checks that the answer is the refusal the row names.
func requireRefusal(t *testing.T, row singleMoveRow, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want %v %q", row.wantErr, row.wantErrText)
	}
	if row.wantErr != nil && !errors.Is(err, row.wantErr) {
		t.Errorf("error %q does not wrap %q", err, row.wantErr)
	}
	for _, text := range row.wantErrText {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("error %q does not carry %q", err, text)
		}
	}
}

// requireProposal checks that the proposal is the one the row wants, with a
// well-formed digest and a move record of its own.
func requireProposal(t *testing.T, f *singleMoveFixture, row singleMoveRow, before *v1beta1.InferenceService, got plan.Proposal) {
	t.Helper()
	if !digestShape.MatchString(got.InputDigest) {
		t.Errorf("digest %q is not 64 hex characters", got.InputDigest)
	}
	want := wantSingleMoveProposal(got.InputDigest)
	if row.want != nil {
		row.want(f, &want)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("proposal (-want +got):\n%s", diff)
	}
	// The proposal's move record is a copy of the plan's, not the same object.
	got.SingleMove.Selected = "changed"
	if diff := cmp.Diff(before, f.source); diff != "" {
		t.Errorf("the proposal shares the source's move record (-before +after):\n%s", diff)
	}
}

// TestSingleMoveProposalDigest holds what the pass's digest covers: the
// source spec, its labels, the annotations that carry placement intent and
// the matched clusters; the cleanup release request is not intent.
func TestSingleMoveProposalDigest(t *testing.T) {
	digestOf := func(t *testing.T, edit func(f *singleMoveFixture)) string {
		t.Helper()
		f := newSingleMoveFixture(t)
		if edit != nil {
			edit(f)
		}
		got, err := f.propose(t)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return got.InputDigest
	}
	same := digestOf(t, nil)
	for _, tc := range []struct {
		name    string
		edit    func(f *singleMoveFixture)
		changes bool
	}{
		{name: "the same picture gives the same digest"},
		{name: "a spec change moves the digest", changes: true, edit: func(f *singleMoveFixture) { f.source.Spec.Engine.MinReplicas = ptr.To(4) }},
		{name: "a label change moves the digest", changes: true, edit: func(f *singleMoveFixture) { f.source.Labels = map[string]string{"team": "a"} }},
		{name: "an annotation moves the digest", changes: true, edit: func(f *singleMoveFixture) { f.source.Annotations = map[string]string{"example.com/note": "a"} }},
		{name: "the cleanup release request does not move the digest", edit: func(f *singleMoveFixture) {
			f.source.Annotations = map[string]string{constants.PlacementReleaseCleanupWaitAnnotation: "1"}
		}},
		{name: "a change of the matched clusters moves the digest", changes: true, edit: func(f *singleMoveFixture) { f.standing.matches["member-c"] = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := digestOf(t, tc.edit)
			if (got != same) != tc.changes {
				t.Errorf("digest %s, baseline %s, want changed=%v", got, same, tc.changes)
			}
		})
	}
}
