package engine

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/tpudefrag"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// tpuSliceCandidate turns the fixture's candidate into a move onto a new
// OME-provisioned TPU slice: no target nodes and no GPU footprint.
func tpuSliceCandidate(c policy.Candidate) policy.Candidate {
	c.TPUSlice = &policy.TPUSlicePlan{Topology: "2x2x2", FreePartitions: 1}
	c.HintTargetNodes, c.PlacementTargetNodes, c.FootprintGPUs = nil, nil, 0
	return c
}

// TestDispatcherSubmitsTPUSliceMoveWithoutSimulation checks a TPU slice move is
// written once, with no hints, without calling the simulator, and is not
// resubmitted after a leader restart.
func TestDispatcherSubmitsTPUSliceMoveWithoutSimulation(t *testing.T) {
	for _, gang := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "gang"}[gang], func(t *testing.T) {
			d, cl, snap, c, cfg, a := dispatchFixture(t, gang)
			c = tpuSliceCandidate(c)
			d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}
			d.Simulator = simulationFunc(func(context.Context, scheduling.Request) (scheduling.Result, error) {
				t.Fatal("a TPU slice move must not be simulated")
				return scheduling.Result{}, nil
			})
			_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
			got := decisionFor(t, decisions, "prod/a")
			if got.DispatchStatus != "submitted" || got.RequestUUID == "" || cl.patches != 1 {
				t.Fatalf("TPU slice move was not submitted exactly once: %+v patches=%d", got, cl.patches)
			}
			_, j, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil || len(j.Entries) != 1 || len(j.Entries[0].Targets) != 0 {
				t.Fatalf("bad durable state: %+v %v", j, err)
			}
			req, err := requestForEntry(j.Entries[0])
			if err != nil || req.FromNode != "source" || len(req.HintTargetNodes) != 0 || req.Reason != policy.ReasonNodeMaintenance {
				t.Fatalf("wrong migration API payload: %+v %v", req, err)
			}
			// A restarted leader replays the same source and must not submit again.
			d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, &Arbiter{Ledger: NewLedger()})
			if cl.patches != 1 {
				t.Fatalf("duplicate TPU slice request after restart: %d", cl.patches)
			}
		})
	}
}

// TestDispatcherWithholdsTPUSliceMoveWhenPolicyNoLongerAgrees checks a move is
// withheld when the fresh policy replay finds no free partition.
func TestDispatcherWithholdsTPUSliceMoveWhenPolicyNoLongerAgrees(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	c = tpuSliceCandidate(c)
	// The fresh policy replay finds no free partition any more.
	withdrawn := c
	withdrawn.Executable, withdrawn.TPUSlice, withdrawn.AdvisoryReason = false, nil, policy.AdvisoryNoTPUSliceCapacity
	d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{withdrawn}}}
	_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	got := decisionFor(t, decisions, "prod/a")
	if got.DispatchStatus != "withheld" || got.DispatchReason != "PolicyNoLongerEligible" || cl.patches != 0 {
		t.Fatalf("stale TPU slice move was not withheld: %+v patches=%d", got, cl.patches)
	}
}

// clusterWideLists counts the lists from a lossless reader that would read
// every Pod or Node of a cluster.
func clusterWideLists(cl client.WithWatch, count *int) client.Reader {
	return interceptor.NewClient(cl, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			o := (&client.ListOptions{}).ApplyOptions(opts)
			switch list.(type) {
			case *corev1.NodeList:
				*count++
			case *corev1.PodList:
				if o.Namespace == "" || o.LabelSelector == nil {
					*count++
				}
			}
			return c.List(ctx, list, opts...)
		},
	})
}

// TestTPUSliceMovePreflightListsNoPodClusterWide checks a move onto a new TPU
// slice is submitted without listing every Pod or Node from the lossless
// reader, which does not fit a large cluster's memory or the preflight's age
// bound, while a simulated move still reads the whole scheduling input.
func TestTPUSliceMovePreflightListsNoPodClusterWide(t *testing.T) {
	for _, gang := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "gang"}[gang], func(t *testing.T) {
			d, cl, snap, c, cfg, a := dispatchFixture(t, gang)
			c = tpuSliceCandidate(c)
			d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}
			wide := 0
			d.CacheReader = cl.Client
			d.Reader = clusterWideLists(cl.Client.(client.WithWatch), &wide)
			_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
			if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 1 {
				t.Fatalf("TPU slice move was not submitted: %+v patches=%d", got, cl.patches)
			}
			if wide != 0 {
				t.Fatalf("TPU slice preflight listed every Pod or Node %d times from the lossless reader", wide)
			}
		})
	}

	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	wide := 0
	d.CacheReader = cl.Client
	d.Reader = clusterWideLists(cl.Client.(client.WithWatch), &wide)
	_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 1 {
		t.Fatalf("simulated move was not submitted: %+v patches=%d", got, cl.patches)
	}
	if wide == 0 {
		t.Fatal("a simulated move must still capture the whole scheduling input losslessly")
	}
}

// TestSimulatedMoveReadsNoCache checks a simulated move replays the policy on
// the lossless reader only, never through the cache.
func TestSimulatedMoveReadsNoCache(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	cached := 0
	d.CacheReader = interceptor.NewClient(cl.Client.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			cached++
			return c.List(ctx, list, opts...)
		},
	})
	_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 1 {
		t.Fatalf("simulated move was not submitted: %+v patches=%d", got, cl.patches)
	}
	if cached != 0 {
		t.Fatalf("a simulated move read through the cache %d times", cached)
	}
}

// TestTPUSliceRetryListsNoPodClusterWide checks a prepared TPU slice request
// retried from the journal, whose candidate carries no plan, still avoids
// listing every Pod or Node from the lossless reader.
func TestTPUSliceRetryListsNoPodClusterWide(t *testing.T) {
	for _, gang := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "gang"}[gang], func(t *testing.T) {
			d, cl, snap, c, cfg, a := dispatchFixture(t, gang)
			c = tpuSliceCandidate(c)
			d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}
			d.CacheReader = cl.Client
			cl.failBeforeApply = true
			d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
			if before := onlyRetryEntry(t, cl.Client); before.Phase != dispatchPrepared || len(before.Targets) != 0 {
				t.Fatalf("expected a prepared TPU slice request with no targets: %+v", before)
			}
			cl.failBeforeApply = false
			wide := 0
			d.Reader = clusterWideLists(cl.Client.(client.WithWatch), &wide)
			_, decisions := d.Execute(context.Background(), snap, nil, cfg, &Arbiter{Ledger: NewLedger()})
			if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 2 {
				t.Fatalf("prepared TPU slice request was not retried: %+v patches=%d", got, cl.patches)
			}
			if wide != 0 {
				t.Fatalf("TPU slice retry listed every Pod or Node %d times from the lossless reader", wide)
			}
		})
	}
}

// TestTPUSliceMoveSubmitsWhenCacheLags checks a TPU slice move is fenced on the
// owner it read losslessly: an InferenceService write the cache has not seen
// yet does not withhold it as OwnerChanged.
func TestTPUSliceMoveSubmitsWhenCacheLags(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	c = tpuSliceCandidate(c)
	d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}
	ctx := context.Background()
	var objects []client.Object
	var nodes corev1.NodeList
	var pods corev1.PodList
	var isvcs v1beta1.InferenceServiceList
	var irs v1beta1.InferenceReplicaList
	for _, list := range []client.ObjectList{&nodes, &pods, &isvcs, &irs} {
		if err := cl.Client.List(ctx, list); err != nil {
			t.Fatal(err)
		}
	}
	for i := range nodes.Items {
		objects = append(objects, &nodes.Items[i])
	}
	for i := range pods.Items {
		objects = append(objects, &pods.Items[i])
	}
	for i := range isvcs.Items {
		objects = append(objects, &isvcs.Items[i])
	}
	for i := range irs.Items {
		objects = append(objects, &irs.Items[i])
	}
	d.CacheReader = fake.NewClientBuilder().WithScheme(cl.Client.Scheme()).WithObjects(objects...).Build()

	var owner v1beta1.InferenceService
	if err := cl.Client.Get(ctx, c.Workload, &owner); err != nil {
		t.Fatal(err)
	}
	if owner.Annotations == nil {
		owner.Annotations = map[string]string{}
	}
	owner.Annotations["example.com/touched"] = "after the cache"
	if err := cl.Client.Update(ctx, &owner); err != nil {
		t.Fatal(err)
	}
	_, decisions := d.Execute(ctx, snap, []policy.Candidate{c}, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 1 {
		t.Fatalf("a lagging cache must not withhold the TPU slice move: %+v patches=%d", got, cl.patches)
	}
}

// TestDispatchWithoutSimulatorSubmitsOnlyTPUSliceMoves checks that without a
// simulation worker a move onto a new TPU slice is still submitted, while a
// move that needs a simulation is withheld on its own as SimulationUnavailable.
func TestDispatchWithoutSimulatorSubmitsOnlyTPUSliceMoves(t *testing.T) {
	for _, gang := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "gang"}[gang], func(t *testing.T) {
			d, cl, snap, c, cfg, a := dispatchFixture(t, gang)
			c = tpuSliceCandidate(c)
			d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}
			d.Simulator, d.CacheReader = nil, cl.Client
			_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
			if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 1 {
				t.Fatalf("TPU slice move without a simulator was not submitted: %+v patches=%d", got, cl.patches)
			}
		})
	}

	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	d.Simulator, d.CacheReader = nil, cl.Client
	_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "withheld" || got.DispatchReason != "SimulationUnavailable" || cl.patches != 0 {
		t.Fatalf("a move that needs a simulation must be withheld without a simulator: %+v patches=%d", got, cl.patches)
	}
}

// TestDispatcherJournalsTPUDefragmentationMovesUnderTheirPolicy checks a TPU
// defragmentation move is submitted without simulation, carries its reason,
// and a restarted leader recovers its policy from the journal so the prepared
// request is replayed against the same policy.
func TestDispatcherJournalsTPUDefragmentationMovesUnderTheirPolicy(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	c = tpuSliceCandidate(c)
	c.Policy, c.Reason = tpudefrag.PolicyName, policy.ReasonTPUFragmentation
	d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}
	d.Simulator = nil
	_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 1 {
		t.Fatalf("TPU defragmentation move was not submitted: %+v patches=%d", got, cl.patches)
	}
	_, j, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
	if err != nil || len(j.Entries) != 1 {
		t.Fatalf("bad durable state: %+v %v", j, err)
	}
	req, err := requestForEntry(j.Entries[0])
	if err != nil || req.Reason != policy.ReasonTPUFragmentation {
		t.Fatalf("request reason = %+v %v", req, err)
	}
	if got := entryCandidate(j.Entries[0]); got.Policy != tpudefrag.PolicyName || got.Reason != policy.ReasonTPUFragmentation {
		t.Fatalf("journal entry maps to policy %q reason %q", got.Policy, got.Reason)
	}
}
