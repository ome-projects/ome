package acceleratorquota

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

const rootName = "root"

// nodeState is the slice of status this suite asserts on. Comparing a whole map
// of these with cmp.Diff reports every node that drifted in one failure, rather
// than stopping at whichever field an assertion happened to check first — which
// matters here because the controller is whole-tree: one node's edit can move
// another node's verdict, and a regression usually shows up on the node you were
// not looking at.
//
// Timestamps and messages are excluded deliberately. LastTransitionTime is not
// reproducible, and messages carry running totals that would make every table
// entry a transcription exercise; the cases that turn on message content assert
// it separately.
type nodeState struct {
	Parent   string
	Ready    metav1.ConditionStatus
	Degraded metav1.ConditionStatus
	Reason   string
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return s
}

func budget(nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   "google.com/tpu",
		ResourceFlavor: "tpu7x",
		Nominal:        resource.MustParse(nominal),
	}
}

func cohort(name, parent string, budgets ...v1beta1.AcceleratorBudget) *v1beta1.AcceleratorQuota {
	q := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:    v1beta1.AcceleratorQuotaRoleCohort,
			Budgets: budgets,
		},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

func leaf(name, parent string, budgets ...v1beta1.AcceleratorBudget) *v1beta1.AcceleratorQuota {
	return &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
			ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: parent},
			Budgets:   budgets,
		},
	}
}

func newReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
		Build()
	return &Reconciler{
		Client:    c,
		Scheme:    s,
		Log:       logf.Log.WithName("test"),
		APIReader: c,
		Options:   tree.Options{RootName: rootName, MaxDepth: 5},
	}, c
}

// observe projects every node's status into the comparable shape above.
func observe(t *testing.T, c client.Client) map[string]nodeState {
	t.Helper()
	var list v1beta1.AcceleratorQuotaList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list: %v", err)
	}
	out := make(map[string]nodeState, len(list.Items))
	for i := range list.Items {
		q := &list.Items[i]
		st := nodeState{Parent: q.Status.Parent}
		if c := apimeta.FindStatusCondition(q.Status.Conditions, v1beta1.AcceleratorQuotaReady); c != nil {
			st.Ready = c.Status
		}
		if c := apimeta.FindStatusCondition(q.Status.Conditions, v1beta1.AcceleratorQuotaDegraded); c != nil {
			st.Degraded = c.Status
			st.Reason = c.Reason
		}
		out[q.Name] = st
	}
	return out
}

func resourceVersions(t *testing.T, c client.Client) map[string]string {
	t.Helper()
	var list v1beta1.AcceleratorQuotaList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list: %v", err)
	}
	out := make(map[string]string, len(list.Items))
	for i := range list.Items {
		out[list.Items[i].Name] = list.Items[i].ResourceVersion
	}
	return out
}

func degradedMessage(t *testing.T, c client.Client, name string) string {
	t.Helper()
	var q v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, &q); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	cond := apimeta.FindStatusCondition(q.Status.Conditions, v1beta1.AcceleratorQuotaDegraded)
	if cond == nil {
		t.Fatalf("%s: no Degraded condition", name)
	}
	return cond.Message
}

// ok and bad are the two whole-node verdicts, spelled once so a table row reads
// as the shape of the tree rather than a wall of condition constants.
func ok(parent string) nodeState {
	return nodeState{
		Parent: parent,
		Ready:  metav1.ConditionTrue, Degraded: metav1.ConditionFalse,
		Reason: v1beta1.AcceleratorQuotaReasonAdmitted,
	}
}

func bad(parent, reason string) nodeState {
	return nodeState{
		Parent: parent,
		Ready:  metav1.ConditionFalse, Degraded: metav1.ConditionTrue,
		Reason: reason,
	}
}

// TestReconcile drives one pass over a fixed tree and compares the resulting
// status of EVERY node at once.
func TestReconcile(t *testing.T) {
	tests := []struct {
		name   string
		quotas []client.Object
		want   map[string]nodeState
		// wantMessage, when set, additionally requires the named node's Degraded
		// message to contain the substring — used where the numbers in the
		// message are the point.
		wantMessage map[string]string
	}{
		{
			name: "well-formed tree reports position and Ready",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("100")),
				leaf("team-a", "org", budget("60")),
				leaf("team-b", "org", budget("40")),
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"org":    ok(rootName),
				"team-a": ok("org"),
				"team-b": ok("org"),
			},
		},
		{
			// The parent is blamed, and the freeze reaches every descendant —
			// otherwise they would materialize under a parent that is not there.
			name: "containment bust blames the parent and freezes its subtree",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("50")),
				leaf("big", "org", budget("60")),
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"org":    bad(rootName, v1beta1.AcceleratorQuotaReasonContainmentViolated),
				"big":    bad("org", v1beta1.AcceleratorQuotaReasonContainmentViolated),
			},
			wantMessage: map[string]string{
				"org": "children total 60",
				// The descendant inherits the ancestor's cause verbatim, so an
				// operator is pointed at the node that needs the edit.
				"big": "children total 60",
			},
		},
		{
			// An unreachable node must carry no position at all; a leftover path
			// is what a materializer would act on.
			name: "dangling parent marks the node and its descendants",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				cohort("x1", "ghost"),
				leaf("x2", "x1", budget("1")),
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"x1":     bad("", v1beta1.AcceleratorQuotaReasonParentMissing),
				"x2":     bad("x1", v1beta1.AcceleratorQuotaReasonUnreachable),
			},
			wantMessage: map[string]string{"x2": `ancestor "x1" is unresolved`},
		},
		{
			// A broken node must not cost the controller its view of the sound
			// ones, or one bad edit freezes the fleet.
			name: "a broken node does not disturb a sound sibling subtree",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				leaf("good", rootName, budget("10")),
				leaf("orphan", "ghost", budget("1")),
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"good":   ok(rootName),
				"orphan": bad("", v1beta1.AcceleratorQuotaReasonParentMissing),
			},
		},
		{
			name: "leaf with no budget is rejected by the node-kind check",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				leaf("empty", rootName),
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"empty":  bad(rootName, v1beta1.AcceleratorQuotaReasonNodeKindInvalid),
			},
		},
		{
			name:   "an empty fleet is not a violation",
			quotas: nil,
			want:   map[string]nodeState{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, c := newReconciler(t, tc.quotas...)
			if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if diff := cmp.Diff(tc.want, observe(t, c)); diff != "" {
				t.Errorf("node status mismatch (-want +got):\n%s", diff)
			}
			for node, want := range tc.wantMessage {
				if got := degradedMessage(t, c, node); !strings.Contains(got, want) {
					t.Errorf("%s: Degraded message = %q, want it to contain %q", node, got, want)
				}
			}
		})
	}
}

// TestReconcileAfterEdit drives two passes with an edit in between. It is
// separate from TestReconcile because the interesting property is what CHANGES:
// a verdict moving to a node the edit did not touch, a stale condition clearing,
// or — for a no-op edit — nothing being written at all.
func TestReconcileAfterEdit(t *testing.T) {
	tests := []struct {
		name   string
		quotas []client.Object
		// edit runs between the two reconciles. Nil means the second pass sees
		// exactly what the first did.
		edit func(t *testing.T, c client.Client)
		want map[string]nodeState
		// wantUnchanged names the nodes whose resourceVersion must not move
		// across the second pass.
		wantUnchanged []string
	}{
		{
			// The edit is on the CHILD; the node whose verdict flips is the
			// PARENT. A per-object reconciler would miss this.
			name: "growing a child past its parent degrades the parent",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("100")),
				leaf("team-a", "org", budget("60")),
			},
			edit: func(t *testing.T, c client.Client) {
				setNominal(t, c, "team-a", "500")
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"org":    bad(rootName, v1beta1.AcceleratorQuotaReasonContainmentViolated),
				"team-a": bad("org", v1beta1.AcceleratorQuotaReasonContainmentViolated),
			},
		},
		{
			name: "repairing a budget clears Degraded on the whole subtree",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("50")),
				leaf("big", "org", budget("60")),
			},
			edit: func(t *testing.T, c client.Client) {
				setNominal(t, c, "big", "40")
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"org":    ok(rootName),
				"big":    ok("org"),
			},
		},
		{
			// A resync tick must not rewrite status, or the controller spins its
			// own watch forever.
			name: "a no-op pass writes nothing",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				leaf("team-a", rootName, budget("60")),
			},
			edit: nil,
			want: map[string]nodeState{
				rootName: ok(""),
				"team-a": ok(rootName),
			},
			wantUnchanged: []string{rootName, "team-a"},
		},
		{
			// Deleting a parent must clear the child's previously-written
			// position, not leave it advertising a path into a tree that is gone.
			name: "deleting a parent clears its child's position",
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("100")),
				leaf("team-a", "org", budget("60")),
			},
			edit: func(t *testing.T, c client.Client) {
				del := &v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: "org"}}
				if err := c.Delete(context.Background(), del); err != nil {
					t.Fatalf("delete org: %v", err)
				}
			},
			want: map[string]nodeState{
				rootName: ok(""),
				"team-a": bad("", v1beta1.AcceleratorQuotaReasonParentMissing),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, c := newReconciler(t, tc.quotas...)
			if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
				t.Fatalf("first Reconcile: %v", err)
			}
			before := resourceVersions(t, c)

			if tc.edit != nil {
				tc.edit(t, c)
			}
			if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
				t.Fatalf("second Reconcile: %v", err)
			}

			if diff := cmp.Diff(tc.want, observe(t, c)); diff != "" {
				t.Errorf("node status mismatch (-want +got):\n%s", diff)
			}
			after := resourceVersions(t, c)
			for _, name := range tc.wantUnchanged {
				if before[name] != after[name] {
					t.Errorf("%s: status was rewritten on a no-op pass (resourceVersion %s -> %s)",
						name, before[name], after[name])
				}
			}
		})
	}
}

func setNominal(t *testing.T, c client.Client, name, nominal string) {
	t.Helper()
	var q v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, &q); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	q.Spec.Budgets[0].Nominal = resource.MustParse(nominal)
	if err := c.Update(context.Background(), &q); err != nil {
		t.Fatalf("update %s: %v", name, err)
	}
}

// TestReconcileResult covers the pass-level outcome — the error and the requeue —
// independently of what any node's status says.
func TestReconcileResult(t *testing.T) {
	tests := []struct {
		name             string
		rootName         string
		resync           time.Duration
		quotas           []client.Object
		wantErr          error
		wantRequeueAfter time.Duration
	}{
		{
			name:     "resync interval is requeued",
			rootName: rootName,
			resync:   90 * time.Second,
			quotas:   []client.Object{cohort(rootName, "", budget("1"))},

			wantRequeueAfter: 90 * time.Second,
		},
		{
			name:     "an unset interval leaves reconciles event-driven",
			rootName: rootName,
			quotas:   []client.Object{cohort(rootName, "", budget("1"))},
		},
		{
			// A violation is user-caused and unfixable by retrying, so it must
			// not become an error: that would swap a calm resync cadence for
			// backoff hot-looping and drop the tick that lets a clock-clearable
			// violation recover.
			name:     "a violating tree is not an error",
			rootName: rootName,
			resync:   30 * time.Second,
			quotas: []client.Object{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("50")),
				leaf("big", "org", budget("60")),
			},
			wantRequeueAfter: 30 * time.Second,
		},
		{
			// A configuration failure must surface, not be mistaken for a fleet
			// with nothing to reconcile.
			name:     "an unset root name is a hard error",
			rootName: "",
			quotas:   []client.Object{cohort(rootName, "", budget("1"))},
			wantErr:  tree.ErrRootNameUnset,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newReconciler(t, tc.quotas...)
			r.Options.RootName = tc.rootName
			r.ResyncInterval = tc.resync

			res, err := r.Reconcile(context.Background(), ctrl.Request{})
			switch {
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			case tc.wantErr == nil && err != nil:
				t.Fatalf("unexpected error: %v", err)
			}
			if res.RequeueAfter != tc.wantRequeueAfter {
				t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, tc.wantRequeueAfter)
			}
		})
	}
}

// TestReconcileReadsThroughAPIReader pins the reason APIReader exists: a node
// the cache has not caught up on must still be judged, or the controller clears
// a Degraded that is still true.
func TestReconcileReadsThroughAPIReader(t *testing.T) {
	r, c := newReconciler(t, cohort(rootName, "", budget("128")))
	ctx := context.Background()

	if err := c.Create(ctx, leaf("late", "ghost", budget("1"))); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	want := map[string]nodeState{
		rootName: ok(""),
		"late":   bad("", v1beta1.AcceleratorQuotaReasonParentMissing),
	}
	if diff := cmp.Diff(want, observe(t, c)); diff != "" {
		t.Errorf("node status mismatch (-want +got):\n%s", diff)
	}
}

func TestSetupWithManager(t *testing.T) {
	tests := []struct {
		name     string
		rootName string
		wantErr  error
	}{
		{name: "rejects a missing root name", rootName: "", wantErr: tree.ErrRootNameUnset},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Reconciler{
				APIReader: fake.NewClientBuilder().Build(),
				Options:   tree.Options{RootName: tc.rootName},
			}
			err := r.SetupWithManager(nil)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// A projection's own generation is object-local, so it says nothing about which
// source revision the copy reflects. The echoed number is what makes remote
// progress comparable from the management plane -- and it has to mean
// "materialized", not "received", or the hub reads work that has not happened.
func TestMaterializedSourceGeneration(t *testing.T) {
	projected := func(stamp string, held int64) *v1beta1.AcceleratorQuota {
		q := leaf("team", rootName)
		if stamp != "" {
			q.Annotations = map[string]string{
				v1beta1.AcceleratorQuotaSourceGenerationAnnotation: stamp,
			}
		}
		q.Status.SourceGeneration = held
		return q
	}
	materialized := func(ok bool) v1beta1.AcceleratorQuotaStatus {
		status := metav1.ConditionFalse
		if ok {
			status = metav1.ConditionTrue
		}
		return v1beta1.AcceleratorQuotaStatus{Conditions: []metav1.Condition{{
			Type:               v1beta1.AcceleratorQuotaMaterialized,
			Status:             status,
			Reason:             "Test",
			LastTransitionTime: metav1.Now(),
		}}}
	}

	tests := []struct {
		name    string
		current *v1beta1.AcceleratorQuota
		status  v1beta1.AcceleratorQuotaStatus
		want    int64
	}{
		{
			name:    "a materialized projection reports what it was given",
			current: projected("7", 0),
			status:  materialized(true),
			want:    7,
		},
		{
			// Received is not materialized. Reporting 9 here would tell the hub
			// its newest revision is live on a member that has not written it.
			name:    "a projection that has not materialized holds its last number",
			current: projected("9", 7),
			status:  materialized(false),
			want:    7,
		},
		{
			// An admin-authored node has no source anywhere else, so any number
			// here would be an invention.
			name:    "a locally authored node reports nothing",
			current: projected("", 0),
			status:  materialized(true),
			want:    0,
		},
		{
			// The annotation is the projector's to write, so a value that will
			// not parse was edited by hand. Zero would read as "nothing
			// materialized yet", which is worse than stale.
			name:    "an unparsable stamp holds the last number rather than resetting",
			current: projected("not-a-number", 4),
			status:  materialized(true),
			want:    4,
		},
		{
			name:    "no Materialized condition at all is not materialized",
			current: projected("9", 3),
			status:  v1beta1.AcceleratorQuotaStatus{},
			want:    3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := materializedSourceGeneration(tc.current, tc.status); got != tc.want {
				t.Errorf("materializedSourceGeneration() = %d, want %d", got, tc.want)
			}
		})
	}
}

// The echo has to keep moving. Every field written to status needs a matching
// entry in equalStatus, because the no-op short-circuit skips the patch
// entirely when it thinks nothing changed -- so a field it does not compare is
// written once and then frozen for the life of the object. That has already
// happened once in this package, to Budgets.
func TestSourceGenerationFollowsTheProjector(t *testing.T) {
	stamp := func(q *v1beta1.AcceleratorQuota, gen string) *v1beta1.AcceleratorQuota {
		q.Annotations = map[string]string{
			v1beta1.AcceleratorQuotaSourceGenerationAnnotation: gen,
		}
		q.Labels = map[string]string{v1beta1.AcceleratorQuotaOriginLabel: "hub-1"}
		return q
	}

	ctx := context.Background()
	r, c := materializingReconciler(t, &recordingBackend{},
		cohort(rootName, "", budget("100")),
		stamp(leaf("team", rootName, budget("100")), "7"),
	)

	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	var got v1beta1.AcceleratorQuota
	if err := c.Get(ctx, types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if got.Status.SourceGeneration != 7 {
		t.Fatalf("first pass: SourceGeneration = %d, want 7", got.Status.SourceGeneration)
	}

	// The hub re-projects a newer revision. Nothing else about the node moves --
	// same spec, same generation, same conditions -- so this second pass is
	// exactly the one a missing comparator entry would skip.
	got.Annotations[v1beta1.AcceleratorQuotaSourceGenerationAnnotation] = "8"
	if err := c.Update(ctx, &got); err != nil {
		t.Fatalf("Update() = %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if got.Status.SourceGeneration != 8 {
		t.Errorf("second pass: SourceGeneration = %d, want 8 -- the echo froze",
			got.Status.SourceGeneration)
	}
}
