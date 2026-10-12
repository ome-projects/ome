package modelusage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/artifactinventory"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/evictionplanner"
)

type testClient struct {
	client.Client
	gets, patches, lists, unindexedLists int
	getError, listError                  error
	getHook                              func(client.Object)
	patchHook                            func(client.Object, client.Patch) error
}

func (c *testClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.gets++
	if c.getError != nil {
		return c.getError
	}
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if c.getHook != nil {
		c.getHook(obj)
	}
	return nil
}

func (c *testClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.lists++
	options := &client.ListOptions{}
	options.ApplyOptions(opts)
	if options.FieldSelector == nil || options.FieldSelector.Empty() || options.Namespace == "" {
		c.unindexedLists++
	}
	if c.listError != nil {
		return c.listError
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *testClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	c.patches++
	if c.patchHook != nil {
		if err := c.patchHook(obj, p); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, obj, p, opts...)
}

func baseModel(raw string) *v1beta1.BaseModel {
	m := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{
		Namespace:         modelKey.Namespace,
		Name:              modelKey.Name,
		UID:               "model-uid",
		ResourceVersion:   "7",
		CreationTimestamp: metav1.NewTime(testTime.Add(-time.Hour)),
		Annotations:       map[string]string{},
	}}
	if raw != "" {
		m.Annotations[constants.ModelUsageHistoryAnnotation] = raw
	}
	return m
}

func readHistory(t *testing.T, c client.Reader, key types.NamespacedName) map[string]string {
	t.Helper()
	m := &v1beta1.BaseModel{}
	if err := c.Get(context.Background(), key, m); err != nil {
		t.Fatal(err)
	}
	h := map[string]string{}
	if raw := m.Annotations[constants.ModelUsageHistoryAnnotation]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &h); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

func evaluator(c *testClient, q *ReferenceQuery, now *time.Time) *Evaluator {
	return NewEvaluator(c, c, q, func() time.Time { return *now })
}

func reconcile(t *testing.T, e *Evaluator, m *v1beta1.BaseModel, at time.Time) evictionplanner.Eligibility {
	t.Helper()
	r, err := e.Reconcile(context.Background(), Request{Model: client.ObjectKeyFromObject(m), ReferenceRemovedAt: at}, m.UID, false)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestIdleInitializationConditionalPatchAndRestart(t *testing.T) {
	now := testTime
	m := baseModel("")
	m.Annotations["keep"] = "annotation"
	m.Labels = map[string]string{"keep": "label"}
	m.Finalizers = []string{"keep"}
	c, q := queryClient(t, m)
	c.patchHook = func(obj client.Object, p client.Patch) error {
		data, err := p.Data(obj)
		if err != nil {
			return err
		}
		var body struct {
			Metadata struct {
				UID             types.UID         `json:"uid"`
				ResourceVersion string            `json:"resourceVersion"`
				Annotations     map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(data, &body); err != nil {
			return err
		}
		if p.Type() != types.MergePatchType || body.Metadata.UID != m.UID ||
			body.Metadata.ResourceVersion != "7" || len(body.Metadata.Annotations) != 1 ||
			body.Metadata.Annotations[constants.ModelUsageHistoryAnnotation] == "" {
			t.Fatalf("not a conditional history-only patch: %s", data)
		}
		return nil
	}
	r := reconcile(t, evaluator(c, q, &now), m, time.Time{})
	if !r.Eligible || r.IdleSince == nil || !r.IdleSince.Equal(now) {
		t.Fatalf("initial idle: %+v", r)
	}
	h := readHistory(t, c, modelKey)
	if h["idleSince"] != now.Format(time.RFC3339) || h["lastObservedDemandEndedAt"] != "" {
		t.Fatalf("invented end: %v", h)
	}
	stored := &v1beta1.BaseModel{}
	if err := c.Get(context.Background(), modelKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Annotations["keep"] != "annotation" || stored.Labels["keep"] != "label" || len(stored.Finalizers) != 1 {
		t.Fatal("unrelated metadata changed")
	}
	now = now.Add(time.Hour)
	r = reconcile(t, evaluator(c, q, &now), m, time.Time{})
	if !r.IdleSince.Equal(testTime) || c.patches != 1 {
		t.Fatalf("restart reset idle: %+v patches=%d", r, c.patches)
	}
}

func TestReleaseRequiresAllServicesAndResidualWorkloadsGone(t *testing.T) {
	now := testTime
	m := baseModel(`{"idleSince":"2026-10-12T11:00:00Z"}`)
	one := service("one", "model")
	two := service("two", "model")
	pod := &corev1.Pod{ObjectMeta: workload("pod", "model")}
	c, q := queryClient(t, m, one, two, pod)
	e := evaluator(c, q, &now)
	for _, obj := range []client.Object{one, two} {
		if err := c.Delete(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
		r := reconcile(t, e, m, now)
		if r.Eligible || c.patches != 0 {
			t.Fatalf("remaining refs released model: %+v patches=%d", r, c.patches)
		}
		now = now.Add(time.Minute)
	}
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	removed := now
	now = now.Add(time.Hour)
	r := reconcile(t, e, m, removed)
	h := readHistory(t, c, modelKey)
	if !r.Eligible || !r.IdleSince.Equal(removed) || h["lastObservedDemandEndedAt"] != removed.Format(time.RFC3339) {
		t.Fatalf("last release: %+v %v", r, h)
	}
	// Retry the same event later, and then an older event: neither resets history.
	for _, at := range []time.Time{removed, removed.Add(-time.Minute)} {
		now = now.Add(time.Hour)
		r = reconcile(t, e, m, at)
		if !r.IdleSince.Equal(removed) || c.patches != 1 {
			t.Fatalf("retry reset end: %+v patches=%d", r, c.patches)
		}
	}
	newer := removed.Add(time.Minute)
	r = reconcile(t, e, m, newer)
	if !r.IdleSince.Equal(newer) || c.patches != 2 {
		t.Fatalf("newer release ignored: %+v", r)
	}
}

func TestHistoryPriorityAndIndependentValidation(t *testing.T) {
	old := testTime.Add(-30 * time.Minute).Format(time.RFC3339)
	future := testTime.Add(time.Hour).Format(time.RFC3339)
	for _, tc := range []struct {
		name, raw, want string
		patches         int
	}{
		{
			"end takes precedence",
			fmt.Sprintf(`{ "idleSince":%q,"lastObservedDemandEndedAt":%q,"extra":true }`, testTime.Add(-time.Minute).Format(time.RFC3339), old),
			old, 0,
		},
		{"end alone", fmt.Sprintf(`{"lastObservedDemandEndedAt":%q}`, old), old, 0},
		{"bad idle valid end", fmt.Sprintf(`{"idleSince":"bad","lastObservedDemandEndedAt":%q}`, old), old, 1},
		{"valid idle bad end", fmt.Sprintf(`{"idleSince":%q,"lastObservedDemandEndedAt":42}`, old), old, 1},
		{"future", fmt.Sprintf(`{"idleSince":%q,"lastObservedDemandEndedAt":%q}`, future, future), testTime.Format(time.RFC3339), 1},
		{"pre-creation", `{"idleSince":"2026-10-12T10:00:00Z","lastObservedDemandEndedAt":"2026-10-12T10:00:00Z"}`, testTime.Format(time.RFC3339), 1},
		{"bad json", "bad", testTime.Format(time.RFC3339), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := testTime
			m := baseModel(tc.raw)
			c, q := queryClient(t, m)
			r := reconcile(t, evaluator(c, q, &now), m, time.Time{})
			if !r.Eligible || r.IdleSince.Format(time.RFC3339) != tc.want || c.patches != tc.patches {
				t.Fatalf("history: %+v patches=%d", r, c.patches)
			}
			if tc.patches == 0 {
				stored := &v1beta1.BaseModel{}
				_ = c.Get(context.Background(), modelKey, stored)
				if stored.Annotations[constants.ModelUsageHistoryAnnotation] != tc.raw {
					t.Fatal("valid bytes reformatted")
				}
			}
		})
	}
}

func TestActiveDemandPreservesHistoryAndConsumesRelease(t *testing.T) {
	now := testTime
	raw := `{"idleSince":"2026-10-12T11:00:00Z","lastObservedDemandEndedAt":"2026-10-12T11:10:00Z"}`
	m := baseModel(raw)
	c, q := queryClient(t, m, service("active", "model"))
	r := reconcile(t, evaluator(c, q, &now), m, now)
	if r.Eligible || c.patches != 0 {
		t.Fatalf("active model history changed: %+v patches=%d", r, c.patches)
	}
	stored := &v1beta1.BaseModel{}
	_ = c.Get(context.Background(), modelKey, stored)
	if stored.Annotations[constants.ModelUsageHistoryAnnotation] != raw {
		t.Fatal("active history mutated")
	}
}

func TestInvalidReleaseDoesNotInventDemandEnd(t *testing.T) {
	for _, at := range []time.Time{testTime.Add(-2 * time.Hour), testTime.Add(time.Hour)} {
		now := testTime
		m := baseModel("")
		c, q := queryClient(t, m)
		r := reconcile(t, evaluator(c, q, &now), m, at)
		h := readHistory(t, c, modelKey)
		if !r.Eligible || !r.IdleSince.Equal(now) || h["lastObservedDemandEndedAt"] != "" {
			t.Fatalf("invalid release accepted: %+v %v", r, h)
		}
	}
}

func TestConflictRefreshesModelAndCurrentReferences(t *testing.T) {
	now := testTime
	m := baseModel("")
	c, q := queryClient(t, m)
	firstLists := 0
	c.patchHook = func(_ client.Object, _ client.Patch) error {
		if c.patches == 1 {
			firstLists = c.lists
			if err := c.Create(context.Background(), service("new-use", "model")); err != nil {
				return err
			}
			fresh := &v1beta1.BaseModel{}
			if err := c.Client.Get(context.Background(), modelKey, fresh); err != nil {
				return err
			}
			fresh.Annotations = map[string]string{"concurrent": "keep"}
			if err := c.Update(context.Background(), fresh); err != nil {
				return err
			}
		}
		return nil // The fake API rejects this stale resourceVersion.
	}
	r := reconcile(t, evaluator(c, q, &now), m, now)
	if r.Eligible || c.gets != 2 || c.patches != 1 || c.lists <= firstLists {
		t.Fatalf("stale query reused: %+v gets=%d patches=%d lists=%d", r, c.gets, c.patches, c.lists)
	}
	h := readHistory(t, c, modelKey)
	if len(h) != 0 {
		t.Fatalf("stale history written: %v", h)
	}
}

func TestFailureProtectsAndConflictsAreBounded(t *testing.T) {
	for _, mode := range []string{"get", "list", "unsynced", "patch", "conflict", "unresolved"} {
		t.Run(mode, func(t *testing.T) {
			now := testTime
			m := baseModel("")
			c, q := queryClient(t, m)
			switch mode {
			case "get":
				c.getError = errors.New("read failed")
			case "list":
				c.listError = errors.New("list failed")
			case "unsynced":
				q.hasSynced = func() bool { return false }
			case "patch":
				c.patchHook = func(client.Object, client.Patch) error { return errors.New("patch failed") }
			case "conflict":
				c.patchHook = func(client.Object, client.Patch) error {
					return apierrors.NewConflict(schema.GroupResource{Resource: "basemodels"}, m.Name, errors.New("changed"))
				}
			case "unresolved":
				if err := c.Create(context.Background(), &corev1.Pod{ObjectMeta: workload("unknown", "")}); err != nil {
					t.Fatal(err)
				}
			}
			r, err := evaluator(c, q, &now).Reconcile(context.Background(), Request{Model: modelKey, ReferenceRemovedAt: now}, m.UID, false)
			if r.Eligible || r.IdleSince != nil || err == nil {
				t.Fatalf("failure accepted: %+v %v", r, err)
			}
			if mode == "conflict" && (c.patches != 3 || c.gets != 3) {
				t.Fatalf("attempts gets=%d patches=%d", c.gets, c.patches)
			}
			if mode != "patch" && mode != "conflict" && c.patches != 0 {
				t.Fatal("history patched despite uncertain references")
			}
		})
	}
}

func TestUIDReplacementAndIncompleteIdentityProtect(t *testing.T) {
	for _, mode := range []string{"expected UID", "missing UID", "missing version", "replacement after conflict", "history-only replacement after conflict"} {
		t.Run(mode, func(t *testing.T) {
			now := testTime
			m := baseModel("")
			c, q := queryClient(t, m)
			expected := m.UID
			switch mode {
			case "expected UID":
				expected = "prior-uid"
			case "missing UID":
				c.getHook = func(obj client.Object) { obj.SetUID("") }
			case "missing version":
				c.getHook = func(obj client.Object) { obj.SetResourceVersion("") }
			case "replacement after conflict", "history-only replacement after conflict":
				if mode == "history-only replacement after conflict" {
					expected = ""
				}
				c.patchHook = func(client.Object, client.Patch) error {
					if err := c.Delete(context.Background(), m); err != nil {
						return err
					}
					replacement := baseModel("")
					replacement.UID = "replacement"
					replacement.ResourceVersion = ""
					if err := c.Create(context.Background(), replacement); err != nil {
						return err
					}
					return apierrors.NewConflict(schema.GroupResource{Resource: "basemodels"}, m.Name, errors.New("replaced"))
				}
			}
			r, err := evaluator(c, q, &now).Reconcile(context.Background(), Request{Model: modelKey, ReferenceRemovedAt: now}, expected, false)
			if r.Eligible || err == nil {
				t.Fatalf("identity accepted: %+v %v", r, err)
			}
			if !strings.Contains(mode, "replacement after conflict") && c.patches != 0 {
				t.Fatal("patched incomplete identity")
			}
		})
	}
}

func TestLifecycleExclusionsDoNotWriteHistory(t *testing.T) {
	for _, mode := range []string{"deleting", "vendor", "reserved", "evicted", "in flight"} {
		t.Run(mode, func(t *testing.T) {
			now := testTime
			m := baseModel("")
			switch mode {
			case "deleting":
				m.DeletionTimestamp = &metav1.Time{Time: now}
				m.Finalizers = []string{"keep"}
			case "vendor":
				uri := "vendor://model"
				m.Spec.Storage = &v1beta1.StorageSpec{StorageUri: &uri}
			case "reserved":
				m.Labels = map[string]string{constants.ReserveModelArtifact: "TRUE"}
			case "evicted":
				m.Annotations["ome.io/artifact-residency"] = "Evicted"
			}
			c, q := queryClient(t, m)
			r, err := evaluator(c, q, &now).Reconcile(context.Background(), Request{Model: modelKey, ReferenceRemovedAt: now}, m.UID, mode == "in flight")
			if err != nil || r.Eligible || r.Reason == "" || c.patches != 0 {
				t.Fatalf("exclusion failed: %+v %v patches=%d", r, err, c.patches)
			}
		})
	}
}

func TestProtectedReferenceExcludesSharedArtifactFromPlanner(t *testing.T) {
	now := testTime
	idle := baseModel(`{"idleSince":"2026-10-12T11:00:00Z"}`)
	used := idle.DeepCopy()
	used.Name = "used"
	used.UID = "used-uid"
	c, q := queryClient(t, idle, used, service("use", "used"))
	e := evaluator(c, q, &now)
	eligibility := map[types.UID]evictionplanner.Eligibility{idle.UID: reconcile(t, e, idle, time.Time{}), used.UID: reconcile(t, e, used, time.Time{})}
	size := int64(500)
	plan, err := evictionplanner.Build(evictionplanner.Input{
		Now:              now,
		EligibilityByUID: eligibility,
		Nodes: []evictionplanner.Node{{
			Name:           "node",
			CapacityBytes:  1000,
			AvailableBytes: 50,
			HighWatermark:  .9,
			LowWatermark:   .8,
			Inventory: &artifactinventory.Inventory{
				NodeName: "node",
				Artifacts: []artifactinventory.Artifact{{
					Path:                   "/models/shared",
					EstimatedArtifactBytes: &size,
					EvictionEligible:       true,
					Models: []artifactinventory.ModelRef{
						{Namespace: idle.Namespace, Name: idle.Name, UID: idle.UID},
						{Namespace: used.Namespace, Name: used.Name, UID: used.UID},
					},
				}},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Models) != 0 || len(plan.Nodes[0].ExcludedArtifacts) != 1 || !strings.Contains(plan.Nodes[0].ExcludedArtifacts[0].Reason, "demand") {
		t.Fatalf("shared artifact selected: %+v", plan)
	}
}

func TestConflictRetryRetainsOriginalEventTime(t *testing.T) {
	now := testTime
	removedAt := now.Add(-time.Minute)
	m := baseModel("")
	c, q := queryClient(t, m)
	c.patchHook = func(client.Object, client.Patch) error {
		if c.patches == 1 {
			now = now.Add(time.Hour)
			return apierrors.NewConflict(schema.GroupResource{Resource: "basemodels"}, m.Name, errors.New("changed"))
		}
		return nil
	}
	result := reconcile(t, evaluator(c, q, &now), m, removedAt)
	if !result.Eligible || !result.IdleSince.Equal(removedAt) || c.patches != 2 {
		t.Fatalf("retry changed event timestamp: %+v patches=%d", result, c.patches)
	}
	h := readHistory(t, c, modelKey)
	if h["lastObservedDemandEndedAt"] != removedAt.Format(time.RFC3339) {
		t.Fatalf("wrong persisted event time: %v", h)
	}
}
