package modeleviction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/artifactinventory"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/evictionplanner"
	"sigs.k8s.io/ome/pkg/modelusage"
)

var roundTime = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

type capacityFunc func(context.Context, *corev1.Node) (CapacitySample, error)

func (f capacityFunc) Read(ctx context.Context, n *corev1.Node) (CapacitySample, error) {
	return f(ctx, n)
}

type inventoryFunc func(context.Context, string) (*artifactinventory.Inventory, error)

func (f inventoryFunc) Build(ctx context.Context, n string) (*artifactinventory.Inventory, error) {
	return f(ctx, n)
}

type evaluatorFunc func(context.Context, modelusage.Request, types.UID, bool) (evictionplanner.Eligibility, error)

func (f evaluatorFunc) Reconcile(ctx context.Context, r modelusage.Request, uid types.UID, inFlight bool) (evictionplanner.Eligibility, error) {
	return f(ctx, r, uid, inFlight)
}

type fakeIndexer struct{ builder *fake.ClientBuilder }

func (i fakeIndexer) IndexField(_ context.Context, obj client.Object, field string, extract client.IndexerFunc) error {
	i.builder.WithIndex(obj, field, extract)
	return nil
}

// Any write outside the one history annotation fails this dry-run integration test.
type historyOnlyClient struct{ client.Client }

func (c historyOnlyClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	raw, err := patch.Data(obj)
	if err != nil {
		return err
	}
	var data struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return err
	}
	if len(data.Metadata.Annotations) != 1 || data.Metadata.Annotations[constants.ModelUsageHistoryAnnotation] == "" {
		return fmt.Errorf("unexpected dry-run patch: %s", raw)
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func fakeSources(t *testing.T, objects ...client.Object) (client.Client, *modelusage.ReferenceQuery) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...)
	q := modelusage.NewReferenceQuery(nil, func() bool { return true }, modelusage.SourceOptions{})
	require.NoError(t, q.RegisterIndexes(context.Background(), fakeIndexer{b}))
	c := historyOnlyClient{b.Build()}
	return c, modelusage.NewReferenceQuery(c, func() bool { return true }, modelusage.SourceOptions{})
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"model-pool": "true"}}}
}

func sample(name string, available int64) CapacitySample {
	return CapacitySample{NodeName: name, CapacityBytes: 1000, AvailableBytes: available, ObservedAt: roundTime, ReadAt: roundTime}
}

func TestRoundIntegratesInventoryUsageAndBatchPlanner(t *testing.T) {
	ctx := context.Background()
	var objects []client.Object
	for _, name := range []string{"n1", "n2", "cool", "failed-capacity", "failed-mapping"} {
		objects = append(objects, node(name))
	}
	for _, name := range []string{"idle", "busy"} {
		m := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Namespace: "customer", Name: name, UID: types.UID(name), CreationTimestamp: metav1.NewTime(roundTime.Add(-time.Hour))}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: ptr.To("/models/" + name), StorageUri: ptr.To("oci://n/store/b/models/o/" + name), Parameters: &map[string]string{"region": "region"}}}}
		objects = append(objects, m, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ome.io/v1beta1", "kind": "ReplicationJob", "metadata": map[string]any{"namespace": "default", "name": name, "uid": "job-" + name},
			"spec":   map[string]any{"destination": map[string]any{"storageUri": *m.Spec.Storage.StorageUri, "parameters": map[string]any{"region": "region"}}},
			"status": map[string]any{"status": "Completed", "observed": map[string]any{"sourceArtifactSizeBytes": int64(200)}},
		}})
	}
	for _, name := range []string{"n1", "n2"} {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ome", Name: name, UID: types.UID(name)}, Data: map[string]string{}}
		for _, model := range []string{"idle", "busy"} {
			cm.Data[constants.GetModelConfigMapKey("customer", model, false)] = fmt.Sprintf(`{"name":%q,"status":"Ready","modelUID":%q}`, model, model)
		}
		objects = append(objects, cm)
	}
	objects = append(objects, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "customer", Name: "endpoint"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "busy"}}})
	c, q := fakeSources(t, objects...)
	inventory := artifactinventory.NewBuilder(c, "ome")
	eval := modelusage.NewEvaluator(c, c, q, func() time.Time { return roundTime })
	calls := map[string]int{}
	var mu sync.Mutex
	inventoryReads := inventoryFunc(func(ctx context.Context, n string) (*artifactinventory.Inventory, error) {
		mu.Lock()
		calls[n]++
		mu.Unlock()
		return inventory.Build(ctx, n)
	})
	evaluations := 0
	evals := evaluatorFunc(func(ctx context.Context, r modelusage.Request, uid types.UID, inFlight bool) (evictionplanner.Eligibility, error) {
		evaluations++
		return eval.Reconcile(ctx, r, uid, inFlight)
	})
	capacity := capacityFunc(func(_ context.Context, n *corev1.Node) (CapacitySample, error) {
		if n.Name == "failed-capacity" {
			return CapacitySample{}, errors.New("stats unavailable")
		}
		available := int64(200)
		if n.Name == "n2" {
			available = 240
		}
		if n.Name == "cool" {
			available = 400
		}
		return sample(n.Name, available), nil
	})
	options := DefaultOptions()
	options.NodeSelector = "model-pool=true"
	controller, err := NewController(c, inventoryReads, evals, capacity, options)
	require.NoError(t, err)
	controller.clock = clocktesting.NewFakeClock(roundTime)
	report, err := controller.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, report.DryRun)
	require.Len(t, report.Observations, 5)
	require.Zero(t, calls["cool"])
	require.Zero(t, calls["failed-capacity"])
	require.Equal(t, 2, evaluations, "one current eligibility query per model, not per node")
	require.Len(t, report.Plan.Models, 1)
	require.Equal(t, "idle", report.Plan.Models[0].Name)
	statuses := map[string]string{}
	for _, n := range report.Plan.Nodes {
		statuses[n.Name] = n.Status
	}
	require.Equal(t, "skipped", statuses["n2"])
	require.Equal(t, "unevaluated", statuses["failed-mapping"])
	for _, name := range []string{"idle", "busy"} {
		m := &v1beta1.BaseModel{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "customer", Name: name}, m))
		require.Empty(t, m.Annotations["ome.io/artifact-residency"])
		if name == "idle" {
			require.NotEmpty(t, m.Annotations[constants.ModelUsageHistoryAnnotation])
		}
	}
}

func TestRoundDropsExpiredSamplesAfterMapping(t *testing.T) {
	c, _ := fakeSources(t, node("n1"))
	clock := clocktesting.NewFakeClock(roundTime)
	options := DefaultOptions()
	options.NodeSelector = "model-pool=true"
	inventory := inventoryFunc(func(_ context.Context, n string) (*artifactinventory.Inventory, error) {
		clock.Step(options.MaxSampleAge + time.Second)
		return &artifactinventory.Inventory{NodeName: n}, nil
	})
	capacity := capacityFunc(func(_ context.Context, n *corev1.Node) (CapacitySample, error) { return sample(n.Name, 100), nil })
	controller, err := NewController(c, inventory, evaluatorFunc(func(context.Context, modelusage.Request, types.UID, bool) (evictionplanner.Eligibility, error) {
		t.Fatal("empty mapping must not evaluate models")
		return evictionplanner.Eligibility{}, nil
	}), capacity, options)
	require.NoError(t, err)
	controller.clock = clock
	report, err := controller.RunOnce(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Plan.Models)
	require.Empty(t, report.Plan.Nodes)
	require.NotEmpty(t, report.Observations[0].Error)
}

func TestOptionsRejectUnsafeOrInvalidInputs(t *testing.T) {
	for _, change := range []func(*Options){
		func(o *Options) { o.NodeSelector = "" }, func(o *Options) { o.NodeSelector = "bad===" },
		func(o *Options) { o.HighWatermark = math.NaN() }, func(o *Options) { o.LowWatermark = o.HighWatermark },
		func(o *Options) { o.Interval = 0 }, func(o *Options) { o.Workers = 0 }, func(o *Options) { o.RoundTimeout = 0 },
	} {
		options := DefaultOptions()
		options.NodeSelector = "model-pool=true"
		change(&options)
		_, err := NewController(nil, nil, nil, nil, options)
		require.Error(t, err)
	}
}
