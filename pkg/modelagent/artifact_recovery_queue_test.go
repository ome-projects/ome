package modelagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeinformers "sigs.k8s.io/ome/pkg/client/informers/externalversions"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestArtifactRecoveryBacklogQuiescesAfterCurrentAcknowledgement(t *testing.T) {
	g, initial := newAcknowledgementGopher(t)
	g.metrics = NewMetrics(prometheus.NewRegistry())
	ctx := context.Background()
	sha := strings.Repeat("a", 40)
	initial.BaseModel.Spec.Storage.StorageUri = stringPtr("hf://org/model@" + sha)
	g.modelClient = omefake.NewSimpleClientset(initial.BaseModel)
	manifest := testHfSnapshotManifest(map[string]string{"weights": "weights"})
	manifest.SHA = sha
	entered := make(chan struct{}, 10)
	release := make(chan struct{}, 10)
	var validations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validations.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(manifest)
	}))
	defer server.Close()
	g.xetConfig.Endpoint = server.URL
	factory := omeinformers.NewSharedInformerFactory(g.modelClient, 0)
	models, clusters := factory.Ome().V1beta1().BaseModels(), factory.Ome().V1beta1().ClusterBaseModels()
	require.NoError(t, models.Informer().GetIndexer().Add(initial.BaseModel))
	incoming := make(chan *GopherTask, 20)
	scout := &Scout{ctx: ctx, kubeClient: g.kubeClient, nodeName: g.configMapReconciler.nodeName,
		configMapNamespace: g.configMapReconciler.namespace, baseModelLister: models.Lister(),
		clusterBaseModelLister: clusters.Lister(), gopherChan: incoming, logger: g.logger}
	tick := func() {
		require.NoError(t, scout.reconcileArtifactAcknowledgements(ctx))
		for len(incoming) != 0 {
			g.enqueueTask(<-incoming)
		}
	}
	tick()
	require.Equal(t, 1, g.taskQueue.len())
	for round := 0; round < 3; round++ {
		if g.taskQueue.len() == 0 {
			break
		}
		next, ok := g.taskQueue.popNormal()
		require.True(t, ok)
		done := make(chan error, 1)
		go func() { done <- g.processTask(next) }()
		select {
		case <-entered:
		case err := <-done:
			require.NoError(t, err)
			require.Greater(t, round, 0, "missing acknowledgement must validate at least once")
			continue // A now-satisfied periodic recovery may complete without revalidation.
		case <-time.After(5 * time.Second):
			t.Fatal("validation did not start")
		}
		node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, string(Updating), node.Labels[constants.GetBaseModelLabel(initial.BaseModel.Namespace, initial.BaseModel.Name)])
		require.Equal(t, ModelStatusUpdating, directArtifactEntry(t, g, next).Status)
		// Two recovery ticks while a healthy manifest validation is in flight.
		for tickNumber := 0; tickNumber < 20; tickNumber++ {
			tick()
		}
		pendingWhileActive := g.taskQueue.len()
		release <- struct{}{}
		require.NoError(t, <-done)
		require.LessOrEqual(t, pendingWhileActive, 1, "periodic ticks coalesce while validation is active")
		entry := directArtifactEntry(t, g, next)
		require.Equal(t, ModelStatusReady, entry.Status)
		require.Equal(t, "request-1", entry.ArtifactRehydrationID)
		queued := g.taskQueue.len()
		tick()
		require.Equal(t, queued, g.taskQueue.len(), "fresh Scout ticks recognize the complete acknowledgement")
		t.Logf("round %d: successful validation left %d pending identical recovery tasks", round+1, queued)
	}
	require.Equal(t, int32(1), validations.Load(), "queued recovery must not revalidate and withdraw a now-current acknowledgement")
	require.Zero(t, g.taskQueue.len(), "completed recovery must quiesce instead of replenishing its backlog")
}

func TestSatisfiedArtifactRecoveryDoesNotSupersedeExplicitOverride(t *testing.T) {
	g, task, scout, incoming, validations := newArtifactRecoveryQueueFixture(t)
	ctx := context.Background()
	require.NoError(t, scout.reconcileArtifactAcknowledgements(ctx))
	recovery := <-incoming
	explicit := *task
	// Sticky shared admission also applies after a Model opted out of reuse.
	// Its existing sequence fence must not be advanced by a satisfied no-op.
	explicit.SharedArtifact, recovery.SharedArtifact = true, true
	g.enqueueTask(&explicit)
	g.enqueueTask(recovery)
	first, ok := g.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, &explicit, first)
	second, ok := g.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, recovery, second)
	// Another completed validation supplied the acknowledgement while both
	// tasks were pending. A later worker picks up recovery first.
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	require.NoError(t, g.processTask(second))
	require.Zero(t, validations.Load(), "satisfied periodic work must not validate or enter task admission")
	require.NoError(t, g.processTask(first))
	require.Equal(t, int32(1), validations.Load(), "the earlier explicit override must still validate")
}

func TestArtifactRecoveryMissingEvidenceStillValidates(t *testing.T) {
	for _, missing := range []string{"label", "ConfigMap"} {
		t.Run(missing, func(t *testing.T) {
			g, task, scout, incoming, validations := newArtifactRecoveryQueueFixture(t)
			ctx := context.Background()
			require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
			if missing == "ConfigMap" {
				require.NoError(t, g.kubeClient.CoreV1().ConfigMaps(g.configMapReconciler.namespace).Delete(ctx, g.configMapReconciler.nodeName, metav1.DeleteOptions{}))
			} else {
				node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				delete(node.Labels, constants.GetModelArtifactRequestLabel(task.BaseModel.UID))
				_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			require.NoError(t, scout.reconcileArtifactAcknowledgements(ctx))
			require.Len(t, incoming, 1)
			require.NoError(t, g.processTask(<-incoming))
			require.Equal(t, int32(1), validations.Load())
			require.NoError(t, scout.reconcileArtifactAcknowledgements(ctx))
			require.Empty(t, incoming)
		})
	}
}

func TestArtifactRecoveryCoalescingKeepsLatestIntentAndExplicitWork(t *testing.T) {
	g, task, scout, incoming, _ := newArtifactRecoveryQueueFixture(t)
	require.NoError(t, scout.reconcileArtifactAcknowledgements(context.Background()))
	recovery := <-incoming
	explicit := *task
	explicit.TaskType = DownloadOverride
	g.enqueueTask(&explicit)
	g.enqueueTask(recovery)
	latest := *recovery
	latest.Sequence = 0
	latest.BaseModel = recovery.BaseModel.DeepCopy()
	latest.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "newer"
	latest.SamePathWaitStartedAt = time.Now() // Cover coalescing across priority buckets.
	g.enqueueTask(&latest)
	// A delayed retry cannot displace a newer periodic intent.
	g.enqueueTask(recovery)
	require.Equal(t, 2, g.taskQueue.len())
	got, ok := g.taskQueue.popHighPriority()
	require.True(t, ok)
	require.Same(t, &latest, got)
	got, ok = g.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, &explicit, got)
	require.Zero(t, g.taskQueue.len())
}

func newArtifactRecoveryQueueFixture(t *testing.T) (*Gopher, *GopherTask, *Scout, chan *GopherTask, *atomic.Int32) {
	t.Helper()
	g, task := newAcknowledgementGopher(t)
	g.metrics = NewMetrics(prometheus.NewRegistry())
	sha := strings.Repeat("a", 40)
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("hf://org/model@" + sha)
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	manifest := testHfSnapshotManifest(map[string]string{"weights": "weights"})
	manifest.SHA = sha
	validations := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		validations.Add(1)
		_ = json.NewEncoder(w).Encode(manifest)
	}))
	t.Cleanup(server.Close)
	g.xetConfig.Endpoint = server.URL
	factory := omeinformers.NewSharedInformerFactory(g.modelClient, 0)
	models, clusters := factory.Ome().V1beta1().BaseModels(), factory.Ome().V1beta1().ClusterBaseModels()
	require.NoError(t, models.Informer().GetIndexer().Add(task.BaseModel))
	incoming := make(chan *GopherTask, 20)
	scout := &Scout{ctx: context.Background(), kubeClient: g.kubeClient, nodeName: g.configMapReconciler.nodeName,
		configMapNamespace: g.configMapReconciler.namespace, baseModelLister: models.Lister(),
		clusterBaseModelLister: clusters.Lister(), gopherChan: incoming, logger: g.logger}
	return g, task, scout, incoming, validations
}
