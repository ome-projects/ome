package modelagent

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelLister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

// Exercise the real Scout update -> Gopher enqueue -> scheduler path. Indexers
// stand in for the informer cache; no Kubernetes or storage service is needed.
type reprioritizationFixture struct {
	cluster bool
	indexer cache.Indexer
	scout   *Scout
	gopher  *Gopher
	channel chan *GopherTask
}

func newReprioritizationFixture(t *testing.T, cluster bool, capacity int) *reprioritizationFixture {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	channel := make(chan *GopherTask, 10)
	logger := zap.NewNop().Sugar()
	g := &Gopher{taskQueue: newGopherTaskQueue(capacity), logger: logger}
	if cluster {
		g.clusterBaseModelLister = modelLister.NewClusterBaseModelLister(indexer)
	} else {
		g.baseModelLister = modelLister.NewBaseModelLister(indexer)
	}
	t.Cleanup(g.taskQueue.close)
	return &reprioritizationFixture{
		cluster: cluster, indexer: indexer, gopher: g, channel: channel,
		scout: &Scout{gopherChan: channel, logger: logger, nodeInfo: &corev1.Node{}},
	}
}

func (f *reprioritizationFixture) task(model *v1beta1.BaseModel, kind GopherTaskType) *GopherTask {
	task := &GopherTask{TaskType: kind, DownloadPriority: effectiveModelDownloadPriority(model.Spec.Storage, &model.Status)}
	if f.cluster {
		task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: model.ObjectMeta, Spec: model.Spec, Status: model.Status}
		task.ClusterBaseModel.Namespace = ""
	} else {
		task.BaseModel = model
	}
	return task
}

func (f *reprioritizationFixture) store(t *testing.T, model *v1beta1.BaseModel) {
	t.Helper()
	task := f.task(model, Download)
	if f.cluster {
		require.NoError(t, f.indexer.Update(task.ClusterBaseModel))
	} else {
		require.NoError(t, f.indexer.Update(task.BaseModel))
	}
}

func (f *reprioritizationFixture) update(t *testing.T, old, latest *v1beta1.BaseModel) {
	t.Helper()
	f.store(t, latest)
	if f.cluster {
		f.scout.updateClusterBaseModel(f.task(old, Download).ClusterBaseModel, f.task(latest, Download).ClusterBaseModel)
	} else {
		f.scout.updateBaseModel(old, latest)
	}
}

func (f *reprioritizationFixture) dispatch(t *testing.T, want GopherTaskType) {
	t.Helper()
	require.Len(t, f.channel, 1)
	task := <-f.channel
	require.Equal(t, want, task.TaskType)
	f.gopher.enqueueTask(task)
}

func priorityTestModel(priority v1beta1.ModelDownloadPriority, demand bool) *v1beta1.BaseModel {
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models", UID: "model-uid"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			StorageUri: ptr("hf://org/model"), DownloadPriority: &priority,
		}},
	}
	if demand {
		model.Status.DownloadScheduling = &v1beta1.ModelDownloadSchedulingStatus{ServingDemand: true, ReferenceCount: 1}
	}
	return model
}

func TestScoutPriorityChangesReorderExistingWork(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, capacity := range []int{1, 8} {
			for _, change := range []string{"demand-added", "demand-removed", "projection-disabled", "explicit-promoted", "explicit-demoted"} {
				t.Run(fmt.Sprintf("cluster=%t/capacity=%d/%s", cluster, capacity, change), func(t *testing.T) {
					f := newReprioritizationFixture(t, cluster, capacity)
					old := priorityTestModel(v1beta1.ModelDownloadPriorityBackground, false)
					if change == "demand-removed" || change == "projection-disabled" {
						old = priorityTestModel(v1beta1.ModelDownloadPriorityBackground, true)
					} else if change == "explicit-demoted" {
						old = priorityTestModel(v1beta1.ModelDownloadPriorityHigh, false)
					}
					latest := old.DeepCopy()
					switch change {
					case "demand-added":
						latest.Status.DownloadScheduling = &v1beta1.ModelDownloadSchedulingStatus{ServingDemand: true, ReferenceCount: 1}
					case "demand-removed":
						latest.Status.DownloadScheduling = &v1beta1.ModelDownloadSchedulingStatus{}
					case "projection-disabled":
						latest.Status.DownloadScheduling = nil
					case "explicit-promoted":
						latest.Spec.Storage.DownloadPriority = ptr(v1beta1.ModelDownloadPriorityHigh)
					case "explicit-demoted":
						latest.Spec.Storage.DownloadPriority = ptr(v1beta1.ModelDownloadPriorityBackground)
					}
					f.store(t, old)
					queued := f.task(old, DownloadOverride)
					queued.NormalPriorityOnly = true
					queued.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
					queued.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{ShapeAlias: "a10"}
					peer := isolationTestTask("peer", "hf://org/peer", v1beta1.ModelDownloadPriorityStandard, Download)
					f.gopher.enqueueTask(peer)
					f.gopher.enqueueTask(queued)
					f.update(t, old, latest)
					f.dispatch(t, Reprioritize)
					require.Equal(t, 2, f.gopher.taskQueue.len())
					wantPriority := effectiveModelDownloadPriority(latest.Spec.Storage, &latest.Status)
					first, ok := f.gopher.taskQueue.popNormal()
					require.True(t, ok)
					second, ok := f.gopher.taskQueue.popNormal()
					require.True(t, ok)
					actual := first
					if wantPriority == v1beta1.ModelDownloadPriorityBackground {
						assert.Same(t, peer, first)
						actual = second
					} else {
						assert.Same(t, peer, second)
					}
					expected := *queued
					expected.DownloadPriority = wantPriority
					assert.Equal(t, &expected, actual, "change scheduling only, preserving artifact inputs and retry intent")
					assert.Zero(t, f.gopher.taskQueue.len())
				})
			}
		}
	}
}

func TestScoutPriorityOnlyDoesNotRestartDownloads(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, state := range []string{"ready", "running", "deleting", "recreated", "ineligible"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, state), func(t *testing.T) {
				f := newReprioritizationFixture(t, cluster, 1)
				old := priorityTestModel(v1beta1.ModelDownloadPriorityStandard, false)
				if state == "ready" {
					old.Status.State = v1beta1.LifeCycleStateReady
				} else if state == "ineligible" {
					old.Spec.Storage.NodeSelector = map[string]string{"gpu": "other-node"}
				}
				latest := old.DeepCopy()
				latest.Spec.Storage.DownloadPriority = ptr(v1beta1.ModelDownloadPriorityHigh)
				f.update(t, old, latest)
				if state == "ineligible" {
					require.Empty(t, f.channel)
					return
				}
				switch state {
				case "running":
					f.gopher.activeDownloads = map[string]activeDownload{"model-uid": {token: "active", cancel: func() { t.Error("priority update canceled an active transfer") }}}
				case "deleting":
					f.gopher.enqueueTask(f.task(old, Delete))
				case "recreated":
					replacement := old.DeepCopy()
					replacement.UID = "replacement-uid"
					f.store(t, replacement)
					f.gopher.enqueueTask(f.task(replacement, Download))
				}
				f.dispatch(t, Reprioritize)
				switch state {
				case "deleting":
					require.Equal(t, 1, f.gopher.taskQueue.len())
					actual, ok := f.gopher.taskQueue.popHighPriority()
					require.True(t, ok)
					assert.Equal(t, Delete, actual.TaskType)
				case "recreated":
					require.Equal(t, 1, f.gopher.taskQueue.len())
					actual, ok := f.gopher.taskQueue.popNormal()
					require.True(t, ok)
					assert.Equal(t, "replacement-uid", getModelUID(actual))
					assert.Equal(t, v1beta1.ModelDownloadPriorityStandard, actual.DownloadPriority)
				default:
					assert.Zero(t, f.gopher.taskQueue.len(), "priority-only events must not create download work")
				}
			})
		}
	}
}

func TestScoutSchedulingUpdatesDoNotOverrideOtherChanges(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, change := range []string{"reference-count", "explicit-high-with-demand", "artifact", "delete", "eligibility-loss"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, change), func(t *testing.T) {
				f := newReprioritizationFixture(t, cluster, 8)
				old := priorityTestModel(v1beta1.ModelDownloadPriorityStandard, true)
				latest := old.DeepCopy()
				want := GopherTaskType("")
				switch change {
				case "reference-count":
					latest.Status.DownloadScheduling.ReferenceCount = 2
				case "explicit-high-with-demand":
					latest.Spec.Storage.DownloadPriority = ptr(v1beta1.ModelDownloadPriorityHigh)
				case "artifact":
					latest.Spec.Storage.StorageUri = ptr("hf://org/replacement")
					latest.Status.DownloadScheduling = nil
					want = DownloadOverride
				case "delete":
					now := metav1.Now()
					latest.DeletionTimestamp = &now
					latest.Status.DownloadScheduling = nil
					want = Delete
				case "eligibility-loss":
					latest.Spec.Storage.NodeSelector = map[string]string{"gpu": "other-node"}
					latest.Status.DownloadScheduling = nil
					want = Delete
				}
				f.update(t, old, latest)
				if want == "" {
					assert.Empty(t, f.channel)
				} else {
					f.dispatch(t, want)
				}
			})
		}
	}
}

func TestPriorityRefreshForDelayedRetriesAndStaleEvents(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, demand := range []bool{false, true} {
			t.Run(fmt.Sprintf("cluster=%t/demand=%t", cluster, demand), func(t *testing.T) {
				f := newReprioritizationFixture(t, cluster, 8)
				old := priorityTestModel(v1beta1.ModelDownloadPriorityBackground, !demand)
				latest := priorityTestModel(v1beta1.ModelDownloadPriorityBackground, demand)
				f.store(t, latest)
				retry := f.task(old, Download)
				retry.NormalPriorityOnly = true
				retry.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
				f.gopher.enqueueTask(retry)
				f.gopher.enqueueTask(f.task(old, Reprioritize))
				require.Equal(t, 1, f.gopher.taskQueue.len())
				actual, ok := f.gopher.taskQueue.popNormal()
				require.True(t, ok)
				assert.Equal(t, effectiveModelDownloadPriority(latest.Spec.Storage, &latest.Status), actual.DownloadPriority)
				assert.Equal(t, retry.SamePathWaitStartedAt, actual.SamePathWaitStartedAt)
				assert.Equal(t, effectiveModelDownloadPriority(old.Spec.Storage, &old.Status), retry.DownloadPriority, "do not mutate worker-owned retry")
			})
		}
	}
}

func TestScoutPriorityUpdateThroughDispatcher(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprintf("cluster=%t", cluster), func(t *testing.T) {
			f := newReprioritizationFixture(t, cluster, 1)
			old := priorityTestModel(v1beta1.ModelDownloadPriorityBackground, false)
			f.store(t, old)
			peer := isolationTestTask("peer", "hf://org/peer", v1beta1.ModelDownloadPriorityStandard, Download)
			f.gopher.enqueueTask(peer)
			f.gopher.enqueueTask(f.task(old, Download))
			f.gopher.gopherChan = f.channel
			stop, done := make(chan struct{}), make(chan struct{})
			go func() { f.gopher.dispatchTasks(stop); close(done) }()
			t.Cleanup(func() {
				close(stop)
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("dispatcher did not stop")
				}
			})
			latest := priorityTestModel(v1beta1.ModelDownloadPriorityBackground, true)
			f.update(t, old, latest)
			require.Eventually(t, func() bool {
				f.gopher.taskQueue.mutex.Lock()
				defer f.gopher.taskQueue.mutex.Unlock()
				return len(f.gopher.taskQueue.urgentDownload) == 1
			}, time.Second, time.Millisecond)
			actual, ok := f.gopher.taskQueue.popNormal()
			require.True(t, ok)
			assert.Equal(t, "model-uid", getModelUID(actual))
			actual, ok = f.gopher.taskQueue.popNormal()
			require.True(t, ok)
			assert.Same(t, peer, actual)
			assert.Zero(t, f.gopher.taskQueue.len())
		})
	}
}

func TestQueueReprioritizationPreservesSameLaneFIFO(t *testing.T) {
	for _, lane := range []string{"reuse", "startup-revalidation"} {
		t.Run(lane, func(t *testing.T) {
			f := newReprioritizationFixture(t, false, 1)
			old := priorityTestModel(v1beta1.ModelDownloadPriorityStandard, false)
			first := f.task(old, Download)
			peer := isolationTestTask("peer", "hf://org/peer", v1beta1.ModelDownloadPriorityStandard, Download)
			if lane == "reuse" {
				first.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
				peer.SamePathWaitStartedAt = time.Now()
			} else {
				first.RevalidationReplay, peer.RevalidationReplay = true, true
				first.NormalPriorityOnly, peer.NormalPriorityOnly = true, true
			}
			f.gopher.taskQueue.enqueue(first)
			f.gopher.taskQueue.enqueue(peer)
			latest := priorityTestModel(v1beta1.ModelDownloadPriorityStandard, true)
			f.update(t, old, latest)
			f.dispatch(t, Reprioritize)
			pop := f.gopher.taskQueue.popNormal
			if lane == "reuse" {
				pop = f.gopher.taskQueue.popHighPriority
			}
			actual, ok := pop()
			require.True(t, ok)
			expected := *first
			expected.DownloadPriority = v1beta1.ModelDownloadPriorityHigh
			assert.Equal(t, &expected, actual)
			actual, ok = pop()
			require.True(t, ok)
			assert.Same(t, peer, actual)
		})
	}
}
