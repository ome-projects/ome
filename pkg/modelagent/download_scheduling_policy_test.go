package modelagent

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestDownloadSchedulingPolicyValidation(t *testing.T) {
	for _, policy := range []string{"priority", "fifo"} {
		g := &Gopher{taskQueue: newGopherTaskQueue()}
		require.NoError(t, WithDownloadSchedulingPolicy(policy)(g))
		assert.Equal(t, policy, g.taskQueue.downloadSchedulingPolicy)
	}
	for _, policy := range []string{"", "FIFO", "fair", " fifo"} {
		g := &Gopher{taskQueue: newGopherTaskQueue()}
		require.ErrorContains(t, WithDownloadSchedulingPolicy(policy)(g), "expected priority or fifo")
		assert.Empty(t, g.taskQueue.downloadSchedulingPolicy)
	}
}

func TestFIFODownloadOrderIgnoresPrioritiesAndRevalidation(t *testing.T) {
	for _, capacity := range []int{1, 8} {
		for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
			t.Run(fmt.Sprintf("capacity=%d/%s", capacity, uri), func(t *testing.T) {
				g := &Gopher{taskQueue: newGopherTaskQueue(capacity)}
				require.NoError(t, WithDownloadSchedulingPolicy("fifo")(g))
				priorities := []v1beta1.ModelDownloadPriority{
					v1beta1.ModelDownloadPriorityBackground,
					v1beta1.ModelDownloadPriorityStandard,
					v1beta1.ModelDownloadPriorityHigh,
				}
				for i, priority := range priorities {
					task := isolationTestTask(string(priority), uri, priority, DownloadOverride)
					task.NormalPriorityOnly = true
					task.RevalidationReplay = i == 0
					require.True(t, g.taskQueue.enqueue(task).accepted)
					assert.Equal(t, priority, task.DownloadPriority, "do not mutate the producer's task")
				}
				for _, priority := range priorities {
					task, ok := g.taskQueue.popNormal()
					require.True(t, ok)
					assert.Equal(t, string(priority), getModelUID(task))
					assert.Equal(t, v1beta1.ModelDownloadPriorityStandard, task.DownloadPriority)
					assert.Equal(t, DownloadOverride, task.TaskType)
				}
				assert.Zero(t, g.taskQueue.len())
				g.taskQueue.close()
			})
		}
	}
}

func TestFIFOScoutPriorityOnlyPreservesWork(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, capacity := range []int{1, 8} {
			for _, demand := range []bool{false, true} {
				t.Run(fmt.Sprintf("cluster=%t/capacity=%d/demand=%t", cluster, capacity, demand), func(t *testing.T) {
					f := newReprioritizationFixture(t, cluster, capacity)
					require.NoError(t, WithDownloadSchedulingPolicy("fifo")(f.gopher))
					old := priorityTestModel(v1beta1.ModelDownloadPriorityBackground, false)
					latest := priorityTestModel(v1beta1.ModelDownloadPriorityHigh, demand)
					f.store(t, old)
					peer := isolationTestTask("peer", "hf://org/peer", v1beta1.ModelDownloadPriorityStandard, Download)
					f.gopher.enqueueTask(peer)
					queued := f.task(old, DownloadOverride)
					queued.NormalPriorityOnly = true
					queued.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
					f.gopher.enqueueTask(queued)
					f.update(t, old, latest)
					f.dispatch(t, Reprioritize)
					first, ok := f.gopher.taskQueue.popNormal()
					require.True(t, ok)
					assert.Equal(t, "peer", getModelUID(first))
					second, ok := f.gopher.taskQueue.popNormal()
					require.True(t, ok)
					expected := *queued
					expected.DownloadPriority = v1beta1.ModelDownloadPriorityStandard
					assert.Equal(t, &expected, second, "priority-only updates preserve queue order and retry intent")
					f.gopher.activeDownloads = map[string]activeDownload{"model-uid": {
						cancel: func() { t.Error("priority-only update canceled a download") },
					}}
					f.update(t, latest, old)
					f.dispatch(t, Reprioritize)
					assert.Zero(t, f.gopher.taskQueue.len(), "do not restart active or completed downloads")
				})
			}
		}
	}
}

func TestFIFOPreservesCleanupReuseAndUIDIsolation(t *testing.T) {
	f := newReprioritizationFixture(t, false, 1)
	require.NoError(t, WithDownloadSchedulingPolicy("fifo")(f.gopher))
	old := priorityTestModel(v1beta1.ModelDownloadPriorityHigh, true)
	replacement := old.DeepCopy()
	replacement.UID = "replacement-uid"
	f.store(t, replacement)
	canceled := false
	f.gopher.activeDownloads = map[string]activeDownload{"model-uid": {cancel: func() { canceled = true }}}
	f.gopher.enqueueTask(f.task(replacement, Download))
	f.gopher.enqueueTask(f.task(old, Delete))
	require.True(t, canceled, "deleting the old UID still cancels its transfer")
	deleted, ok := f.gopher.taskQueue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, Delete, deleted.TaskType)
	assert.Equal(t, "model-uid", getModelUID(deleted))
	queued, ok := f.gopher.taskQueue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "replacement-uid", getModelUID(queued), "old-UID cleanup must not discard its replacement")

	reuse := isolationTestTask("reuse", "oci://n/ns/b/bucket/o/model", v1beta1.ModelDownloadPriorityBackground, Download)
	require.True(t, f.gopher.taskQueue.enqueue(reuse).accepted)
	require.Len(t, f.gopher.taskQueue.high, 1, "FIFO must preserve local reuse probes")
	probe, ok := f.gopher.taskQueue.popHighPriority()
	require.True(t, ok, "reuse probes keep their dedicated worker")
	assert.Equal(t, "reuse", getModelUID(probe))
	f.gopher.demoteToNormalPriority(probe)
	require.Equal(t, 1, f.gopher.taskQueue.len())
	f.gopher.taskQueue.close()
	_, ok = f.gopher.taskQueue.popHighPriority()
	assert.False(t, ok, "a failed reuse probe must not download on a cleanup worker")
	transfer, ok := f.gopher.taskQueue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "reuse", getModelUID(transfer))
}
