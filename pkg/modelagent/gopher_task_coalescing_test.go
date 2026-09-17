package modelagent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestGopherTaskQueueRetryKeepsFreshDownload(t *testing.T) {
	for _, kind := range []string{"BaseModel", "ClusterBaseModel"} {
		for _, capacity := range []int{1, 8} {
			for _, retryClass := range []string{"startup-revalidation", "same-path-wait", "demoted-download"} {
				name := kind + "/" + retryClass
				if capacity == 1 {
					name += "/deferred"
				} else {
					name += "/runnable"
				}
				t.Run(name, func(t *testing.T) {
					queue := newGopherTaskQueue(capacity)
					t.Cleanup(queue.close)
					blocker := coalescingTestTask(kind, "blocker", Download)
					fresh := coalescingTestTask(kind, "model", DownloadOverride)
					peer := coalescingTestTask(kind, "peer", Download)
					queue.enqueue(blocker)
					queue.enqueue(fresh)
					queue.enqueue(peer)

					retry := coalescingTestTask(kind, "model", Download)
					switch retryClass {
					case "startup-revalidation":
						retry.NormalPriorityOnly = true
						retry.RevalidationReplay = true
					case "same-path-wait":
						retry.SamePathWaitStartedAt = time.Now()
					case "demoted-download":
						retry.NormalPriorityOnly = true
					}
					result := queue.enqueue(retry)
					require.True(t, result.accepted)
					assert.Equal(t, capacity == 1, result.deferred)
					require.Equal(t, 3, queue.len(), "the retry must coalesce into the existing download")

					// Dequeuing must retain both the fresh intent and its FIFO position.
					first, ok := queue.popHighPriority()
					require.True(t, ok)
					require.Same(t, blocker, first)
					second, ok := queue.popHighPriority()
					require.True(t, ok)
					require.Same(t, fresh, second)
					third, ok := queue.popHighPriority()
					require.True(t, ok)
					require.Same(t, peer, third)
					assert.Zero(t, queue.len())
				})
			}
		}
	}
}

func TestGopherTaskQueueCoalescesContinuationPriority(t *testing.T) {
	for _, strongerFirst := range []bool{false, true} {
		name := "promotion"
		if strongerFirst {
			name = "preserve-priority"
		}
		t.Run(name, func(t *testing.T) {
			queue := newGopherTaskQueue(1)
			t.Cleanup(queue.close)
			blocker := coalescingTestTask("BaseModel", "blocker", Download)
			queue.enqueue(blocker)
			stronger := coalescingTestTask("BaseModel", "model", Download)
			stronger.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
			weaker := coalescingTestTask("BaseModel", "model", Download)
			weaker.NormalPriorityOnly = true
			weaker.RevalidationReplay = true
			if strongerFirst {
				queue.enqueue(stronger)
			}
			queue.enqueue(weaker)
			if !strongerFirst {
				queue.enqueue(stronger)
			}
			require.Equal(t, 2, queue.len())
			queue.popHighPriority()
			require.NotEmpty(t, queue.high, "the highest-priority continuation must survive")
			task, ok := queue.popHighPriority()
			require.True(t, ok)
			assert.Same(t, stronger, task, "preserve the existing wait deadline as well as priority")
		})
	}
}

func TestGopherTaskQueueFreshUpdateCanLowerPriority(t *testing.T) {
	queue := newGopherTaskQueue(1)
	t.Cleanup(queue.close)
	queue.enqueue(coalescingTestTask("BaseModel", "blocker", Download))
	old := coalescingTestTask("BaseModel", "model", Download)
	queue.enqueue(old)
	latest := coalescingTestTask("BaseModel", "model", Download)
	latest.BaseModel.ResourceVersion = "new-revision"
	latest.DownloadPriority = v1beta1.ModelDownloadPriorityBackground
	queue.enqueue(latest)
	old.SamePathWaitStartedAt = time.Now()
	queue.enqueue(old)
	require.Equal(t, 2, queue.len())
	queue.popHighPriority()
	task, ok := queue.popNormal()
	require.True(t, ok)
	assert.Same(t, latest, task, "a new informer observation must be able to remove old serving demand")
	assert.Zero(t, queue.len())
}

func TestGopherTaskQueueCoalescesEqualLaneWithoutLosingOverride(t *testing.T) {
	for _, overrideFirst := range []bool{false, true} {
		name := "upgrade-intent"
		if overrideFirst {
			name = "preserve-intent"
		}
		t.Run(name, func(t *testing.T) {
			queue := newGopherTaskQueue(1)
			t.Cleanup(queue.close)
			queue.enqueue(coalescingTestTask("BaseModel", "blocker", Download))
			override := coalescingTestTask("BaseModel", "model", DownloadOverride)
			override.NormalPriorityOnly = true
			download := coalescingTestTask("BaseModel", "model", Download)
			download.NormalPriorityOnly = true
			if overrideFirst {
				queue.enqueue(override)
			}
			queue.enqueue(download)
			if !overrideFirst {
				queue.enqueue(override)
			}
			require.Equal(t, 2, queue.len())
			queue.popHighPriority()
			task, ok := queue.popNormal()
			require.True(t, ok)
			assert.Same(t, override, task)
		})
	}
}

func TestGopherTaskQueueEquivalentRetriesKeepFIFOAndWaitDeadline(t *testing.T) {
	queue := newGopherTaskQueue(1)
	t.Cleanup(queue.close)
	queue.enqueue(coalescingTestTask("BaseModel", "blocker", Download))
	wait := coalescingTestTask("BaseModel", "model", Download)
	wait.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
	peer := coalescingTestTask("BaseModel", "peer", Download)
	peer.SamePathWaitStartedAt = time.Now()
	queue.enqueue(wait)
	queue.enqueue(peer)
	for i := 0; i < 100; i++ {
		queue.enqueue(wait)
		retry := coalescingTestTask("BaseModel", "model", Download)
		retry.SamePathWaitStartedAt = time.Now()
		queue.enqueue(retry)
	}
	require.Equal(t, 3, queue.len(), "repeated retries must not grow the queue")
	queue.popHighPriority()
	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Same(t, wait, task, "retain the original wait deadline and FIFO position")
	task, ok = queue.popHighPriority()
	require.True(t, ok)
	assert.Same(t, peer, task)
}

func TestGopherTaskQueueDeleteFencesRetriesWithoutLosingRecreation(t *testing.T) {
	queue := newGopherTaskQueue(1)
	t.Cleanup(queue.close)
	blocker := coalescingTestTask("BaseModel", "blocker", Download)
	queue.enqueue(blocker)
	queue.enqueue(coalescingTestTask("BaseModel", "model", Download))
	recreated := coalescingTestTask("BaseModel", "model", Download)
	recreated.BaseModel.UID = "replacement-uid"
	queue.enqueue(recreated)
	deleted := coalescingTestTask("BaseModel", "model", Delete)
	queue.enqueue(deleted)
	retry := coalescingTestTask("BaseModel", "model", Download)
	retry.NormalPriorityOnly = true
	queue.enqueue(retry)
	require.Equal(t, 3, queue.len())
	for _, want := range []*GopherTask{deleted, blocker, recreated} {
		got, ok := queue.popHighPriority()
		require.True(t, ok)
		assert.Same(t, want, got)
	}
	assert.Zero(t, queue.len())
}

func coalescingTestTask(kind, name string, action GopherTaskType) *GopherTask {
	metadata := metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), ResourceVersion: "revision"}
	task := &GopherTask{TaskType: action, DownloadPriority: v1beta1.ModelDownloadPriorityHigh}
	if kind == "ClusterBaseModel" {
		task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: metadata}
	} else {
		metadata.Namespace = "service-ns"
		task.BaseModel = &v1beta1.BaseModel{ObjectMeta: metadata}
	}
	return task
}
