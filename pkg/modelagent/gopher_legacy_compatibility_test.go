package modelagent

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestLegacyDownloadsDoNotWaitOnSharedTaskCoordinator(t *testing.T) {
	var tracker gopherTaskTracker
	first := tracker.beginLegacyTask("model", func() {})
	second := tracker.beginLegacyTask("model", func() {})
	require.NotSame(t, first, second)
	_, outcome := tracker.beginDownload("model", tracker.ensureSequence(0), func() {})
	require.Equal(t, gopherTaskWait, outcome, "a shared transition must wait for legacy writers")
	tracker.finishLegacyTask(first)
	_, outcome = tracker.beginDownload("model", tracker.ensureSequence(0), func() {})
	require.Equal(t, gopherTaskWait, outcome)
	tracker.finishLegacyTask(second)
	_, outcome = tracker.beginDownload("model", tracker.ensureSequence(0), func() {})
	require.Equal(t, gopherTaskProceed, outcome)
}

func TestOptInFencesOlderQueuedOrdinaryTask(t *testing.T) {
	s, shared, _ := newTestHfArtifactGopher(t)
	ordinary := *shared
	ordinary.BaseModel = shared.BaseModel.DeepCopy()
	ordinary.BaseModel.Spec.Storage.DownloadPolicy = nil
	s.enqueueTask(&ordinary)
	s.enqueueTask(shared)
	_, finish, proceed, err := s.beginTask(shared)
	require.NoError(t, err)
	require.True(t, proceed)
	finish(false)
	_, finish, proceed, err = s.beginTask(&ordinary)
	defer finish(false)
	require.NoError(t, err)
	require.False(t, proceed, "old non-reuse spec must not undo a newer shared task")
}

func TestOptInWaitsForOrdinaryDeletion(t *testing.T) {
	s, shared, _ := newTestHfArtifactGopher(t)
	s.gopherChan = make(chan *GopherTask, 1)
	s.samePathWaitDelay = time.Millisecond
	ordinary := *shared
	ordinary.TaskType = Delete
	ordinary.BaseModel = shared.BaseModel.DeepCopy()
	ordinary.BaseModel.Spec.Storage.DownloadPolicy = nil
	_, finishDelete, proceed, err := s.beginTask(&ordinary)
	require.NoError(t, err)
	require.True(t, proceed)
	defer finishDelete(false)
	_, finish, proceed, err := s.beginTask(shared)
	defer finish(false)
	require.NoError(t, err)
	require.False(t, proceed, "shared writes must wait through the ordinary delete grace period")
	select {
	case <-s.gopherChan:
	case <-time.After(time.Second):
		t.Fatal("waiting shared task was not requeued")
	}
}

func TestOrdinaryDeleteKeepsExistingQueueDiscardBehavior(t *testing.T) {
	_, download, _ := newTestHfArtifactGopher(t)
	download.Sequence = 2
	deletion := *download
	deletion.TaskType = Delete
	deletion.Sequence = 1
	deletion.SharedArtifact = false
	require.Empty(t, removeSupersededTasks([]*GopherTask{download}, &deletion))
	deletion.SharedArtifact = true
	require.Len(t, removeSupersededTasks([]*GopherTask{download}, &deletion), 1)
}

func TestOrdinaryTaskSkipsSharedStateLookup(t *testing.T) {
	s, task, _ := newTestHfArtifactGopher(t)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	s.captureStartupReadyModels(context.Background())
	client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	gets := countConfigMapGets(client)
	ctx, finish, proceed, err := s.beginTask(task)
	require.NoError(t, err)
	require.True(t, proceed)
	defer finish(false)
	require.False(t, task.SharedArtifact)
	handled, waiting, err := s.processHfOCIArtifact(ctx, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.False(t, handled)
	require.False(t, waiting)
	handled, waiting, err = s.processSharedHfArtifactDelete(ctx, task)
	require.NoError(t, err)
	require.False(t, handled)
	require.False(t, waiting)
	require.Equal(t, gets, countConfigMapGets(client), "ordinary routing and source hooks must not read shared state")
}

func TestActiveSharedTaskWaitDoesNotExpireQueuedIntent(t *testing.T) {
	s, task, _ := newTestHfArtifactGopher(t)
	s.gopherChan = make(chan *GopherTask, 1)
	s.samePathWaitDelay = time.Millisecond
	s.samePathWaitTimeout = time.Second
	task.SamePathWaitStartedAt = time.Now().Add(-time.Hour)
	require.NoError(t, s.waitForActiveTask(task, gopherTaskWait))
	select {
	case retry := <-s.gopherChan:
		require.Same(t, task, retry)
	case <-time.After(time.Second):
		t.Fatal("active owner must not make queued work disappear")
	}
}

func TestLegacyCancellationKeepsLatestDownloadSemantics(t *testing.T) {
	var tracker gopherTaskTracker
	firstCtx, firstCancel := context.WithCancel(context.Background())
	defer firstCancel()
	secondCtx, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()
	first := tracker.beginLegacyTask("model", firstCancel)
	tracker.beginLegacyTask("model", secondCancel)
	tracker.finishLegacyTask(first)
	tracker.cancelLegacyDownload("model")
	require.NoError(t, firstCtx.Err())
	require.ErrorIs(t, secondCtx.Err(), context.Canceled)
}

func TestSharedRoutingRetainsOwnershipAfterPolicyRemoval(t *testing.T) {
	var routing gopherArtifactRouting
	routing.observeSnapshot(map[string]string{
		"ordinary":                     `{"name":"ordinary","status":"Ready"}`,
		"child":                        `{"hfArtifactKey":"artifact.huggingface.example"}`,
		"artifact.huggingface.example": `{"children":{"other-child":"/models/other"}}`,
	})
	require.False(t, routing.children["ordinary"])
	require.True(t, routing.children["child"])
	require.True(t, routing.children["other-child"])
	routing.observeSnapshot(nil)
	require.True(t, routing.children["child"], "do not lose ownership routing during cleanup")
}

func TestSharedOptOutWaitsForOwnershipLookup(t *testing.T) {
	for _, source := range []string{"oci", "hf"} {
		t.Run(source, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			handler := s.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			s.captureStartupReadyModels(context.Background())
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			path := filepath.Join(input.ModelStoreRoot, "replacement")
			task.BaseModel.Spec.Storage.Path = &path
			if source == "hf" {
				uri := "hf://org/model"
				task.BaseModel.Spec.Storage.StorageUri = &uri
			}
			ctx, finish, proceed, err := s.beginTask(task)
			require.NoError(t, err)
			require.True(t, proceed)
			defer finish(false)
			require.True(t, task.SharedArtifact)
			client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
			client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, fmt.Errorf("ownership lookup unavailable")
			})
			s.samePathWaitDelay = time.Millisecond
			var waiting bool
			if source == "oci" {
				var handled bool
				handled, waiting, err = s.processHfOCIArtifact(ctx, task, task.BaseModel.Spec, true)
				require.True(t, handled)
			} else {
				waiting, err = s.detachHfArtifactForDefaultDownload(ctx, task, task.BaseModel.Spec, true)
			}
			require.NoError(t, err)
			require.True(t, waiting, "opt-out must establish old ownership before downloading a replacement")
			select {
			case <-s.gopherChan:
			case <-time.After(time.Second):
				t.Fatal("shared opt-out was not requeued")
			}
			require.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
		})
	}
}
