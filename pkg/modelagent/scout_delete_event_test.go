package modelagent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

// newDeleteConfirmationTestScout returns a Scout with a fast-retrying delete
// queue and a buffered gopher channel.
func newDeleteConfirmationTestScout(client *omefake.Clientset) (*Scout, chan *GopherTask) {
	ch := make(chan *GopherTask, 1)
	return &Scout{
		ctx:         context.Background(),
		gopherChan:  ch,
		omeClient:   client,
		deleteQueue: newDeleteConfirmationQueue(time.Millisecond, time.Millisecond),
		logger:      zap.NewNop().Sugar(),
	}, ch
}

// processNext runs one processNextDeleteConfirmation call and fails the test
// instead of hanging when no item becomes ready.
func processNext(t *testing.T, scout *Scout) bool {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- scout.processNextDeleteConfirmation() }()
	select {
	case ok := <-done:
		return ok
	case <-time.After(5 * time.Second):
		scout.deleteQueue.ShutDown()
		t.Fatal("no delete confirmation became ready")
		return false
	}
}

func TestOnModelDeleted_ConfirmsWithLiveRead(t *testing.T) {
	const (
		name      = "model"
		namespace = "models"
		uid       = types.UID("uid-1")
	)
	deleting := metav1.Now()

	tests := []struct {
		name         string
		liveUID      types.UID
		liveDeleting bool
		liveMissing  bool
		getErr       error
		wantTasks    int
		wantRequeue  bool
	}{
		{
			name:      "object still exists with same UID",
			liveUID:   uid,
			wantTasks: 0,
		},
		{
			name:        "object is gone",
			liveMissing: true,
			wantTasks:   1,
		},
		{
			name:      "object was recreated with a new UID",
			liveUID:   "uid-2",
			wantTasks: 1,
		},
		{
			name:         "object is being deleted",
			liveUID:      uid,
			liveDeleting: true,
			wantTasks:    1,
		},
		{
			name:        "live read fails",
			getErr:      fmt.Errorf("service unavailable"),
			wantTasks:   0,
			wantRequeue: true,
		},
		{
			name:        "live read times out",
			getErr:      context.DeadlineExceeded,
			wantTasks:   0,
			wantRequeue: true,
		},
	}

	for _, tt := range tests {
		meta := func() metav1.ObjectMeta {
			m := metav1.ObjectMeta{Name: name, UID: tt.liveUID}
			if tt.liveDeleting {
				m.DeletionTimestamp = &deleting
				m.Finalizers = []string{"test"}
			}
			return m
		}
		newClient := func(objects ...runtime.Object) *omefake.Clientset {
			client := omefake.NewSimpleClientset(objects...)
			if tt.getErr != nil {
				client.PrependReactor("get", "*", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tt.getErr
				})
			}
			return client
		}
		// check runs the queued confirmation and verifies the outcome.
		check := func(t *testing.T, client *omefake.Clientset, scout *Scout, ch chan *GopherTask, item deleteConfirmation) {
			assert.Empty(t, client.Actions(), "informer callback must not call the API server")
			assert.Equal(t, 1, scout.deleteQueue.Len())

			require.True(t, processNext(t, scout))

			assert.Len(t, ch, tt.wantTasks)
			if tt.wantRequeue {
				assert.Equal(t, 1, scout.deleteQueue.NumRequeues(item))
			} else {
				assert.Zero(t, scout.deleteQueue.NumRequeues(item))
			}
		}

		t.Run("BaseModel/"+tt.name, func(t *testing.T) {
			var objects []runtime.Object
			if !tt.liveMissing {
				live := &v1beta1.BaseModel{ObjectMeta: meta()}
				live.Namespace = namespace
				objects = append(objects, live)
			}
			client := newClient(objects...)
			scout, ch := newDeleteConfirmationTestScout(client)
			deleted := &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: uid},
			}

			scout.onBaseModelDeleted(deleted)

			check(t, client, scout, ch, deleteConfirmation{baseModel: deleted})
			if tt.wantTasks > 0 {
				task := <-ch
				assert.Equal(t, Delete, task.TaskType)
				assert.Equal(t, uid, task.BaseModel.UID)
			}
		})

		t.Run("ClusterBaseModel/"+tt.name, func(t *testing.T) {
			var objects []runtime.Object
			if !tt.liveMissing {
				objects = append(objects, &v1beta1.ClusterBaseModel{ObjectMeta: meta()})
			}
			client := newClient(objects...)
			scout, ch := newDeleteConfirmationTestScout(client)
			deleted := &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
			}

			scout.onClusterBaseModelDeleted(deleted)

			check(t, client, scout, ch, deleteConfirmation{clusterBaseModel: deleted})
			if tt.wantTasks > 0 {
				task := <-ch
				assert.Equal(t, Delete, task.TaskType)
				assert.Equal(t, uid, task.ClusterBaseModel.UID)
			}
		})
	}
}

func TestConfirmDelete_RetriesUntilReadSucceeds(t *testing.T) {
	const failures = 2
	client := omefake.NewSimpleClientset()
	calls := 0
	client.PrependReactor("get", "clusterbasemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls <= failures {
			return true, nil, fmt.Errorf("connection refused")
		}
		return false, nil, nil // fall through to the tracker, which returns NotFound
	})
	scout, ch := newDeleteConfirmationTestScout(client)
	deleted := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "uid-1"}}
	item := deleteConfirmation{clusterBaseModel: deleted}

	scout.onClusterBaseModelDeleted(deleted)

	for i := 1; i <= failures; i++ {
		require.True(t, processNext(t, scout))
		assert.Empty(t, ch)
		assert.Equal(t, i, scout.deleteQueue.NumRequeues(item))
	}
	require.True(t, processNext(t, scout))

	require.Len(t, ch, 1)
	task := <-ch
	assert.Equal(t, Delete, task.TaskType)
	assert.Same(t, deleted, task.ClusterBaseModel)
	assert.Zero(t, scout.deleteQueue.NumRequeues(item))
	assert.Zero(t, scout.deleteQueue.Len())
	assert.Equal(t, failures+1, calls)
}

func TestProcessNextDeleteConfirmation_StopsOnShutdown(t *testing.T) {
	scout, ch := newDeleteConfirmationTestScout(omefake.NewSimpleClientset())
	scout.onBaseModelDeleted(&v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models"}})

	scout.deleteQueue.ShutDown()

	assert.False(t, processNext(t, scout))
	assert.Empty(t, ch)
}

func TestConfirmDelete_DoesNotBlockAfterGopherStops(t *testing.T) {
	scout, _ := newDeleteConfirmationTestScout(omefake.NewSimpleClientset())
	scout.gopherChan = make(chan *GopherTask)
	stop := make(chan struct{})
	close(stop)
	scout.deleteWorkerStop = stop
	defer scout.deleteQueue.ShutDown()

	done := make(chan struct{})
	go func() {
		scout.confirmDelete(deleteConfirmation{baseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "gone", Namespace: "models"},
		}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delete confirmation blocked on the stopped gopher")
	}
}

func TestGetWithTimeout_BoundsRead(t *testing.T) {
	const timeout = 10 * time.Millisecond
	var hadDeadline bool

	start := time.Now()
	_, err := getWithTimeout(context.Background(), timeout, func(ctx context.Context) (metav1.Object, error) {
		_, hadDeadline = ctx.Deadline()
		<-ctx.Done()
		return nil, ctx.Err()
	})

	assert.True(t, hadDeadline)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 5*time.Second)
}
