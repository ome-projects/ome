package modelagent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omev1beta1lister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

func TestScoutBaseModelDeleteHandoffLogs(t *testing.T) {
	for _, namespace := range []string{"default", "dac-test"} {
		for _, source := range []string{"delete_event", "deletion_timestamp", "startup_reconciliation"} {
			t.Run(namespace+"/"+source, func(t *testing.T) {
				model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      "same-model-name",
				}}
				modelID := namespace + "/" + model.Name
				enqueueStart := "Delete task enqueue start: " + modelID
				enqueueComplete := "Delete task enqueue complete: " + modelID
				core, logs := observer.New(zap.InfoLevel)
				started := make(chan struct{})
				tasks := make(chan *GopherTask)
				scout := &Scout{
					gopherChan: tasks,
					logger: zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
						if entry.Message == enqueueStart {
							close(started)
						}
						return nil
					})).Sugar(),
				}
				var processDelete func()
				switch source {
				case "delete_event":
					processDelete = func() { scout.deleteBaseModel(model) }
				case "deletion_timestamp":
					oldModel := model.DeepCopy()
					now := metav1.Now()
					model.DeletionTimestamp = &now
					processDelete = func() { scout.updateBaseModel(oldModel, model) }
				case "startup_reconciliation":
					now := metav1.Now()
					model.DeletionTimestamp = &now
					indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
					require.NoError(t, indexer.Add(model))
					scout.baseModelLister = omev1beta1lister.NewBaseModelLister(indexer)
					scout.clusterBaseModelLister = omev1beta1lister.NewClusterBaseModelLister(
						cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
					processDelete = scout.reconcilePendingDeletions
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					processDelete()
				}()
				t.Cleanup(func() {
					select {
					case <-done:
					case <-tasks:
					case <-time.After(5 * time.Second):
						t.Error("delete handler did not finish or reach task handoff")
					}
				})

				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("missing enqueue start log before channel handoff")
				}
				// An unbuffered send cannot complete until the receiver accepts the task.
				assert.Zero(t, logs.FilterMessage(enqueueComplete).Len())
				select {
				case task := <-tasks:
					assert.Equal(t, Delete, task.TaskType)
					assert.Same(t, model, task.BaseModel)
					assert.False(t, task.NodeIneligible)
				case <-time.After(5 * time.Second):
					t.Fatal("Scout did not hand off its delete task")
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("delete handler did not return after task handoff")
				}
				assert.Equal(t, 1, logs.FilterMessage(enqueueStart).Len())
				assert.Equal(t, 1, logs.FilterMessage(enqueueComplete).Len())
				deleteEvents := logs.FilterMessage("BaseModel delete event received: " + modelID).Len()
				if source == "delete_event" {
					assert.Equal(t, 1, deleteEvents)
				} else {
					assert.Zero(t, deleteEvents, "only informer Delete events should be logged as received")
				}
				if source == "deletion_timestamp" {
					assert.Equal(t, 1, logs.FilterMessage(
						"Resource has deletion timestamp: BaseModel '"+modelID+"', processing delete").Len())
				}
			})
		}
	}
}
