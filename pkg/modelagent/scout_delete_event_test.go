package modelagent

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

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
			name:      "live read fails",
			getErr:    fmt.Errorf("service unavailable"),
			wantTasks: 0,
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
		newScout := func(objects ...runtime.Object) (*Scout, chan *GopherTask) {
			client := omefake.NewSimpleClientset(objects...)
			if tt.getErr != nil {
				client.PrependReactor("get", "*", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tt.getErr
				})
			}
			ch := make(chan *GopherTask, 1)
			return &Scout{
				ctx:        context.Background(),
				gopherChan: ch,
				omeClient:  client,
				logger:     zap.NewNop().Sugar(),
			}, ch
		}

		t.Run("BaseModel/"+tt.name, func(t *testing.T) {
			var objects []runtime.Object
			if !tt.liveMissing {
				live := &v1beta1.BaseModel{ObjectMeta: meta()}
				live.Namespace = namespace
				objects = append(objects, live)
			}
			scout, ch := newScout(objects...)

			scout.onBaseModelDeleted(&v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: uid},
			})

			assert.Len(t, ch, tt.wantTasks)
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
			scout, ch := newScout(objects...)

			scout.onClusterBaseModelDeleted(&v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
			})

			assert.Len(t, ch, tt.wantTasks)
			if tt.wantTasks > 0 {
				task := <-ch
				assert.Equal(t, Delete, task.TaskType)
				assert.Equal(t, uid, task.ClusterBaseModel.UID)
			}
		})
	}
}
