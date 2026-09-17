package effective

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

type syncGetHookClient struct {
	ctrlclient.Client
	afterGet func(ctrlclient.Object)
}

func (c *syncGetHookClient) Get(ctx context.Context, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	if err := c.Client.Get(ctx, key, object, opts...); err != nil {
		return err
	}
	c.afterGet(object)
	return nil
}

func TestRuntimeSyncReadEvidenceRefusesCanceledWrongGVKAndDisabledSources(t *testing.T) {
	for _, scenario := range []string{"canceled successful read", "wrong GVK", "disabled namespaced source"} {
		t.Run(scenario, func(t *testing.T) {
			_, _, live := runtimeSyncSourceFixture(t)
			var stored, received ctrlclient.Object = live, &v1beta1.ClusterServingRuntime{}
			if scenario == "disabled namespaced source" {
				stored = &v1beta1.ServingRuntime{ObjectMeta: live.ObjectMeta, Spec: live.Spec}
				stored.SetNamespace("workloads")
				stored.(*v1beta1.ServingRuntime).Spec.Disabled = ptr.To(true)
				received = &v1beta1.ServingRuntime{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &syncGetHookClient{Client: ctrlfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(stored).Build(), afterGet: func(object ctrlclient.Object) {
				if scenario == "canceled successful read" {
					cancel()
				}
				if scenario == "wrong GVK" {
					object.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{Group: "PRIVATE_GROUP", Version: "v1beta1", Kind: "ClusterServingRuntime"})
				}
			}}
			reads := map[string]string{}
			reader := &syncReadClient{Client: client, reads: reads, timeout: time.Second}
			err := reader.Get(ctx, ctrlclient.ObjectKeyFromObject(stored), received)
			if scenario == "canceled successful read" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, ErrRuntimeSyncEvidence)
			}
			require.Empty(t, reads, "refused data cannot become authority")
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}

func TestRuntimeSyncReadEvidenceBindsNamedModelAndRejectsContradictoryMetadata(t *testing.T) {
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "workloads", UID: "model-uid", ResourceVersion: "17", Generation: 2}}
	client := ctrlfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(model).Build()
	reads := map[string]string{}
	reader := &syncReadClient{Client: client, reads: reads, timeout: time.Second}
	first := &v1beta1.BaseModel{}
	key := ctrlclient.ObjectKeyFromObject(model)
	require.NoError(t, reader.Get(context.Background(), key, first))
	require.Equal(t, map[string]string{"BaseModel/workloads/model": syncDigest(first)}, reads)
	bound := reads["BaseModel/workloads/model"]
	first.Annotations = map[string]string{"private": "PRIVATE_CHANGED"}
	require.NoError(t, client.Update(context.Background(), first))
	err := reader.Get(context.Background(), key, &v1beta1.BaseModel{})
	require.ErrorIs(t, err, ErrRuntimeSyncEvidence)
	require.Equal(t, bound, reads["BaseModel/workloads/model"], "the first proof is never overwritten by contradictory evidence")
	require.NotContains(t, err.Error(), "PRIVATE")
}

func TestRuntimeSyncPredictedRevisionReadFailuresPreserveCancellationAndPrivacy(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			v, revision, live := runtimeSyncSourceFixture(t)
			_, hash, err := runtimerevision.Hash(&live.Spec)
			require.NoError(t, err)
			predicted := runtimerevision.Name(runtimerevision.KindClusterServingRuntime, "", "runtime", hash)
			kube := kfake.NewSimpleClientset(revision)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			predictedReads := 0
			kube.PrependReactor("get", "controllerrevisions", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if action.(ktesting.GetAction).GetName() != predicted {
					return false, nil, nil
				}
				predictedReads++
				if canceled {
					cancel()
					return true, nil, fmt.Errorf("PRIVATE_API: %w", context.Canceled)
				}
				return true, nil, errors.New("PRIVATE_API")
			})
			evidence, err := runtimeSyncSourceResolver(t, kube, live).Resolve(ctx, v)
			require.Equal(t, 1, predictedReads)
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, ErrRuntimeSyncEvidence)
			}
			require.Empty(t, evidence.TargetHash())
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}
