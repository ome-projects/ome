package modelagent

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestVerifiedOwnerHandoffPreservesOutstandingClaims(t *testing.T) {
	for _, state := range []string{"ordinary", "direct receipt", "shared receipt", "shared child", "legacy parent", "legacy child", "one-sided parent", "completed same UID", "nil verification", "rejected verification"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			repository, client := newTestHfArtifactRepository(t, map[string]string{})
			c := repository.configMaps
			key := "default.basemodel.model"
			entry := ModelEntry{Name: "model", ModelUID: "old", Status: ModelStatusReady}
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			switch state {
			case "direct receipt":
				entry.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: "old", Path: "/models/model"}
			case "shared receipt":
				entry.HfArtifactPendingDeletion = &HfArtifactPendingDeletion{ModelUID: "old", ChildPath: "/models/model"}
			case "shared child":
				entry.HfArtifactKey = "parent"
			case "legacy parent":
				entry.Config = &ModelConfig{Artifact: Artifact{ChildrenPaths: []string{"/models/child"}}}
			case "legacy child":
				entry.Config = &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"other": "/models/parent"}}}
			case "one-sided parent":
				parent := testHfArtifactEntry(t)
				parent.Children = map[string]string{key: "/models/model"}
				_, err = writeHfArtifactEntry(cm.Data, parent)
				require.NoError(t, err)
			case "completed same UID":
				entry.ModelUID, entry.Status = "new", ModelStatusEvicted
			}
			_, err = writeModelEntry(cm.Data, key, entry)
			require.NoError(t, err)
			_, err = client.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			verify := func() error { return nil }
			if state == "nil verification" {
				verify = nil
			} else if state == "rejected verification" {
				verify = func() error { return fmt.Errorf("not current") }
			}
			err = c.handoffOrdinaryModelOwner(ctx, key, "new", verify)
			actual, getErr := c.getConfigMap(ctx)
			require.NoError(t, getErr)
			if state != "ordinary" {
				require.Error(t, err)
				require.Equal(t, cm.Data, actual.Data)
				return
			}
			require.NoError(t, err)
			replacement, err := existingModelEntry(actual.Data, key)
			require.NoError(t, err)
			require.EqualValues(t, "new", replacement.ModelUID)
			require.Equal(t, ModelStatusUpdating, replacement.Status)
			require.True(t, c.isModelMutationBlocked(key, "old"))
		})
	}
}

func TestVerifiedOwnerHandoffRetriesWithFreshProof(t *testing.T) {
	for _, boundary := range []string{"lost response", "proof changed"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			key := "default.basemodel.model"
			data := map[string]string{}
			_, err := writeModelEntry(data, key, ModelEntry{Name: "model", ModelUID: "old", Status: ModelStatusReady})
			require.NoError(t, err)
			repository, client := newTestHfArtifactRepository(t, data)
			c := repository.configMaps
			c.modelCache[key] = &CacheEntry{ModelName: "model", ModelUID: "old", ModelStatus: ModelStatusReady}
			current, intercepted := true, false
			proofs := 0
			verify := func() error {
				proofs++
				if !current {
					return fmt.Errorf("proof changed")
				}
				return nil
			}
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if intercepted {
					return false, nil, nil
				}
				intercepted = true
				if boundary == "proof changed" {
					current = false
					return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), c.nodeName, fmt.Errorf("retry"))
				}
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				return true, nil, fmt.Errorf("lost response")
			})
			require.Error(t, c.handoffOrdinaryModelOwner(ctx, key, "new", verify))
			require.EqualValues(t, "old", c.modelCache[key].ModelUID, "cache fencing follows confirmed completion")
			if boundary == "lost response" {
				require.NoError(t, c.handoffOrdinaryModelOwner(ctx, key, "new", verify))
				require.EqualValues(t, "new", c.modelCache[key].ModelUID)
				require.True(t, c.isModelMutationBlocked(key, "old"))
			}
			require.GreaterOrEqual(t, proofs, 2)
		})
	}
}
