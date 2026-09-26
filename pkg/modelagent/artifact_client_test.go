package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestGopherPinsStartupNodeIdentity(t *testing.T) {
	for _, boundary := range []string{"success", "missing node", "empty UID", "different pin"} {
		t.Run(boundary, func(t *testing.T) {
			repository, kube := newTestHfArtifactRepository(t, nil)
			maps := repository.configMaps
			modelClient := omefake.NewSimpleClientset()
			labels := NewNodeLabelReconciler(maps.nodeName, kube, 1, maps.logger)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: maps.nodeName, UID: "startup-node"}}
			if boundary == "empty UID" {
				node.UID = ""
			}
			if boundary != "missing node" {
				_, err := kube.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			if boundary == "different pin" {
				labels.nodeUID = "previous-node"
			}
			g, err := NewGopher(nil, maps, &xet.Config{}, kube, 1, 1, 1, t.TempDir(),
				make(chan *GopherTask, 1), 0, labels, nil, maps.logger, nil, nil, modelClient,
				WithModelVerificationConcurrency(3))
			if boundary != "success" {
				require.Error(t, err)
				require.Nil(t, g)
				return
			}
			require.NoError(t, err)
			require.Equal(t, node.UID, g.nodeUID)
			require.Equal(t, node.UID, labels.nodeUID)
			require.Same(t, modelClient, g.modelClient)
			require.NotNil(t, g.modelVerificationLimiter)
			require.Equal(t, 3, g.modelVerificationLimiter.limit())
		})
	}
}
