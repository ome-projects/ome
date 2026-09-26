package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/modelparser"
)

func TestDirectEvictionSerializesLocalReader(t *testing.T) {
	for _, order := range []string{"eviction first", "reader first", "external legacy"} {
		t.Run(order, func(t *testing.T) {
			ctx := context.Background()
			g, target, path := newEvictionTestModel(t)
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.Labels["kubernetes.io/hostname"] = node.Name
			_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			readPath := path
			if order == "external legacy" {
				readPath = t.TempDir()
			}
			other := target.BaseModel.DeepCopy()
			other.Name, other.UID = "local-reader", "local-reader"
			other.Annotations = map[string]string{ConfigParsingAnnotation: "true"}
			other.Spec.Storage.StorageUri = stringPtr("local://" + readPath)
			other.Spec.Storage.Path = nil
			download := &GopherTask{TaskType: Download, BaseModel: other}
			api := g.modelClient.(*omefake.Clientset)
			peerAPI := omefake.NewSimpleClientset()
			peerAPI.ReactionChain = nil
			peerAPI.AddReactor("*", "*", k8stesting.ObjectReaction(api.Tracker()))
			peerKube := k8sfake.NewSimpleClientset()
			peerKube.ReactionChain = nil
			peerKube.AddReactor("*", "*", k8stesting.ObjectReaction(g.kubeClient.(*k8sfake.Clientset).Tracker()))
			peer := &Gopher{
				configMapReconciler: NewConfigMapReconciler(node.Name, g.configMapReconciler.namespace, peerKube, g.logger),
				nodeLabelReconciler: NewNodeLabelReconciler(node.Name, peerKube, 1, g.logger),
				modelConfigParser:   modelparser.NewModelConfigParser(peerAPI, g.logger),
				kubeClient:          peerKube, modelClient: peerAPI, modelRootDir: g.modelRootDir, logger: g.logger,
				metrics: NewMetrics(prometheus.NewRegistry()), gopherChan: make(chan *GopherTask, 1), samePathWaitDelay: time.Millisecond,
			}
			peer.artifactRouting.known = true
			if order == "reader first" {
				require.NoError(t, api.Tracker().Add(other))
				checked := false
				peerKube.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
					if strings.Contains(string(action.(k8stesting.PatchAction).GetPatch()), `"Ready"`) {
						checked = true
						require.ErrorContains(t, g.evictDirectArtifact(ctx, target), "active writer")
					}
					return false, nil, nil
				})
				require.NoError(t, peer.processTask(download))
				require.True(t, checked, "the actual Ready publication must hold the family lock")
				require.FileExists(t, filepath.Join(path, "weights"))
			} else {
				scans := 0
				api.PrependReactor("list", "clusterbasemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
					scans++
					if scans != 2 {
						return false, nil, nil
					}
					require.NoError(t, api.Tracker().Add(other))
					require.NoError(t, peer.processTask(download))
					return true, &v1beta1.ClusterBaseModelList{}, nil
				})
				require.NoError(t, g.evictDirectArtifact(ctx, target))
				require.Equal(t, 2, scans)
				if order == "eviction first" {
					require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, download).Status)
					select {
					case retry := <-peer.gopherChan:
						require.Same(t, download, retry)
					case <-time.After(time.Second):
						t.Fatal("local reader was not requeued while eviction held the family")
					}
					require.Error(t, peer.processTask(download), "the missing local artifact cannot become Ready")
					require.NoDirExists(t, path, "read-only admission must not recreate evicted files")
					require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, download).Status)
				}
			}
			entry := directArtifactEntry(t, peer, download)
			require.Empty(t, entry.DirectArtifactPath, "a reader must not claim direct deletion ownership")
			if order != "eviction first" {
				require.Equal(t, ModelStatusReady, entry.Status)
			}
			lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: path})
			require.NoError(t, err)
			require.True(t, acquired, "completion or missing-file failure must release the family lock")
			require.NoError(t, lock.Close())
			_, err = os.Stat(readPath)
			if order != "eviction first" {
				require.NoError(t, err)
			}
		})
	}
}

func TestLocalReaderAliasDoesNotSpanRemovableFamilies(t *testing.T) {
	for _, sameFamily := range []bool{false, true} {
		g, task, path := newDirectArtifactTestModel(t)
		target := filepath.Join(g.modelRootDir, "other-family")
		if sameFamily {
			target = filepath.Join(path, "same-family")
		}
		require.NoError(t, os.MkdirAll(target, 0700))
		alias := filepath.Join(path, "alias")
		require.NoError(t, os.Symlink(target, alias))
		task.BaseModel.Spec.Storage.StorageUri = stringPtr("local://" + alias)
		task.BaseModel.Spec.Storage.Path = nil
		g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
		unlock, acquired, err := g.acquireDirectArtifactOperation(context.Background(), task, true)
		if acquired {
			unlock()
		}
		if sameFamily {
			require.NoError(t, err)
			require.True(t, acquired)
		} else {
			require.Error(t, err, "one family lock cannot protect both a removable alias and a different target")
			require.False(t, acquired)
		}
	}
}

func TestLocalReaderValidatesEveryTraversedManagedFamily(t *testing.T) {
	for _, layout := range []string{"hidden other family", "hidden outward alias", "hidden same family", "external legacy", "parent traversal"} {
		t.Run(layout, func(t *testing.T) {
			ctx := context.Background()
			g, target, owner := newEvictionTestModel(t)
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.Labels["kubernetes.io/hostname"] = node.Name
			_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)

			destination := filepath.Join(g.modelRootDir, "other-family", "data")
			intermediate := filepath.Join(owner, "alias")
			switch layout {
			case "hidden outward alias", "external legacy":
				destination = t.TempDir()
				if layout == "external legacy" {
					intermediate = filepath.Join(t.TempDir(), "alias")
				}
			case "hidden same family":
				destination = filepath.Join(owner, "data")
			}
			require.NoError(t, os.MkdirAll(destination, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(destination, "weights"), []byte("reader weights"), 0600))
			require.NoError(t, os.Symlink(destination, intermediate))
			outside := filepath.Join(t.TempDir(), "outside-alias")
			if layout == "parent traversal" {
				// Even a real directory traversed before '..' must remain present.
				intermediate = owner + "/../other-family/data"
			}
			require.NoError(t, os.Symlink(intermediate, outside))
			model := target.BaseModel.DeepCopy()
			model.Name, model.UID = "alias-reader", "alias-reader"
			model.Annotations = map[string]string{ConfigParsingAnnotation: "true"}
			model.Spec.Storage.StorageUri = stringPtr("local://" + outside)
			model.Spec.Storage.Path = nil
			reader := &GopherTask{TaskType: Download, BaseModel: model}
			api := g.modelClient.(*omefake.Clientset)
			require.NoError(t, api.Tracker().Add(model))
			peerAPI := omefake.NewSimpleClientset()
			peerAPI.ReactionChain = nil
			peerAPI.AddReactor("*", "*", k8stesting.ObjectReaction(api.Tracker()))
			peerKube := k8sfake.NewSimpleClientset()
			peerKube.ReactionChain = nil
			peerKube.AddReactor("*", "*", k8stesting.ObjectReaction(g.kubeClient.(*k8sfake.Clientset).Tracker()))
			peer := &Gopher{
				configMapReconciler: NewConfigMapReconciler(node.Name, g.configMapReconciler.namespace, peerKube, g.logger),
				nodeLabelReconciler: NewNodeLabelReconciler(node.Name, peerKube, 1, g.logger),
				modelConfigParser:   modelparser.NewModelConfigParser(peerAPI, g.logger),
				kubeClient:          peerKube, modelClient: peerAPI, modelRootDir: g.modelRootDir, logger: g.logger,
				metrics: NewMetrics(prometheus.NewRegistry()), gopherChan: make(chan *GopherTask, 1), samePathWaitDelay: time.Millisecond,
			}
			peer.artifactRouting.known = true
			ready := false
			peerKube.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if strings.Contains(string(action.(k8stesting.PatchAction).GetPatch()), `"Ready"`) {
					ready = true
					evictErr := g.evictDirectArtifact(ctx, target)
					if layout == "hidden same family" {
						require.ErrorContains(t, evictErr, "active writer")
					} else if layout == "external legacy" {
						require.NoError(t, evictErr)
					}
				}
				return false, nil, nil
			})
			err = peer.processTask(reader)
			require.NoError(t, err)
			entry := directArtifactEntry(t, peer, reader)
			require.Empty(t, entry.DirectArtifactPath, "readers never claim deletion ownership")
			if layout == "hidden same family" || layout == "external legacy" {
				require.True(t, ready)
				require.Equal(t, ModelStatusReady, entry.Status)
			} else {
				require.False(t, ready)
				require.NotEqual(t, ModelStatusReady, entry.Status)
				_, err = peer.directArtifactOperationPath(reader, true)
				require.Error(t, err, "an unprotected intermediate family must reject admission")
				select {
				case retry := <-peer.gopherChan:
					require.Same(t, reader, retry)
				case <-time.After(time.Second):
					t.Fatal("unsafe local reader was not requeued")
				}
			}
			require.Equal(t, "local://"+outside, *reader.BaseModel.Spec.Storage.StorageUri)
			require.FileExists(t, filepath.Join(outside, "weights"), "admission must preserve the original source path")
		})
	}
}
