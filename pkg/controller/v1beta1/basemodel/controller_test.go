package basemodel

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func TestBaseModelReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Create scheme
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	// batchv1 needed for cleanupStalePVCArtifacts which lists Jobs on
	// every non-PVC reconcile (idempotent no-op when none exist; the
	// List itself still requires the scheme).
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name       string
		baseModel  *v1beta1.BaseModel
		setupMocks func(client.Client)
		validate   func(*testing.T, client.Client, *v1beta1.BaseModel, ctrl.Result, error)
		wantErr    bool
	}{
		{
			name: "New BaseModel gets finalizer added",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "safetensors",
					},
					Storage: &v1beta1.StorageSpec{
						StorageUri: stringPtr("oci://bucket/model"),
					},
				},
			},
			setupMocks: func(c client.Client) {
				// No setup needed for this test
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				// Fetch the updated BaseModel
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify finalizer was added
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.BaseModelFinalizer))
			},
		},
		{
			name: "BaseModel with ConfigMap status updates to Ready",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "ready-model",
					Namespace:  "default",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "pytorch",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create node
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker-node-1",
					},
				}
				err = c.Create(context.TODO(), node)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create ConfigMap with Ready status
				modelEntry := shared.ModelEntry{
					Status: shared.ModelStatusReady,
					Config: &shared.ModelConfig{
						ModelType:         "gpt2",
						ModelArchitecture: "GPT2LMHeadModel",
						MaxTokens:         2048,
					},
				}
				entryData, _ := json.Marshal(modelEntry)

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "worker-node-1",
						Namespace: constants.OMENamespace,
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"default.basemodel.ready-model": string(entryData),
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				// Fetch the updated BaseModel
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify status was updated
				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				g.Expect(updated.Status.NodesReady).To(gomega.ContainElement("worker-node-1"))
				g.Expect(updated.Status.NodesFailed).To(gomega.BeEmpty())

				// Verify spec was updated with config
				g.Expect(updated.Spec.ModelType).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelType).To(gomega.Equal("gpt2"))
				g.Expect(updated.Spec.ModelArchitecture).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelArchitecture).To(gomega.Equal("GPT2LMHeadModel"))
				g.Expect(updated.Spec.MaxTokens).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.MaxTokens).To(gomega.Equal(int32(2048)))
			},
		},
		{
			name: "BaseModel with multiple nodes - mixed status",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "mixed-status-model",
					Namespace:  "test-ns",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "onnx",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2", "node-3"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with different statuses
				statuses := map[string]shared.ModelStatus{
					"node-1": shared.ModelStatusReady,
					"node-2": shared.ModelStatusFailed,
					"node-3": shared.ModelStatusUpdating,
				}

				for nodeName, status := range statuses {
					modelEntry := shared.ModelEntry{
						Status: status,
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"test-ns.basemodel.mixed-status-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Should be Ready because at least one node is ready
				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				g.Expect(updated.Status.NodesReady).To(gomega.ContainElement("node-1"))
				g.Expect(updated.Status.NodesFailed).To(gomega.ContainElement("node-2"))
				g.Expect(updated.Status.NodesReady).To(gomega.HaveLen(1))
				g.Expect(updated.Status.NodesFailed).To(gomega.HaveLen(1))
			},
		},
		{
			name: "BaseModel deletion removes finalizer when no ConfigMaps exist",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "delete-me-no-configmaps",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "tensorflow",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace but no ConfigMaps
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)

				// The object should exist (fake client behavior) but finalizer should be removed
				if err == nil {
					// Verify finalizer was removed
					g.Expect(updated.Finalizers).NotTo(gomega.ContainElement(constants.BaseModelFinalizer))
				} else {
					// If object is not found, that's also acceptable as it means deletion completed
					g.Expect(errors.IsNotFound(err)).To(gomega.BeTrue())
				}
			},
		},
		{
			name: "BaseModel deletion waits when ConfigMap entries exist but are not deleted",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deletion-waiting-model",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "pytorch",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with entries not yet deleted
				for _, nodeName := range []string{"node-1", "node-2"} {
					modelEntry := shared.ModelEntry{
						Status: shared.ModelStatusReady, // Not marked for deletion
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"default.basemodel.deletion-waiting-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Finalizer should still be present since deletion is waiting for ConfigMaps to be cleared
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.BaseModelFinalizer))

				// The reconciler should have set a requeue delay when waiting for ConfigMaps to be cleared
				g.Expect(result.RequeueAfter).To(gomega.Equal(time.Second * 30))
			},
		},
		{
			name: "BaseModel deletion removes finalizer when ConfigMap entries are marked as deleted",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deletion-complete-model",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "onnx",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with entries marked as deleted
				for _, nodeName := range []string{"node-1", "node-2"} {
					modelEntry := shared.ModelEntry{
						Status: shared.ModelStatusDeleted, // Marked for deletion
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"default.basemodel.deletion-complete-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)

				// Finalizer should be removed since all entries are marked as deleted
				if err == nil {
					g.Expect(updated.Finalizers).NotTo(gomega.ContainElement(constants.BaseModelFinalizer))
				} else {
					g.Expect(errors.IsNotFound(err)).To(gomega.BeTrue())
				}
			},
		},
		{
			name: "BaseModel deletion with mix of deleted and active entries waits",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "mixed-deletion-model",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "safetensors",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2", "node-3"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with mixed deletion status
				statuses := map[string]shared.ModelStatus{
					"node-1": shared.ModelStatusDeleted, // Marked for deletion
					"node-2": shared.ModelStatusReady,   // Not deleted
					"node-3": shared.ModelStatusFailed,  // Not deleted
				}

				for nodeName, status := range statuses {
					modelEntry := shared.ModelEntry{
						Status: status,
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"default.basemodel.mixed-deletion-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Finalizer should still be present since not all entries are marked for deletion
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.BaseModelFinalizer))
			},
		},
		{
			name: "BaseModel with deleted node ignores ConfigMap",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "deleted-node-model",
					Namespace:  "default",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "huggingface",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ConfigMap for non-existent node
				modelEntry := shared.ModelEntry{
					Status: shared.ModelStatusReady,
				}
				entryData, _ := json.Marshal(modelEntry)

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "deleted-node",
						Namespace: constants.OMENamespace,
						UID:       types.UID("deleted-node-configmap"),
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"default.basemodel.deleted-node-model": string(entryData),
					},
				}
				err := c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Should remain in InTransit state (no valid nodes)
				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateInTransit))
				g.Expect(updated.Status.NodesReady).To(gomega.BeEmpty())
				g.Expect(updated.Status.NodesFailed).To(gomega.BeEmpty())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.baseModel).
				WithStatusSubresource(tt.baseModel).
				Build()

			// Setup test mocks
			tt.setupMocks(c)

			// Run reconciliation
			reconciler := &BaseModelReconciler{
				Client:    c,
				APIReader: c,
				Scheme:    c.Scheme(),
			}

			result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: tt.baseModel.Namespace,
					Name:      tt.baseModel.Name,
				},
			})
			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Run validation
			tt.validate(t, c, tt.baseModel, result, err)
		})
	}
}

func TestClusterBaseModelReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Create scheme
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	// batchv1 needed for cleanupStalePVCArtifacts which lists Jobs on
	// every non-PVC reconcile (idempotent no-op when none exist; the
	// List itself still requires the scheme).
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name             string
		clusterBaseModel *v1beta1.ClusterBaseModel
		setupMocks       func(client.Client)
		validate         func(*testing.T, client.Client, *v1beta1.ClusterBaseModel)
		wantErr          bool
	}{
		{
			name: "New ClusterBaseModel gets finalizer",
			clusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-model",
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "gguf",
					},
				},
			},
			setupMocks: func(c client.Client) {},
			validate: func(t *testing.T, c client.Client, clusterBaseModel *v1beta1.ClusterBaseModel) {
				updated := &v1beta1.ClusterBaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name: clusterBaseModel.Name,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.ClusterBaseModelFinalizer))
			},
		},
		{
			name: "ClusterBaseModel status update from multiple nodes",
			clusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "multi-node-cluster-model",
					Finalizers: []string{constants.ClusterBaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "vllm",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for i := 1; i <= 3; i++ {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: fmt.Sprintf("cluster-node-%d", i),
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps - all ready
				for i := 1; i <= 3; i++ {
					modelEntry := shared.ModelEntry{
						Status: shared.ModelStatusReady,
						Config: &shared.ModelConfig{
							ModelFramework: map[string]string{
								"name":    "transformers",
								"version": "4.21.0",
							},
						},
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      fmt.Sprintf("cluster-node-%d", i),
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"clusterbasemodel.multi-node-cluster-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, clusterBaseModel *v1beta1.ClusterBaseModel) {
				updated := &v1beta1.ClusterBaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name: clusterBaseModel.Name,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				g.Expect(updated.Status.NodesReady).To(gomega.HaveLen(3))
				g.Expect(updated.Status.NodesReady).To(gomega.ContainElements("cluster-node-1", "cluster-node-2", "cluster-node-3"))

				// Verify spec updates from config
				g.Expect(updated.Spec.ModelFramework).ToNot(gomega.BeNil())
				g.Expect(updated.Spec.ModelFramework.Name).To(gomega.Equal("transformers"))
				g.Expect(updated.Spec.ModelFramework.Version).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelFramework.Version).To(gomega.Equal("4.21.0"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.clusterBaseModel).
				WithStatusSubresource(tt.clusterBaseModel).
				Build()

			if tt.setupMocks != nil {
				tt.setupMocks(c)
			}

			reconciler := &ClusterBaseModelReconciler{
				Client:    c,
				APIReader: c,
				Log:       ctrl.Log.WithName("test"),
				Scheme:    scheme,
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: tt.clusterBaseModel.Name,
				},
			}
			result, err := reconciler.Reconcile(context.TODO(), req)

			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))

				if tt.validate != nil {
					tt.validate(t, c, tt.clusterBaseModel)
				}
			}
		})
	}
}

func TestUpdateSpecWithConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name         string
		initialSpec  *v1beta1.BaseModelSpec
		config       *shared.ModelConfig
		expectUpdate bool
		validateSpec func(*v1beta1.BaseModelSpec)
	}{
		{
			name: "Complete config update",
			initialSpec: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "", // Empty so it can be updated
				},
			},
			config: &shared.ModelConfig{
				ModelType:          "llama",
				ModelArchitecture:  "LlamaForCausalLM",
				Quantization:       "nvfp4",
				ModelParameterSize: "7B",
				ModelCapabilities:  []string{"TEXT_GENERATION", "CHAT"},
				ModelFramework: map[string]string{
					"name":    "transformers",
					"version": "4.21.0",
				},
				ModelFormat: map[string]string{
					"name":    "safetensors",
					"version": "0.3.0",
				},
				MaxTokens: 4096,
			},
			expectUpdate: true,
			validateSpec: func(spec *v1beta1.BaseModelSpec) {
				g.Expect(spec.ModelType).ToNot(gomega.BeNil())
				g.Expect(*spec.ModelType).To(gomega.Equal("llama"))
				g.Expect(spec.ModelArchitecture).ToNot(gomega.BeNil())
				g.Expect(*spec.ModelArchitecture).To(gomega.Equal("LlamaForCausalLM"))
				g.Expect(spec.Quantization).ToNot(gomega.BeNil())
				g.Expect(*spec.Quantization).To(gomega.Equal(v1beta1.ModelQuantization("nvfp4")))
				g.Expect(spec.ModelParameterSize).ToNot(gomega.BeNil())
				g.Expect(*spec.ModelParameterSize).To(gomega.Equal("7B"))
				g.Expect(spec.ModelCapabilities).To(gomega.Equal([]string{"TEXT_GENERATION", "CHAT"}))
				g.Expect(spec.ModelFramework).ToNot(gomega.BeNil())
				g.Expect(spec.ModelFramework.Name).To(gomega.Equal("transformers"))
				g.Expect(*spec.ModelFramework.Version).To(gomega.Equal("4.21.0"))
				g.Expect(spec.ModelFormat.Name).To(gomega.Equal("safetensors"))
				g.Expect(*spec.ModelFormat.Version).To(gomega.Equal("0.3.0"))
				g.Expect(spec.MaxTokens).ToNot(gomega.BeNil())
				g.Expect(*spec.MaxTokens).To(gomega.Equal(int32(4096)))
			},
		},
		{
			name: "No update when fields already set",
			initialSpec: &v1beta1.BaseModelSpec{
				ModelType:         stringPtr("existing-type"),
				ModelArchitecture: stringPtr("existing-arch"),
				Quantization:      quantPtr("existing-quant"),
				ModelFormat: v1beta1.ModelFormat{
					Name:    "existing-format",
					Version: stringPtr("1.0.0"),
				},
				MaxTokens: int32Ptr(2048),
			},
			config: &shared.ModelConfig{
				ModelType:         "new-type",
				ModelArchitecture: "new-arch",
				Quantization:      "new-quant",
				ModelFormat: map[string]string{
					"name":    "new-format",
					"version": "2.0.0",
				},
				MaxTokens: 4096,
			},
			expectUpdate: false,
			validateSpec: func(spec *v1beta1.BaseModelSpec) {
				// Values should remain unchanged
				g.Expect(*spec.ModelType).To(gomega.Equal("existing-type"))
				g.Expect(*spec.ModelArchitecture).To(gomega.Equal("existing-arch"))
				g.Expect(*spec.Quantization).To(gomega.Equal(v1beta1.ModelQuantization("existing-quant")))
				g.Expect(spec.ModelFormat.Name).To(gomega.Equal("existing-format"))
				g.Expect(*spec.ModelFormat.Version).To(gomega.Equal("1.0.0"))
				g.Expect(*spec.MaxTokens).To(gomega.Equal(int32(2048)))
			},
		},
		{
			name:         "Nil inputs return false",
			initialSpec:  nil,
			config:       &shared.ModelConfig{},
			expectUpdate: false,
		},
		{
			name: "Nil config returns false",
			initialSpec: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{Name: "test"},
			},
			config:       nil,
			expectUpdate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated := shared.UpdateSpecWithConfig(tt.initialSpec, tt.config)
			g.Expect(updated).To(gomega.Equal(tt.expectUpdate))

			if tt.validateSpec != nil && tt.initialSpec != nil {
				tt.validateSpec(tt.initialSpec)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}

func quantPtr(q v1beta1.ModelQuantization) *v1beta1.ModelQuantization {
	return &q
}

func int32Ptr(i int32) *int32 {
	return &i
}
