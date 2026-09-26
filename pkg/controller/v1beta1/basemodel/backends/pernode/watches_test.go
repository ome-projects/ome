package pernode

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestNodePlacementFanoutWithoutConfigMaps(t *testing.T) {
	ctx := context.Background()
	base, cluster := restorationModel(false), restorationModel(true)
	c := restorationClient(t, base, cluster)
	node := restorationNode("node-a")
	p := CreateNodePlacementPredicate()
	require.True(t, p.Create(event.CreateEvent{Object: node}))
	require.True(t, p.Delete(event.DeleteEvent{Object: node}))
	changed := node.DeepCopy()
	changed.Labels["pool"] = "other"
	require.True(t, p.Update(event.UpdateEvent{ObjectOld: node, ObjectNew: changed}))
	require.False(t, p.Update(event.UpdateEvent{ObjectOld: node, ObjectNew: node.DeepCopy()}))
	changed = node.DeepCopy()
	changed.Spec.Unschedulable = true
	changed.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	require.False(t, p.Update(event.UpdateEvent{ObjectOld: node, ObjectNew: changed}))
	for _, namespaced := range []bool{false, true} {
		requests := MapNodeToRestorationRequests(ctx, c, ctrl.Log, node, namespaced)
		require.Len(t, requests, 1)
		expected := cluster
		if namespaced {
			expected = base
		}
		require.Equal(t, client.ObjectKeyFromObject(expected), requests[0].NamespacedName)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: node.Name, Namespace: constants.OMENamespace}}
	require.NoError(t, c.Create(ctx, cm))
	require.Len(t, MapNodeToRestorationRequests(ctx, c, ctrl.Log, node, true), 1)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{}), "placement fanout must not delete reports")
	require.False(t, CreateNodeDeletionPredicate().Create(event.CreateEvent{Object: node}))
	require.False(t, CreateNodeDeletionPredicate().Update(event.UpdateEvent{ObjectOld: node, ObjectNew: changed}))
}

func TestConfigMapUpdateEnqueuesOldAndMalformedNewKeys(t *testing.T) {
	old := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: constants.OMENamespace, Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"}}, Data: map[string]string{"ns.basemodel.old": `{"status":"Ready"}`}}
	current := old.DeepCopy()
	current.Labels = nil
	current.Data = map[string]string{"ns.basemodel.new": "{"}
	e := event.UpdateEvent{ObjectOld: old, ObjectNew: current}
	require.True(t, CreateModelStatusConfigMapPredicate().Update(e))
	h := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
		return MapConfigMapToModelRequests(obj, ctrl.Log, true)
	})
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer queue.ShutDown()
	h.Update(context.Background(), e, queue)
	require.Equal(t, 2, queue.Len())
	var names []string
	for queue.Len() > 0 {
		req, _ := queue.Get()
		names = append(names, req.Name)
		queue.Done(req)
	}
	require.ElementsMatch(t, []string{"old", "new"}, names)
}

func TestMapConfigMapToModelRequests(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	logger := ctrl.Log.WithName("test")

	tests := []struct {
		name          string
		configMap     *corev1.ConfigMap
		isNamespaced  bool
		expectedCount int
		expectedFirst *types.NamespacedName
	}{
		{
			name: "BaseModel mapping",
			configMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{
					"default.basemodel.my-model":     `{"status":"Ready"}`,
					"test-ns.basemodel.other-model":  `{"status":"Failed"}`,
					"clusterbasemodel.cluster-model": `{"status":"Ready"}`, // Should be ignored
				},
			},
			isNamespaced:  true,
			expectedCount: 2,
			expectedFirst: nil, // Don't check specific order since map iteration is random
		},
		{
			name: "ClusterBaseModel mapping",
			configMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cluster-node",
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{
					"clusterbasemodel.global-model":    `{"status":"Ready"}`,
					"clusterbasemodel.multi.part.name": `{"status":"InTransit"}`,
					"basemodel.default.local-model":    `{"status":"Ready"}`, // Should be ignored
				},
			},
			isNamespaced:  false,
			expectedCount: 2,
			expectedFirst: nil, // Don't check specific order since map iteration is random
		},
		{
			name: "Invalid JSON data",
			configMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-node",
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{
					"default.basemodel.broken-model": `{invalid json}`,
					"default.basemodel.valid-model":  `{"status":"Ready"}`,
				},
			},
			isNamespaced:  true,
			expectedCount: 2, // A malformed report must also invalidate old status.
		},
		{
			name:          "Non-ConfigMap object",
			configMap:     nil, // Will pass a different object type
			isNamespaced:  true,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requests []reconcile.Request

			if tt.configMap != nil {
				requests = MapConfigMapToModelRequests(tt.configMap, logger, tt.isNamespaced)
			} else {
				// Pass a non-ConfigMap object
				requests = MapConfigMapToModelRequests(&corev1.Pod{}, logger, true)
			}

			g.Expect(requests).To(gomega.HaveLen(tt.expectedCount))

			if tt.expectedFirst != nil && len(requests) > 0 {
				g.Expect(requests[0].NamespacedName).To(gomega.Equal(*tt.expectedFirst))
			} else if tt.expectedCount > 0 {
				// Instead of checking order, verify that all expected requests are present
				// For BaseModel mapping case
				if tt.name == "BaseModel mapping" {
					foundDefault := false
					foundTestNs := false

					for _, req := range requests {
						if req.NamespacedName.Namespace == "default" && req.NamespacedName.Name == "my-model" {
							foundDefault = true
						}
						if req.NamespacedName.Namespace == "test-ns" && req.NamespacedName.Name == "other-model" {
							foundTestNs = true
						}
					}

					g.Expect(foundDefault).To(gomega.BeTrue(), "Should find default.my-model")
					g.Expect(foundTestNs).To(gomega.BeTrue(), "Should find test-ns.other-model")
				}
			}
		})
	}
}

func TestCreateModelStatusConfigMapPredicate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	predicate := CreateModelStatusConfigMapPredicate()

	tests := []struct {
		name     string
		obj      client.Object
		expected bool
	}{
		{
			name: "Valid model status ConfigMap",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
					Labels: map[string]string{
						constants.ModelStatusConfigMapLabel: "true",
					},
				},
			},
			expected: true,
		},
		{
			name: "ConfigMap in wrong namespace",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: "wrong-namespace",
					Labels: map[string]string{
						constants.ModelStatusConfigMapLabel: "true",
					},
				},
			},
			expected: false,
		},
		{
			name: "ConfigMap without label",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
				},
			},
			expected: false,
		},
		{
			name: "ConfigMap with wrong label value",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
					Labels: map[string]string{
						constants.ModelStatusConfigMapLabel: "false",
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test CreateFunc
			result := predicate.Create(event.TypedCreateEvent[client.Object]{Object: tt.obj})
			g.Expect(result).To(gomega.Equal(tt.expected))

			// Test UpdateFunc
			result = predicate.Update(event.TypedUpdateEvent[client.Object]{ObjectNew: tt.obj})
			g.Expect(result).To(gomega.Equal(tt.expected))

			// Test DeleteFunc
			result = predicate.Delete(event.TypedDeleteEvent[client.Object]{Object: tt.obj})
			g.Expect(result).To(gomega.Equal(tt.expected))
		})
	}
}

func TestCreateNodeDeletionPredicate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	pred := CreateNodeDeletionPredicate()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}

	// CreateFunc should return false
	createResult := pred.Create(event.TypedCreateEvent[client.Object]{Object: node})
	g.Expect(createResult).To(gomega.BeFalse(), "CreateFunc should return false")

	// UpdateFunc should return false
	updateResult := pred.Update(event.TypedUpdateEvent[client.Object]{ObjectNew: node, ObjectOld: node})
	g.Expect(updateResult).To(gomega.BeFalse(), "UpdateFunc should return false")

	// DeleteFunc should return true
	deleteResult := pred.Delete(event.TypedDeleteEvent[client.Object]{Object: node})
	g.Expect(deleteResult).To(gomega.BeTrue(), "DeleteFunc should return true")
}

func TestHandleNodeDeletion(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name       string
		nodeName   string
		setupMocks func(client.Client)
		validate   func(*testing.T, client.Client, string)
	}{
		{
			name:     "Node deletion cleans up associated ConfigMap",
			nodeName: "node-with-configmap",
			setupMocks: func(c client.Client) {
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "node-with-configmap",
						Namespace: constants.OMENamespace,
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"clusterbasemodel.test-model": `{"status":"Ready"}`,
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, nodeName string) {
				configMap := &corev1.ConfigMap{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Namespace: constants.OMENamespace,
					Name:      nodeName,
				}, configMap)
				g.Expect(errors.IsNotFound(err)).To(gomega.BeTrue(), "ConfigMap should be deleted")
			},
		},
		{
			name:     "Node deletion with no ConfigMap does nothing",
			nodeName: "node-without-configmap",
			setupMocks: func(c client.Client) {
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, nodeName string) {
				// No ConfigMap to check - just ensure no error occurred
			},
		},
		{
			name:     "Node deletion skips non-model-status ConfigMap",
			nodeName: "node-with-other-configmap",
			setupMocks: func(c client.Client) {
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "node-with-other-configmap",
						Namespace: constants.OMENamespace,
					},
					Data: map[string]string{
						"some-key": "some-value",
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, nodeName string) {
				configMap := &corev1.ConfigMap{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Namespace: constants.OMENamespace,
					Name:      nodeName,
				}, configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred(), "ConfigMap should still exist")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				Build()

			tt.setupMocks(c)

			log := ctrl.Log.WithName("test")

			deletedNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.nodeName,
				},
			}

			requests := HandleNodeDeletion(context.TODO(), c, log, deletedNode)

			g.Expect(requests).To(gomega.BeNil())

			tt.validate(t, c, tt.nodeName)
		})
	}
}
