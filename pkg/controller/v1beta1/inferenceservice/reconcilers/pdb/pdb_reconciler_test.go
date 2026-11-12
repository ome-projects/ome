package pdb

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// verbCalls counts the calls of the four verbs the reconciler may issue.
type verbCalls struct {
	gets, creates, updates, deletes *fakeclient.Calls
}

// countedClient layers one counter per verb over base, so a test reads the
// store through base and counts what the reconciler did through the view.
func countedClient(t *testing.T, base client.WithWatch) (client.WithWatch, verbCalls) {
	t.Helper()
	n := verbCalls{
		gets:    fakeclient.CountingVerb(fakeclient.Get),
		creates: fakeclient.CountingVerb(fakeclient.Create),
		updates: fakeclient.CountingVerb(fakeclient.Update),
		deletes: fakeclient.CountingVerb(fakeclient.Delete),
	}
	return fakeclient.Wrap(t, base, n.gets, n.creates, n.updates, n.deletes), n
}

func intOrStringPtr(value intstr.IntOrString) *intstr.IntOrString {
	return &value
}

func testOwner() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "test-ns",
			UID:       types.UID("isvc-uid"),
		},
	}
}

func testRequest() Request {
	return Request{
		Owner: testOwner(),
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-engine",
			Namespace: "test-ns",
		},
		Selector: map[string]string{
			"example.com/component": "engine",
		},
		Budget: &Budget{MaxUnavailable: intOrStringPtr(intstr.FromInt(1))},
	}
}

func controllerRef(owner *v1beta1.InferenceService) metav1.OwnerReference {
	return *metav1.NewControllerRef(owner, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
}

func observerRef() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "example.com/v1",
		Kind:       "Observer",
		Name:       "budget-observer",
		UID:        types.UID("observer-uid"),
	}
}

func foreignControllerRef() metav1.OwnerReference {
	ref := controllerRef(testOwner())
	ref.Name = "other"
	ref.UID = types.UID("other-uid")
	return ref
}

func pdbForRequest(request Request) *policyv1.PodDisruptionBudget {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:            request.ObjectMeta.Name,
			Namespace:       request.ObjectMeta.Namespace,
			UID:             types.UID("pdb-uid"),
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{controllerRef(request.Owner)},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: request.Selector},
		},
	}
	if request.Budget != nil {
		pdb.Spec.MinAvailable = request.Budget.MinAvailable
		pdb.Spec.MaxUnavailable = request.Budget.MaxUnavailable
	}
	return pdb
}

func TestPDBReconcilerCreateUsesExplicitRequest(t *testing.T) {
	tests := []struct {
		name   string
		budget *Budget
	}{
		{
			name:   "minimum available",
			budget: &Budget{MinAvailable: intOrStringPtr(intstr.FromString("50%"))},
		},
		{
			name:   "maximum unavailable",
			budget: &Budget{MaxUnavailable: intOrStringPtr(intstr.FromInt(2))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := fakeclient.Scheme(t)
			cl := fakeclient.New(t, fakeclient.WithScheme(scheme))
			request := testRequest()
			request.Selector = map[string]string{
				"example.com/workload": "custom",
				"example.com/tier":     "serving",
			}
			request.Budget = tt.budget
			request.ObjectMeta.Labels = map[string]string{"example.com/managed": "true"}
			request.ObjectMeta.Annotations = map[string]string{"example.com/note": "keep"}
			request.ObjectMeta.OwnerReferences = []metav1.OwnerReference{observerRef()}

			obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
			require.NoError(t, err)
			require.NotNil(t, obj)

			got := &policyv1.PodDisruptionBudget{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
				Namespace: request.ObjectMeta.Namespace,
				Name:      request.ObjectMeta.Name,
			}, got))
			assert.Equal(t, request.Selector, got.Spec.Selector.MatchLabels)
			assert.Equal(t, tt.budget.MinAvailable, got.Spec.MinAvailable)
			assert.Equal(t, tt.budget.MaxUnavailable, got.Spec.MaxUnavailable)
			assert.Equal(t, request.ObjectMeta.Labels, got.Labels)
			assert.Equal(t, request.ObjectMeta.Annotations, got.Annotations)
			assert.Contains(t, got.OwnerReferences, observerRef())
			controller := metav1.GetControllerOf(got)
			require.NotNil(t, controller)
			assert.Equal(t, request.Owner.UID, controller.UID)
			assert.Equal(t, "InferenceService", controller.Kind)
		})
	}
}

func TestPDBReconcilerRejectsInvalidRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Request)
		message string
	}{
		{
			name:    "nil owner",
			mutate:  func(request *Request) { request.Owner = nil },
			message: "owner",
		},
		{
			name: "empty owner UID",
			mutate: func(request *Request) {
				request.Owner = request.Owner.DeepCopy()
				request.Owner.UID = ""
			},
			message: "UID",
		},
		{
			name:    "empty name",
			mutate:  func(request *Request) { request.ObjectMeta.Name = "" },
			message: "name is required",
		},
		{
			name:    "empty namespace",
			mutate:  func(request *Request) { request.ObjectMeta.Namespace = "" },
			message: "namespace is required",
		},
		{
			name: "namespace mismatch",
			mutate: func(request *Request) {
				request.ObjectMeta.Namespace = "other-ns"
			},
			message: "namespace",
		},
		{
			name:    "nil selector",
			mutate:  func(request *Request) { request.Selector = nil },
			message: "selector",
		},
		{
			name:    "empty selector",
			mutate:  func(request *Request) { request.Selector = map[string]string{} },
			message: "selector",
		},
		{
			name: "both budget fields",
			mutate: func(request *Request) {
				request.Budget = &Budget{
					MinAvailable:   intOrStringPtr(intstr.FromInt(1)),
					MaxUnavailable: intOrStringPtr(intstr.FromInt(1)),
				}
			},
			message: "exactly one",
		},
		{
			name:    "neither budget field",
			mutate:  func(request *Request) { request.Budget = &Budget{} },
			message: "exactly one",
		},
		{
			name: "negative integer",
			mutate: func(request *Request) {
				request.Budget = &Budget{MinAvailable: intOrStringPtr(intstr.FromInt(-1))}
			},
			message: "minAvailable",
		},
		{
			name: "malformed percentage",
			mutate: func(request *Request) {
				request.Budget = &Budget{MaxUnavailable: intOrStringPtr(intstr.FromString("25.5%"))}
			},
			message: "maxUnavailable",
		},
		{
			name: "non-percentage string",
			mutate: func(request *Request) {
				request.Budget = &Budget{MaxUnavailable: intOrStringPtr(intstr.FromString("1"))}
			},
			message: "maxUnavailable",
		},
		{
			name: "percentage over one hundred",
			mutate: func(request *Request) {
				request.Budget = &Budget{MaxUnavailable: intOrStringPtr(intstr.FromString("101%"))}
			},
			message: "maxUnavailable",
		},
		{
			name: "invalid IntOrString type",
			mutate: func(request *Request) {
				request.Budget = &Budget{MaxUnavailable: intOrStringPtr(intstr.IntOrString{Type: 2})}
			},
			message: "maxUnavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := fakeclient.Scheme(t)
			baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme))
			cl, calls := countedClient(t, baseClient)
			request := testRequest()
			tt.mutate(&request)

			obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
			assert.Nil(t, obj)
			assert.ErrorContains(t, err, tt.message)
			assert.Zero(t, calls.gets.Count())
			assert.Zero(t, calls.creates.Count())
			assert.Zero(t, calls.updates.Count())
			assert.Zero(t, calls.deletes.Count())
		})
	}
}

func TestPDBReconcilerCreateSanitizesMetadata(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	now := metav1.Now()
	gracePeriod := int64(30)
	request.ObjectMeta = metav1.ObjectMeta{
		Name:                       request.ObjectMeta.Name,
		GenerateName:               "ignored-",
		Namespace:                  request.ObjectMeta.Namespace,
		SelfLink:                   "/apis/policy/v1/namespaces/test-ns/poddisruptionbudgets/demo-engine",
		UID:                        types.UID("request-uid"),
		ResourceVersion:            "99",
		Generation:                 7,
		CreationTimestamp:          now,
		DeletionTimestamp:          &now,
		DeletionGracePeriodSeconds: &gracePeriod,
		Labels:                     map[string]string{"example.com/managed": "true"},
		Annotations:                map[string]string{"example.com/note": "keep"},
		OwnerReferences:            []metav1.OwnerReference{observerRef(), foreignControllerRef()},
		Finalizers:                 []string{"example.com/finalizer"},
		ManagedFields:              []metav1.ManagedFieldsEntry{{Manager: "test"}},
	}

	var created *policyv1.PodDisruptionBudget
	cl := fakeclient.New(t,
		fakeclient.WithScheme(scheme),
		fakeclient.WithInterceptors(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
				created = obj.(*policyv1.PodDisruptionBudget).DeepCopy()
				return nil
			},
		}))

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.NotNil(t, created)
	assert.Equal(t, metav1.ObjectMeta{
		Name:            request.ObjectMeta.Name,
		Namespace:       request.ObjectMeta.Namespace,
		Labels:          request.ObjectMeta.Labels,
		Annotations:     request.ObjectMeta.Annotations,
		OwnerReferences: []metav1.OwnerReference{observerRef(), controllerRef(request.Owner)},
	}, created.ObjectMeta)
}

func TestPDBReconcilerOwnedEqualDoesNotWrite(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	policy := policyv1.AlwaysAllow
	existing.Spec.UnhealthyPodEvictionPolicy = &policy
	existing.Labels = map[string]string{"example.com/injected": "keep"}
	existing.Finalizers = []string{"example.com/finalizer"}
	existing.Status = policyv1.PodDisruptionBudgetStatus{CurrentHealthy: 3, DesiredHealthy: 2}

	baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
	cl, calls := countedClient(t, baseClient)

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Zero(t, calls.creates.Count())
	assert.Zero(t, calls.updates.Count())
	assert.Zero(t, calls.deletes.Count())
}

func TestPDBReconcilerPreflightRejectsUnavailableObjectWithoutWrites(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*policyv1.PodDisruptionBudget)
		assertError func(*testing.T, error)
	}{
		{
			name: "foreign controller",
			mutate: func(existing *policyv1.PodDisruptionBudget) {
				existing.OwnerReferences = []metav1.OwnerReference{foreignControllerRef()}
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				assert.True(t, apierrors.IsConflict(err), "error = %v", err)
			},
		},
		{
			name: "terminating owned object",
			mutate: func(existing *policyv1.PodDisruptionBudget) {
				existing.Finalizers = []string{"example.com/finalizer"}
				now := metav1.Now()
				existing.DeletionTimestamp = &now
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				assert.ErrorContains(t, err, "terminating")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := fakeclient.Scheme(t)
			request := testRequest()
			existing := pdbForRequest(request)
			tt.mutate(existing)
			baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
			cl, calls := countedClient(t, baseClient)

			err := NewPDBReconcilerWithReader(cl, nil, scheme).Preflight(context.Background(), request)
			tt.assertError(t, err)
			assert.Equal(t, 1, calls.gets.Count())
			assert.Zero(t, calls.creates.Count())
			assert.Zero(t, calls.updates.Count())
			assert.Zero(t, calls.deletes.Count())
		})
	}
}

func TestPDBReconcilerPreflightSkipsLookupWithoutDesiredBudget(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	request.Budget = nil
	request.Selector = nil
	cl, calls := countedClient(t, fakeclient.New(t, fakeclient.WithScheme(scheme)))

	require.NoError(t, NewPDBReconcilerWithReader(cl, nil, scheme).Preflight(context.Background(), request))
	assert.Zero(t, calls.gets.Count())
	assert.Zero(t, calls.creates.Count())
	assert.Zero(t, calls.updates.Count())
	assert.Zero(t, calls.deletes.Count())
}

func TestPDBReconcilerPreflightUsesLiveReader(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	existing.OwnerReferences = []metav1.OwnerReference{foreignControllerRef()}
	cachedClient := fakeclient.New(t, fakeclient.WithScheme(scheme))
	liveReader := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))

	err := NewPDBReconcilerWithReader(cachedClient, liveReader, scheme).Preflight(
		context.Background(), request,
	)
	assert.True(t, apierrors.IsConflict(err), "error = %v", err)
}

func TestPDBReconcilerMutationUsesLiveReader(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	existing.OwnerReferences = []metav1.OwnerReference{foreignControllerRef()}
	cachedClient, calls := countedClient(t, fakeclient.New(t, fakeclient.WithScheme(scheme)))
	liveReader := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))

	_, err := NewPDBReconcilerWithReader(cachedClient, liveReader, scheme).Reconcile(
		context.Background(), request,
	)
	assert.True(t, apierrors.IsConflict(err), "error = %v", err)
	assert.Zero(t, calls.creates.Count())
	assert.Zero(t, calls.updates.Count())
	assert.Zero(t, calls.deletes.Count())
}

func TestPDBReconcilerDefersSelectorCutoverUntilTargetIsReady(t *testing.T) {
	tests := []struct {
		name   string
		budget *Budget
	}{
		{
			name:   "update",
			budget: &Budget{MinAvailable: intOrStringPtr(intstr.FromInt(2))},
		},
		{
			name: "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := fakeclient.Scheme(t)
			originalRequest := testRequest()
			existing := pdbForRequest(originalRequest)
			baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
			cl, calls := countedClient(t, baseClient)
			request := testRequest()
			request.Selector = map[string]string{"example.com/component": "target"}
			request.Budget = tt.budget
			request.CutoverFromSelector = originalRequest.Selector

			obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
			require.NoError(t, err)
			require.NotNil(t, obj)
			assert.Equal(t, originalRequest.Selector, obj.Spec.Selector.MatchLabels)
			assert.Zero(t, calls.updates.Count())
			assert.Zero(t, calls.deletes.Count())

			request.SelectorCutoverReady = true
			obj, err = NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
			require.NoError(t, err)
			if tt.budget == nil {
				assert.Nil(t, obj)
				err := baseClient.Get(context.Background(), requestKey(request), &policyv1.PodDisruptionBudget{})
				assert.True(t, apierrors.IsNotFound(err), "PodDisruptionBudget must be deleted, got %v", err)
				return
			}
			require.NotNil(t, obj)
			assert.Equal(t, request.Selector, obj.Spec.Selector.MatchLabels)
			assert.Equal(t, 1, calls.updates.Count())
		})
	}
}

func TestPDBReconcilerRepairsUnexpectedSelectorDriftBeforeTargetReady(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	request.CutoverFromSelector = map[string]string{"example.com/component": "source"}
	request.Selector = map[string]string{"example.com/component": "target"}
	existing := pdbForRequest(request)
	existing.Spec.Selector = &metav1.LabelSelector{}
	baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
	cl, calls := countedClient(t, baseClient)

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, request.Selector, obj.Spec.Selector.MatchLabels)
	assert.Equal(t, 1, calls.updates.Count())
}

func TestPDBReconcilerDesiredPresentRejectsTerminatingOwnedPDB(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	existing.Finalizers = []string{"example.com/finalizer"}
	now := metav1.Now()
	existing.DeletionTimestamp = &now

	baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
	cl, calls := countedClient(t, baseClient)

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	assert.Nil(t, obj)
	assert.ErrorContains(t, err, "terminating")
	assert.Zero(t, calls.creates.Count())
	assert.Zero(t, calls.updates.Count())
	assert.Zero(t, calls.deletes.Count())

	got := &policyv1.PodDisruptionBudget{}
	require.NoError(t, baseClient.Get(context.Background(), types.NamespacedName{
		Namespace: existing.Namespace,
		Name:      existing.Name,
	}, got))
	assert.Equal(t, existing.UID, got.UID)
	assert.NotNil(t, got.DeletionTimestamp)
}

func TestPDBReconcilerOwnedDriftPreservesUnmanagedFields(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	existing.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"example.com/old": "selector"}}
	existing.Spec.MaxUnavailable = intOrStringPtr(intstr.FromInt(3))
	policy := policyv1.AlwaysAllow
	existing.Spec.UnhealthyPodEvictionPolicy = &policy
	existing.Labels = map[string]string{"example.com/injected": "keep"}
	existing.Annotations = map[string]string{"example.com/note": "keep"}
	existing.Finalizers = []string{"example.com/finalizer"}
	existing.OwnerReferences = append(existing.OwnerReferences, observerRef())
	existing.Status = policyv1.PodDisruptionBudgetStatus{
		ObservedGeneration: 7,
		CurrentHealthy:     3,
		DesiredHealthy:     2,
		ExpectedPods:       4,
	}
	request.Budget = &Budget{MinAvailable: intOrStringPtr(intstr.FromString("50%"))}
	request.SelectorCutoverReady = true
	request.ObjectMeta.Labels = map[string]string{"example.com/request": "ignored-on-update"}
	request.ObjectMeta.Annotations = map[string]string{"example.com/request": "ignored-on-update"}

	baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
	cl, calls := countedClient(t, baseClient)

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, 1, calls.updates.Count())

	got := &policyv1.PodDisruptionBudget{}
	require.NoError(t, baseClient.Get(context.Background(), types.NamespacedName{
		Namespace: request.ObjectMeta.Namespace,
		Name:      request.ObjectMeta.Name,
	}, got))
	assert.Equal(t, request.Selector, got.Spec.Selector.MatchLabels)
	assert.Equal(t, request.Budget.MinAvailable, got.Spec.MinAvailable)
	assert.Nil(t, got.Spec.MaxUnavailable)
	assert.Equal(t, existing.Spec.UnhealthyPodEvictionPolicy, got.Spec.UnhealthyPodEvictionPolicy)
	assert.Equal(t, existing.Labels, got.Labels)
	assert.Equal(t, existing.Annotations, got.Annotations)
	assert.Equal(t, existing.Finalizers, got.Finalizers)
	assert.Equal(t, existing.OwnerReferences, got.OwnerReferences)
	assert.Equal(t, existing.Status, got.Status)
}

func TestPDBReconcilerNormalizesSameUIDControllerReference(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	driftedController := controllerRef(request.Owner)
	driftedController.APIVersion = "legacy.example/v1"
	driftedController.Kind = "LegacyInferenceService"
	driftedController.Name = "stale-name"
	driftedController.BlockOwnerDeletion = nil
	existing.OwnerReferences = []metav1.OwnerReference{driftedController, observerRef()}

	baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
	cl, calls := countedClient(t, baseClient)

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, 1, calls.updates.Count())

	got := &policyv1.PodDisruptionBudget{}
	require.NoError(t, baseClient.Get(context.Background(), types.NamespacedName{
		Namespace: request.ObjectMeta.Namespace,
		Name:      request.ObjectMeta.Name,
	}, got))
	controller := metav1.GetControllerOf(got)
	require.NotNil(t, controller)
	assert.Equal(t, controllerRef(request.Owner), *controller)
	assert.Contains(t, got.OwnerReferences, observerRef())
}

func TestPDBReconcilerDesiredPresentRejectsForeignPDB(t *testing.T) {
	tests := []struct {
		name      string
		ownerRefs []metav1.OwnerReference
	}{
		{name: "ownerless"},
		{name: "foreign controller", ownerRefs: []metav1.OwnerReference{foreignControllerRef()}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := fakeclient.Scheme(t)
			request := testRequest()
			existing := pdbForRequest(request)
			existing.OwnerReferences = tt.ownerRefs
			existing.Spec.MaxUnavailable = intOrStringPtr(intstr.FromInt(3))
			original := existing.DeepCopy()
			baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
			cl, calls := countedClient(t, baseClient)

			_, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
			assert.True(t, apierrors.IsConflict(err), "error = %v", err)
			assert.Zero(t, calls.creates.Count())
			assert.Zero(t, calls.updates.Count())
			assert.Zero(t, calls.deletes.Count())

			got := &policyv1.PodDisruptionBudget{}
			require.NoError(t, baseClient.Get(context.Background(), types.NamespacedName{
				Namespace: request.ObjectMeta.Namespace,
				Name:      request.ObjectMeta.Name,
			}, got))
			assert.Equal(t, original.Spec, got.Spec)
			assert.Equal(t, original.OwnerReferences, got.OwnerReferences)
		})
	}
}

func TestPDBReconcilerDesiredAbsentPreservesForeignPDB(t *testing.T) {
	tests := []struct {
		name      string
		ownerRefs []metav1.OwnerReference
	}{
		{name: "ownerless"},
		{name: "foreign controller", ownerRefs: []metav1.OwnerReference{foreignControllerRef()}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := fakeclient.Scheme(t)
			request := testRequest()
			request.Budget = nil
			request.Selector = nil
			existing := pdbForRequest(testRequest())
			existing.OwnerReferences = tt.ownerRefs
			baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing))
			cl, calls := countedClient(t, baseClient)

			obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
			require.NoError(t, err)
			require.NotNil(t, obj)
			assert.Equal(t, existing.UID, obj.UID)
			assert.Zero(t, calls.deletes.Count())
		})
	}
}

func TestPDBReconcilerDesiredAbsentNotFoundIsNoOp(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	baseClient := fakeclient.New(t, fakeclient.WithScheme(scheme))
	cl, calls := countedClient(t, baseClient)
	request := testRequest()
	request.Budget = nil
	request.Selector = nil

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.Nil(t, obj)
	assert.Equal(t, 1, calls.gets.Count())
	assert.Zero(t, calls.creates.Count())
	assert.Zero(t, calls.updates.Count())
	assert.Zero(t, calls.deletes.Count())
}

func TestPDBReconcilerDesiredAbsentDeletesOwnedWithPreconditions(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	request.Budget = nil
	existing := pdbForRequest(testRequest())

	deleteCalls := 0
	cl := fakeclient.New(t,
		fakeclient.WithScheme(scheme),
		fakeclient.WithObjects(existing),
		fakeclient.WithInterceptors(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deleteCalls++
				deleteOptions := &client.DeleteOptions{}
				for _, opt := range opts {
					opt.ApplyToDelete(deleteOptions)
				}
				require.NotNil(t, deleteOptions.Preconditions)
				require.NotNil(t, deleteOptions.Preconditions.UID)
				assert.Equal(t, obj.GetUID(), *deleteOptions.Preconditions.UID)
				require.NotNil(t, deleteOptions.Preconditions.ResourceVersion)
				assert.Equal(t, obj.GetResourceVersion(), *deleteOptions.Preconditions.ResourceVersion)
				return nil
			},
		}))

	obj, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.Nil(t, obj)
	assert.Equal(t, 1, deleteCalls)
}

func TestPDBReconcilerOperationFailuresPropagate(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *runtime.Scheme) (client.Client, Request)
		marker  error
	}{
		{
			name:   "get",
			marker: errors.New("get failure"),
			prepare: func(t *testing.T, scheme *runtime.Scheme) (client.Client, Request) {
				marker := errors.New("get failure")
				return fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, marker)), testRequest()
			},
		},
		{
			name:   "create",
			marker: errors.New("create failure"),
			prepare: func(t *testing.T, scheme *runtime.Scheme) (client.Client, Request) {
				marker := errors.New("create failure")
				return fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.FailingVerb(fakeclient.Create, fakeclient.EveryCall, marker)), testRequest()
			},
		},
		{
			name:   "update",
			marker: errors.New("update failure"),
			prepare: func(t *testing.T, scheme *runtime.Scheme) (client.Client, Request) {
				request := testRequest()
				existing := pdbForRequest(request)
				existing.Spec.MaxUnavailable = intOrStringPtr(intstr.FromInt(3))
				marker := errors.New("update failure")
				return fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing), fakeclient.FailingVerb(fakeclient.Update, fakeclient.EveryCall, marker)), request
			},
		},
		{
			name:   "delete",
			marker: errors.New("delete failure"),
			prepare: func(t *testing.T, scheme *runtime.Scheme) (client.Client, Request) {
				request := testRequest()
				existing := pdbForRequest(request)
				request.Budget = nil
				request.Selector = nil
				marker := errors.New("delete failure")
				return fakeclient.New(t, fakeclient.WithScheme(scheme), fakeclient.WithObjects(existing), fakeclient.FailingVerb(fakeclient.Delete, fakeclient.EveryCall, marker)), request
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := fakeclient.Scheme(t)
			cl, request := tt.prepare(t, scheme)
			_, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
			assert.ErrorContains(t, err, tt.marker.Error())
		})
	}
}

func TestPDBReconcilerUpdateConflictPreservesConcurrentState(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	existing.Spec.MaxUnavailable = intOrStringPtr(intstr.FromInt(3))
	key := types.NamespacedName{Namespace: existing.Namespace, Name: existing.Name}

	cl := fakeclient.New(t,
		fakeclient.WithScheme(scheme),
		fakeclient.WithObjects(existing),
		fakeclient.WithInterceptors(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				fresh := &policyv1.PodDisruptionBudget{}
				require.NoError(t, c.Get(ctx, key, fresh))
				fresh.Annotations = map[string]string{"example.com/concurrent": "keep"}
				require.NoError(t, c.Update(ctx, fresh))
				return c.Update(ctx, obj, opts...)
			},
		}))

	_, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	assert.True(t, apierrors.IsConflict(err), "error = %v", err)

	got := &policyv1.PodDisruptionBudget{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	assert.Equal(t, "keep", got.Annotations["example.com/concurrent"])
	assert.Equal(t, int32(3), got.Spec.MaxUnavailable.IntVal)
}

func TestPDBReconcilerDeleteConflictPreservesOwnerHandoff(t *testing.T) {
	scheme := fakeclient.Scheme(t)
	request := testRequest()
	existing := pdbForRequest(request)
	request.Budget = nil
	request.Selector = nil
	key := types.NamespacedName{Namespace: existing.Namespace, Name: existing.Name}

	cl := fakeclient.New(t,
		fakeclient.WithScheme(scheme),
		fakeclient.WithObjects(existing),
		fakeclient.WithInterceptors(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				fresh := &policyv1.PodDisruptionBudget{}
				require.NoError(t, c.Get(ctx, key, fresh))
				fresh.OwnerReferences = []metav1.OwnerReference{foreignControllerRef()}
				require.NoError(t, c.Update(ctx, fresh))
				return c.Delete(ctx, obj, opts...)
			},
		}))

	_, err := NewPDBReconcilerWithReader(cl, nil, scheme).Reconcile(context.Background(), request)
	assert.True(t, apierrors.IsConflict(err), "error = %v", err)

	got := &policyv1.PodDisruptionBudget{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	controller := metav1.GetControllerOf(got)
	require.NotNil(t, controller)
	assert.Equal(t, foreignControllerRef().UID, controller.UID)
}
