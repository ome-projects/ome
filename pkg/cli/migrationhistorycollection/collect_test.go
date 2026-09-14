package migrationhistorycollection

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

var testLimits = paging.Limits{
	PageSize: 2, MaxItems: 4, MaxPages: 3, RequestTimeout: time.Second,
}

func TestCollectReadsBoundedAuthoritativeAndAuditEvidence(t *testing.T) {
	t.Parallel()

	parent := testISVC("chat", "prod")
	omeClient := omefake.NewSimpleClientset(parent)
	omeClient.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		opts := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		switch opts.Continue {
		case "":
			return true, &omev1beta1.InferenceReplicaList{
				ListMeta: metav1.ListMeta{Continue: "next"},
				Items:    []omev1beta1.InferenceReplica{testIR(parent, "chat-engine", omev1beta1.EngineComponent)},
			}, nil
		case "next":
			return true, &omev1beta1.InferenceReplicaList{
				Items: []omev1beta1.InferenceReplica{testIR(parent, "chat-router", omev1beta1.RouterComponent)},
			}, nil
		default:
			return true, nil, errors.New("unexpected continue token")
		}
	})
	audit := testAuditConfigMap(parent, `{"entries":[{"requestUUID":"req-a"}]}`)
	kubeClient := k8sfake.NewSimpleClientset(audit)

	got, err := Collect(context.Background(), omeClient.OmeV1beta1(), kubeClient, "prod", "chat", testLimits)

	require.NoError(t, err)
	require.NotNil(t, got.InferenceService)
	assert.Equal(t, "chat", got.InferenceService.Name)
	require.Len(t, got.InferenceReplicas, 2)
	assert.Equal(t, ReplicaObservation{
		Availability: AvailabilityAvailable, ObservedPages: 2, ObservedItems: 2,
	}, got.Replicas)
	assert.Equal(t, AuditObservation{
		Namespace: "prod", Name: "chat-ome-migration-audit", Availability: AvailabilityAvailable,
		HistoryJSON: `{"entries":[{"requestUUID":"req-a"}]}`,
	}, got.Audit)

	omeActions := omeClient.Actions()
	require.Len(t, omeActions, 3)
	get := omeActions[0].(ktesting.GetAction)
	assert.Equal(t, "prod", get.GetNamespace())
	assert.Equal(t, "chat", get.GetName())
	for i, action := range omeActions[1:] {
		list := action.(ktesting.ListAction)
		value, found := list.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServicePodLabelKey)
		assert.True(t, found)
		assert.Equal(t, "chat", value)
		opts := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Equal(t, int64(2), opts.Limit)
		assert.Equal(t, []string{"", "next"}[i], opts.Continue)
	}
	require.Len(t, kubeClient.Actions(), 1)
	cmGet := kubeClient.Actions()[0].(ktesting.GetAction)
	assert.Equal(t, "configmaps", cmGet.GetResource().Resource)
	assert.Equal(t, "prod", cmGet.GetNamespace())
	assert.Equal(t, "chat-ome-migration-audit", cmGet.GetName())

	// Returned values are detached from both client responses.
	got.InferenceService.Name = "changed"
	got.InferenceReplicas[0].Name = "changed"
	got.Audit.HistoryJSON = "changed"
	assert.Equal(t, "chat", parent.Name)
	assert.Equal(t, `{"entries":[{"requestUUID":"req-a"}]}`, audit.Data[AuditHistoryKey])
}

func TestCollectClassifiesOptionalAuditFailuresWithoutLosingRequiredEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want Availability
	}{
		{name: "absent", err: apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "hidden-secret-name"), want: AvailabilityAbsent},
		{name: "forbidden", err: apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "hidden-secret-name", errors.New("hidden-secret-cause")), want: AvailabilityForbidden},
		{name: "unreadable", err: errors.New("hidden-secret-cause"), want: AvailabilityUnreadable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent := testISVC("chat", "prod")
			omeClient := omefake.NewSimpleClientset(parent)
			kubeClient := k8sfake.NewSimpleClientset()
			kubeClient.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, test.err
			})

			got, err := Collect(context.Background(), omeClient.OmeV1beta1(), kubeClient, "prod", "chat", testLimits)

			require.NoError(t, err)
			assert.Equal(t, test.want, got.Audit.Availability)
			assert.Empty(t, got.Audit.HistoryJSON)
			assert.Equal(t, AvailabilityAvailable, got.Replicas.Availability)
		})
	}
}

func TestCollectClassifiesReplicaListFailuresAsPartialEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want Availability
	}{
		{name: "forbidden", err: apierrors.NewForbidden(schema.GroupResource{Resource: "inferencereplicas"}, "", errors.New("hidden-secret-cause")), want: AvailabilityForbidden},
		{name: "unreadable", err: errors.New("hidden-secret-cause"), want: AvailabilityUnreadable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent := testISVC("chat", "prod")
			omeClient := omefake.NewSimpleClientset(parent)
			omeClient.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, test.err
			})

			got, err := Collect(context.Background(), omeClient.OmeV1beta1(), k8sfake.NewSimpleClientset(), "prod", "chat", testLimits)

			require.NoError(t, err)
			assert.Equal(t, test.want, got.Replicas.Availability)
			assert.Empty(t, got.InferenceReplicas)
			assert.Equal(t, AvailabilityAbsent, got.Audit.Availability)
			encoded := got.Replicas.String()
			assert.NotContains(t, encoded, "hidden-secret")
		})
	}
}

func TestCollectLongNameUsesBoundedNamespaceScanAndExactOwnership(t *testing.T) {
	t.Parallel()

	parent := testISVC(strings.Repeat("a", 240), "prod")
	parent.UID = "uid-parent"
	valid := testIR(parent, "valid", omev1beta1.EngineComponent)
	wrongOwner := testIR(parent, "wrong-owner", omev1beta1.RouterComponent)
	wrongOwner.OwnerReferences[0].UID = "someone-else"
	wrongParent := testIR(parent, "wrong-parent", omev1beta1.DecoderComponent)
	wrongParent.Spec.ParentRef.Name = "someone-else"
	omeClient := omefake.NewSimpleClientset(parent)
	omeClient.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		opts := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Empty(t, opts.LabelSelector)
		return true, &omev1beta1.InferenceReplicaList{
			ListMeta: metav1.ListMeta{Continue: "more"},
			Items:    []omev1beta1.InferenceReplica{wrongOwner, valid, wrongParent},
		}, nil
	})
	limits := testLimits
	limits.MaxItems = 3

	got, err := Collect(context.Background(), omeClient.OmeV1beta1(), k8sfake.NewSimpleClientset(), "prod", parent.Name, limits)

	require.NoError(t, err)
	require.Len(t, got.InferenceReplicas, 1)
	assert.Equal(t, "valid", got.InferenceReplicas[0].Name)
	assert.Equal(t, ReplicaObservation{
		Availability: AvailabilityAvailable, ObservedPages: 1, ObservedItems: 3, Truncated: true,
	}, got.Replicas)
	assert.Equal(t, AvailabilityInvalid, got.Audit.Availability, "derived ConfigMap name is not a DNS subdomain")
}

func TestCollectRejectsMismatchedOrUnownedAuditObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*corev1.ConfigMap)
	}{
		{name: "name", mutate: func(cm *corev1.ConfigMap) { cm.Name = "other" }},
		{name: "namespace", mutate: func(cm *corev1.ConfigMap) { cm.Namespace = "other" }},
		{name: "owner UID", mutate: func(cm *corev1.ConfigMap) { cm.OwnerReferences[0].UID = "secret-stale-uid" }},
		{name: "owner kind", mutate: func(cm *corev1.ConfigMap) { cm.OwnerReferences[0].Kind = "SecretKind" }},
		{name: "multiple controllers", mutate: func(cm *corev1.ConfigMap) { cm.OwnerReferences = append(cm.OwnerReferences, cm.OwnerReferences[0]) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent := testISVC("chat", "prod")
			cm := testAuditConfigMap(parent, `{"entries":[]}`)
			test.mutate(cm)
			kubeClient := k8sfake.NewSimpleClientset()
			kubeClient.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, cm.DeepCopy(), nil
			})

			got, err := Collect(context.Background(), omefake.NewSimpleClientset(parent).OmeV1beta1(), kubeClient, "prod", "chat", testLimits)

			require.NoError(t, err)
			assert.Equal(t, AvailabilityInvalid, got.Audit.Availability)
			assert.Empty(t, got.Audit.HistoryJSON)
		})
	}
}

func TestCollectValidatesInputsAndRequiredParentBeforeSecondaryReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ome       bool
		kube      bool
		namespace string
		isvc      string
		limits    paging.Limits
		want      error
	}{
		{name: "OME client", kube: true, namespace: "prod", isvc: "chat", limits: testLimits, want: ErrOMEClientRequired},
		{name: "Kube client", ome: true, namespace: "prod", isvc: "chat", limits: testLimits, want: ErrKubeClientRequired},
		{name: "namespace", ome: true, kube: true, namespace: "Bad_NS", isvc: "chat", limits: testLimits, want: ErrNamespaceInvalid},
		{name: "name", ome: true, kube: true, namespace: "prod", isvc: "Bad_Name", limits: testLimits, want: ErrInferenceServiceNameInvalid},
		{name: "limits", ome: true, kube: true, namespace: "prod", isvc: "chat", limits: paging.Limits{}, want: ErrLimitsInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var omeClient = omefake.NewSimpleClientset().OmeV1beta1()
			if !test.ome {
				omeClient = nil
			}
			var kubeClient = k8sfake.NewSimpleClientset()
			if !test.kube {
				kubeClient = nil
			}
			_, err := Collect(context.Background(), omeClient, kubeClient, test.namespace, test.isvc, test.limits)
			require.ErrorIs(t, err, test.want)
		})
	}

	parent := testISVC("chat", "prod")
	parent.UID = ""
	omeClient := omefake.NewSimpleClientset(parent)
	kubeClient := k8sfake.NewSimpleClientset()
	_, err := Collect(context.Background(), omeClient.OmeV1beta1(), kubeClient, "prod", "chat", testLimits)
	require.ErrorIs(t, err, ErrInferenceServiceIdentityInvalid)
	assert.Empty(t, kubeClient.Actions())
}

func TestCollectPropagatesCancellationAndRequiredParentErrors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	omeClient := omefake.NewSimpleClientset(testISVC("chat", "prod"))
	kubeClient := k8sfake.NewSimpleClientset()
	_, err := Collect(ctx, omeClient.OmeV1beta1(), kubeClient, "prod", "chat", testLimits)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, omeClient.Actions())
	assert.Empty(t, kubeClient.Actions())

	boom := errors.New("required parent read failed")
	omeClient = omefake.NewSimpleClientset()
	omeClient.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, boom
	})
	_, err = Collect(context.Background(), omeClient.OmeV1beta1(), kubeClient, "prod", "chat", testLimits)
	require.ErrorIs(t, err, boom)
	assert.Empty(t, kubeClient.Actions())
}

func testISVC(name, namespace string) *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: namespace, UID: types.UID("uid-" + name), Generation: 4,
	}}
}

func testIR(parent *omev1beta1.InferenceService, name string, component omev1beta1.ComponentType) omev1beta1.InferenceReplica {
	controller := true
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: parent.Namespace, UID: types.UID("uid-" + name),
			Labels: map[string]string{constants.InferenceServicePodLabelKey: parent.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: parent.Name, UID: parent.UID, Controller: &controller,
			}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{
			ParentRef: omev1beta1.ParentReference{Name: parent.Name}, Component: component,
		},
	}
}

func testAuditConfigMap(parent *omev1beta1.InferenceService, history string) *corev1.ConfigMap {
	controller := true
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: parent.Name + AuditConfigMapSuffix, Namespace: parent.Namespace,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
			Name: parent.Name, UID: parent.UID, Controller: &controller,
		}},
	}, Data: map[string]string{AuditHistoryKey: history}}
}
