package inferenceservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func isvcForURL() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
	}
}

// The caller separates "the Service is not there" from a real failure by asking
// apierrors.IsNotFound, so the API status error has to survive the trip. Adding
// context around it keeps that (errors.Wrapf preserves the match); swapping it
// for a plain error would not, and would make every InferenceService's first
// reconcile a failed one again.
func TestSetExternalServiceURLSurfacesNotFoundToTheCaller(t *testing.T) {
	r := &InferenceServiceReconciler{
		Client: fakeclient.New(t),
	}

	err := r.setExternalServiceURL(context.Background(), isvcForURL(),
		&controllerconfig.IngressConfig{UrlScheme: "https"})

	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err),
		"caller keys off apierrors.IsNotFound; got %#v", err)
}

func TestSetExternalServiceURLUsesTheServicePortAndScheme(t *testing.T) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 30080}}},
	}
	r := &InferenceServiceReconciler{
		Client: fakeclient.New(t, fakeclient.WithObjects(svc)),
	}

	isvc := isvcForURL()
	require.NoError(t, r.setExternalServiceURL(context.Background(), isvc,
		&controllerconfig.IngressConfig{UrlScheme: "https"}))

	require.NotNil(t, isvc.Status.URL)
	assert.Equal(t, "https", isvc.Status.URL.Scheme)
	assert.Equal(t, "sample.default.svc.cluster.local:30080", isvc.Status.URL.Host)
	require.NotNil(t, isvc.Status.Address)
	assert.Equal(t, isvc.Status.URL.Host, isvc.Status.Address.URL.Host)
}

// A Service with no ports is not an error; the InferenceService port stands in,
// so the URL is still addressable rather than being left unset.
func TestSetExternalServiceURLFallsBackToTheDefaultPort(t *testing.T) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
	}
	r := &InferenceServiceReconciler{
		Client: fakeclient.New(t, fakeclient.WithObjects(svc)),
	}

	isvc := isvcForURL()
	require.NoError(t, r.setExternalServiceURL(context.Background(), isvc,
		&controllerconfig.IngressConfig{UrlScheme: "http"}))

	require.NotNil(t, isvc.Status.URL)
	assert.Equal(t, "sample.default.svc.cluster.local:8080", isvc.Status.URL.Host)
}
