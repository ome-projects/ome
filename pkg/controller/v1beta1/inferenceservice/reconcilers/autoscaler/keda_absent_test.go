package autoscaler

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestDeleteScaledObjectIfExists_ToleratesAbsentKedaCRD pins that OMENative
// autoscaler dispatch does not fail the whole ISVC reconcile on a cluster
// without KEDA: cleaning up a stale ScaledObject returns a NoKindMatchError
// (CRD absent -> no REST mapping), which must be treated as a no-op, not an
// error.
func TestDeleteScaledObjectIfExists_ToleratesAbsentKedaCRD(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, kedav1.AddToScheme(scheme)) // type is in scheme; CRD is not in the cluster

	noMatch := &meta.NoKindMatchError{
		GroupKind:        schema.GroupKind{Group: "keda.sh", Kind: "ScaledObject"},
		SearchedVersions: []string{"v1alpha1"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return noMatch
			},
		}).
		Build()

	require.NoError(t,
		deleteScaledObjectIfExists(context.Background(), c, "test-ns", "dummy-engine", dispatchOwner("dummy-engine"), nil, preserveForeignScaler),
		"ScaledObject cleanup must be a no-op when the KEDA CRD is absent")
}
