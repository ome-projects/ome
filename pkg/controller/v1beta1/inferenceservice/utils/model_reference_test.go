package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPrimaryModelReferenceUsesExplicitScope(t *testing.T) {
	for _, tc := range []struct {
		kind  *string
		group *string
		want  types.UID
	}{
		{kind: ptr.To("BaseModel"), want: "namespaced"},
		{kind: ptr.To("ClusterBaseModel"), want: "cluster"},
		{kind: ptr.To("ClusterBaseModel"), group: ptr.To("ome.io"), want: "cluster"},
		{kind: nil, want: "namespaced"},
		{kind: ptr.To(""), group: ptr.To(""), want: "namespaced"},
		{kind: ptr.To("OtherKind")},
		{kind: ptr.To("BaseModel"), group: ptr.To("other.io")},
	} {
		t.Run(fmt.Sprintf("%s/%s", ptr.Deref(tc.kind, "nil"), ptr.Deref(tc.group, "nil")), func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			objects := []client.Object{&v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "same-name", Namespace: "models", UID: "namespaced"}}, &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "same-name", UID: "cluster"}}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "models"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "same-name", Kind: tc.kind, APIGroup: tc.group}}}
			_, meta, _, err := ReconcileBaseModelWithStatus(c, isvc)
			if tc.want == "" {
				require.Error(t, err)
				require.Nil(t, meta)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, meta.UID)
			}
		})
	}
}

func TestPrimaryModelExplicitKindDoesNotFallBack(t *testing.T) {
	for _, kind := range []string{"BaseModel", "ClusterBaseModel", ""} {
		for _, local := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/local=%t", kind, local), func(t *testing.T) {
				scheme := runtime.NewScheme()
				require.NoError(t, v1beta1.AddToScheme(scheme))
				var object client.Object = &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "cluster"}}
				if local {
					object = &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models", UID: "local"}}
				}
				c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(object).Build()
				isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "models"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model", Kind: ptr.To(kind)}}}
				_, meta, _, err := ReconcileBaseModelWithStatus(c, isvc)
				if kind == "BaseModel" && !local || kind == "ClusterBaseModel" && local {
					require.Error(t, err)
					require.Nil(t, meta)
				} else {
					require.NoError(t, err)
					require.Equal(t, object.GetUID(), meta.UID)
				}
			})
		}
	}
}
