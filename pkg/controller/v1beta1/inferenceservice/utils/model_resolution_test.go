package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestPrimaryModelResolutionCompatibility(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	for _, tc := range []struct {
		name, gate, kind, group, wantUID, wantErr string
		gated, local, cluster                     bool
	}{
		{name: "legacy defaulted kind namespaced only", kind: "ClusterBaseModel", local: true, wantUID: "local-uid"},
		{name: "legacy defaulted kind collision", kind: "ClusterBaseModel", local: true, cluster: true, wantUID: "local-uid"},
		{name: "legacy cluster fallback", kind: "ClusterBaseModel", cluster: true, wantUID: "cluster-uid"},
		{name: "pending cluster collision", gated: true, gate: "pending", kind: "ClusterBaseModel", local: true, cluster: true, wantUID: "cluster-uid"},
		{name: "admitted cluster collision", gated: true, gate: "admitted", kind: "ClusterBaseModel", local: true, cluster: true, wantUID: "cluster-uid"},
		{name: "gated namespaced collision", gated: true, gate: "admitted", kind: "BaseModel", local: true, cluster: true, wantUID: "local-uid"},
		{name: "gated missing cluster never falls back", gated: true, gate: "pending", kind: "ClusterBaseModel", local: true, wantErr: "not found"},
		{name: "gated missing namespaced never falls back", gated: true, gate: "pending", kind: "BaseModel", cluster: true, wantErr: "not found"},
		{name: "gated unqualified compatibility", gated: true, gate: "admitted", local: true, cluster: true, wantUID: "local-uid"},
		{name: "gated unsupported kind", gated: true, gate: "pending", kind: "OtherModel", local: true, wantErr: "unsupported model kind"},
		{name: "gated unsupported group", gated: true, gate: "pending", kind: "BaseModel", group: "other.io", local: true, wantErr: "unsupported model API group"},
		{name: "empty gate does not select legacy resolution", gated: true, kind: "ClusterBaseModel", local: true, cluster: true, wantUID: "cluster-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objects []client.Object
			if tc.local {
				objects = append(objects, &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "team", UID: "local-uid"}})
			}
			if tc.cluster {
				objects = append(objects, &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "cluster-uid"}})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "team", Annotations: map[string]string{}}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
			if tc.kind != "" {
				isvc.Spec.Model.Kind = &tc.kind
			}
			if tc.group != "" {
				isvc.Spec.Model.APIGroup = &tc.group
			}
			if tc.gated {
				isvc.Annotations[constants.ArtifactStartupGateAnnotation] = tc.gate
				isvc.Annotations[constants.ArtifactModelUIDAnnotation] = tc.wantUID
			}
			_, metadata, _, err := ReconcileBaseModelWithStatus(c, isvc)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tc.wantUID, metadata.UID)
			if tc.gate == constants.ArtifactStartupGateAdmitted {
				require.NoError(t, ValidateArtifactStartupGate(context.Background(), c, isvc), "gate and controller must resolve the same UID")
			}
		})
	}
}
