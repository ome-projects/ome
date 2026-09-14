package basemodel

import (
	"context"
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func stageStorage(path *string) *v1beta1.StorageSpec {
	return &v1beta1.StorageSpec{
		StorageUri: ptr.To("stage:///mnt/nfs-src/qwen/Qwen3-32B"),
		Path:       path,
	}
}

func TestBaseModelValidator_HandleStage(t *testing.T) {
	v := &BaseModelValidator{Decoder: newDecoder(t)}

	tests := []struct {
		name        string
		path        *string
		wantAllowed bool
	}{
		{name: "stage with absolute path allowed", path: ptr.To("/mnt/data/models/qwen3-32b"), wantAllowed: true},
		{name: "stage without path denied", path: nil, wantAllowed: false},
		{name: "stage with relative path denied", path: ptr.To("models/qwen3-32b"), wantAllowed: false},
		{name: "stage with path inside source denied", path: ptr.To("/mnt/nfs-src/qwen/Qwen3-32B/copy"), wantAllowed: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bm := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "models"}}
			bm.Spec.Storage = stageStorage(tc.path)
			raw, err := json.Marshal(bm)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			resp := v.Handle(context.TODO(), admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: raw}},
			})
			if resp.Allowed != tc.wantAllowed {
				t.Fatalf("allowed=%v, want %v (result: %+v)", resp.Allowed, tc.wantAllowed, resp.Result)
			}
		})
	}
}

func TestClusterBaseModelValidator_HandleStage(t *testing.T) {
	v := &ClusterBaseModelValidator{Decoder: newDecoder(t)}

	cbm := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "m"}}
	cbm.Spec.Storage = stageStorage(nil)
	raw, err := json.Marshal(cbm)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := v.Handle(context.TODO(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: raw}},
	})

	if resp.Allowed {
		t.Fatalf("expected stage:// without spec.storage.path to be denied, got %+v", resp.Result)
	}
}
