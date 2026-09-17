package controllerconfig

import (
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestParseOMENativeStatusConfig(t *testing.T) {
	bound := func(v uint64) *uint64 { return &v }
	tests := []struct {
		name     string
		data     map[string]string
		want     *OMENativeStatusConfig
		wantErr  string
		wantBind uint64
	}{
		{
			name:    "missing block aborts",
			data:    map[string]string{},
			wantErr: `missing the required "omenativeStatus" block`,
		},
		{
			name: "DenseV1 without bound",
			data: map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1"}`},
			want: &OMENativeStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1},
		},
		{
			name:     "DenseV1 with bound",
			data:     map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1","maxDecodedInstances":5000}`},
			want:     &OMENativeStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1, MaxDecodedInstances: bound(5000)},
			wantBind: 5000,
		},
		{
			name:     "ColumnarV2 with bound",
			data:     map[string]string{OMENativeStatusConfigName: `{"maxDecodedInstances": 2000, "instanceStatusEncoding": "ColumnarV2"}`},
			want:     &OMENativeStatusConfig{InstanceStatusEncoding: irstatus.EncodingColumnarV2, MaxDecodedInstances: bound(2000)},
			wantBind: 2000,
		},
		{
			name:    "ColumnarV2 without bound",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"ColumnarV2"}`},
			wantErr: "maxDecodedInstances is required when instanceStatusEncoding is ColumnarV2",
		},
		{
			name:    "empty block",
			data:    map[string]string{OMENativeStatusConfigName: `{}`},
			wantErr: "instanceStatusEncoding is required",
		},
		{
			name:    "unknown encoding",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"SparseV3"}`},
			wantErr: `instanceStatusEncoding "SparseV3" is not supported`,
		},
		{
			name:    "lowercase encoding is unknown",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"densev1"}`},
			wantErr: `instanceStatusEncoding "densev1" is not supported`,
		},
		{
			name:    "zero bound",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1","maxDecodedInstances":0}`},
			wantErr: "maxDecodedInstances must be a positive integer, got 0",
		},
		{
			name:    "negative bound",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"ColumnarV2","maxDecodedInstances":-1}`},
			wantErr: "maxDecodedInstances must be a positive integer, got -1",
		},
		{
			name:    "fractional bound",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1","maxDecodedInstances":1.5}`},
			wantErr: "maxDecodedInstances must be a positive integer",
		},
		{
			name:    "string bound",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1","maxDecodedInstances":"100"}`},
			wantErr: "maxDecodedInstances must be a positive integer",
		},
		{
			name:    "non-string encoding",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":1}`},
			wantErr: "instanceStatusEncoding must be a string",
		},
		{
			name:    "unknown field",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1","maxDecodedInstance":10}`},
			wantErr: `unknown field "maxDecodedInstance"`,
		},
		{
			name:    "duplicate field",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1","instanceStatusEncoding":"ColumnarV2"}`},
			wantErr: `duplicate field "instanceStatusEncoding"`,
		},
		{
			name:    "trailing data",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1"} {}`},
			wantErr: "trailing data after the JSON object",
		},
		{
			name:    "not an object",
			data:    map[string]string{OMENativeStatusConfigName: `["DenseV1"]`},
			wantErr: "expected a JSON object",
		},
		{
			name:    "empty string",
			data:    map[string]string{OMENativeStatusConfigName: ``},
			wantErr: "expected a JSON object",
		},
		{
			name:    "truncated object",
			data:    map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1"`},
			wantErr: "malformed JSON object",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseOMENativeStatusConfig(&v1.ConfigMap{Data: tt.data})
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got config %+v", tt.wantErr, got)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.InstanceStatusEncoding != tt.want.InstanceStatusEncoding {
				t.Fatalf("encoding = %q, want %q", got.InstanceStatusEncoding, tt.want.InstanceStatusEncoding)
			}
			switch {
			case tt.want.MaxDecodedInstances == nil && got.MaxDecodedInstances != nil:
				t.Fatalf("bound = %d, want absent", *got.MaxDecodedInstances)
			case tt.want.MaxDecodedInstances != nil && (got.MaxDecodedInstances == nil || *got.MaxDecodedInstances != *tt.want.MaxDecodedInstances):
				t.Fatalf("bound = %v, want %d", got.MaxDecodedInstances, *tt.want.MaxDecodedInstances)
			}
			if got.DecodeBound() != tt.wantBind {
				t.Fatalf("DecodeBound() = %d, want %d", got.DecodeBound(), tt.wantBind)
			}
		})
	}
}

func TestNewOMENativeStatusConfigReadsTheConfigMap(t *testing.T) {
	clientset := fake.NewSimpleClientset(&v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       map[string]string{OMENativeStatusConfigName: `{"instanceStatusEncoding":"DenseV1"}`},
	})
	cfg, err := NewOMENativeStatusConfig(clientset)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.InstanceStatusEncoding != irstatus.EncodingDenseV1 || cfg.MaxDecodedInstances != nil {
		t.Fatalf("unexpected config %+v", cfg)
	}

	if _, err := NewOMENativeStatusConfig(fake.NewSimpleClientset()); err == nil {
		t.Fatal("missing ConfigMap must be an error")
	}
}

func TestOMENativeStatusConfigDecodeBoundNil(t *testing.T) {
	var cfg *OMENativeStatusConfig
	if cfg.DecodeBound() != 0 {
		t.Fatal("nil config must report no bound")
	}
}
