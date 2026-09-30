package validation

import (
	"strings"
	"testing"
)

func TestValidateInferenceReplicaTemplateSource(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		runners                      int
		modelRef, runtimeRef, pinned bool
		wantErr                      string
	}{
		{name: "runners only", runners: 1},
		{name: "model and runtime", modelRef: true, runtimeRef: true},
		{name: "model alone auto-selects", modelRef: true},
		{name: "runtime alone renders the piece as-is", runtimeRef: true},
		{name: "nothing", wantErr: "spec.runners is required unless spec.modelRef or spec.runtimeRef is set"},
		{name: "runners with model", runners: 1, modelRef: true, wantErr: "spec.runners and spec.modelRef/spec.runtimeRef are exclusive"},
		{name: "runners with runtime", runners: 1, runtimeRef: true, wantErr: "spec.runners and spec.modelRef/spec.runtimeRef are exclusive"},
		{name: "two runners with both refs", runners: 2, modelRef: true, runtimeRef: true, wantErr: "spec.runners and spec.modelRef/spec.runtimeRef are exclusive"},
		{name: "pinned runtime", runtimeRef: true, pinned: true, wantErr: "spec.runtimeRef.autoSync=false and spec.runtimeRef.revision are not honored on an InferenceReplica"},
		{name: "pinned runtime with model", modelRef: true, runtimeRef: true, pinned: true, wantErr: "spec.runtimeRef.autoSync=false and spec.runtimeRef.revision are not honored on an InferenceReplica"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateInferenceReplicaTemplateSource(tc.runners, tc.modelRef, tc.runtimeRef, tc.pinned)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
