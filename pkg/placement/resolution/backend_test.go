package resolution

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestResolveNative(t *testing.T) {
	for _, tt := range []struct {
		name        string
		edit        func(*testing.T, *runtimeFixture)
		wantErr     bool
		unsupported bool
	}{
		{name: "native without namespace or hardware"},
		{name: "automatic native runtime", edit: func(_ *testing.T, f *runtimeFixture) { f.model(); f.service.Spec.Runtime = nil }},
		{name: "explicit pin", edit: func(t *testing.T, f *runtimeFixture) { f.namespace = "operator-system"; f.pin(t, true) }},
		{name: "pin namespace unknown", wantErr: true, edit: func(t *testing.T, f *runtimeFixture) {
			f.namespace = "operator-system"
			f.pin(t, true)
			f.namespace = ""
		}},
		{name: "runtime missing", wantErr: true, edit: func(_ *testing.T, f *runtimeFixture) { f.service.Spec.Runtime.Name = "missing" }},
		{name: "engine undeclared", wantErr: true, edit: func(_ *testing.T, f *runtimeFixture) { f.service.Spec.Engine = nil }},
		{name: "virtual service", wantErr: true, unsupported: true, edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.DeploymentMode = ptr.To(constants.VirtualDeployment)
		}},
		{name: "virtual annotation overrides native", wantErr: true, unsupported: true, edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
		}},
		{name: "raw runtime engine", wantErr: true, unsupported: true, edit: func(_ *testing.T, f *runtimeFixture) {
			f.runtime.Spec.EngineConfig.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
		}},
		{name: "multinode decoder", wantErr: true, unsupported: true, edit: func(_ *testing.T, f *runtimeFixture) {
			f.runtime.Spec.DecoderConfig.Annotations = map[string]string{constants.DeploymentMode: string(constants.MultiNode)}
		}},
		{name: "raw router", wantErr: true, unsupported: true, edit: func(_ *testing.T, f *runtimeFixture) {
			f.runtime.Spec.RouterConfig.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
		}},
		{name: "undeclared router ignored", edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.Router = nil
			f.runtime.Spec.RouterConfig.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			f.namespace = ""
			f.service.Spec.DeploymentMode = ptr.To(constants.OMENative)
			if tt.edit != nil {
				tt.edit(t, &f)
			}
			got, err := (Resolver{Client: fixtureClient(t, f), OperatorNamespace: f.namespace}).ResolveNative(t.Context(), f.service, f.standing)
			if diff := cmp.Diff([]bool{tt.wantErr, tt.unsupported}, []bool{err != nil, errors.Is(err, ErrUnsupportedBackend)}); diff != "" {
				t.Fatalf("backend resolution (-want +got):\n%s\n%v", diff, err)
			}
			if err == nil {
				if err := got.Check(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else if got != nil {
				t.Fatal("failed resolution returned usable backend inputs")
			}
		})
	}
}
