package snapshot

import (
	"encoding/json"
	"fmt"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestWorkloadComponentModesPreservePublicPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		modes   [3]string
		present [3]bool
	}{
		{"default", `{"spec":{"engine":{},"decoder":{},"router":{}}}`,
			[3]string{"RawDeployment", "RawDeployment", "RawDeployment"}, [3]bool{true, true, true}},
		{"component annotation overrides spec and shape", `{"spec":{"deploymentMode":"OMENative","engine":{"annotations":{"ome.io/deploymentMode":"RawDeployment"},"worker":{}},"decoder":{"annotations":{"ome.io/deploymentMode":"MultiNode"},"leader":{}},"router":{"annotations":{"ome.io/deploymentMode":"VirtualDeployment"}}}}`,
			[3]string{"RawDeployment", "MultiNode", "VirtualDeployment"}, [3]bool{true, true, true}},
		{"spec overrides shape", `{"spec":{"deploymentMode":"RawDeployment","engine":{"leader":{}},"decoder":{"worker":{}},"router":{}}}`,
			[3]string{"RawDeployment", "RawDeployment", "RawDeployment"}, [3]bool{true, true, true}},
		{"invalid annotations fall through to spec", `{"spec":{"deploymentMode":"OMENative","engine":{"annotations":{"ome.io/deploymentMode":"omenative"}},"decoder":{"annotations":{"ome.io/deploymentMode":" OMENative"}},"router":{"annotations":{"ome.io/deploymentMode":""}}}}`,
			[3]string{"OMENative", "OMENative", "OMENative"}, [3]bool{true, true, true}},
		{"invalid spec falls through to shape", `{"spec":{"deploymentMode":"unknown","engine":{"leader":{}},"decoder":{"worker":{}},"router":{}}}`,
			[3]string{"OMENative", "OMENative", "RawDeployment"}, [3]bool{true, true, true}},
		{"empty spec and annotations fall through", `{"spec":{"deploymentMode":"","engine":{"annotations":{"ome.io/deploymentMode":""}},"decoder":{},"router":{}}}`,
			[3]string{"RawDeployment", "RawDeployment", "RawDeployment"}, [3]bool{true, true, true}},
		{"worker and leader independently infer native", `{"spec":{"engine":{"worker":{"size":0}},"decoder":{"leader":{}},"router":{}}}`,
			[3]string{"OMENative", "OMENative", "RawDeployment"}, [3]bool{true, true, true}},
		{"absent optional components stay raw", `{"spec":{"deploymentMode":"OMENative","engine":{}}}`,
			[3]string{"OMENative", "RawDeployment", "RawDeployment"}, [3]bool{true, false, false}},
		{"missing engine preserves all raw fallback", `{"spec":{"deploymentMode":"OMENative","decoder":{"annotations":{"ome.io/deploymentMode":"OMENative"},"worker":{}},"router":{"annotations":{"ome.io/deploymentMode":"MultiNode"}}}}`,
			[3]string{"RawDeployment", "RawDeployment", "RawDeployment"}, [3]bool{false, true, true}},
		{"empty service", `{}`,
			[3]string{"RawDeployment", "RawDeployment", "RawDeployment"}, [3]bool{}},
		{"top level annotation is not a component override", `{"metadata":{"annotations":{"ome.io/deploymentMode":"OMENative"}},"spec":{"engine":{},"decoder":{},"router":{}}}`,
			[3]string{"RawDeployment", "RawDeployment", "RawDeployment"}, [3]bool{true, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkComponentModes(t, tc.raw, tc.modes, tc.present)
		})
	}
}

func TestWorkloadComponentModesPreserveLegacyExplicitModes(t *testing.T) {
	// These are the existing observation semantics, including modes accepted
	// through legacy annotations; this test does not relax CRD admission.
	for _, mode := range []string{"RawDeployment", "MultiNode", "VirtualDeployment", "OMENative"} {
		t.Run(mode, func(t *testing.T) {
			want := [3]string{mode, mode, mode}
			present := [3]bool{true, true, true}
			checkComponentModes(t, fmt.Sprintf(`{"spec":{"deploymentMode":%q,"engine":{},"decoder":{},"router":{}}}`, mode), want, present)
			checkComponentModes(t, fmt.Sprintf(`{"spec":{"engine":{"annotations":{"ome.io/deploymentMode":%q}},"decoder":{"annotations":{"ome.io/deploymentMode":%q}},"router":{"annotations":{"ome.io/deploymentMode":%q}}}}`, mode, mode, mode), want, present)
		})
	}
}

func checkComponentModes(t *testing.T, raw string, modes [3]string, present [3]bool) {
	t.Helper()
	var isvc v1beta1.InferenceService
	if err := json.Unmarshal([]byte(raw), &isvc); err != nil {
		t.Fatal(err)
	}
	got := workloadComponentSpecs(&isvc)
	if len(got) != 3 {
		t.Fatalf("got %d components, want 3", len(got))
	}
	for i, component := range []string{"engine", "decoder", "router"} {
		if string(got[i].ctype) != component || string(got[i].mode) != modes[i] || got[i].present != present[i] {
			t.Errorf("%s: got %+v, want mode=%s present=%t", component, got[i], modes[i], present[i])
		}
	}
}
