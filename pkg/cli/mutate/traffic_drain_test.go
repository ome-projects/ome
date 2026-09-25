package mutate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/trafficdrain"
)

func TestPrepareTrafficDrainBuildsGuardedTargetedPatch(t *testing.T) {
	t.Run("nil annotations add map and escaped annotation path", func(t *testing.T) {
		service := trafficDrainTarget(nil)
		plan, err := PrepareTrafficDrain(service, TrafficDrainRequest{
			Action: "drain", ID: "maintenance-a", Cluster: "worker-a.example", Reason: "node maintenance",
		})
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.ActionTarget{
			Kind: "InferenceService", Namespace: "prod", Name: "chat", UID: "uid-chat", ResourceVersion: "42",
		}, plan.Target())
		assert.JSONEq(t, `[
			{"op":"test","path":"/metadata/uid","value":"uid-chat"},
			{"op":"test","path":"/metadata/resourceVersion","value":"42"},
			{"op":"add","path":"/metadata/annotations","value":{}},
			{"op":"add","path":"/metadata/annotations/ome.io~1traffic-drain","value":"{\"maintenance-a\":{\"cluster\":\"worker-a.example\",\"reason\":\"node maintenance\"}}"}
		]`, string(plan.Patch()))
	})

	t.Run("existing annotations and drain IDs are preserved", func(t *testing.T) {
		service := trafficDrainTarget(map[string]string{
			"example.com/keep":               "exact-value",
			constants.TrafficDrainAnnotation: `{"z-hold":{"reason":"capacity incident","cluster":"worker-z"}}`,
		})
		plan, err := PrepareTrafficDrain(service, TrafficDrainRequest{
			Action: "drain", ID: "a-hold", Cluster: "worker-a", Reason: "planned maintenance",
		})
		require.NoError(t, err)
		assert.JSONEq(t, `[
			{"op":"test","path":"/metadata/uid","value":"uid-chat"},
			{"op":"test","path":"/metadata/resourceVersion","value":"42"},
			{"op":"replace","path":"/metadata/annotations/ome.io~1traffic-drain","value":"{\"a-hold\":{\"cluster\":\"worker-a\",\"reason\":\"planned maintenance\"},\"z-hold\":{\"cluster\":\"worker-z\",\"reason\":\"capacity incident\"}}"}
		]`, string(plan.Patch()))
		assert.Equal(t, "exact-value", service.Annotations["example.com/keep"], "plan construction must not mutate the source")
	})
}

func TestTrafficDrainPatchEscapesAnnotationPathAndLogicalValue(t *testing.T) {
	const reason = `rack "A" \\ planned <maintenance>`
	plan, err := PrepareTrafficDrain(trafficDrainTarget(map[string]string{}), TrafficDrainRequest{
		Action: "drain", ID: "escape-check", Cluster: "worker-a.example", Reason: reason,
	})
	require.NoError(t, err)
	var operations []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	require.NoError(t, json.Unmarshal(plan.Patch(), &operations))
	require.Len(t, operations, 3)
	assert.Equal(t, "/metadata/annotations/ome.io~1traffic-drain", operations[2].Path)
	var annotation string
	require.NoError(t, json.Unmarshal(operations[2].Value, &annotation))
	overrides, err := trafficdrain.Parse(annotation)
	require.NoError(t, err)
	assert.Equal(t, []trafficdrain.Override{{ID: "escape-check", Cluster: "worker-a.example", Reason: reason}}, overrides)
}

func TestPrepareTrafficUndrainBuildsTargetedPatch(t *testing.T) {
	t.Run("remove only selected ID and retain the annotation", func(t *testing.T) {
		service := trafficDrainTarget(map[string]string{
			"example.com/keep":               "yes",
			constants.TrafficDrainAnnotation: `{"remove-me":{"cluster":"worker-a","reason":"done"},"keep-me":{"cluster":"worker-b","reason":"still draining"}}`,
		})
		plan, err := PrepareTrafficDrain(service, TrafficDrainRequest{Action: "undrain", ID: "remove-me"})
		require.NoError(t, err)
		assert.JSONEq(t, `[
			{"op":"test","path":"/metadata/uid","value":"uid-chat"},
			{"op":"test","path":"/metadata/resourceVersion","value":"42"},
			{"op":"replace","path":"/metadata/annotations/ome.io~1traffic-drain","value":"{\"keep-me\":{\"cluster\":\"worker-b\",\"reason\":\"still draining\"}}"}
		]`, string(plan.Patch()))
		assert.Equal(t, "yes", service.Annotations["example.com/keep"])
	})

	t.Run("last ID removes only the traffic annotation", func(t *testing.T) {
		service := trafficDrainTarget(map[string]string{
			"example.com/keep":               "yes",
			constants.TrafficDrainAnnotation: `{"remove-me":{"cluster":"worker-a","reason":"done"}}`,
		})
		plan, err := PrepareTrafficDrain(service, TrafficDrainRequest{Action: "undrain", ID: "remove-me"})
		require.NoError(t, err)
		assert.JSONEq(t, `[
			{"op":"test","path":"/metadata/uid","value":"uid-chat"},
			{"op":"test","path":"/metadata/resourceVersion","value":"42"},
			{"op":"remove","path":"/metadata/annotations/ome.io~1traffic-drain"}
		]`, string(plan.Patch()))
	})
}

func TestTrafficDrainPatchRejectsConcurrentIdentityChange(t *testing.T) {
	plan, err := PrepareTrafficDrain(trafficDrainTarget(nil), TrafficDrainRequest{
		Action: "drain", ID: "maintenance-a", Cluster: "worker-a", Reason: "planned maintenance",
	})
	require.NoError(t, err)
	patch, err := jsonpatch.DecodePatch(plan.Patch())
	require.NoError(t, err)
	concurrent := []byte(`{"metadata":{"uid":"uid-chat","resourceVersion":"43"}}`)
	_, err = patch.Apply(concurrent)
	require.Error(t, err, "resourceVersion test must fail before any annotation operation")

	recreated := []byte(`{"metadata":{"uid":"uid-recreated","resourceVersion":"42"}}`)
	_, err = patch.Apply(recreated)
	require.Error(t, err, "UID test must fail for a same-name replacement")
}

func TestTrafficDrainPlanDoesNotSerializeOrFormatPrivateState(t *testing.T) {
	const reason = "PRIVATE_OPERATOR_CONTEXT"
	plan, err := PrepareTrafficDrain(trafficDrainTarget(nil), TrafficDrainRequest{
		Action: "drain", ID: "maintenance-a", Cluster: "worker-a", Reason: reason,
	})
	require.NoError(t, err)
	_, err = json.Marshal(plan)
	require.Error(t, err)
	for _, formatted := range []string{fmt.Sprint(plan), fmt.Sprintf("%+v", plan), fmt.Sprintf("%#v", plan)} {
		assert.Equal(t, "<mutate.TrafficDrainPlan redacted>", formatted)
		assert.NotContains(t, formatted, reason)
	}
}

func TestPrepareTrafficDrainFailsClosed(t *testing.T) {
	tooManySourceAnnotations := map[string]string{
		"ome.io/accelerator-requirements": "gpu=true",
		"ome.io/cluster-selector":         "region=west",
	}
	for i := 0; i < 255; i++ {
		tooManySourceAnnotations["example.com/key-"+leftPad3(i)] = "x"
	}
	fullAnnotationMap := map[string]string{}
	for i := 0; i < 256; i++ {
		fullAnnotationMap["example.com/full-"+leftPad3(i)] = "x"
	}
	tests := []struct {
		name        string
		annotations map[string]string
		request     TrafficDrainRequest
		want        error
	}{
		{name: "malformed annotation", annotations: map[string]string{constants.TrafficDrainAnnotation: `{`}, request: TrafficDrainRequest{Action: "drain", ID: "new", Cluster: "worker-a", Reason: "safe"}, want: ErrTrafficDrainAnnotation},
		{name: "duplicate ID in annotation", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"same":{"cluster":"a","reason":"one"},"same":{"cluster":"b","reason":"two"}}`}, request: TrafficDrainRequest{Action: "undrain", ID: "same"}, want: ErrTrafficDrainAnnotation},
		{name: "unknown field in annotation", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"same":{"cluster":"a","reason":"one","weight":"0"}}`}, request: TrafficDrainRequest{Action: "undrain", ID: "same"}, want: ErrTrafficDrainAnnotation},
		{name: "oversized annotation", annotations: map[string]string{constants.TrafficDrainAnnotation: strings.Repeat("x", trafficDrainMaxAnnotationBytes+1)}, request: TrafficDrainRequest{Action: "undrain", ID: "same"}, want: ErrTrafficDrainBounds},
		{name: "too many overrides", annotations: trafficDrainManyAnnotations(t, trafficDrainMaxOverrides+1), request: TrafficDrainRequest{Action: "undrain", ID: "id-000"}, want: ErrTrafficDrainBounds},
		{name: "adding beyond override limit", annotations: trafficDrainManyAnnotations(t, trafficDrainMaxOverrides), request: TrafficDrainRequest{Action: "drain", ID: "one-more", Cluster: "worker-a", Reason: "safe"}, want: ErrTrafficDrainBounds},
		{name: "projected metadata exceeds bound", annotations: map[string]string{"example.com/fill": strings.Repeat("x", 65480)}, request: TrafficDrainRequest{Action: "drain", ID: "new", Cluster: "worker-a", Reason: "safe"}, want: ErrTrafficDrainBounds},
		{name: "source-only keys do not bypass annotation count", annotations: tooManySourceAnnotations, request: TrafficDrainRequest{Action: "drain", ID: "new", Cluster: "worker-a", Reason: "safe"}, want: ErrTrafficDrainBounds},
		{name: "new annotation exceeds map count", annotations: fullAnnotationMap, request: TrafficDrainRequest{Action: "drain", ID: "new", Cluster: "worker-a", Reason: "safe"}, want: ErrTrafficDrainBounds},
		{name: "existing unsafe ID", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"BAD ID":{"cluster":"a","reason":"one"}}`}, request: TrafficDrainRequest{Action: "undrain", ID: "safe-id"}, want: ErrTrafficDrainAnnotation},
		{name: "existing unsafe cluster", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"safe-id":{"cluster":"BAD_CLUSTER","reason":"one"}}`}, request: TrafficDrainRequest{Action: "undrain", ID: "safe-id"}, want: ErrTrafficDrainAnnotation},
		{name: "existing unsafe reason", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"safe-id":{"cluster":"worker-a","reason":"line\nbreak"}}`}, request: TrafficDrainRequest{Action: "undrain", ID: "safe-id"}, want: ErrTrafficDrainAnnotation},
		{name: "drain refuses existing ID even when identical", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"same":{"cluster":"worker-a","reason":"safe"}}`}, request: TrafficDrainRequest{Action: "drain", ID: "same", Cluster: "worker-a", Reason: "safe"}, want: ErrTrafficDrainExists},
		{name: "undrain refuses absent annotation", request: TrafficDrainRequest{Action: "undrain", ID: "missing"}, want: ErrTrafficDrainAbsent},
		{name: "undrain refuses absent ID", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"other":{"cluster":"worker-a","reason":"safe"}}`}, request: TrafficDrainRequest{Action: "undrain", ID: "missing"}, want: ErrTrafficDrainAbsent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := PrepareTrafficDrain(trafficDrainTarget(tc.annotations), tc.request)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestValidateTrafficDrainRequestMatchesAdmissionAndKubernetesNames(t *testing.T) {
	valid := []TrafficDrainRequest{
		{Action: "drain", ID: "maintenance-1", Cluster: "worker-a.example", Reason: "Replace node pool #2 after 02:00 UTC"},
		{Action: "drain", ID: "unicode-reason", Cluster: "worker-a", Reason: "planned maintenance — rack 2"},
		{Action: "undrain", ID: "maintenance-1"},
	}
	for _, request := range valid {
		require.NoError(t, ValidateTrafficDrainRequest(request), request)
	}

	tests := []TrafficDrainRequest{
		{},
		{Action: "delete", ID: "safe"},
		{Action: "drain", ID: "", Cluster: "worker-a", Reason: "safe"},
		{Action: "drain", ID: "Bad_ID", Cluster: "worker-a", Reason: "safe"},
		{Action: "drain", ID: strings.Repeat("a", 64), Cluster: "worker-a", Reason: "safe"},
		{Action: "drain", ID: "safe", Cluster: "BAD_CLUSTER", Reason: "safe"},
		{Action: "drain", ID: "safe", Cluster: "worker-a", Reason: ""},
		{Action: "drain", ID: "safe", Cluster: "worker-a", Reason: " leading"},
		{Action: "drain", ID: "safe", Cluster: "worker-a", Reason: "line\nbreak"},
		{Action: "drain", ID: "safe", Cluster: "worker-a", Reason: strings.Repeat("r", trafficDrainMaxReasonBytes+1)},
		{Action: "drain", ID: "safe", Cluster: "worker-a", Reason: "sk-123456789012345678901234"},
		{Action: "undrain", ID: "safe", Cluster: "worker-a"},
		{Action: "undrain", ID: "safe", Reason: "not used"},
	}
	for _, request := range tests {
		require.Error(t, ValidateTrafficDrainRequest(request), request)
	}
}

func TestPrepareTrafficDrainRequiresEligibleSourceAndRejectsDerived(t *testing.T) {
	request := TrafficDrainRequest{Action: "drain", ID: "safe", Cluster: "worker-a", Reason: "maintenance"}

	structured := trafficDrainTarget(nil)
	structured.Finalizers = []string{"ome.io/placement", "ome.io/placement-endpoint"}
	_, err := PrepareTrafficDrain(structured, request)
	require.NoError(t, err, "a structured placement source is an eligible mutation target")

	legacy := trafficDrainTarget(map[string]string{"ome.io/cluster-selector": "region=west"})
	legacy.Spec.Placement = nil
	_, err = PrepareTrafficDrain(legacy, request)
	require.NoError(t, err, "a legacy placement source is an eligible mutation target")

	plain := trafficDrainTarget(nil)
	plain.Spec.Placement = nil
	_, err = PrepareTrafficDrain(plain, request)
	require.ErrorIs(t, err, ErrTrafficDrainIneligible)

	modeOnly := trafficDrainTarget(nil)
	modeOnly.Spec.Placement = &omev1beta1.PlacementSpec{Mode: omev1beta1.PlacementModeAll}
	_, err = PrepareTrafficDrain(modeOnly, request)
	require.ErrorIs(t, err, ErrTrafficDrainIneligible)

	for _, marker := range []struct {
		annotation bool
		key        string
	}{
		{key: constants.PlacementOrigin},
		{key: constants.PlacementControlPlane},
		{annotation: true, key: constants.PlacementOriginUID},
	} {
		t.Run(marker.key, func(t *testing.T) {
			derived := trafficDrainTarget(nil)
			if marker.annotation {
				derived.Annotations = map[string]string{marker.key: "source-uid"}
			} else {
				derived.Labels = map[string]string{marker.key: "source-uid"}
			}
			_, err := PrepareTrafficDrain(derived, request)
			require.ErrorIs(t, err, ErrPlacement)
		})
	}
}

func TestTrafficDrainPreviewIsBoundedAndExact(t *testing.T) {
	plan, err := PrepareTrafficDrain(trafficDrainTarget(nil), TrafficDrainRequest{
		Action: "drain", ID: "maintenance-a", Cluster: "worker-a", Reason: strings.Repeat("safe reason ", 30)[:trafficDrainMaxReasonBytes],
	})
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, "moirai", reportv1alpha1.DryRunClient))
	for _, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}
	assert.Contains(t, out.String(), "maintenance-a")
	assert.Contains(t, out.String(), "worker-a")
	assert.Contains(t, out.String(), "API acceptance is not TrafficMap convergence")
	assert.Contains(t, out.String(), "kubectl ome traffic status")

	var longOut bytes.Buffer
	require.NoError(t, plan.WritePreview(&longOut, strings.Repeat("c", 256), reportv1alpha1.DryRunClient))
	for _, line := range strings.Split(strings.TrimSuffix(longOut.String(), "\n"), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}

	for _, tc := range []struct {
		name    string
		plan    TrafficDrainPlan
		context string
	}{
		{name: "empty plan", context: "moirai"},
		{name: "unsafe context", plan: plan, context: "bad\ncontext"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sink bytes.Buffer
			require.Error(t, tc.plan.WritePreview(&sink, tc.context, reportv1alpha1.DryRunNone))
		})
	}
	require.Error(t, plan.WritePreview(errorWriter{}, "moirai", reportv1alpha1.DryRunNone))
}

func TestTrafficUndrainPreviewShowsTheExactRemovedOverride(t *testing.T) {
	plan, err := PrepareTrafficDrain(trafficDrainTarget(map[string]string{
		constants.TrafficDrainAnnotation: `{"maintenance-a":{"cluster":"worker-a","reason":"rack work complete"}}`,
	}), TrafficDrainRequest{Action: "undrain", ID: "maintenance-a"})
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficActionDetails{
		OverrideID: "maintenance-a", Cluster: "worker-a", OverridesBefore: 1, OverridesAfter: 0,
	}, plan.Details())
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, "moirai", reportv1alpha1.DryRunNone))
	assert.Contains(t, out.String(), "worker-a")
	assert.Contains(t, out.String(), `"rack work complete"`)
}

func trafficDrainTarget(annotations map[string]string) *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{
		TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), ResourceVersion: "42", Generation: 3,
			Annotations: annotations,
		},
		Spec: omev1beta1.InferenceServiceSpec{Placement: &omev1beta1.PlacementSpec{Requirements: "accelerator=test"}},
	}
}

func trafficDrainManyAnnotations(t *testing.T, count int) map[string]string {
	t.Helper()
	value := make(map[string]map[string]string, count)
	for i := 0; i < count; i++ {
		value["id-"+leftPad3(i)] = map[string]string{"cluster": "worker-a", "reason": "safe"}
	}
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return map[string]string{constants.TrafficDrainAnnotation: string(raw)}
}

func leftPad3(value int) string {
	if value < 10 {
		return "00" + string(rune('0'+value))
	}
	if value < 100 {
		return "0" + string(rune('0'+value/10)) + string(rune('0'+value%10))
	}
	return string(rune('0'+value/100)) + string(rune('0'+value/10%10)) + string(rune('0'+value%10))
}
