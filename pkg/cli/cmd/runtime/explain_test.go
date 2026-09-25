package runtime

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func scheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	s := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(v1beta1.AddToScheme(s))
	return s
}

func ptr[T any](v T) *T { return &v }

// runtimeListMetadataClient fills the list metadata omitted by controller-
// runtime's fake client. A real API server returns a list resourceVersion and
// honors the exact resourceVersion requested for the second runtime kind.
type runtimeListMetadataClient struct{ ctrlclient.Client }

func (c runtimeListMetadataClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	options ...ctrlclient.ListOption,
) error {
	if err := c.Client.List(ctx, list, options...); err != nil {
		return err
	}
	listOptions := (&ctrlclient.ListOptions{}).ApplyOptions(options)
	resourceVersion := "fixture-rv"
	if listOptions.Raw != nil && listOptions.Raw.ResourceVersion != "" {
		resourceVersion = listOptions.Raw.ResourceVersion
	}
	switch typed := list.(type) {
	case *v1beta1.ServingRuntimeList:
		typed.ResourceVersion = resourceVersion
	case *v1beta1.ClusterServingRuntimeList:
		typed.ResourceVersion = resourceVersion
	}
	return nil
}

func withRuntimeListMetadata(client ctrlclient.Client) ctrlclient.Client {
	return runtimeListMetadataClient{Client: client}
}

func execute(t *testing.T, f factory.Factory, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &out}
	cmd := NewCmd(f, streams)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// row returns the whitespace-split fields of the output line whose first
// column is name, so tests can assert specific columns (e.g. COMPATIBLE)
// instead of substring-searching the whole table.
func row(t *testing.T, out, name string) []string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == name {
			return fields
		}
	}
	t.Fatalf("no output row for %q in:\n%s", name, out)
	return nil
}

// autoSelectFormat builds a SupportedModelFormat that compareSupportedModelFormats
// (pkg/runtimeselector/matcher.go) will match against a BaseModel with the
// given ModelFormat.Name, and that CalculateScore will treat as auto-select
// eligible -- both required for the runtime to appear in GetCompatibleRuntimes'
// output at all. See pkg/runtimeselector/selector_test.go for the reference
// fixture shape.
func autoSelectFormat(format string) v1beta1.SupportedModelFormat {
	return v1beta1.SupportedModelFormat{
		ModelFormat: &v1beta1.ModelFormat{Name: format},
		AutoSelect:  ptr(true),
	}
}

func TestExplainRequiresExactlyOneTarget(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "d"}, "explain")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one of --model or --isvc")

	_, err = execute(t, factory.Static{NS: "d"}, "explain", "--model", "m", "--isvc", "s")
	require.Error(t, err)
}

func TestExplainRejectsInvalidRecoveryOverrideBeforeAcquisition(t *testing.T) {
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "private-model"},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{Name: "safetensors"},
		},
	}
	ome := omefake.NewSimpleClientset(model)
	runtimeClient := withRuntimeListMetadata(
		ctrlfake.NewClientBuilder().WithScheme(scheme(t)).Build(),
	)
	f := &acquisitionFactory{
		ome: ome, runtime: runtimeClient, namespace: "team-a",
	}
	invalid := defaultRuntimeExplainLimits()
	invalid.MaxRequests = invalid.MaxPages*2 + 1
	var out, errOut bytes.Buffer
	o := &explainOptions{
		IOStreams: genericiooptions.IOStreams{Out: &out, ErrOut: &errOut},
		Model:     "private-model", candidateLimits: invalid,
	}

	err := o.Run(context.Background(), f)

	require.EqualError(t, err, "runtime explain paging limits are invalid")
	assert.Zero(t, f.namespaceGet)
	assert.Zero(t, f.omeGet)
	assert.Zero(t, f.kubeGet)
	assert.Zero(t, f.runtimeGet)
	assert.Empty(t, ome.Actions())
	assert.Empty(t, out.String())
	assert.Empty(t, errOut.String())
}

func TestExplainRanksRuntimes(t *testing.T) {
	format := "safetensors"
	size := "70B"
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat:        v1beta1.ModelFormat{Name: format},
			ModelParameterSize: &size,
		},
	}
	compatible := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "srt-llama"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat(format)},
		},
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(model, compatible).Build()
	f := factory.Static{
		OME:     omefake.NewSimpleClientset(model),
		Runtime: withRuntimeListMetadata(ctrl),
		NS:      "team-a",
	}
	out, err := execute(t, f, "explain", "--model", "llama-70b")
	require.NoError(t, err)
	assert.Contains(t, out, "RUNTIME")

	got := row(t, out, "srt-llama")
	require.Len(t, got, 6, "row: %v", got)
	assert.Equal(t, "Cluster", got[1])
	assert.Equal(t, "Yes", got[2], "srt-llama supports the model's format and has autoSelect enabled, so GetCompatibleRuntimes must return it")
}

// TestExplainListsIncompatibleRuntimeWithReason pins down the semantics
// documented in explain.go: GetCompatibleRuntimes only ever returns matches
// (see pkg/runtimeselector/selector.go evaluateRuntime), so a runtime it
// silently drops must still show up here, marked incompatible, with a reason.
func TestExplainListsIncompatibleRuntimeWithReason(t *testing.T) {
	format := "safetensors"
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: format}},
	}
	compatible := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "srt-llama"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat(format)},
		},
	}
	// srt-onnx supports a different format entirely: GetCompatibleRuntimes
	// drops it silently, but explain must still list it as incompatible.
	incompatible := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "srt-onnx"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat("onnx")},
		},
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(model, compatible, incompatible).Build()
	f := factory.Static{
		OME:     omefake.NewSimpleClientset(model),
		Runtime: withRuntimeListMetadata(ctrl),
		NS:      "team-a",
	}
	out, err := execute(t, f, "explain", "--model", "llama-70b")
	require.NoError(t, err)

	rejected := row(t, out, "srt-onnx")
	assert.Equal(t, "No", rejected[2])
	assert.Contains(t, strings.Join(strings.Fields(out), " "), "not in supported formats",
		"rejection reason should explain the format mismatch")

	accepted := row(t, out, "srt-llama")
	assert.Equal(t, "Yes", accepted[2])
}

// TestExplainPrefersNamespacedBaseModel exercises resolveTarget's precedence:
// a namespaced BaseModel must win over a ClusterBaseModel of the same name.
func TestExplainPrefersNamespacedBaseModel(t *testing.T) {
	format := "safetensors"
	nsModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "team-a"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: format}},
	}
	// Same name, different (unsupported-by-any-runtime) format: if
	// resolveTarget picked this one by mistake, srt-llama would never match.
	clusterModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "onnx"}},
	}
	rt := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "srt-llama"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat(format)},
		},
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(rt).Build()
	f := factory.Static{
		OME:     omefake.NewSimpleClientset(nsModel, clusterModel),
		Runtime: withRuntimeListMetadata(ctrl),
		NS:      "team-a",
	}
	out, err := execute(t, f, "explain", "--model", "llama-70b")
	require.NoError(t, err)
	got := row(t, out, "srt-llama")
	assert.Equal(t, "Yes", got[2])
}

// TestExplainISVCPath covers --isvc resolution (model looked up via
// spec.model) and the spec.runtime pin note.
func TestExplainISVCPath(t *testing.T) {
	format := "safetensors"
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: format}},
	}
	rt := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "srt-llama"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat(format)},
		},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "my-isvc", Namespace: "team-a"},
		Spec: v1beta1.InferenceServiceSpec{
			Model:   &v1beta1.ModelRef{Name: "llama-70b"},
			Runtime: &v1beta1.ServingRuntimeRef{Name: "srt-llama"},
		},
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(rt).Build()
	f := factory.Static{
		OME:     omefake.NewSimpleClientset(model, isvc),
		Runtime: withRuntimeListMetadata(ctrl),
		NS:      "team-a",
	}
	out, err := execute(t, f, "explain", "--isvc", "my-isvc")
	require.NoError(t, err)
	got := row(t, out, "srt-llama")
	assert.Equal(t, "Yes", got[2])
	assert.Contains(t, out, "spec.runtime is explicit")
}

func TestExplainISVCWithoutModelErrors(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "my-isvc", Namespace: "team-a"},
	}
	f := factory.Static{
		OME: omefake.NewSimpleClientset(isvc),
		NS:  "team-a",
	}
	_, err := execute(t, f, "explain", "--isvc", "my-isvc")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no spec.model")
}

func TestExplainModelNotFound(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(),
		NS:  "team-a",
	}
	_, err := execute(t, f, "explain", "--model", "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestExplainNamesMatchingFormatCause pins down defect (a), case 2: the
// supportedModelFormats entry that actually matches the model has no
// autoSelect=true, but a *different*, non-matching entry on the same runtime
// does. ValidateRuntime ignores autoSelect entirely (it only checks
// compatibility) so it returns nil here; the old code took that nil to mean
// "must be missing an autoSelect entry altogether" and printed that hardcoded
// reason, which is false -- srt-mixed has one, just not on the matching
// format. See pkg/runtimeselector/scorer.go CalculateScore: it skips only
// explicitly autoSelect=false formats, so the matching (explicitly
// non-autoSelect) format contributes nothing and the non-matching autoSelect
// format scores 0 too (wrong ModelFormat.Name), leaving the runtime out of
// GetCompatibleRuntimes' matches entirely.
func TestExplainNamesMatchingFormatCause(t *testing.T) {
	format := "safetensors"
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: format}},
	}
	rt := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "srt-mixed"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				// Matches the model's format, but explicitly opts out of auto-select.
				{ModelFormat: &v1beta1.ModelFormat{Name: format}, AutoSelect: ptr(false)},
				// Auto-select enabled, but for a format that does not match this model.
				{ModelFormat: &v1beta1.ModelFormat{Name: "onnx"}, AutoSelect: ptr(true)},
			},
		},
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(model, rt).Build()
	f := factory.Static{
		OME:     omefake.NewSimpleClientset(model),
		Runtime: withRuntimeListMetadata(ctrl),
		NS:      "team-a",
	}
	out, err := execute(t, f, "explain", "--model", "llama-70b")
	require.NoError(t, err)

	got := row(t, out, "srt-mixed")
	require.GreaterOrEqual(t, len(got), 3, "row: %v", got)
	assert.Equal(t, "No", got[2])
	compact := strings.Join(strings.Fields(out), " ")
	assert.Contains(t, compact, "not autoSelect-enabled", "reason must name the matching-format cause")
	assert.Contains(t, out, "safetensors", "reason should identify the matching format by name")
	assert.NotContains(t, compact, "no supportedModelFormats[].autoSelect=true entry",
		"srt-mixed DOES have an autoSelect=true entry (onnx) -- must not claim none exists")
}

// TestExplainNamesZeroScoreCause pins down defect (a), case 3: the matching
// format IS autoSelect-enabled, but its explicit Priority of 0 drives
// scorer.CalculateScore (pkg/runtimeselector/scorer.go) to 0, which
// evaluateRuntime (pkg/runtimeselector/selector.go) treats the same as "no
// match". ValidateRuntime is blind to score entirely, so it returns nil; the
// reason must name the zero score/priority instead of falsely claiming no
// autoSelect entry exists.
func TestExplainNamesZeroScoreCause(t *testing.T) {
	format := "safetensors"
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: format}},
	}
	rt := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "srt-zero-priority"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat: &v1beta1.ModelFormat{Name: format},
					AutoSelect:  ptr(true),
					Priority:    ptr(int32(0)),
				},
			},
		},
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(model, rt).Build()
	f := factory.Static{
		OME:     omefake.NewSimpleClientset(model),
		Runtime: withRuntimeListMetadata(ctrl),
		NS:      "team-a",
	}
	out, err := execute(t, f, "explain", "--model", "llama-70b")
	require.NoError(t, err)

	got := row(t, out, "srt-zero-priority")
	require.GreaterOrEqual(t, len(got), 3, "row: %v", got)
	assert.Equal(t, "No", got[2])
	compact := strings.Join(strings.Fields(out), " ")
	assert.Contains(t, compact, "score is 0", "reason should name the zero auto-select score")
	assert.Contains(t, compact, "priority 0", "reason should point at the explicit zero priority")
	assert.NotContains(t, compact, "no supportedModelFormats[].autoSelect=true entry",
		"srt-zero-priority DOES have an autoSelect=true entry -- must not claim none exists")
}

// TestExplainClusterRowReflectsClusterSpecWhenShadowed pins down defect (b):
// ValidateRuntime resolves a runtime by name only, namespaced-first
// (pkg/runtimeselector/fetcher.go GetRuntime), so re-validating a
// cluster-scoped candidate whose name collides with a namespace-scoped
// runtime of the same name must not silently grade the namespaced object.
// Same-named shadowing across scopes is an intentional pattern
// (pkg/runtimeinheritance), not user error, so explain must still get it
// right.
func TestExplainClusterRowReflectsClusterSpecWhenShadowed(t *testing.T) {
	format := "safetensors"
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: format}},
	}
	// Namespaced "shared" is fully compatible and auto-select eligible --
	// GetCompatibleRuntimes must return it as a "Yes" row, unaffected by the
	// cluster row's fix.
	nsRuntime := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "team-a"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat(format)},
		},
	}
	// Cluster "shared" supports a totally different format. ValidateRuntime("shared")
	// would resolve the NAMESPACED runtime above (namespaced-first, name-only),
	// see it's compatible, and return nil -- masking the cluster object's real
	// (and different) incompatibility if explain.go trusted that nil here.
	clusterRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "shared"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat("onnx")},
		},
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(model, nsRuntime, clusterRuntime).Build()
	f := factory.Static{
		OME:     omefake.NewSimpleClientset(model),
		Runtime: withRuntimeListMetadata(ctrl),
		NS:      "team-a",
	}
	out, err := execute(t, f, "explain", "--model", "llama-70b")
	require.NoError(t, err)

	var nsLine, clusterLine string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "shared" {
			continue
		}
		switch fields[1] {
		case "Namespaced":
			nsLine = line
		case "Cluster":
			clusterLine = line
		}
	}
	require.NotEmpty(t, nsLine, "expected a Namespaced row for shared:\n%s", out)
	require.NotEmpty(t, clusterLine, "expected a Cluster row for shared:\n%s", out)

	nsFields := strings.Fields(nsLine)
	assert.Equal(t, "Yes", nsFields[2], "namespaced runtime is compatible and must be unaffected by the cluster row's fix")

	clusterFields := strings.Fields(clusterLine)
	assert.Equal(t, "No", clusterFields[2])
	// The cluster runtime's real defect is a format mismatch (onnx vs
	// safetensors) -- the reason must come from THAT object, not the
	// namespaced runtime's compatibility, and not a generic
	// autoSelect-missing claim (the cluster runtime's own format DOES have
	// autoSelect=true).
	assert.Contains(t, strings.Join(strings.Fields(out), " "), "runtime=onnx",
		"cluster row's reason should come from the cluster object's own format mismatch")
}
