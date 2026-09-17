package validation

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// canarySpec builds a spec with a single engine-component canary group carrying
// the given steps (v2 spec.rollout.groups[].canary).
func canarySpec(steps ...v1beta1.RolloutGroupStep) *v1beta1.InferenceServiceSpec {
	mode := constants.OMENative
	return &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{},
		Rollout: &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{
				{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Canary:     &v1beta1.GroupCanary{Steps: steps},
				},
			},
		},
	}
}

// canarySpecMode builds a canary spec with an engine component at the given
// deployment mode, for the OMENative-gate tests.
func canarySpecMode(mode constants.DeploymentModeType, steps ...v1beta1.RolloutGroupStep) *v1beta1.InferenceServiceSpec {
	s := canarySpec(steps...)
	s.DeploymentMode = &mode
	s.Engine = &v1beta1.EngineSpec{}
	return s
}

func TestValidateCanary_OK(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("10%"), Traffic: 10},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100},
	)
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("valid canary rejected: %v", err)
	}
}

func TestValidateCanary_Empty(t *testing.T) {
	if err := ValidateCanary(canarySpec()); err == nil {
		t.Fatal("empty steps must be rejected")
	}
}

func TestValidateCanary_FinalWeightNot100(t *testing.T) {
	spec := canarySpec(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 50})
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("final step traffic must be 100")
	}
}

func TestValidateCanary_TrafficToZeroCapacity(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromInt(0), Traffic: 10},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("traffic>0 with zero Capacity must be rejected")
	}
}

func TestValidateCanary_TrafficToZeroPercentCapacity(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("0%"), Traffic: 10},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal(`traffic>0 with "0%" Capacity must be rejected`)
	}
}

// TestParseCanaryCapacity covers the strict parser: the runtime int/percent
// resolver maps anything unparsable to zero, so the parser must reject every
// form the resolver would drop and accept exactly the forms it resolves.
func TestParseCanaryCapacity(t *testing.T) {
	cases := []struct {
		name      string
		in        intstr.IntOrString
		value     int
		isPercent bool
		wantErr   bool
	}{
		{name: "int zero", in: intstr.FromInt(0), value: 0},
		{name: "int positive", in: intstr.FromInt(3), value: 3},
		{name: "int negative", in: intstr.FromInt(-1), wantErr: true},
		{name: "percent zero", in: intstr.FromString("0%"), value: 0, isPercent: true},
		{name: "percent mid", in: intstr.FromString("50%"), value: 50, isPercent: true},
		{name: "percent full", in: intstr.FromString("100%"), value: 100, isPercent: true},
		{name: "percent explicit plus sign", in: intstr.FromString("+10%"), value: 10, isPercent: true},
		{name: "malformed word", in: intstr.FromString("abc"), wantErr: true},
		{name: "empty string", in: intstr.FromString(""), wantErr: true},
		{name: "bare percent sign", in: intstr.FromString("%"), wantErr: true},
		{name: "non-numeric percent", in: intstr.FromString("abc%"), wantErr: true},
		{name: "negative percent", in: intstr.FromString("-10%"), wantErr: true},
		{name: "percent above 100", in: intstr.FromString("150%"), wantErr: true},
		{name: "inner whitespace", in: intstr.FromString("10 %"), wantErr: true},
		{name: "quoted absolute count", in: intstr.FromString("3"), wantErr: true},
		{name: "fractional percent", in: intstr.FromString("10.5%"), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, isPercent, err := parseCanaryCapacity(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseCanaryCapacity(%v) = (%d, %v, nil), want error", tc.in, value, isPercent)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCanaryCapacity(%v): unexpected error: %v", tc.in, err)
			}
			if value != tc.value || isPercent != tc.isPercent {
				t.Fatalf("parseCanaryCapacity(%v) = (%d, %v), want (%d, %v)", tc.in, value, isPercent, tc.value, tc.isPercent)
			}
		})
	}
}

// TestValidateCanary_CapacityStrictlyParsed: capacities the runtime resolver
// would silently map to zero (or that are semantically out of range) are
// rejected at admission, with the exact steps[i].capacity field path.
func TestValidateCanary_CapacityStrictlyParsed(t *testing.T) {
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	cases := []struct {
		name     string
		capacity intstr.IntOrString
	}{
		{name: "malformed string", capacity: intstr.FromString("abc")},
		{name: "negative integer", capacity: intstr.FromInt(-1)},
		{name: "negative percent", capacity: intstr.FromString("-10%")},
		{name: "percent above 100", capacity: intstr.FromString("150%")},
		{name: "quoted absolute count", capacity: intstr.FromString("3")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := canarySpec(v1beta1.RolloutGroupStep{Capacity: tc.capacity, Traffic: 10}, final)
			err := ValidateCanary(spec)
			if err == nil || !strings.Contains(err.Error(), ReasonCanaryInvalid) {
				t.Fatalf("capacity %v must be rejected with %s, got: %v", tc.capacity, ReasonCanaryInvalid, err)
			}
			if !strings.Contains(err.Error(), "spec.rollout.groups[0].canary.steps[0].capacity") {
				t.Fatalf("error must carry the exact field path, got: %v", err)
			}
		})
	}
}

// TestValidateCanary_MalformedFinalCapacityRejected: a malformed final-step
// capacity slipping through would let the done sentinel run on a plan whose
// last declared capacity is unparsable.
func TestValidateCanary_MalformedFinalCapacityRejected(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("10%"), Traffic: 10},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("abc"), Traffic: 100},
	)
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), "spec.rollout.groups[0].canary.steps[1].capacity") {
		t.Fatalf("malformed final capacity must be rejected with its field path, got: %v", err)
	}
}

func TestValidateCanary_FinalPercentCapacityNot100(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("10%"), Traffic: 10},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 100},
	)
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), "spec.rollout.groups[0].canary.steps[1].capacity") {
		t.Fatalf("final percent capacity below 100%% must be rejected with its field path, got: %v", err)
	}
}

// TestValidateCanary_ValidPlansStayAccepted: every capacity form the runtime
// resolver handles today keeps admitting — absolute integer steps, an absolute
// final step (admission cannot compare it to desired replicas), and a zero
// capacity paired with zero traffic.
func TestValidateCanary_ValidPlansStayAccepted(t *testing.T) {
	cases := []struct {
		name  string
		steps []v1beta1.RolloutGroupStep
	}{
		{
			name: "percent steps",
			steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 10},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
		{
			name: "absolute integer step",
			steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromInt(1), Traffic: 10},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
		{
			name: "absolute final step",
			steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50},
				{Capacity: intstr.FromInt(5), Traffic: 100},
			},
		},
		{
			name: "zero capacity with zero traffic",
			steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromInt(0), Traffic: 0},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateCanary(canarySpec(tc.steps...)); err != nil {
				t.Fatalf("valid plan rejected: %v", err)
			}
		})
	}
}

func TestValidateCanary_Unset(t *testing.T) {
	if err := ValidateCanary(&v1beta1.InferenceServiceSpec{}); err != nil {
		t.Fatalf("unset canary must pass: %v", err)
	}
	var nilSpec *v1beta1.InferenceServiceSpec
	if err := ValidateCanary(nilSpec); err != nil {
		t.Fatalf("nil spec must pass: %v", err)
	}
}

func TestValidateCanary_RequiresOMENative(t *testing.T) {
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	// Non-OMENative component → rejected.
	spec := canarySpecMode(constants.RawDeployment, final)
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonCanaryRequiresOMENative) {
		t.Fatalf("canary on RawDeployment must be rejected, got: %v", err)
	}
	// OMENative component → accepted.
	if err := ValidateCanary(canarySpecMode(constants.OMENative, final)); err != nil {
		t.Fatalf("canary on OMENative must pass: %v", err)
	}
	// Unset mode (empty) → rejected (canary needs explicit OMENative).
	if err := ValidateCanary(canarySpecMode("", final)); err == nil {
		t.Fatal("canary with unset deploymentMode must be rejected")
	}
}

func TestValidateCanary_DecreasingWeight(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("60%"), Traffic: 30},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("decreasing traffic must be rejected")
	}
}

func TestValidateCanary_BarePauseUnderManualOK(t *testing.T) {
	// A bare pause (no duration) is a manual gate: "hold here for the promote
	// annotation." Valid.
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100},
	)
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("bare pause (manual) must pass: %v", err)
	}
}

func TestValidateCanary_TimedStepOK(t *testing.T) {
	// A step with pause.duration and no analysis is a timed gate. Valid (the
	// old "duration-under-manual" rejection is gone — the gate is per-step now).
	spec := canarySpec(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: time.Minute}}},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100},
	)
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("timed step (pause.duration) must pass: %v", err)
	}
}

func goodAnalysis() *v1beta1.RolloutAnalysis {
	return &v1beta1.RolloutAnalysis{
		Interval:     metav1.Duration{Duration: time.Minute},
		FailureLimit: 3,
		Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "rate(x[1m])", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
	}
}

// analysisCanary builds a canary whose single (final) step carries the given
// self-contained analysis.
func analysisCanary(step *v1beta1.RolloutAnalysis) *v1beta1.InferenceServiceSpec {
	return canarySpec(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: step})
}

func TestValidateCanary_AnalysisOK(t *testing.T) {
	if err := ValidateCanary(analysisCanary(goodAnalysis())); err != nil {
		t.Fatalf("valid analysis step rejected: %v", err)
	}
}

func TestValidateCanary_AnalysisIncompleteRejected(t *testing.T) {
	// interval + failureLimit set but no metrics — the step can never pass, rejected.
	spec := analysisCanary(&v1beta1.RolloutAnalysis{Interval: metav1.Duration{Duration: time.Minute}, FailureLimit: 1})
	if err := ValidateCanary(spec); err == nil || !strings.Contains(err.Error(), ReasonAnalysisInvalid) {
		t.Fatalf("analysis step with no metrics must be rejected, got %v", err)
	}
}

func TestValidateCanary_AnalysisBadThreshold(t *testing.T) {
	a := goodAnalysis()
	a.Metrics[0].Threshold = "notanumber"
	if err := ValidateCanary(analysisCanary(a)); err == nil || !strings.Contains(err.Error(), ReasonAnalysisInvalid) {
		t.Fatalf("non-numeric threshold must be rejected, got %v", err)
	}
}

func TestValidateCanary_MultipleCanaryGroupsRejected(t *testing.T) {
	// The canary engine drives exactly one canary group (GetCanaryGroup returns
	// the first); a second canary group would be executed by neither engine and
	// roll ungated. Until cross-group sequencing lands, admission rejects more
	// than one canary group rather than silently mis-executing the rest.
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	spec := &v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{final}}},
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{final}}},
		},
	}}
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonMultipleCanaryGroups) {
		t.Fatalf("multiple canary groups: got %v want error with %s", err, ReasonMultipleCanaryGroups)
	}
}

func TestValidateCanary_UndeclaredComponentRejected(t *testing.T) {
	// A canary group listing a Component not declared on the ISVC wedges at
	// runtime: primaryComponent picks it (router > engine > decoder) but it has no
	// spec/IR, so the step machine never advances while the real component is
	// pinned at the step partition. Reject at admission.
	mode := constants.OMENative
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{}, // router is NOT declared
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{final}}},
		}},
	}
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonCanaryInvalid) {
		t.Fatalf("undeclared canary component: got %v want error with %s", err, ReasonCanaryInvalid)
	}
}

func TestValidateCanary_InvalidComponentRejected(t *testing.T) {
	mode := constants.OMENative
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{"frontend"}, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{final}}},
		}},
	}
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonCanaryInvalid) {
		t.Fatalf("invalid canary component name: got %v want error with %s", err, ReasonCanaryInvalid)
	}
}

// TestValidateCanary_EntrypointRequired: a canary group must contain the ISVC's
// entrypoint of every unit it covers: the router for the router unit, the engine
// for the engine unit. A group that stages a secondary without its unit entrypoint
// has no Component carrying the roll. An engine unit on a router-fronted ISVC is
// legal — the router discovers engine endpoints by label selector with no revision
// term, so a capacity step moves roughly that fraction of real requests.
func TestValidateCanary_UnitEntrypointRequired(t *testing.T) {
	mode := constants.OMENative
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	build := func(groupComps ...v1beta1.ComponentType) *v1beta1.InferenceServiceSpec {
		return &v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &v1beta1.EngineSpec{},
			Decoder:        &v1beta1.DecoderSpec{},
			Router:         &v1beta1.RouterSpec{}, // router-fronted → entrypoint is the router
			Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
				{Components: groupComps, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{final}}},
			}},
		}
	}
	// The engine unit alone, on a router-fronted ISVC, is a complete unit.
	if err := ValidateCanary(build(v1beta1.EngineComponent, v1beta1.DecoderComponent)); err != nil {
		t.Fatalf("engine unit on a router-fronted isvc must pass: %v", err)
	}
	// Both units in one group stays valid.
	if err := ValidateCanary(build(v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent)); err != nil {
		t.Fatalf("canary group spanning both units must pass: %v", err)
	}
	// A secondary without its unit entrypoint has nothing driving the roll.
	err := ValidateCanary(build(v1beta1.RouterComponent, v1beta1.DecoderComponent))
	if err == nil || !strings.Contains(err.Error(), ReasonCanaryInvalid) {
		t.Fatalf("decoder without the engine entrypoint must be rejected, got: %v", err)
	}
}

// TestValidateCanary_MaintainRatioRejected: the canary engine never reads
// MaintainRatio, so setting it on a canary group is a silent no-op (no ratio
// guard during the roll). Admission rejects it rather than mislead the operator.
func TestValidateCanary_MaintainRatioRejected(t *testing.T) {
	mode := constants.OMENative
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{}, // engine-fronted → entrypoint engine is in-group
		Decoder:        &v1beta1.DecoderSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
				Canary:        &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{final}},
			},
		}},
	}
	if err := ValidateCanary(spec); err == nil || !strings.Contains(err.Error(), ReasonCanaryInvalid) {
		t.Fatalf("maintainRatio on a canary group must be rejected, got: %v", err)
	}
}

// canaryUnitSpec builds an OMENative spec with the requested Components
// declared, and the given canary groups.
func canaryUnitSpec(withRouter, withDecoder bool, groups ...[]v1beta1.ComponentType) *v1beta1.InferenceServiceSpec {
	mode := constants.OMENative
	final := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
	spec := &v1beta1.InferenceServiceSpec{DeploymentMode: &mode, Engine: &v1beta1.EngineSpec{}}
	if withRouter {
		spec.Router = &v1beta1.RouterSpec{}
	}
	if withDecoder {
		spec.Decoder = &v1beta1.DecoderSpec{}
	}
	var gs []v1beta1.RolloutGroup
	for _, comps := range groups {
		gs = append(gs, v1beta1.RolloutGroup{
			Components: comps,
			Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{final}},
		})
	}
	spec.Rollout = &v1beta1.RolloutSpec{Groups: gs}
	return spec
}

// Independent units may each carry a canary run: a router run and an engine run
// own disjoint Components, so neither can contend for the other's revision,
// step counter or traffic entry.
func TestValidateCanary_TwoUnitsEachGetARun(t *testing.T) {
	spec := canaryUnitSpec(true, false,
		[]v1beta1.ComponentType{v1beta1.RouterComponent},
		[]v1beta1.ComponentType{v1beta1.EngineComponent},
	)
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("one canary run per unit must be accepted: %v", err)
	}
}

// Two runs driving one unit would contend for its revision and traffic entry.
func TestValidateCanary_SameUnitTwiceRejected(t *testing.T) {
	spec := canaryUnitSpec(true, true,
		[]v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		[]v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.DecoderComponent},
	)
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonMultipleCanaryGroups) {
		t.Fatalf("two canary runs on the engine unit must be rejected, got: %v", err)
	}
}

// An engine-only canary on a router-fronted ISVC is legal: the router discovers
// engine endpoints by label selector with no revision term, so a capacity step
// moves roughly that fraction of real requests even though the engine is not
// the external entrypoint.
func TestValidateCanary_EngineUnitBehindRouterAccepted(t *testing.T) {
	spec := canaryUnitSpec(true, false, []v1beta1.ComponentType{v1beta1.EngineComponent})
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("engine-only canary behind a router must be accepted: %v", err)
	}
}

// Splitting a declared P/D pair across a canary boundary can leave a
// new-protocol prefill with no pairable decode, which fails requests outright.
func TestValidateCanary_SplitPDPairRejected(t *testing.T) {
	for _, comps := range [][]v1beta1.ComponentType{
		{v1beta1.EngineComponent},
		{v1beta1.RouterComponent, v1beta1.EngineComponent},
	} {
		spec := canaryUnitSpec(true, true, comps)
		err := ValidateCanary(spec)
		if err == nil || !strings.Contains(err.Error(), ReasonCanaryInvalid) {
			t.Errorf("%v on a pd isvc must be rejected, got: %v", comps, err)
		}
	}
}

// The single-group shape that spans both units keeps its current semantics.
func TestValidateCanary_BothUnitsInOneGroupStillValid(t *testing.T) {
	spec := canaryUnitSpec(true, true,
		[]v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent})
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("a group spanning both units must stay valid: %v", err)
	}
}
