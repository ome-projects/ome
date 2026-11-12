package rolloutrun

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

var (
	weightedProvider = validation.RouteProviderCapabilities{Provider: "GatewayAPI", Capabilities: []string{constants.RouteCapabilityWeightedRevisionRouting}, Known: true}
	bareProvider     = validation.RouteProviderCapabilities{Provider: "KubernetesIngress", Known: true}
	undeclared       = validation.RouteProviderCapabilities{Provider: "None"}
)

func trafficIntent(isvc *v1beta1.InferenceService) *apis.Condition {
	return isvc.Status.GetCondition(v1beta1.TrafficIntentReady)
}

func engineCanary() v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	}
}

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// The hold event the run layer records when a plan cannot open; the text
// stands for the event's reason, which the test does not name.
const planHeldEvent = "the new revision is held, the previous revision keeps serving"

func TestOpenHoldsWhenRouteProviderCannotWeightRevisions(t *testing.T) {
	isvc := isvcFixture(engineCanary())
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	in.RouteCapabilities = bareProvider
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Opened || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("a canary the route provider cannot carry must be held at open, got %+v active=%v", out, v1beta1.RolloutRunActive(isvc))
	}
	cond := planReady(isvc)
	if cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != v1beta1.RolloutPlanReasonRouteProviderUnsupported {
		t.Fatalf("RolloutPlanReady = %+v", cond)
	}
	for _, want := range []string{"groups[0].canary.steps[].traffic", `"KubernetesIngress"`, constants.RouteCapabilityWeightedRevisionRouting} {
		if !strings.Contains(cond.Message, want) {
			t.Fatalf("hold message %q should name %q", cond.Message, want)
		}
	}
	intent := trafficIntent(isvc)
	if intent == nil || intent.Status != corev1.ConditionFalse || intent.Reason != v1beta1.TrafficIntentReasonUnsupported {
		t.Fatalf("TrafficIntentReady = %+v", intent)
	}
	if ready := isvc.Status.GetCondition(apis.ConditionReady); ready != nil {
		t.Fatalf("the advisory condition must not touch the aggregate Ready, got %+v", ready)
	}
	events := strings.Join(drainEvents(rec), "\n")
	if !strings.Contains(events, EventTrafficIntentUnsupported) || !strings.Contains(events, planHeldEvent) {
		t.Fatalf("expected a %s event and the hold event, got:\n%s", EventTrafficIntentUnsupported, events)
	}
}

func TestOpenReportsUndeclaredRouteProvider(t *testing.T) {
	isvc := isvcFixture(engineCanary())
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	in.RouteCapabilities = undeclared
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	out, err := Reconcile(context.Background(), in)
	if err != nil || !out.Opened {
		t.Fatalf("an undeclared provider must not gate the run: err=%v out=%+v", err, out)
	}
	intent := trafficIntent(isvc)
	if intent == nil || intent.Status != corev1.ConditionUnknown || intent.Reason != v1beta1.TrafficIntentReasonCapabilitiesUnknown {
		t.Fatalf("TrafficIntentReady = %+v", intent)
	}
	if !strings.Contains(intent.Message, `"None"`) || !strings.Contains(intent.Message, "spec.rollout.groups[0].canary.steps[].traffic") {
		t.Fatalf("message %q should name the provider and the field", intent.Message)
	}
	if events := strings.Join(drainEvents(rec), "\n"); !strings.Contains(events, EventTrafficIntentUnverified) {
		t.Fatalf("expected %s event, got:\n%s", EventTrafficIntentUnverified, events)
	}

	// Re-running with the same standing is quiet: the condition is unchanged
	// and no event repeats.
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if events := drainEvents(rec); len(events) != 0 {
		t.Fatalf("unchanged standing must not re-emit, got %v", events)
	}
}

// The report is advisory: on its own it writes exactly one condition and
// never creates or moves the aggregate Ready, whatever the standing.
func TestReportTrafficIntentWritesOnlyItsOwnCondition(t *testing.T) {
	for name, caps := range map[string]validation.RouteProviderCapabilities{
		"unsupported": bareProvider,
		"unknown":     undeclared,
		"supported":   weightedProvider,
	} {
		t.Run(name, func(t *testing.T) {
			isvc := isvcFixture(engineCanary())
			in := Inputs{ISVC: isvc, RouteCapabilities: caps}
			reportTrafficIntent(in, metav1.Time{Time: in.Now})
			conds := isvc.Status.GetConditions()
			if len(conds) != 1 || conds[0].Type != v1beta1.TrafficIntentReady || conds[0].Severity != apis.ConditionSeverityInfo {
				t.Fatalf("conditions = %+v, want only an Info-severity TrafficIntentReady", conds)
			}
			if ready := isvc.Status.GetCondition(apis.ConditionReady); ready != nil {
				t.Fatalf("aggregate Ready must stay untouched, got %+v", ready)
			}
		})
	}
}

func TestOpenProceedsWhenRouteProviderWeightsRevisions(t *testing.T) {
	isvc := isvcFixture(engineCanary())
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	in.RouteCapabilities = weightedProvider

	out, err := Reconcile(context.Background(), in)
	if err != nil || !out.Opened {
		t.Fatalf("a capable provider must open the run: err=%v out=%+v", err, out)
	}
	intent := trafficIntent(isvc)
	if intent == nil || intent.Status != corev1.ConditionTrue || intent.Reason != v1beta1.TrafficIntentReasonSupported {
		t.Fatalf("TrafficIntentReady = %+v", intent)
	}
}

func TestCanaryBehindRouterAsksNothingOfRouteProvider(t *testing.T) {
	isvc := isvcFixture(engineCanary())
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	in.RouteCapabilities = bareProvider

	out, err := Reconcile(context.Background(), in)
	if err != nil || !out.Opened {
		t.Fatalf("an engine canary behind a router shifts traffic by capacity and must open: err=%v out=%+v", err, out)
	}
	if intent := trafficIntent(isvc); intent != nil {
		t.Fatalf("no per-revision route intent, condition must stay absent, got %+v", intent)
	}
}

func TestTrafficIntentClearsWhenIntentRemoved(t *testing.T) {
	isvc := isvcFixture(engineCanary())
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	in.RouteCapabilities = bareProvider
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if intent := trafficIntent(isvc); intent == nil || intent.Status != corev1.ConditionFalse {
		t.Fatalf("TrafficIntentReady = %+v", intent)
	}

	isvc.Spec.Rollout.Groups[0] = v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	}
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	intent := trafficIntent(isvc)
	if intent == nil || intent.Status != corev1.ConditionTrue || intent.Reason != v1beta1.TrafficIntentReasonNoIntent {
		t.Fatalf("TrafficIntentReady after the canary is removed = %+v", intent)
	}
}

// A plan reread (the ome.io/rollout-repin verb) onto a canary the provider
// cannot carry is refused: the run keeps the plan it recorded and the
// refusal names the reason.
func TestAPlanRereadOntoUnsupportedRouteIntentIsRefused(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	in.RouteCapabilities = bareProvider
	if out, err := Reconcile(context.Background(), in); err != nil || !out.Opened {
		t.Fatalf("blueGreen must open on any provider: err=%v out=%+v", err, out)
	}
	recorded := isvc.Status.Rollout.ActiveRun.RunID

	isvc.Spec.Rollout.Groups[0] = engineCanary()
	annotate(isvc, "ome.io/rollout-repin", "now")
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	active := isvc.Status.Rollout.ActiveRun
	if active == nil || active.RunID != recorded || active.Plan.Groups[0].Group.Canary != nil {
		t.Fatalf("a plan reread onto intent the provider cannot carry must keep the recorded plan, got %+v", active)
	}
	refused := "the current source does not render (" + v1beta1.RolloutPlanReasonRouteProviderUnsupported + ")"
	if events := strings.Join(drainEvents(rec), "\n"); !strings.Contains(events, refused) {
		t.Fatalf("expected the reread refused with reason %s, got:\n%s", v1beta1.RolloutPlanReasonRouteProviderUnsupported, events)
	}
}
