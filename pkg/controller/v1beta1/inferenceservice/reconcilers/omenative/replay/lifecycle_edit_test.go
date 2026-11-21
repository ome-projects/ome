package replay

import (
	"context"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// lifecycleEditWalk is two engines behind two routers with no rollout group.
// The engine declares SurgeThenDrain with both budgets and the restart
// policy None, and the routers declare no lifecycle. Version 2 is pushed on
// the engine at tick 2 and its first replacement pod starts. The events of
// tick 3 are the test's own.
const lifecycleEditWalk = `scenario: lifecycle-edit
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1, restartPolicy: None, updateStrategy: {type: SurgeThenDrain, rollingUpdate: {maxSurge: 1, maxUnavailable: 1}}}
      router: {replicas: 2, image: registry.example.com/router:v1}
  config: {requeue: 10s}
timeline:
  - tick: 1
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
      - member.surgePodCreated: {component: engine, index: 0}
  - tick: 3
    events:
`

// runLifecycleEdit plays the walk with the given events at tick 3 and one
// quiet tick after them. It returns the view of every tick and the trace.
func runLifecycleEdit(t *testing.T, events string) ([]TickView, string) {
	t.Helper()
	s, err := Parse([]byte(lifecycleEditWalk+events+"  - tick: 4\n"), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	var views []TickView
	trace, err := Run(context.Background(), s, Options{Observer: func(view TickView) { views = append(views, view) }})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	if len(views) != 4 {
		t.Fatalf("the walk has 4 ticks and the observer saw %d", len(views))
	}
	return views, trace
}

// podNamesOf lists the pods of the engine and of the routers in one view.
func podNamesOf(view TickView) []string {
	var names []string
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent} {
		for _, pod := range view.Pods[c] {
			names = append(names, pod.Name)
		}
	}
	return names
}

// linesWith lists the trace lines of one tick that carry a text.
func linesWith(trace, tick, text string) []string {
	var out []string
	for _, line := range strings.Split(trace, "\n") {
		if strings.HasPrefix(line, tick+" ") && strings.Contains(line, text) {
			out = append(out, line)
		}
	}
	return out
}

// checkTheRollIsLeftAlone holds an edit at tick 3 to what a lifecycle edit
// may move: the service's generation by one, and nothing of the roll. The
// engine keeps its roll target, no revision is minted and no pod of either
// Component comes or goes.
func checkTheRollIsLeftAlone(t *testing.T, views []TickView) {
	t.Helper()
	before, edit, after := views[1], views[2], views[3]
	if before.Service.Generation+1 != edit.Service.Generation || edit.Service.Generation != after.Service.Generation {
		t.Errorf("the service's generation reads %d, %d and %d on ticks 2, 3 and 4; one edit moves it by one", before.Service.Generation, edit.Service.Generation, after.Service.Generation)
	}
	if was, now := before.Replicas[v1beta1.EngineComponent].Status.UpdateRevision, after.Replicas[v1beta1.EngineComponent].Status.UpdateRevision; was == "" || was != now {
		t.Errorf("the engine's roll target moved from %q to %q", was, now)
	}
	if len(before.Revisions) != len(after.Revisions) {
		t.Errorf("%d revisions stand before the edit and %d after it; a lifecycle edit mints none", len(before.Revisions), len(after.Revisions))
	}
	if was, now := podNamesOf(before), podNamesOf(after); len(was) == 0 || !slices.Equal(was, now) {
		t.Errorf("the pods were %v before the edit and are %v after it", was, now)
	}
}

// A strategy edit in the middle of a roll writes the new type on the
// Component's replica and keeps both budgets and the restart policy beside
// it. The trace names the change on the apply line and on the one replica
// write that carries it, and the routers' replica carries no lifecycle.
func TestAStrategyEditReachesTheReplicaAndLeavesTheRollAlone(t *testing.T) {
	views, trace := runLifecycleEdit(t, "      - spec.strategy: {component: engine, strategy: RecreatePod}\n")
	checkTheRollIsLeftAlone(t, views)
	const change = "strategy=SurgeThenDrain→RecreatePod"
	if lines := linesWith(trace, "tick=3", "apply spec.strategy component=engine "+change+" generation=3"); len(lines) != 1 {
		t.Errorf("the edit is not traced as applied once: %v\n%s", lines, trace)
	}
	if lines := linesWith(trace, "tick=3", change); len(lines) != 2 || !strings.Contains(lines[1], "engine") {
		t.Errorf("the change stands on %d lines of tick 3, where the apply line and the engine replica's write carry it: %v", len(lines), lines)
	}
	lifecycle := views[3].Replicas[v1beta1.EngineComponent].Spec.Lifecycle
	if lifecycle == nil || lifecycle.UpdateStrategy == nil || lifecycle.UpdateStrategy.RollingUpdate == nil || lifecycle.RestartPolicy == nil {
		t.Fatalf("the engine's replica carries %+v", lifecycle)
	}
	one := intstr.FromInt32(1)
	rolling := lifecycle.UpdateStrategy.RollingUpdate
	switch {
	case lifecycle.UpdateStrategy.Type != v1beta1.UpdateStrategyRecreatePod:
		t.Errorf("type = %q, want RecreatePod", lifecycle.UpdateStrategy.Type)
	case rolling.MaxSurge == nil || *rolling.MaxSurge != one || rolling.MaxUnavailable == nil || *rolling.MaxUnavailable != one:
		t.Errorf("the budgets read maxSurge %v and maxUnavailable %v, and the edit names neither", rolling.MaxSurge, rolling.MaxUnavailable)
	case *lifecycle.RestartPolicy != v1beta1.InstanceRestartPolicyNone:
		t.Errorf("the restart policy reads %q, and the edit does not name it", *lifecycle.RestartPolicy)
	}
	if lifecycle := views[3].Replicas[v1beta1.RouterComponent].Spec.Lifecycle; lifecycle != nil {
		t.Errorf("the routers declare no lifecycle, and their replica carries %+v", lifecycle)
	}
}

// A restart-policy edit in the middle of a roll writes the new policy on
// the Component's replica and keeps its strategy as declared.
func TestARestartPolicyEditReachesTheReplicaAndLeavesTheRollAlone(t *testing.T) {
	views, trace := runLifecycleEdit(t, "      - spec.restartPolicy: {component: engine, policy: RecreateInstanceOnPodRestart}\n")
	checkTheRollIsLeftAlone(t, views)
	const change = "restartPolicy=None→RecreateInstanceOnPodRestart"
	if lines := linesWith(trace, "tick=3", "apply spec.restartPolicy component=engine "+change+" generation=3"); len(lines) != 1 {
		t.Errorf("the edit is not traced as applied once: %v\n%s", lines, trace)
	}
	if lines := linesWith(trace, "tick=3", change); len(lines) != 2 || !strings.Contains(lines[1], "engine") {
		t.Errorf("the change stands on %d lines of tick 3, where the apply line and the engine replica's write carry it: %v", len(lines), lines)
	}
	lifecycle := views[3].Replicas[v1beta1.EngineComponent].Spec.Lifecycle
	if lifecycle == nil || lifecycle.RestartPolicy == nil || lifecycle.UpdateStrategy == nil {
		t.Fatalf("the engine's replica carries %+v", lifecycle)
	}
	if *lifecycle.RestartPolicy != v1beta1.InstanceRestartPolicyRecreateInstance {
		t.Errorf("restart policy = %q, want RecreateInstanceOnPodRestart", *lifecycle.RestartPolicy)
	}
	if lifecycle.UpdateStrategy.Type != v1beta1.UpdateStrategySurgeThenDrain {
		t.Errorf("the strategy reads %q, and the edit does not name it", lifecycle.UpdateStrategy.Type)
	}
}

// An edit on a Component that declared no lifecycle writes its one field and
// nothing beside it: a strategy type with no budget, or a restart policy
// with no strategy.
func TestALifecycleEditOnAComponentThatDeclaredNoneWritesOneField(t *testing.T) {
	views, trace := runLifecycleEdit(t, "      - spec.strategy: {component: router, strategy: RecreatePod}\n")
	if lines := linesWith(trace, "tick=3", "apply spec.strategy component=router strategy=unset→RecreatePod"); len(lines) != 1 {
		t.Errorf("the edit is not traced as a first declaration: %v", lines)
	}
	lifecycle := views[3].Replicas[v1beta1.RouterComponent].Spec.Lifecycle
	if lifecycle == nil || lifecycle.UpdateStrategy == nil {
		t.Fatalf("the routers' replica carries %+v", lifecycle)
	}
	if strategy := lifecycle.UpdateStrategy; strategy.Type != v1beta1.UpdateStrategyRecreatePod || strategy.RollingUpdate != nil || lifecycle.RestartPolicy != nil {
		t.Errorf("the routers' replica carries %+v with restart policy %v, and the edit names the type alone", strategy, lifecycle.RestartPolicy)
	}

	views, trace = runLifecycleEdit(t, "      - spec.restartPolicy: {component: router, policy: None}\n")
	if lines := linesWith(trace, "tick=3", "apply spec.restartPolicy component=router restartPolicy=unset→None"); len(lines) != 1 {
		t.Errorf("the edit is not traced as a first declaration: %v", lines)
	}
	lifecycle = views[3].Replicas[v1beta1.RouterComponent].Spec.Lifecycle
	if lifecycle == nil || lifecycle.RestartPolicy == nil || *lifecycle.RestartPolicy != v1beta1.InstanceRestartPolicyNone || lifecycle.UpdateStrategy != nil {
		t.Errorf("the routers' replica carries %+v, and the edit names the restart policy alone", lifecycle)
	}
}

// The two events load under a vocabulary that declares neither, as the
// other events of the harness do, and the parser refuses an edit it cannot
// stage: a variant, no Component, a value the API does not admit, the other
// event's argument, and a declared restart policy the API does not admit.
func TestParseRefusesALifecycleEditItCannotStage(t *testing.T) {
	for _, event := range []string{
		"      - spec.strategy: {component: engine, strategy: RecreatePod}\n",
		"      - spec.restartPolicy: {component: engine, policy: None}\n",
	} {
		if _, err := Parse([]byte(scenarioHead+"  - tick: 1\n    events:\n"+event), testVocabulary()); err != nil {
			t.Errorf("%q does not load under a vocabulary that declares no such event: %v", strings.TrimSpace(event), err)
		}
	}
	if !Stageable(StrategyEdit) || !Stageable(RestartPolicyEdit) {
		t.Error("the driver lists no applier for a lifecycle edit")
	}
	cases := map[string]string{
		"strategy with a variant":      "      - spec.strategy[now]: {component: engine, strategy: RecreatePod}\n",
		"strategy with no component":   "      - spec.strategy: {strategy: RecreatePod}\n",
		"strategy the API lacks":       "      - spec.strategy: {component: engine, strategy: Sideways}\n",
		"strategy with no value":       "      - spec.strategy: {component: engine}\n",
		"strategy with a policy":       "      - spec.strategy: {component: engine, policy: None}\n",
		"policy with a variant":        "      - spec.restartPolicy[now]: {component: engine, policy: None}\n",
		"policy with no component":     "      - spec.restartPolicy: {policy: None}\n",
		"policy the API lacks":         "      - spec.restartPolicy: {component: engine, policy: Always}\n",
		"policy with a strategy":       "      - spec.restartPolicy: {component: engine, strategy: RecreatePod}\n",
		"policy written as a bare key": "      - spec.restartPolicy\n",
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(scenarioHead+"  - tick: 1\n    events:\n"+event), testVocabulary()); err == nil {
				t.Fatalf("parsed %q", event)
			}
		})
	}
	declared := strings.Replace(scenarioHead, "image: registry.example.com/runtime:v1}", "image: registry.example.com/runtime:v1, restartPolicy: Always}", 1)
	if _, err := Parse([]byte(declared+"  - tick: 1\n"), testVocabulary()); err == nil || !strings.Contains(err.Error(), "restart policy") {
		t.Errorf("a declared restart policy the API lacks parsed: %v", err)
	}
}

// The driver refuses an edit that would write nothing, and one on a
// Component the service does not declare.
func TestALifecycleEditThatChangesNothingIsRefused(t *testing.T) {
	cases := map[string]struct{ event, want string }{
		"the declared strategy":       {"      - spec.strategy: {component: engine, strategy: SurgeThenDrain}\n", "changes nothing"},
		"the declared restart policy": {"      - spec.restartPolicy: {component: engine, policy: None}\n", "changes nothing"},
		"an undeclared Component":     {"      - spec.strategy: {component: decoder, strategy: RecreatePod}\n", "not declared"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := Parse([]byte(lifecycleEditWalk+tc.event), Vocabulary{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Run(context.Background(), s, Options{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the edit ran: %v", err)
			}
		})
	}
}
