package replay

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// engineReplicaOf runs the plain walk with the engine's component line
// replaced and returns the engine's replica as the last tick sees it.
func engineReplicaOf(t *testing.T, component string) *v1beta1.InferenceReplica {
	t.Helper()
	walk := strings.Replace(plainWalk, "engine: {replicas: 2, image: registry.example.com/runtime:v1}", component, 1)
	if walk == plainWalk {
		t.Fatalf("the plain walk has no engine line to replace")
	}
	s, err := Parse([]byte(walk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	var last TickView
	trace, err := Run(context.Background(), s, Options{Observer: func(view TickView) { last = view }})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	ir := last.Replicas[v1beta1.EngineComponent]
	if ir == nil {
		t.Fatalf("the last tick carries no engine replica")
	}
	return ir
}

// A component that declares an update strategy projects it on its replica
// as declared: the type and both budgets, maxSurge and maxUnavailable. The
// component's partition joins the same rollingUpdate block.
func TestADeclaredUpdateStrategyReachesTheReplica(t *testing.T) {
	ir := engineReplicaOf(t, `engine: {replicas: 2, image: registry.example.com/runtime:v1, partition: 1, updateStrategy: {type: SurgeThenDrain, rollingUpdate: {maxSurge: "34%", maxUnavailable: 0}}}`)
	if ir.Spec.Lifecycle == nil || ir.Spec.Lifecycle.UpdateStrategy == nil || ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate == nil {
		t.Fatalf("the replica carries no strategy: %+v", ir.Spec.Lifecycle)
	}
	strategy := ir.Spec.Lifecycle.UpdateStrategy
	rolling := strategy.RollingUpdate
	surge, unavailable := intstr.FromString("34%"), intstr.FromInt32(0)
	switch {
	case strategy.Type != v1beta1.UpdateStrategyType("SurgeThenDrain"):
		t.Errorf("type = %q, want SurgeThenDrain", strategy.Type)
	case rolling.MaxSurge == nil || *rolling.MaxSurge != surge:
		t.Errorf("maxSurge = %v, want 34%%", rolling.MaxSurge)
	case rolling.MaxUnavailable == nil || *rolling.MaxUnavailable != unavailable:
		t.Errorf("maxUnavailable = %v, want 0", rolling.MaxUnavailable)
	case rolling.Partition == nil || *rolling.Partition != 1:
		t.Errorf("partition = %v, want 1", rolling.Partition)
	}
}

// A component that declares no update strategy and no partition projects
// no strategy on its replica, so a test's own copy decides what the
// gates read.
func TestAComponentThatDeclaresNoStrategyProjectsNone(t *testing.T) {
	ir := engineReplicaOf(t, "engine: {replicas: 2, image: registry.example.com/runtime:v1, partition: 0}")
	if ir.Spec.Lifecycle == nil || ir.Spec.Lifecycle.UpdateStrategy == nil || ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate == nil {
		t.Fatalf("a partition alone projects its rollingUpdate block, and the replica carries %+v", ir.Spec.Lifecycle)
	}
	if strategy := ir.Spec.Lifecycle.UpdateStrategy; strategy.Type != "" || strategy.RollingUpdate.MaxSurge != nil || strategy.RollingUpdate.MaxUnavailable != nil {
		t.Errorf("a partition alone projects no type and no budget, and the replica carries %+v", strategy)
	}
	s, err := Parse([]byte(plainWalk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	var last TickView
	if trace, err := Run(context.Background(), s, Options{Observer: func(view TickView) { last = view }}); err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	if lc := last.Replicas[v1beta1.EngineComponent].Spec.Lifecycle; lc != nil && lc.UpdateStrategy != nil {
		t.Errorf("the plain walk declares nothing, and its replica carries %+v", lc.UpdateStrategy)
	}
}

// The parser refuses a partition under updateStrategy: the component's
// partition field carries it, and spec.partition edits that field.
func TestParseRefusesAPartitionUnderUpdateStrategy(t *testing.T) {
	walk := strings.Replace(plainWalk, "engine: {replicas: 2, image: registry.example.com/runtime:v1}",
		"engine: {replicas: 2, image: registry.example.com/runtime:v1, updateStrategy: {type: RecreatePod, rollingUpdate: {maxUnavailable: 1, partition: 1}}}", 1)
	_, err := Parse([]byte(walk), Vocabulary{})
	if err == nil || !strings.Contains(err.Error(), "names a partition under updateStrategy") {
		t.Fatalf("a partition under updateStrategy parsed: %v", err)
	}
}
