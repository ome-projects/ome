package coordination

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateFleetAllocations = flag.Bool("update", false, "re-record testdata/fleet_allocations.json from this run")

// fleetGateCase is one ask of the gate stack about one more start of the
// engine side of a prefill/decode group: the name the benchmark and the
// allocation golden share, and the ask.
type fleetGateCase struct {
	name string
	ask  func(testing.TB)
}

// fleetGateCases asks at the reference sizes, mid-roll under the surge
// strategy: 150:100 (a reference shape), 300:100 (the second reference
// shape) and 12:8 under RecreatePod (a common shape). Each ask reads and
// decodes every member's replica once, so the cost grows with the Instances
// of the group, not with the Instance being started.
func fleetGateCases() []fleetGateCase {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	recreate := v1beta1.UpdateStrategyRecreatePod
	shapes := []struct {
		name      string
		group     batchGroupFixture
		engine    batchMemberFixture
		decoder   batchMemberFixture
		mechanism workloadtypes.UpdateStrategyType
	}{
		{
			name:      "150:100/surge10",
			group:     batchGroupFixture{maxSurge: "10%", tolerance: ptr.To(int32(20))},
			engine:    batchMemberFixture{desired: 150, surge: "10%", strategy: surge, promoted: 30, surging: 15},
			decoder:   batchMemberFixture{desired: 100, surge: "10%", strategy: surge, promoted: 20},
			mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:      "300:100/surge10",
			group:     batchGroupFixture{maxSurge: "10%", tolerance: ptr.To(int32(20))},
			engine:    batchMemberFixture{desired: 300, surge: "10%", strategy: surge, promoted: 60, surging: 30},
			decoder:   batchMemberFixture{desired: 100, surge: "10%", strategy: surge, promoted: 20},
			mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:      "12:8/recreate10",
			group:     batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(20))},
			engine:    batchMemberFixture{desired: 12, step: "10%", strategy: recreate, promoted: 2, inFlight: 1},
			decoder:   batchMemberFixture{desired: 8, step: "10%", strategy: recreate, promoted: 1},
			mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
	}
	// The context carries a logger, as a pass's does: the root logger's own
	// bookkeeping before the manager sets it is not part of the gate's cost.
	ctx := logr.NewContext(context.Background(), logr.Discard())
	var cases []fleetGateCase
	for _, s := range shapes {
		members := map[v1beta1.ComponentType]batchMemberFixture{eng: s.engine, dec: s.decoder}
		isvc := batchService(s.group, members)
		reads := cacheReaderOf(batchReplica(eng, s.engine), batchReplica(dec, s.decoder))
		target := forwardTarget(isvc, eng)
		mechanism := s.mechanism
		cases = append(cases, fleetGateCase{name: s.name, ask: func(tb testing.TB) {
			_, gate, reason := EvaluateUpdateGate(ctx, reads, isvc, eng, "", target, nil, GroupDefaults{}, mechanism, 0, 0)
			if gate == v1beta1.RolloutHoldGatePlan {
				tb.Fatalf("the plan gate held the start: %s", reason)
			}
		}})
	}
	return cases
}

// BenchmarkEvaluateUpdateGateFleet is the cost of one ask per shape.
func BenchmarkEvaluateUpdateGateFleet(b *testing.B) {
	for _, c := range fleetGateCases() {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c.ask(b)
			}
		})
	}
}

// TestFleetGateAllocationsStayWithinBudget holds the allocations of one ask
// per shape to the recorded golden, so a change in the gate stack's cost is
// made on purpose and carries its reason.
func TestFleetGateAllocationsStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "fleet_allocations.json"), *updateFleetAllocations)
	defer golden.Close()
	for _, c := range fleetGateCases() {
		golden.Check(c.name, func() { c.ask(t) })
	}
}

// cacheReader serves typed objects from memory by deep copy, the way the
// manager's informer cache answers a Get. The fake client's JSON round trip
// per read would otherwise be most of what the benchmark measures.
type cacheReader struct {
	objects map[string]client.Object
}

func cacheReaderOf(objects ...client.Object) cacheReader {
	r := cacheReader{objects: make(map[string]client.Object, len(objects))}
	for _, obj := range objects {
		r.objects[cacheKey(obj, obj.GetNamespace(), obj.GetName())] = obj
	}
	return r
}

func cacheKey(obj client.Object, namespace, name string) string {
	return fmt.Sprintf("%T/%s/%s", obj, namespace, name)
}

func (r cacheReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	stored, ok := r.objects[cacheKey(obj, key.Namespace, key.Name)]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group}, key.Name)
	}
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(stored.DeepCopyObject()).Elem())
	return nil
}

// List answers a pod list with no pods, as a cache with none would: the
// unavailability gate lists the Component's pods on an ask that hands it no
// list, and the shapes here hold none. Any other list is not served.
func (r cacheReader) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	if pods, ok := list.(*corev1.PodList); ok {
		pods.Items = nil
		return nil
	}
	return errors.New("the benchmark reader serves single objects and pod lists only")
}
