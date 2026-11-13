package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	k8sptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
	"sigs.k8s.io/ome/pkg/validation"
)

// Reconcile is one pass of the routing controller over an InferenceService.
// A service that is gone, on its way out, switched off or outside placement
// is forgotten by the prober and the capacity poller, its metric series are
// removed, and its map is reaped. An accepted plan without ClusterAffinity,
// an obsolete placement annotation, a bad routing policy, an unreadable
// drain annotation, an operator block that fails validation, a map the
// service does not control, and an enabled observer with nothing to run it
// each stop the pass with their own error. Otherwise the observers run in
// order, the table is written, the routing conditions follow, the fallback
// series is recorded and the earliest observation deadline requeues the
// pass. One row per rule: the world the pass finds and everything it left.

var (
	ruleKey = types.NamespacedName{Name: testName, Namespace: testNS}

	errReadRefused   = errors.New("read refused")
	errWriteRefused  = errors.New("write refused")
	errStatusRefused = errors.New("status write refused")

	// staleProbeIdentity and staleCapacityKey are state of this map the
	// observers hold before the pass, for a target the service does not have.
	staleProbeIdentity = probeIdentity{OwnerUID: "uid-1", Map: ruleKey, Cluster: "stale", Endpoint: "https://stale.example", PolicyDigest: "stale"}
	staleCapacityKey   = capacityKey{ownerUID: "uid-1", mapKey: ruleKey, cluster: "stale", url: "https://stale.example", policyDigest: "stale"}
)

// reconcileRuleWorld is what one pass finds: the operator's config, the
// objects in the cluster, the client's behaviours and the observers wired.
type reconcileRuleWorld struct {
	config  Config
	objects []client.Object
	options []fakeclient.Option
	prober  *Prober
	poller  *CapacityPoller
}

// reconcileOutcome is everything one pass leaves behind.
type reconcileOutcome struct {
	Result ctrl.Result
	Err    string
	// Map is the map at the key: absent, or its entries and source.
	Map            string
	Routable       string
	OverrideActive string
	// ProbeForgotten and PollerForgotten report whether the stale state the
	// observers held for this map is gone; false with no such observer.
	ProbeForgotten  bool
	PollerForgotten bool
	// FallbackSeries and EntrySeries count the metric series after the pass;
	// one of each is seeded for the map before it.
	FallbackSeries int
	EntrySeries    int
}

type reconcileRule struct {
	name  string
	world func(t *testing.T) reconcileRuleWorld
	want  reconcileOutcome
}

func placedService() *v1beta1.InferenceService {
	return splitISVC([]string{"a"}, []int32{3}, []int32{3})
}

// sourceAbsentMap is a generated map whose source is gone: no owner, the
// source's UID recorded, no publisher claim.
func sourceAbsentMap() *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS, UID: "traffic-map-uid", ResourceVersion: "1"},
		Spec:       v1beta1.TrafficMapSpec{Service: testName},
		Status:     v1beta1.TrafficMapStatus{SourceUID: "deleted-source-uid"},
	}
}

// anotherSourcesMap is a map at the key that records another source and no controller.
func anotherSourcesMap() *v1beta1.TrafficMap {
	tm := sourceAbsentMap()
	tm.Status.SourceUID = "another-source-uid"
	return tm
}

func bareProber() *Prober {
	return &Prober{state: map[probeIdentity]*targetProbeState{}}
}

func barePoller() *CapacityPoller {
	return &CapacityPoller{Clock: clocktesting.NewFakeClock(capacityTestNow), state: map[capacityKey]*capacityState{}}
}

func seedObservers(prober *Prober, poller *CapacityPoller) {
	if prober != nil {
		prober.mu.Lock()
		if prober.state == nil {
			prober.state = map[probeIdentity]*targetProbeState{}
		}
		prober.state[staleProbeIdentity] = &targetProbeState{identity: staleProbeIdentity}
		prober.mu.Unlock()
	}
	if poller != nil {
		poller.mu.Lock()
		if poller.state == nil {
			poller.state = map[capacityKey]*capacityState{}
		}
		poller.state[staleCapacityKey] = &capacityState{}
		poller.mu.Unlock()
	}
}

func probeStateHeld(prober *Prober) bool {
	prober.mu.RLock()
	defer prober.mu.RUnlock()
	_, held := prober.state[staleProbeIdentity]
	return held
}

func capacityStateHeld(poller *CapacityPoller) bool {
	poller.mu.RLock()
	defer poller.mu.RUnlock()
	_, held := poller.state[staleCapacityKey]
	return held
}

func resetRoutingSeries() {
	capacityFallbackClusters.Reset()
	trafficMapEntryWeight.Reset()
	trafficMapEntryHealthy.Reset()
}

func describeMap(tm *v1beta1.TrafficMap) string {
	entries := make([]string, 0, len(tm.Spec.Entries))
	for _, entry := range tm.Spec.Entries {
		entries = append(entries, fmt.Sprintf("%s:%d", entry.Cluster, entry.Weight))
	}
	listed := "none"
	if len(entries) > 0 {
		listed = strings.Join(entries, " ")
	}
	return fmt.Sprintf("entries %s, source %q", listed, tm.Status.SourceUID)
}

func describeCondition(tm *v1beta1.TrafficMap, conditionType string) string {
	condition := apimeta.FindStatusCondition(tm.Status.Conditions, conditionType)
	if condition == nil {
		return "none"
	}
	return string(condition.Status) + "/" + condition.Reason
}

// runReconcileRule seeds the observers and the series, runs one pass and
// reads what it left.
func runReconcileRule(t *testing.T, w reconcileRuleWorld) reconcileOutcome {
	t.Helper()
	resetRoutingSeries()
	t.Cleanup(resetRoutingSeries)
	capacityFallbackClusters.WithLabelValues(testNS, testName).Set(1)
	trafficMapEntryWeight.WithLabelValues(testNS, testName, "stale").Set(1)
	trafficMapEntryHealthy.WithLabelValues(testNS, testName, "stale").Set(1)
	seedObservers(w.prober, w.poller)
	c := newRoutingClient(t, w.objects, w.options...)
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: w.config, Prober: w.prober, Capacity: w.poller}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ruleKey})
	out := reconcileOutcome{Result: result, Map: "absent", Routable: "none", OverrideActive: "none"}
	if err != nil {
		out.Err = err.Error()
	}
	if tm, ok := getTrafficMap(t, c); ok {
		out.Map = describeMap(tm)
		out.Routable = describeCondition(tm, v1beta1.TrafficMapRoutable)
		out.OverrideActive = describeCondition(tm, v1beta1.TrafficMapOverrideActive)
	}
	out.ProbeForgotten = w.prober != nil && !probeStateHeld(w.prober)
	out.PollerForgotten = w.poller != nil && !capacityStateHeld(w.poller)
	out.FallbackSeries = testutil.CollectAndCount(capacityFallbackClusters)
	out.EntrySeries = testutil.CollectAndCount(trafficMapEntryWeight)
	return out
}

// leftAlone is a pass that stopped with err before it forgot or wrote anything.
func leftAlone(err string) reconcileOutcome {
	return reconcileOutcome{Err: err, Map: "absent", Routable: "none", OverrideActive: "none", FallbackSeries: 1, EntrySeries: 1}
}

// forgotten is a pass that forgot the service and reaped its map.
func forgotten() reconcileOutcome {
	return reconcileOutcome{Map: "absent", Routable: "none", OverrideActive: "none", ProbeForgotten: true, PollerForgotten: true}
}

func outcome(base reconcileOutcome, edit func(*reconcileOutcome)) reconcileOutcome {
	edit(&base)
	return base
}

// earlyWorld is a placed service with bare observers holding stale state,
// for the rows that end before the observers run.
func earlyWorld(isvc *v1beta1.InferenceService, objects ...client.Object) reconcileRuleWorld {
	return reconcileRuleWorld{
		config:  controllerTestConfig(),
		objects: append([]client.Object{isvc}, objects...),
		prober:  bareProber(),
		poller:  barePoller(),
	}
}

// observedWorld is a placed service whose endpoint answers probes with 503
// and capacity polls with a report of one servable replica, under the
// operator blocks given, the observers wired with fake clocks.
func observedWorld(t *testing.T, probe ProbeConfig, capacity CapacityConfig, withProber, withPoller bool) reconcileRuleWorld {
	t.Helper()
	report, err := json.Marshal(CapacityReport{Servable: k8sptr.To(int32(1)), ObservedAt: capacityTestNow})
	require.NoError(t, err)
	server := endpointServer(t, func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == capacity.Path {
			_, _ = w.Write(report)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	isvc := placedService()
	isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
	cfg := controllerTestConfig()
	cfg.Probe = probe
	cfg.Capacity = capacity
	world := reconcileRuleWorld{config: cfg, objects: []client.Object{isvc}}
	if withProber {
		world.prober = newTestProber(t, NewObserverClient(), clocktesting.NewFakeClock(testProbeNow), 1, 1, testProbeResponseBytes)
	}
	if withPoller {
		world.poller = newControllerCapacityPoller(t, NewObserverClient(), clocktesting.NewFakeClock(capacityTestNow))
	}
	return world
}

// unreadableDrain is how the pass names an annotation it cannot parse.
var unreadableDrain = "parse " + constants.TrafficDrainAnnotation + ": "

var reconcileRules = []reconcileRule{
	{
		name: "a source read that fails is returned as it is and forgets nothing",
		world: func(t *testing.T) reconcileRuleWorld {
			w := earlyWorld(placedService())
			w.options = []fakeclient.Option{fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, errReadRefused).OnKind(&v1beta1.InferenceService{})}
			return w
		},
		want: leftAlone("read refused"),
	},
	{
		name: "a source the cache and the reader both miss is forgotten and its ownerless map removed",
		world: func(t *testing.T) reconcileRuleWorld {
			return reconcileRuleWorld{config: controllerTestConfig(), objects: []client.Object{sourceAbsentMap()}, prober: bareProber(), poller: barePoller()}
		},
		want: forgotten(),
	},
	{
		name: "a removal that loses the race is retried without an error",
		world: func(t *testing.T) reconcileRuleWorld {
			tm := sourceAbsentMap()
			return reconcileRuleWorld{
				config:  controllerTestConfig(),
				objects: []client.Object{tm},
				options: []fakeclient.Option{fakeclient.FailingVerb(fakeclient.Delete, fakeclient.EveryCall, conflictOnDelete(tm))},
				prober:  bareProber(),
				poller:  barePoller(),
			}
		},
		want: outcome(forgotten(), func(o *reconcileOutcome) {
			o.Result = ctrl.Result{Requeue: true}
			o.Map = `entries none, source "deleted-source-uid"`
		}),
	},
	{
		name: "a source on its way out is forgotten and its map removed",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := deletingISVC()
			return earlyWorld(isvc, ownedTrafficMap(isvc))
		},
		want: forgotten(),
	},
	{
		name: "an accepted plan without ClusterAffinity is refused before anything is forgotten",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{}
			isvc.Spec.Placement.Policy = ""
			w := earlyWorld(isvc, ownedTrafficMap(isvc))
			w.config.Enabled = false
			return w
		},
		want: outcome(leftAlone(validation.ErrAcceptedPlanRequiresClusterAffinity.Error()), func(o *reconcileOutcome) {
			o.Map = `entries none, source ""`
		}),
	},
	{
		name: "routing switched off forgets the service and removes its map",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			w := earlyWorld(isvc, ownedTrafficMap(isvc))
			w.config.Enabled = false
			return w
		},
		want: forgotten(),
	},
	{
		name: "a service that opted out of routing is forgotten",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Spec.Routing = &v1beta1.RoutingSpec{Enabled: k8sptr.To(false)}
			return earlyWorld(isvc, ownedTrafficMap(isvc))
		},
		want: forgotten(),
	},
	{
		name: "a service outside placement is forgotten",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Spec.Placement = nil
			return earlyWorld(isvc, ownedTrafficMap(isvc))
		},
		want: forgotten(),
	},
	{
		name: "an obsolete placement annotation is refused",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Annotations = map[string]string{constants.AcceleratorRequirements: "gpu"}
			return earlyWorld(isvc)
		},
		want: leftAlone("validate placement intent: annotation " + constants.AcceleratorRequirements + " is unsupported; declare spec.placement.mode and migrate restrictions to clusterAffinity"),
	},
	{
		name: "a routing policy with a non-positive capacity factor is refused",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Spec.Routing = &v1beta1.RoutingSpec{CapacityFactors: map[string]resource.Quantity{"a": resource.MustParse("0")}}
			return earlyWorld(isvc)
		},
		want: leftAlone(`validate routing policy: spec.routing.capacityFactors["a"] must be positive, got "0"`),
	},
	{
		name: "an unreadable drain annotation keeps the last map and reports it there",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Annotations = map[string]string{constants.TrafficDrainAnnotation: "{"}
			return earlyWorld(isvc, ownedTrafficMap(isvc))
		},
		want: outcome(leftAlone(unreadableDrain+"close top-level object: EOF"), func(o *reconcileOutcome) {
			o.Map = `entries none, source "uid-1"`
			o.OverrideActive = "Unknown/" + v1beta1.TrafficMapReasonOverridesInvalid
		}),
	},
	{
		name: "the report of an unreadable drain annotation failing to write is joined to the parse error",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Annotations = map[string]string{constants.TrafficDrainAnnotation: "{"}
			w := earlyWorld(isvc, ownedTrafficMap(isvc))
			w.options = []fakeclient.Option{fakeclient.FailingVerb(fakeclient.StatusApply, fakeclient.EveryCall, errStatusRefused)}
			return w
		},
		want: outcome(leftAlone(unreadableDrain+"close top-level object: EOF\nstatus write refused"), func(o *reconcileOutcome) {
			o.Map = `entries none, source ""`
		}),
	},
	{
		name: "an unreadable drain annotation without a map is only an error",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.Annotations = map[string]string{constants.TrafficDrainAnnotation: "{"}
			return earlyWorld(isvc)
		},
		want: leftAlone(unreadableDrain + "close top-level object: EOF"),
	},
	{
		name: "an operator probe block that fails validation is refused",
		world: func(t *testing.T) reconcileRuleWorld {
			w := earlyWorld(placedService())
			w.config.Probe = ProbeConfig{Path: "healthz"}
			return w
		},
		want: leftAlone(`resolve probe policy: routing.probe.path "healthz" must begin with "/"`),
	},
	{
		name: "an operator capacity block that fails validation is refused",
		world: func(t *testing.T) reconcileRuleWorld {
			w := earlyWorld(placedService())
			w.config.Capacity = CapacityConfig{Mode: "Bogus"}
			return w
		},
		want: leftAlone(`resolve capacity policy: routing.capacity.mode "Bogus" must be Ceiling or Reported`),
	},
	{
		name: "a map at the key the service does not control is refused before the observers run",
		world: func(t *testing.T) reconcileRuleWorld {
			return earlyWorld(placedService(), anotherSourcesMap())
		},
		want: outcome(leftAlone(fmt.Sprintf("TrafficMap %s exists but is not controlled by InferenceService UID %q", ruleKey, "uid-1")), func(o *reconcileOutcome) {
			o.Map = `entries none, source "another-source-uid"`
		}),
	},
	{
		name: "an enabled probe policy without a prober is refused",
		world: func(t *testing.T) reconcileRuleWorld {
			w := earlyWorld(placedService())
			w.config.Probe = testProbeConfig()
			w.prober = nil
			return w
		},
		want: leftAlone("endpoint probing is enabled but no prober is configured"),
	},
	{
		name: "an enabled capacity policy without a poller is refused",
		world: func(t *testing.T) reconcileRuleWorld {
			w := earlyWorld(placedService())
			w.config.Capacity = controllerTestCapacityConfig()
			w.poller = nil
			return w
		},
		want: leftAlone("capacity polling is enabled but no poller is configured"),
	},
	{
		name: "a prober that refuses the request stops the pass before the poller runs",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.UID = ""
			return earlyWorld(isvc)
		},
		want: leftAlone("reconcile endpoint probes: probe reconcile owner UID is empty"),
	},
	{
		name: "a poller that refuses the request stops the pass",
		world: func(t *testing.T) reconcileRuleWorld {
			isvc := placedService()
			isvc.UID = ""
			w := earlyWorld(isvc)
			w.prober = nil
			return w
		},
		want: leftAlone("reconcile endpoint capacity: capacity reconcile owner UID is empty"),
	},
	{
		name: "a map write that fails stops the pass before the conditions",
		world: func(t *testing.T) reconcileRuleWorld {
			w := earlyWorld(placedService())
			w.options = []fakeclient.Option{fakeclient.FailingVerb(fakeclient.Create, fakeclient.EveryCall, errWriteRefused).OnKind(&v1beta1.TrafficMap{})}
			return w
		},
		want: outcome(leftAlone("write refused"), func(o *reconcileOutcome) {
			o.ProbeForgotten = true
			o.PollerForgotten = true
		}),
	},
	{
		name: "a condition write that fails returns the error after the map is written",
		world: func(t *testing.T) reconcileRuleWorld {
			w := earlyWorld(placedService())
			w.options = []fakeclient.Option{fakeclient.FailingVerb(fakeclient.StatusApply, fakeclient.EveryCall, errStatusRefused)}
			return w
		},
		want: outcome(leftAlone("status write refused"), func(o *reconcileOutcome) {
			o.Map = `entries a:1, source ""`
			o.ProbeForgotten = true
			o.PollerForgotten = true
		}),
	},
	{
		name: "a placed service gets its table and its conditions, the observers forget it with their policies off, and nothing requeues",
		world: func(t *testing.T) reconcileRuleWorld {
			return earlyWorld(placedService())
		},
		want: reconcileOutcome{
			Map: `entries a:1, source "uid-1"`, Routable: "True/Routable", OverrideActive: "False/NoOverrides",
			ProbeForgotten: true, PollerForgotten: true, FallbackSeries: 0, EntrySeries: 1,
		},
	},
	{
		name: "an enabled probe schedules the next pass at its period",
		world: func(t *testing.T) reconcileRuleWorld {
			return observedWorld(t, testProbeConfig(), CapacityConfig{}, true, false)
		},
		want: reconcileOutcome{
			Result: ctrl.Result{RequeueAfter: testProbeConfig().Period},
			Map:    `entries a:1, source "uid-1"`, Routable: "True/Routable", OverrideActive: "False/NoOverrides",
			ProbeForgotten: true, FallbackSeries: 0, EntrySeries: 1,
		},
	},
	{
		name: "an enabled capacity poll schedules the next pass at its period and records the fallback series",
		world: func(t *testing.T) reconcileRuleWorld {
			return observedWorld(t, ProbeConfig{}, controllerTestCapacityConfig(), false, true)
		},
		want: reconcileOutcome{
			Result: ctrl.Result{RequeueAfter: controllerTestCapacityConfig().Period},
			Map:    `entries a:1, source "uid-1"`, Routable: "True/Routable", OverrideActive: "False/NoOverrides",
			PollerForgotten: true, FallbackSeries: 1, EntrySeries: 1,
		},
	},
	{
		name: "with both observers the earlier deadline requeues the pass",
		world: func(t *testing.T) reconcileRuleWorld {
			return observedWorld(t, testProbeConfig(), controllerTestCapacityConfig(), true, true)
		},
		want: reconcileOutcome{
			Result: ctrl.Result{RequeueAfter: testProbeConfig().Period},
			Map:    `entries a:1, source "uid-1"`, Routable: "True/Routable", OverrideActive: "False/NoOverrides",
			ProbeForgotten: true, PollerForgotten: true, FallbackSeries: 1, EntrySeries: 1,
		},
	},
	{
		name: "a probe policy with no target to probe schedules nothing",
		world: func(t *testing.T) reconcileRuleWorld {
			w := observedWorld(t, testProbeConfig(), CapacityConfig{}, true, false)
			w.objects[0].(*v1beta1.InferenceService).Status.Placement.Candidates[0].Endpoint = nil
			return w
		},
		want: reconcileOutcome{
			Map: `entries none, source "uid-1"`, Routable: "False/NoAddressableHome", OverrideActive: "False/NoOverrides",
			ProbeForgotten: true, FallbackSeries: 0, EntrySeries: 0,
		},
	},
}

func TestReconcileRules(t *testing.T) {
	for _, rule := range reconcileRules {
		t.Run(rule.name, func(t *testing.T) {
			got := runReconcileRule(t, rule.world(t))
			if diff := cmp.Diff(rule.want, got); diff != "" {
				t.Fatalf("what the pass left (-want +got):\n%s", diff)
			}
		})
	}
}
