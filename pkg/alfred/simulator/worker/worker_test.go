package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/ome/alfred-simulator/protocol"
)

const defaultConfig = "apiVersion: kubescheduler.config.k8s.io/v1\nkind: KubeSchedulerConfiguration\nprofiles:\n- schedulerName: default-scheduler\n"
const omeConfig = "apiVersion: kubescheduler.config.k8s.io/v1\nkind: KubeSchedulerConfiguration\nprofiles:\n- schedulerName: default-scheduler\n  plugins:\n    multiPoint:\n      enabled:\n      - name: OMEGangPack\n  pluginConfig:\n  - name: OMEGangPack\n    args:\n      topologyKey: topology.example/domain\n      gcIntervalSeconds: 60\n      podGroupSyncTimeoutSeconds: 1\n"

func testProfile(t *testing.T, config string) *Profile {
	t.Helper()
	p, e := LoadProfile("test", []byte(config))
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func testPod(name, node string) v1.Pod {
	return v1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test", UID: types.UID(name + "-uid")}, Spec: v1.PodSpec{SchedulerName: "default-scheduler", NodeName: node, Containers: []v1.Container{{Name: "main", Image: "example/image", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"example.com/gpu": resource.MustParse("1")}, Limits: v1.ResourceList{"example.com/gpu": resource.MustParse("1")}}}}}}
}
func testNode(name string, gpu string) *v1.Node {
	resources := v1.ResourceList{v1.ResourceCPU: resource.MustParse("8"), v1.ResourceMemory: resource.MustParse("32Gi"), v1.ResourcePods: resource.MustParse("100"), "example.com/gpu": resource.MustParse(gpu)}
	return &v1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), Labels: map[string]string{"topology.example/domain": name, "kubernetes.io/hostname": name}}, Status: v1.NodeStatus{Capacity: resources, Allocatable: resources}}
}
func addObject(t *testing.T, r *protocol.Request, o any) {
	t.Helper()
	b, e := json.Marshal(o)
	if e != nil {
		t.Fatal(e)
	}
	r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Raw: b})
}
func testRequest(t *testing.T, p *Profile) protocol.Request {
	t.Helper()
	source := testPod("source", "source")
	r := protocol.Request{SchemaVersion: "v1", RequestID: "request", SnapshotID: "snapshot", SnapshotTime: metav1.Now(), Profile: p.Identity, SourcePods: []v1.Pod{source}, ReplacementPods: []v1.Pod{testPod("replacement", "")}, ExcludedNodes: []string{"source"}}
	addObject(t, &r, testNode("source", "1"))
	addObject(t, &r, testNode("destination", "1"))
	addObject(t, &r, &v1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "test"}})
	addObject(t, &r, source)
	return r
}
func evaluateTest(t *testing.T, p *Profile, r protocol.Request) protocol.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, e := p.Evaluate(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	return got
}

// Omitting occupied Pods from the private cache would incorrectly accept used destinations.
func TestEvaluateSourceOccupancy(t *testing.T) {
	p := testProfile(t, defaultConfig)
	for _, occupied := range []bool{false, true} {
		t.Run(map[bool]string{false: "free", true: "occupied"}[occupied], func(t *testing.T) {
			r := testRequest(t, p)
			if occupied {
				addObject(t, &r, testPod("other", "destination"))
			}
			got := evaluateTest(t, p, r)
			if occupied {
				if got.Decision == protocol.DecisionFeasible {
					t.Fatalf("occupied destination accepted: %+v", got)
				}
			} else if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 1 || got.Placements[0].NodeName != "destination" {
				t.Fatalf("free destination rejected: %+v", got)
			}
		})
	}
}

// Migration excludes only from_node. Another source node remains a possible
// destination, but its original Pod must still consume scheduler capacity.
func TestMigrationKeepsOtherSourceSchedulableAndOccupied(t *testing.T) {
	p := testProfile(t, defaultConfig)
	for _, tc := range []struct {
		name, gpus string
		feasible   bool
	}{{"room beside occupied source", "3", true}, {"occupied source consumes capacity", "2", false}} {
		t.Run(tc.name, func(t *testing.T) {
			r := testRequest(t, p)
			r.MigrationFromNode = "source"
			replaceObject(t, &r, 1, testNode("destination", tc.gpus))
			second := testPod("second-source", "destination")
			r.SourcePods = append(r.SourcePods, second)
			addObject(t, &r, second)
			r.ReplacementPods = append(r.ReplacementPods, testPod("replacement-two", ""))
			for i := range r.ReplacementPods {
				r.ReplacementPods[i].Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{Key: v1.LabelHostname, Operator: v1.NodeSelectorOpNotIn, Values: []string{"source"}}}}}}}}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			got, err := p.Evaluate(ctx, r)
			if err != nil {
				t.Fatal(err)
			}
			if (got.Decision == protocol.DecisionFeasible) != tc.feasible {
				t.Fatalf("got %+v, feasible want %v", got, tc.feasible)
			}
			if tc.feasible {
				if len(got.Placements) != 2 {
					t.Fatalf("partial placement: %+v", got)
				}
				for _, placement := range got.Placements {
					if placement.NodeName != "destination" {
						t.Fatalf("unexpected placement: %+v", placement)
					}
				}
			} else if len(got.Placements) != 0 {
				t.Fatalf("partial placement escaped: %+v", got)
			}
		})
	}
}

func TestEvaluateOMEGang(t *testing.T) {
	p := testProfile(t, omeConfig)
	r := gangRequest(t, p)
	got := evaluateTest(t, p, r)
	if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 2 {
		t.Fatalf("whole gang did not complete: %+v", got)
	}
}

func gangRequest(t *testing.T, p *Profile) protocol.Request {
	r := testRequest(t, p)
	second := testNode("destination-two", "1")
	second.Labels["topology.example/domain"] = "destination"
	addObject(t, &r, second)
	r.RequireGang = true
	r.ReplacementPods = append(r.ReplacementPods, testPod("replacement-two", ""))
	for i := range r.ReplacementPods {
		r.ReplacementPods[i].Labels = map[string]string{"scheduling.x-k8s.io/pod-group": "replacement-group"}
	}
	addObject(t, &r, map[string]any{"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroup", "metadata": map[string]any{"name": "replacement-group", "namespace": "test", "uid": "replacement-group-uid"}, "spec": map[string]any{"minMember": 2, "scheduleTimeoutSeconds": 4}})
	return r
}

func TestOMEGangRetriesAnotherDomain(t *testing.T) {
	p := testProfile(t, omeConfig)
	r := gangRequest(t, p)
	for _, nodeName := range []string{"destination", "destination-two"} {
		other := testPod("port-"+nodeName, nodeName)
		other.Spec.Containers[0].Resources = v1.ResourceRequirements{}
		other.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080, Protocol: v1.ProtocolTCP}}
		addObject(t, &r, other)
	}
	for _, name := range []string{"alternative-one", "alternative-two", "alternative-three"} {
		n := testNode(name, "1")
		n.Labels["topology.example/domain"] = "alternative"
		addObject(t, &r, n)
	}
	for i := range r.ReplacementPods {
		r.ReplacementPods[i].Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080, Protocol: v1.ProtocolTCP}}
	}
	got := evaluateTest(t, p, r)
	if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 2 {
		t.Fatalf("gang failed to retry alternative domain: %+v", got)
	}
	for _, placement := range got.Placements {
		if placement.NodeName == "destination" || placement.NodeName == "destination-two" {
			t.Fatalf("port-blocked domain was used: %+v", got)
		}
	}
}

func TestOMEGangRejectsIncompleteOrReusedGroup(t *testing.T) {
	p := testProfile(t, omeConfig)
	for _, which := range []string{"incomplete", "missing", "reused", "mixed", "missing UID", "no gang profile"} {
		t.Run(which, func(t *testing.T) {
			r := gangRequest(t, p)
			profile := p
			switch which {
			case "mixed":
				r.RequireGang = false
				other := testPod("standalone", "")
				r.ReplacementPods = append(r.ReplacementPods, other)
			case "missing UID":
				var group map[string]any
				if err := json.Unmarshal(r.ClusterObjects[len(r.ClusterObjects)-1].Raw, &group); err != nil {
					t.Fatal(err)
				}
				delete(group["metadata"].(map[string]any), "uid")
				replaceObject(t, &r, len(r.ClusterObjects)-1, group)
			case "incomplete":
				r.ReplacementPods = r.ReplacementPods[:1]
			case "missing":
				r.ClusterObjects = r.ClusterObjects[:len(r.ClusterObjects)-1]
			case "reused":
				r.SourcePods[0].Labels = map[string]string{podGroupLabel: "replacement-group"}
				replaceObject(t, &r, 3, r.SourcePods[0])
			case "no gang profile":
				profile = testProfile(t, defaultConfig)
				r.Profile = profile.Identity
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			if result, err := profile.Evaluate(ctx, r); err == nil || result.Decision == protocol.DecisionFeasible {
				t.Fatalf("invalid gang accepted: %+v %v", result, err)
			}
		})
	}
}

func TestOMEGangCancellationDuringPermit(t *testing.T) {
	p := testProfile(t, omeConfig)
	r := gangRequest(t, p)
	r.ReplacementPods[1].Spec.SchedulingGates = []v1.PodSchedulingGate{{Name: "test.example/hold"}}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	got, err := p.Evaluate(ctx, r)
	if err != nil || got.Decision != protocol.DecisionUnsupported || len(got.Placements) != 0 {
		t.Fatalf("partial gang escaped cancellation: %+v %v", got, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Permit was not cancelled promptly")
	}
}

func replaceObject(t *testing.T, r *protocol.Request, index int, o any) {
	t.Helper()
	b, e := json.Marshal(o)
	if e != nil {
		t.Fatal(e)
	}
	r.ClusterObjects[index] = runtime.RawExtension{Raw: b}
}

// These cases catch a cache that retains only GPU occupancy or drops topology metadata.
func TestEvaluateConstraints(t *testing.T) {
	p := testProfile(t, defaultConfig)
	for name, edit := range map[string]func(*protocol.Request){
		"cpu occupancy": func(r *protocol.Request) {
			other := testPod("cpu-user", "destination")
			other.Spec.Containers[0].Resources = v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("8")}}
			addObject(t, r, other)
			r.ReplacementPods[0].Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("1")
		},
		"host port": func(r *protocol.Request) {
			other := testPod("port-user", "destination")
			other.Spec.Containers[0].Resources = v1.ResourceRequirements{}
			other.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080, Protocol: v1.ProtocolTCP}}
			addObject(t, r, other)
			r.ReplacementPods[0].Spec.Containers[0].Ports = other.Spec.Containers[0].Ports
		},
		"node selector": func(r *protocol.Request) {
			r.ReplacementPods[0].Spec.NodeSelector = map[string]string{"missing-label": "value"}
		},
		"taint": func(r *protocol.Request) {
			n := testNode("destination", "1")
			n.Spec.Taints = []v1.Taint{{Key: "dedicated", Value: "other", Effect: v1.TaintEffectNoSchedule}}
			replaceObject(t, r, 1, n)
		},
		"required affinity": func(r *protocol.Request) {
			r.ReplacementPods[0].Spec.Affinity = &v1.Affinity{PodAffinity: &v1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "missing"}}}}}}
		},
		"required anti affinity source": func(r *protocol.Request) {
			r.SourcePods[0].Labels = map[string]string{"app": "source"}
			replaceObject(t, r, 3, r.SourcePods[0])
			n := testNode("destination", "1")
			n.Labels["topology.example/domain"] = "source"
			replaceObject(t, r, 1, n)
			r.ReplacementPods[0].Spec.Affinity = &v1.Affinity{PodAntiAffinity: &v1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{TopologyKey: "topology.example/domain", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "source"}}}}}}
		},
		"excluded with permissive toleration": func(r *protocol.Request) {
			r.ReplacementPods[0].Spec.Containers[0].Resources = v1.ResourceRequirements{}
			r.ReplacementPods[0].Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": "source"}
			r.ReplacementPods[0].Spec.Tolerations = []v1.Toleration{{Operator: v1.TolerationOpExists}}
		},
		"hard topology spread": func(r *protocol.Request) {
			other := testPod("topology-user", "destination")
			other.Spec.Containers[0].Resources = v1.ResourceRequirements{}
			other.Labels = map[string]string{"app": "spread"}
			addObject(t, r, other)
			r.ReplacementPods[0].Labels = map[string]string{"app": "spread"}
			r.ReplacementPods[0].Spec.TopologySpreadConstraints = []v1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "kubernetes.io/hostname", WhenUnsatisfiable: v1.DoNotSchedule, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "spread"}}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := testRequest(t, p)
			edit(&r)
			got := evaluateTest(t, p, r)
			if got.Decision == protocol.DecisionFeasible || len(got.Placements) != 0 {
				t.Fatalf("constraint ignored: %+v", got)
			}
		})
	}
}

func TestEvaluateIndependentRunsAndUIDLessReplacements(t *testing.T) {
	p := testProfile(t, defaultConfig)
	r := testRequest(t, p)
	r.ReplacementPods[0].UID = ""
	r.ReplacementPods[0].Spec.SchedulerName = ""
	for i := 0; i < 2; i++ {
		got := evaluateTest(t, p, r)
		if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 1 || got.Placements[0].Pod.UID != "" {
			t.Fatalf("private state or UID leaked: %+v", got)
		}
	}
}

func TestEvaluateTolerationMemoryAndSourceAffinity(t *testing.T) {
	p := testProfile(t, defaultConfig)
	r := testRequest(t, p)
	n := testNode("destination", "1")
	n.Spec.Taints = []v1.Taint{{Key: "dedicated", Value: "model", Effect: v1.TaintEffectNoSchedule}}
	n.Labels["topology.example/domain"] = "source"
	replaceObject(t, &r, 1, n)
	r.SourcePods[0].Labels = map[string]string{"app": "peer"}
	replaceObject(t, &r, 3, r.SourcePods[0])
	pod := &r.ReplacementPods[0]
	pod.Spec.Tolerations = []v1.Toleration{{Key: "dedicated", Operator: v1.TolerationOpEqual, Value: "model", Effect: v1.TaintEffectNoSchedule}}
	pod.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("2Gi")
	pod.Spec.Affinity = &v1.Affinity{PodAffinity: &v1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{TopologyKey: "topology.example/domain", Namespaces: []string{"test"}, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "peer"}}}}}}
	got := evaluateTest(t, p, r)
	if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 1 || got.Placements[0].NodeName != "destination" {
		t.Fatalf("valid constraints rejected or source topology lost: %+v", got)
	}
}

func TestDefaultTopologySpreadUsesSnapshotService(t *testing.T) {
	p := testProfile(t, defaultConfig+"  pluginConfig:\n  - name: PodTopologySpread\n    args:\n      defaultingType: List\n      defaultConstraints:\n      - maxSkew: 1\n        topologyKey: kubernetes.io/hostname\n        whenUnsatisfiable: DoNotSchedule\n")
	r := testRequest(t, p)
	r.ReplacementPods[0].Labels = map[string]string{"app": "spread"}
	other := testPod("spread-occupant", "destination")
	other.Spec.Containers[0].Resources = v1.ResourceRequirements{}
	other.Labels = map[string]string{"app": "spread"}
	addObject(t, &r, other)
	addObject(t, &r, &v1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "spread", Namespace: "test"}, Spec: v1.ServiceSpec{Selector: map[string]string{"app": "spread"}, Ports: []v1.ServicePort{{Port: 80}}}})
	got := evaluateTest(t, p, r)
	if got.Decision == protocol.DecisionFeasible {
		t.Fatalf("hard default spread ignored supplied Service selector: %+v", got)
	}
}

func TestOMEGangNoFitNeverReturnsPartialPlacement(t *testing.T) {
	p := testProfile(t, omeConfig)
	r := gangRequest(t, p)
	addObject(t, &r, testPod("occupied", "destination-two"))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	got, err := p.Evaluate(ctx, r)
	if err != nil || got.Decision != protocol.DecisionUnsupported || len(got.Placements) != 0 {
		t.Fatalf("incomplete gang placement escaped: %+v %v", got, err)
	}
}

func TestPrivateClientDeniesSourceWrites(t *testing.T) {
	p := testProfile(t, defaultConfig)
	r := testRequest(t, p)
	snapshot, e := protocol.Validate(r)
	if e != nil {
		t.Fatal(e)
	}
	client, state := newPrivateClient(r, snapshot)
	ctx := context.Background()
	source := r.SourcePods[0].DeepCopy()
	source.Labels = map[string]string{"mutated": "true"}
	if _, e := client.CoreV1().Pods(source.Namespace).Update(ctx, source, metav1.UpdateOptions{}); e == nil {
		t.Fatal("source update allowed")
	}
	if e := client.CoreV1().Pods(source.Namespace).Delete(ctx, source.Name, metav1.DeleteOptions{}); e == nil {
		t.Fatal("source deletion allowed")
	}
	bound, e := client.CoreV1().Pods(source.Namespace).Get(ctx, source.Name, metav1.GetOptions{})
	if e != nil || bound.Spec.NodeName != "source" || bound.Labels["mutated"] != "" {
		t.Fatalf("source mutated: %+v %v", bound, e)
	}
	if state.denied == nil {
		t.Fatal("forbidden writes did not fail simulation")
	}
	if len(client.Actions()) < 3 {
		t.Fatal("private action history missing")
	}
}
