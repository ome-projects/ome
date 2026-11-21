package coordination

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateServiceWrites = flag.Bool("update-service-writes", false, "re-record testdata/per_revision_service_writes.json and testdata/per_revision_service_write_allocations.json from this run")

// serviceWritePairingProtocol is the pairing protocol the revision's
// ControllerRevision carries, so the routing Service has the protocol label.
const serviceWritePairingProtocol = "transfer-v2"

// serviceWriteWorld is one revision of an engine Component as the writer
// finds it at rest: the ControllerRevision with a pairing protocol, and the
// revision's two Services stored as the apiserver holds them after the
// writer's create, with a value in every field the writer owns (a leader
// and ordinal selector, a published port with an app protocol, the protocol
// label, the owner reference, the headless publish flag). The cache
// stand-in copies on Get and counts reads and writes.
type serviceWriteWorld struct {
	isvc      *v1beta1.InferenceService
	component v1beta1.ComponentType
	hash      string
	routing   RevisionRoutingSelector
	ports     []corev1.ContainerPort
	protocols map[string]string
	cache     *passCache
	pair      perRevisionPair
}

func newServiceWriteWorld(tb testing.TB) *serviceWriteWorld {
	tb.Helper()
	w := &serviceWriteWorld{
		isvc: testISVC(), component: v1beta1.EngineComponent, hash: "hash1",
		routing: RevisionRoutingSelector{LeaderOnly: true, PodOrdinal: true},
		ports:   runnerPorts(), protocols: map[string]string{"http": declaredH2C},
		cache: newPassCache(),
	}
	w.cache.put(pairingCR(w.isvc.Namespace, w.isvc.Name, w.component, w.hash, serviceWritePairingProtocol))
	pair, err := resolvePerRevisionPair(context.Background(), w.cache, w.isvc, w.component, w.hash, w.routing, w.ports, w.protocols)
	if err != nil {
		tb.Fatalf("resolvePerRevisionPair: %v", err)
	}
	w.pair = pair
	w.cache.put(storedByTheAPIServer(w.rendered(tb, pair.routing), "10.0.0.5"))
	w.cache.put(storedByTheAPIServer(w.rendered(tb, pair.headless), corev1.ClusterIPNone))
	return w
}

// rendered is a Service as the writer renders it from its inputs.
func (w *serviceWriteWorld) rendered(tb testing.TB, write perRevisionServiceWrite) *corev1.Service {
	tb.Helper()
	svc, err := write.desired()
	if err != nil {
		tb.Fatalf("render %s: %v", write.name, err)
	}
	return svc
}

// storedByTheAPIServer is a Service as the apiserver holds it after the
// writer's create: the identity, the timestamp and the defaults it fills in
// beside what the writer sent.
func storedByTheAPIServer(svc *corev1.Service, clusterIP string) *corev1.Service {
	stored := svc.DeepCopy()
	stored.UID = types.UID(svc.Name + "-uid")
	stored.ResourceVersion = "7"
	stored.CreationTimestamp = metav1.NewTime(time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC))
	stored.Spec.ClusterIP = clusterIP
	stored.Spec.ClusterIPs = []string{clusterIP}
	stored.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	stored.Spec.IPFamilyPolicy = ptr.To(corev1.IPFamilyPolicySingleStack)
	stored.Spec.SessionAffinity = corev1.ServiceAffinityNone
	stored.Spec.InternalTrafficPolicy = ptr.To(corev1.ServiceInternalTrafficPolicyCluster)
	return stored
}

// pass runs the writer once over the revision's pair through c and returns
// the cache reads and writes of that pass alone.
func (w *serviceWriteWorld) pass(tb testing.TB, c client.Client) (reads, writes int64) {
	tb.Helper()
	w.cache.reads, w.cache.writes = 0, 0
	if _, err := EnsurePerRevisionServices(context.Background(), c, w.isvc, w.component, w.hash, w.routing, w.ports, w.protocols); err != nil {
		tb.Fatalf("EnsurePerRevisionServices: %v", err)
	}
	return w.cache.reads, w.cache.writes
}

// stored reads a Service as the cache holds it.
func (w *serviceWriteWorld) stored(tb testing.TB, name string) *corev1.Service {
	tb.Helper()
	svc := &corev1.Service{}
	if err := w.cache.Get(context.Background(), client.ObjectKey{Namespace: w.isvc.Namespace, Name: name}, svc); err != nil {
		tb.Fatalf("stored Service %s: %v", name, err)
	}
	return svc
}

// change rewrites a stored Service, as another writer or the apiserver
// would between two passes; a nil change removes it.
func (w *serviceWriteWorld) change(tb testing.TB, name string, mutate func(*corev1.Service)) {
	tb.Helper()
	if mutate == nil {
		delete(w.cache.objects, passStoreKeyOf(&corev1.Service{}, w.isvc.Namespace, name))
		return
	}
	svc := w.stored(tb, name)
	mutate(svc)
	w.cache.put(svc)
}

// withOwnedFieldsOf is the Service as a write leaves it: everything as
// found, the five fields the writer owns as rendered.
func withOwnedFieldsOf(found, rendered *corev1.Service) *corev1.Service {
	out := found.DeepCopy()
	out.Labels = rendered.DeepCopy().Labels
	out.OwnerReferences = rendered.DeepCopy().OwnerReferences
	out.Spec.Selector = rendered.DeepCopy().Spec.Selector
	out.Spec.Ports = rendered.DeepCopy().Spec.Ports
	out.Spec.PublishNotReadyAddresses = rendered.Spec.PublishNotReadyAddresses
	return out
}

// asCreated is the Service as the writer's create leaves it in the cache:
// the name, the type and the headless cluster IP set at create, the owned
// fields, and the resource version the stand-in gives a new object.
func asCreated(rendered *corev1.Service) *corev1.Service {
	shell := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: rendered.Name, Namespace: rendered.Namespace, ResourceVersion: "1"},
		Spec:       corev1.ServiceSpec{Type: rendered.Spec.Type, ClusterIP: rendered.Spec.ClusterIP},
	}
	return withOwnedFieldsOf(shell, rendered)
}

// recordedWrite is one object the writer handed to Create or Update.
type recordedWrite struct {
	Op     string          `json:"op"`
	Object *corev1.Service `json:"object"`
}

// writeRecorder keeps every object the writer hands to Create and Update,
// in order, so a golden holds the writes byte for byte.
type writeRecorder struct {
	*passCache
	written []recordedWrite
}

func (r *writeRecorder) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	r.written = append(r.written, recordedWrite{Op: "create", Object: obj.(*corev1.Service).DeepCopy()})
	return r.passCache.Create(ctx, obj, opts...)
}

func (r *writeRecorder) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	r.written = append(r.written, recordedWrite{Op: "update", Object: obj.(*corev1.Service).DeepCopy()})
	return r.passCache.Update(ctx, obj, opts...)
}

// A pass over a pair stored as the writer rendered it reads each Service
// once, beside the revision's ControllerRevision, and writes nothing.
func TestPerRevisionServiceWriter_LeavesAMatchingPairAlone(t *testing.T) {
	w := newServiceWriteWorld(t)
	routing, headless := w.stored(t, w.pair.routing.name), w.stored(t, w.pair.headless.name)
	reads, writes := w.pass(t, w.cache)
	if writes != 0 {
		t.Errorf("a pass over a matching pair wrote %d objects, want none", writes)
	}
	if reads != 3 {
		t.Errorf("a pass over a matching pair read %d objects, want 3: the ControllerRevision and each Service once", reads)
	}
	for name, before := range map[string]*corev1.Service{w.pair.routing.name: routing, w.pair.headless.name: headless} {
		if after := w.stored(t, name); !apiequality.Semantic.DeepEqual(before, after) {
			t.Errorf("%s changed under a pass that had nothing to write", name)
		}
	}
}

// serviceWriteRow is one stored Service changed between two passes; a nil
// change removes it. The side names the Service, "both" the pair.
type serviceWriteRow struct {
	name   string
	side   string
	change func(*corev1.Service)
}

// Every field the writer owns, changed on the stored Service, is written
// back as rendered by the next pass: the labels (the whole map, so a label
// another owner adds is removed), the owner reference, the selector, the
// ports with every field of a port, and publishNotReadyAddresses. A missing
// Service is created with the type and, for the headless one, the cluster
// IP set at create. A write carries the object as read with those fields
// replaced, and the other Service of the pair is not written. The objects
// handed to Create and Update are held byte for byte in
// testdata/per_revision_service_writes.json.
func TestPerRevisionServiceWriter_RewritesEachOwnedFieldThatDrifted(t *testing.T) {
	rows := []serviceWriteRow{
		{name: "a label's value changed", side: "routing", change: func(s *corev1.Service) { s.Labels[constants.OMEComponentLabel] = "decoder" }},
		{name: "the pairing protocol label removed", side: "routing", change: func(s *corev1.Service) { delete(s.Labels, query.LabelPairingProtocol) }},
		{name: "a label added by another owner", side: "routing", change: func(s *corev1.Service) { s.Labels["example.com/team"] = "a" }},
		{name: "the owner reference removed", side: "routing", change: func(s *corev1.Service) { s.OwnerReferences = nil }},
		{name: "the owner reference's controller flag cleared", side: "routing", change: func(s *corev1.Service) { s.OwnerReferences[0].Controller = nil }},
		{name: "the selector widened past the leader", side: "routing", change: func(s *corev1.Service) {
			delete(s.Spec.Selector, query.LabelRunner)
			delete(s.Spec.Selector, query.LabelPodOrdinal)
		}},
		{name: "the selector's revision changed", side: "routing", change: func(s *corev1.Service) { s.Spec.Selector[query.LabelRevisionHash] = "other" }},
		{name: "the port number changed", side: "routing", change: func(s *corev1.Service) { s.Spec.Ports[0].Port = 9000 }},
		{name: "the target port changed to a number", side: "routing", change: func(s *corev1.Service) { s.Spec.Ports[0].TargetPort = intstr.FromInt32(8000) }},
		{name: "the port's protocol changed", side: "routing", change: func(s *corev1.Service) { s.Spec.Ports[0].Protocol = corev1.ProtocolUDP }},
		{name: "the port's app protocol removed", side: "routing", change: func(s *corev1.Service) { s.Spec.Ports[0].AppProtocol = nil }},
		{name: "the port's name changed", side: "routing", change: func(s *corev1.Service) { s.Spec.Ports[0].Name = "web" }},
		{name: "a second port added", side: "routing", change: func(s *corev1.Service) {
			s.Spec.Ports = append(s.Spec.Ports, corev1.ServicePort{Name: "dist", Port: 5000, TargetPort: intstr.FromString("dist"), Protocol: corev1.ProtocolTCP})
		}},
		{name: "the ports emptied", side: "routing", change: func(s *corev1.Service) { s.Spec.Ports = nil }},
		{name: "publishNotReadyAddresses set", side: "routing", change: func(s *corev1.Service) { s.Spec.PublishNotReadyAddresses = true }},
		{name: "missing", side: "routing"},
		{name: "publishNotReadyAddresses cleared", side: "headless", change: func(s *corev1.Service) { s.Spec.PublishNotReadyAddresses = false }},
		{name: "the selector narrowed to the leader", side: "headless", change: func(s *corev1.Service) { s.Spec.Selector[query.LabelRunner] = string(v1beta1.RunnerNameLeader) }},
		{name: "a label removed", side: "headless", change: func(s *corev1.Service) { delete(s.Labels, query.LabelManagedBy) }},
		{name: "missing", side: "headless"},
		{name: "missing", side: "both"},
	}
	writes := map[string][]recordedWrite{}
	for _, row := range rows {
		t.Run(row.side+"/"+row.name, func(t *testing.T) {
			writes[row.side+"/"+row.name] = checkServiceWriteRow(t, row)
		})
	}
	checkServiceWritesGolden(t, writes)
}

// sides are the two Services of the revision's pair, routing first.
func (w *serviceWriteWorld) sides() []perRevisionServiceWrite {
	return []perRevisionServiceWrite{w.pair.routing, w.pair.headless}
}

// sideName names a Service of the pair as the rows do.
func sideName(write perRevisionServiceWrite) string {
	if write.headless {
		return "headless"
	}
	return "routing"
}

// applies reports whether the row changes the given Service.
func (row serviceWriteRow) applies(write perRevisionServiceWrite) bool {
	return row.side == "both" || row.side == sideName(write)
}

// wantAfter is a Service as the pass leaves it under the row: as found when
// the row did not change it, created as rendered when the row removed it,
// else as found with the owned fields as rendered.
func (row serviceWriteRow) wantAfter(write perRevisionServiceWrite, found, rendered *corev1.Service) *corev1.Service {
	switch {
	case !row.applies(write):
		return found
	case row.change == nil:
		return asCreated(rendered)
	default:
		return withOwnedFieldsOf(found, rendered)
	}
}

// checkServiceWriteRow applies one row to a fresh world, runs a pass
// through a recorder, and holds that the pass wrote once per changed
// Service and left each Service as the row's rule says. It returns the
// recorded writes for the golden.
func checkServiceWriteRow(t *testing.T, row serviceWriteRow) []recordedWrite {
	t.Helper()
	w := newServiceWriteWorld(t)
	changed, found := 0, map[string]*corev1.Service{}
	for _, write := range w.sides() {
		if row.applies(write) {
			w.change(t, write.name, row.change)
			changed++
		}
		if row.change != nil || !row.applies(write) {
			found[write.name] = w.stored(t, write.name)
		}
	}
	recorder := &writeRecorder{passCache: w.cache}
	if _, got := w.pass(t, recorder); got != int64(changed) {
		t.Errorf("the pass wrote %d objects, want %d: one per changed Service", got, changed)
	}
	for _, write := range w.sides() {
		want := row.wantAfter(write, found[write.name], w.rendered(t, write))
		if after := w.stored(t, write.name); !apiequality.Semantic.DeepEqual(want, after) {
			t.Errorf("%s after the pass:\n got %s\nwant %s", write.name, serviceJSON(t, after), serviceJSON(t, want))
		}
	}
	return recorder.written
}

// A field the writer does not own, changed on the stored Service, causes
// no write and stays: another owner's annotation or finalizer, the cluster
// IP and the type (set at create only), the apiserver's defaults, the
// status and the resource version.
func TestPerRevisionServiceWriter_WritesNothingForWhatItDoesNotOwn(t *testing.T) {
	rows := []serviceWriteRow{
		{name: "an annotation from another owner", side: "routing", change: func(s *corev1.Service) { s.Annotations = map[string]string{"example.com/note": "kept"} }},
		{name: "a finalizer from another owner", side: "routing", change: func(s *corev1.Service) { s.Finalizers = []string{"example.com/hold"} }},
		{name: "the cluster IP the apiserver assigned", side: "routing", change: func(s *corev1.Service) {
			s.Spec.ClusterIP, s.Spec.ClusterIPs = "10.0.0.9", []string{"10.0.0.9"}
		}},
		{name: "the type after the create", side: "routing", change: func(s *corev1.Service) { s.Spec.Type = corev1.ServiceTypeNodePort }},
		{name: "the session affinity", side: "routing", change: func(s *corev1.Service) { s.Spec.SessionAffinity = corev1.ServiceAffinityClientIP }},
		{name: "the IP families", side: "routing", change: func(s *corev1.Service) {
			s.Spec.IPFamilies = append(s.Spec.IPFamilies, corev1.IPv6Protocol)
			s.Spec.IPFamilyPolicy = ptr.To(corev1.IPFamilyPolicyPreferDualStack)
		}},
		{name: "the internal traffic policy", side: "routing", change: func(s *corev1.Service) {
			s.Spec.InternalTrafficPolicy = ptr.To(corev1.ServiceInternalTrafficPolicyLocal)
		}},
		{name: "the status", side: "routing", change: func(s *corev1.Service) {
			s.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.7"}}
		}},
		{name: "the resource version", side: "routing", change: func(s *corev1.Service) { s.ResourceVersion = "8" }},
		{name: "an annotation from another owner", side: "headless", change: func(s *corev1.Service) { s.Annotations = map[string]string{"example.com/note": "kept"} }},
		{name: "a finalizer from another owner", side: "headless", change: func(s *corev1.Service) { s.Finalizers = []string{"example.com/hold"} }},
		{name: "the status", side: "headless", change: func(s *corev1.Service) {
			s.Status.Conditions = []metav1.Condition{{Type: "example.com/Checked", Status: metav1.ConditionTrue, Reason: "Checked", LastTransitionTime: metav1.NewTime(time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC))}}
		}},
	}
	for _, row := range rows {
		t.Run(row.side+"/"+row.name, func(t *testing.T) {
			w := newServiceWriteWorld(t)
			name := w.pair.routing.name
			if row.side == "headless" {
				name = w.pair.headless.name
			}
			w.change(t, name, row.change)
			before := w.stored(t, name)
			reads, writes := w.pass(t, w.cache)
			if writes != 0 {
				t.Errorf("the pass wrote %d objects over a field the writer does not own, want none", writes)
			}
			if reads != 3 {
				t.Errorf("the pass read %d objects, want 3", reads)
			}
			if after := w.stored(t, name); !apiequality.Semantic.DeepEqual(before, after) {
				t.Errorf("%s changed:\n got %s\nwant %s", name, serviceJSON(t, after), serviceJSON(t, before))
			}
		})
	}
}

func serviceJSON(tb testing.TB, svc *corev1.Service) string {
	tb.Helper()
	data, err := json.Marshal(svc)
	if err != nil {
		tb.Fatalf("marshal Service: %v", err)
	}
	return string(data)
}

// serviceWritesJSON renders the writes as a JSON object with one row per
// line, in row order, so a difference reads as the row it is in.
func serviceWritesJSON(t *testing.T, writes map[string][]recordedWrite) []byte {
	t.Helper()
	var out bytes.Buffer
	out.WriteString("{\n")
	for i, row := range slices.Sorted(maps.Keys(writes)) {
		line, err := json.Marshal(writes[row])
		if err != nil {
			t.Fatalf("marshal the writes of %s: %v", row, err)
		}
		if i > 0 {
			out.WriteString(",\n")
		}
		fmt.Fprintf(&out, "  %q: %s", row, line)
	}
	out.WriteString("\n}\n")
	return out.Bytes()
}

// checkServiceWritesGolden holds the objects the writer handed to Create
// and Update byte for byte, per row, or re-records them.
func checkServiceWritesGolden(t *testing.T, writes map[string][]recordedWrite) {
	t.Helper()
	path := filepath.Join("testdata", "per_revision_service_writes.json")
	got := serviceWritesJSON(t, writes)
	if *updateServiceWrites {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v (rerun with -update-service-writes to record it)", path, err)
	}
	if bytes.Equal(want, got) {
		return
	}
	wantLines, gotLines := bytes.Split(want, []byte("\n")), bytes.Split(got, []byte("\n"))
	for i := range max(len(wantLines), len(gotLines)) {
		var wantLine, gotLine []byte
		if i < len(wantLines) {
			wantLine = wantLines[i]
		}
		if i < len(gotLines) {
			gotLine = gotLines[i]
		}
		if !bytes.Equal(wantLine, gotLine) {
			t.Fatalf("the writes differ from %s at line %d:\n golden: %s\n    run: %s\nIf the writer is meant to write this, rerun with -update-service-writes and name the reason in the commit.", path, i+1, wantLine, gotLine)
		}
	}
}

// serviceWriteBudgetCases are the writes a pass at rest makes for one
// revision: each Service alone and the pair through EnsurePerRevisionServices,
// which reads the revision's ControllerRevision as well.
func serviceWriteBudgetCases(tb testing.TB, w *serviceWriteWorld) []struct {
	name string
	op   func()
} {
	tb.Helper()
	ctx := context.Background()
	single := func(write perRevisionServiceWrite) func() {
		return func() {
			if err := ensureService(ctx, w.cache, write); err != nil {
				tb.Fatalf("ensureService %s: %v", write.name, err)
			}
		}
	}
	return []struct {
		name string
		op   func()
	}{
		{name: "routing", op: single(w.pair.routing)},
		{name: "headless", op: single(w.pair.headless)},
		{name: "pair", op: func() {
			if _, err := EnsurePerRevisionServices(ctx, w.cache, w.isvc, w.component, w.hash, w.routing, w.ports, w.protocols); err != nil {
				tb.Fatalf("EnsurePerRevisionServices: %v", err)
			}
		}},
	}
}

// TestPerRevisionServiceWriteStaysWithinBudget holds the allocations and
// bytes of one write pass at rest over each Service of a revision and over
// the pair. A pass at rest that writes anything fails outright.
func TestPerRevisionServiceWriteStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "per_revision_service_write_allocations.json"), *updateServiceWrites)
	defer golden.Close()
	w := newServiceWriteWorld(t)
	w.cache.writes = 0
	for _, c := range serviceWriteBudgetCases(t, w) {
		golden.Check(c.name+"/at-rest/allocations", c.op)
		golden.CheckBytes(c.name+"/at-rest/bytes", c.op)
	}
	if w.cache.writes != 0 {
		t.Fatalf("a pass at rest wrote %d objects", w.cache.writes)
	}
}

// BenchmarkPerRevisionServiceWrite is the wall time and the allocations of
// one write pass at rest over each Service of a revision and over the pair.
func BenchmarkPerRevisionServiceWrite(b *testing.B) {
	w := newServiceWriteWorld(b)
	for _, c := range serviceWriteBudgetCases(b, w) {
		b.Run(c.name, func(b *testing.B) {
			w.cache.writes = 0
			b.ReportAllocs()
			for b.Loop() {
				c.op()
			}
			if w.cache.writes != 0 {
				b.Fatalf("a pass at rest wrote %d objects", w.cache.writes)
			}
		})
	}
}
