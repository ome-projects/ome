package inferencereplica

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// promotionInstances is how many Instances one pass promotes to Ready: a
// quarter of the largest reference replica, every Instance's pod serving
// before the pass.
const promotionInstances = 1000

// statusWrite is one status write of the replica as the manager sent it:
// the status as the apiserver stores it, and its bytes on the wire.
type statusWrite struct {
	status v1beta1.InferenceReplicaStatus
	bytes  int
}

// statusWriteLog keeps every status write of the replica in order, each
// through the JSON the apiserver receives, so a time is read at the second
// precision it is stored with.
type statusWriteLog struct {
	t      *testing.T
	writes []statusWrite
}

func (l *statusWriteLog) hooks() interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, next client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if ir, ok := obj.(*v1beta1.InferenceReplica); ok && name == "status" {
				l.record(ir)
			}
			return next.SubResource(name).Update(ctx, obj, opts...)
		},
	}
}

func (l *statusWriteLog) record(ir *v1beta1.InferenceReplica) {
	l.t.Helper()
	raw, err := json.Marshal(ir.Status)
	if err != nil {
		l.t.Fatalf("marshal the written status: %v", err)
	}
	var stored v1beta1.InferenceReplicaStatus
	if err := json.Unmarshal(raw, &stored); err != nil {
		l.t.Fatalf("unmarshal the written status: %v", err)
	}
	l.writes = append(l.writes, statusWrite{status: stored, bytes: len(raw)})
}

// promotionFixture is one replica behind the fake apiserver whose Instances
// are all Creating with their pods serving, and the reconciler that
// promotes them, wired to the real clock as the manager wires it and to the
// form the served schema stores each readySince in.
type promotionFixture struct {
	reconciler *Reconciler
	apiserver  client.Client
	key        client.ObjectKey
	component  workloadtypes.ComponentType
	target     *appsv1.ControllerRevision
	writes     *statusWriteLog
}

func newPromotionFixture(t *testing.T, form irstatus.ReadySinceForm) *promotionFixture {
	t.Helper()
	shape := statusPublicationShape{name: "promotion", instances: promotionInstances, pods: 1}
	ir := baselineIR("llama-engine", "default", int32(promotionInstances))
	rows := irstatustest.UniformRows(promotionInstances)
	for i := range rows {
		rows[i].Phase = v1beta1.OMENativeInstanceCreating
	}
	ir.Status = *irstatustest.LogicalStatus(rows)
	ir.Status.ObservedGeneration = ir.Generation
	ir.Status.CurrentRevision, ir.Status.UpdateRevision = irstatustest.Revision, irstatustest.Revision
	selection, err := irstatus.SelectCandidate(ir.Status.DeepCopy(), publicationColumnBound)
	if err != nil {
		t.Fatalf("SelectCandidate: %v", err)
	}
	stored := ir.DeepCopy()
	stored.Status = *selection.SelectedCandidate().Status

	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	pods := statusPublicationPods(ir, shape)
	objects := []client.Object{stored}
	nodes := map[string]bool{}
	for _, pod := range pods {
		objects = append(objects, pod)
		if name := pod.Spec.NodeName; name != "" && !nodes[name] {
			nodes[name] = true
			objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
		}
	}
	objects = append(objects, publicationEndpointSlices(ir, component, pods)...)
	writes := &statusWriteLog{t: t}
	apiserver := fakeclient.New(t,
		fakeclient.WithObjects(objects...),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}),
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor),
		fakeclient.WithInterceptors(writes.hooks()))
	reconciler := &Reconciler{
		Client:                apiserver,
		APIReader:             apiserver,
		Log:                   logf.Log.WithName("promotion"),
		Recorder:              &record.FakeRecorder{},
		Expectations:          workloadtypes.NewExpectations(),
		InstanceStatusDecoder: irstatus.NewDecoder(publicationColumnBound),
		InstanceStatusTarget:  irstatus.EncodingColumnarV2,
		ReadySinceForm:        form,
		Clock:                 clock.RealClock{},
	}
	return &promotionFixture{
		reconciler: reconciler,
		apiserver:  apiserver,
		key:        client.ObjectKeyFromObject(stored),
		component:  component,
		target:     &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: irstatustest.Revision, Namespace: ir.Namespace}},
		writes:     writes,
	}
}

// pass runs one pass over the replica as the manager runs it, the create
// pass and then the status publication, and returns the status writes the
// pass made. The create pass promotes every Creating Instance whose pod
// serves and commits the promotions in one write.
func (f *promotionFixture) pass(t *testing.T) []statusWrite {
	t.Helper()
	ctx := context.Background()
	before := len(f.writes.writes)
	passCopy := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(ctx, irstatus.NewReader(f.apiserver, f.reconciler.InstanceStatusDecoder), f.key, passCopy); err != nil {
		t.Fatalf("decode the pass's copy: %v", err)
	}
	input := f.reconciler.buildReconcileInput(ctx, passCopy, passCopy.Spec.Runners, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	plan, err := workload.BuildPlan(f.component, input.DesiredSpec, input.ObservedState)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	input.ObservedState.IndexRows()
	input.Pacing = &workloadtypes.APIPacing{}
	input.PromoteWindow = &workloadtypes.PromoteWindow{}
	input.PassWake = &workloadtypes.PassWake{}
	input.Swept = &workloadtypes.SweptPods{}
	input.Held = &workloadtypes.HeldInstances{}
	deps := workloadtypes.Deps{Client: f.apiserver, APIReader: f.apiserver, Recorder: f.reconciler.Recorder, Expectations: f.reconciler.Expectations, Clock: f.reconciler.Clock}
	if _, err := workloadops.Create(ctx, deps, input, plan, f.target); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pass := &replicaPass{
		reconciler:           f.reconciler,
		ir:                   passCopy,
		log:                  f.reconciler.Log,
		priorCurrentRevision: passCopy.Status.CurrentRevision,
		specTarget:           f.target,
		plan:                 publicationPlan(f.component, promotionInstances),
		instancesAtStart:     instanceIndices(passCopy.Status.InstanceStatuses),
	}
	if _, err := pass.publishStatus(ctx, ctrl.Result{}, nil); err != nil {
		t.Fatalf("publishStatus: %v", err)
	}
	return f.writes.writes[before:]
}

// atRest is the bytes of the status the writer selects for the rows of a
// written status once they are read back from the store: the form the
// status takes at rest.
func (f *promotionFixture) atRest(t *testing.T, written v1beta1.InferenceReplicaStatus) int {
	t.Helper()
	ir := &v1beta1.InferenceReplica{Status: written}
	if _, err := f.reconciler.InstanceStatusDecoder.Decode(ir); err != nil {
		t.Fatalf("decode the written status: %v", err)
	}
	selection, err := irstatus.SelectCandidateStoring(&ir.Status, publicationColumnBound, f.reconciler.ReadySinceForm)
	if err != nil {
		t.Fatalf("SelectCandidate: %v", err)
	}
	raw, err := json.Marshal(selection.SelectedCandidate().Status)
	if err != nil {
		t.Fatalf("marshal the selected status: %v", err)
	}
	return len(raw)
}

// describeStatusWrite names the form of a written status: the encoding, the
// rows of the plain list, and the entries of the column form with how many
// carry the time an Instance became Ready.
func describeStatusWrite(status *v1beta1.InferenceReplicaStatus) string {
	if status.InstanceStatusColumns == nil {
		return fmt.Sprintf("plain list of %d rows", len(status.InstanceStatuses))
	}
	timed := 0
	for _, entry := range status.InstanceStatusColumns.Entries {
		if entry.ReadySince != nil {
			timed++
		}
	}
	return fmt.Sprintf("column form with %d entries, %d of them carrying a readySince", len(status.InstanceStatusColumns.Entries), timed)
}

// TestPromotingWriteIsTheStatusAtRest: a pass that promotes 1,000 Instances
// to Ready under the real clock writes their rows in the form the writer
// selects for the same rows at rest, so the write that records the
// promotion costs what the status costs afterwards, and a second pass over
// the promoted replica writes nothing.
func TestPromotingWriteIsTheStatusAtRest(t *testing.T) {
	f := newPromotionFixture(t, irstatus.ReadySinceInColumns)
	writes := f.pass(t)
	if len(writes) == 0 {
		t.Fatal("the pass that promotes the Instances wrote no status")
	}
	for i, write := range writes {
		atRest := f.atRest(t, write.status)
		t.Logf("status write %d of the promoting pass: %d B on the wire, %s; the same rows at rest select %d B", i+1, write.bytes, describeStatusWrite(&write.status), atRest)
		if write.bytes != atRest {
			t.Errorf("status write %d of the promoting pass is %d B where the same rows at rest select %d B: the promotion is written in a form its next write replaces", i+1, write.bytes, atRest)
		}
	}
	if again := f.pass(t); len(again) != 0 {
		t.Errorf("a second pass over the promoted replica wrote the status %d times; it is at rest", len(again))
	}
}

// TestPromotingWriteWithoutTheReadySinceColumns: under a served schema that
// lacks the readySince columns the same promoting pass writes the time each
// Instance became Ready in its own entry and no readySince column, still in
// the form the writer selects at rest, every time reads back, and a second
// pass writes nothing. A manager that stores the columns then leaves the
// replica alone until it has something else to write: the form of the
// stored times never causes a write by itself.
func TestPromotingWriteWithoutTheReadySinceColumns(t *testing.T) {
	f := newPromotionFixture(t, irstatus.ReadySinceInEntries)
	writes := f.pass(t)
	if len(writes) == 0 {
		t.Fatal("the pass that promotes the Instances wrote no status")
	}
	for i, write := range writes {
		columns := write.status.InstanceStatusColumns
		if columns == nil || columns.ReadySinceBase != nil || columns.ReadySinceIndexes != nil || columns.ReadySinceOffsets != nil {
			t.Fatalf("status write %d stores a readySince column under a schema that lacks them: %s", i+1, describeStatusWrite(&write.status))
		}
		timed := 0
		for _, entry := range columns.Entries {
			if entry.ReadySince != nil {
				timed++
			}
		}
		atRest := f.atRest(t, write.status)
		t.Logf("status write %d of the promoting pass without the columns: %d B on the wire, %s; the same rows at rest select %d B", i+1, write.bytes, describeStatusWrite(&write.status), atRest)
		if timed != promotionInstances {
			t.Errorf("status write %d carries %d entries with a readySince, want one per promoted Instance (%d)", i+1, timed, promotionInstances)
		}
		if write.bytes != atRest {
			t.Errorf("status write %d is %d B where the same rows at rest select %d B", i+1, write.bytes, atRest)
		}
	}
	readBack := &v1beta1.InferenceReplica{Status: writes[len(writes)-1].status}
	if _, err := f.reconciler.InstanceStatusDecoder.Decode(readBack); err != nil {
		t.Fatalf("decode the written status: %v", err)
	}
	for _, row := range readBack.Status.InstanceStatuses {
		if row.ReadySince == nil {
			t.Fatalf("Instance %d reads back without the time it became Ready", row.Index)
		}
	}
	if again := f.pass(t); len(again) != 0 {
		t.Errorf("a second pass over the promoted replica wrote the status %d times; it is at rest", len(again))
	}
	f.reconciler.ReadySinceForm = irstatus.ReadySinceInColumns
	if again := f.pass(t); len(again) != 0 {
		t.Errorf("a manager that stores the columns rewrote a replica at rest %d times for the form of its times alone", len(again))
	}
}
