// End-to-end tests for the status.migrations-driven Migrate executor:
// the record is the single source of truth (dispatcher selects it, the
// executor resumes from SurgeInstance + Phase and advances the phase
// through MutateMigration). Single-pod and gang walks share one harness.
//
// The harness simulates the environment between Migrate passes:
// "kubelet" flips ContainersReady, an "endpoint controller" maintains
// per-revision EndpointSlices from pod serving state, and an
// "aggregator" refreshes the per-Instance pod counters on the IR —
// exactly the three external actors the real gates wait on.
package ops

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// migFixture is the shared single-pod / gang migration harness.
type migFixture struct {
	c         client.Client
	isvc      *v1beta1.InferenceService
	component workload.ComponentType
	plan      workload.ComponentPlan
	gangSched bool
	// records is the in-memory status.migrations authority the
	// MutateMigration seam mutates; each pass mirrors it onto
	// ObservedState.Migrations exactly like the adapter does.
	records []workload.MigrationRecord
	// clk, when set, is wired into both Deps and ReconcileInput so
	// deadline comparisons are deterministic (expiry tests).
	clk clock.Clock
	// forceDelete, when set, arms the stuck-Terminating escalation on
	// the ReconcileInput (mirrors the adapter's config plumbing).
	forceDelete *workload.ForceDeletePolicy
	// finalizeInstanceResources models per-Instance cleanup owned by the IR
	// adapter. When set, the fixture also wires the guarded batch status writer.
	finalizeInstanceResources func(context.Context, int32) (bool, error)
}

// deps builds the fixture's workload.Deps (clock included when set).
func (f *migFixture) deps() workload.Deps {
	d := legacyTestDeps(f.c)
	d.Clock = f.clk
	return d
}

// mkMigRecord is the shape the accept pass writes: Accepted, Manual,
// no surge index.
func mkMigRecord(uuid string, sourceIdx int32, fromNode string) workload.MigrationRecord {
	return workload.MigrationRecord{
		RequestUUID:    uuid,
		Trigger:        workload.MigrationTriggerManual,
		Phase:          workload.MigrationPhaseAccepted,
		SourceInstance: sourceIdx,
		FromNode:       fromNode,
		Reason:         "maintenance",
		StartedAt:      metav1.Now(),
		Deadline:       metav1.NewTime(time.Now().Add(30 * time.Minute)),
	}
}

func (f *migFixture) record(t *testing.T, uuid string) *workload.MigrationRecord {
	t.Helper()
	r := workload.FindMigrationRecord(f.records, uuid)
	if r == nil {
		t.Fatalf("record %s missing", uuid)
	}
	return r
}

// irKey is the fixture IR's NamespacedName.
func (f *migFixture) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: f.isvc.Namespace, Name: legacyIRName(f.isvc, f.component)}
}

func (f *migFixture) getIR(t *testing.T) *v1beta1.InferenceReplica {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := f.c.Get(context.Background(), f.irKey(), ir); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	return ir
}

// migInstStatusToWorkload / migInstStatusFromWorkload are the fixture's
// full-fidelity InstanceStatus converters (counters included — the
// Available gate reads them; the lossy legacy helpers drop counters).
// Local because importing v1beta1convert from a workload/ops white-box
// test closes an import cycle through the workload root package.
func migInstStatusToWorkload(s v1beta1.OMENativeInstanceStatus) workload.InstanceStatus {
	out := workload.InstanceStatus{
		Index:             s.Index,
		Incarnation:       s.Incarnation,
		Phase:             workload.InstancePhase(s.Phase),
		RunningRevision:   s.RunningRevision,
		TargetRevision:    s.TargetRevision,
		ActiveOrdinal:     s.ActiveOrdinal,
		PodCount:          s.PodCount,
		ReadyPodCount:     s.ReadyPodCount,
		ServingPodCount:   s.ServingPodCount,
		AvailablePodCount: s.AvailablePodCount,
		ScheduledPodCount: s.ScheduledPodCount,
		Admitted:          s.Admitted,
		NodesOccupied:     append([]string(nil), s.NodesOccupied...),
		Conditions:        append([]metav1.Condition(nil), s.Conditions...),
		Operation:         legacyFromV1beta1Op(s.Operation),
	}
	if s.LastFailure != nil {
		out.LastFailure = &workload.InstanceTermination{
			PodName:       s.LastFailure.PodName,
			ContainerName: s.LastFailure.ContainerName,
			Reason:        s.LastFailure.Reason,
			Message:       s.LastFailure.Message,
			Time:          s.LastFailure.Time,
		}
		if s.LastFailure.ExitCode != nil {
			exitCode := *s.LastFailure.ExitCode
			out.LastFailure.ExitCode = &exitCode
		}
	}
	return out
}

func migInstStatusFromWorkload(w workload.InstanceStatus) v1beta1.OMENativeInstanceStatus {
	out := v1beta1.OMENativeInstanceStatus{
		Index:             w.Index,
		Incarnation:       w.Incarnation,
		Phase:             v1beta1.OMENativeInstancePhase(w.Phase),
		RunningRevision:   w.RunningRevision,
		TargetRevision:    w.TargetRevision,
		ActiveOrdinal:     w.ActiveOrdinal,
		PodCount:          w.PodCount,
		ReadyPodCount:     w.ReadyPodCount,
		ServingPodCount:   w.ServingPodCount,
		AvailablePodCount: w.AvailablePodCount,
		ScheduledPodCount: w.ScheduledPodCount,
		Admitted:          w.Admitted,
		NodesOccupied:     append([]string(nil), w.NodesOccupied...),
		Conditions:        append([]metav1.Condition(nil), w.Conditions...),
		Operation:         legacyToV1beta1Op(w.Operation),
	}
	if w.LastFailure != nil {
		out.LastFailure = &v1beta1.InstanceTermination{
			PodName:       w.LastFailure.PodName,
			ContainerName: w.LastFailure.ContainerName,
			Reason:        w.LastFailure.Reason,
			Message:       w.LastFailure.Message,
			Time:          w.LastFailure.Time,
		}
		if w.LastFailure.ExitCode != nil {
			exitCode := *w.LastFailure.ExitCode
			out.LastFailure.ExitCode = &exitCode
		}
	}
	return out
}

// input builds a fresh full-fidelity ReconcileInput off the persisted
// IR — counters included (the Available gate reads them), unlike the
// lossy legacyTestInput conversion.
func (f *migFixture) input(t *testing.T) workload.ReconcileInput {
	t.Helper()
	in := legacyTestInput(f.isvc, f.c, f.component)
	ir := f.getIR(t)
	insts := make([]workload.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, s := range ir.Status.InstanceStatuses {
		insts = append(insts, migInstStatusToWorkload(s))
	}
	in.ObservedState.InstanceStatuses = insts
	in.MutateInstance = f.mutateInstance()
	in.RemoveInstance = legacyRemoveInstance(f.c, f.isvc, f.component)
	in.DesiredSpec.GangSchedulingAvailable = f.gangSched
	in.Clock = f.clk
	in.ForceDelete = f.forceDelete
	if f.finalizeInstanceResources != nil {
		in.FinalizeInstanceResources = f.finalizeInstanceResources
		in.ApplyInstanceMutationsWithRetryBlock = f.applyInstanceMutationsWithRetryBlock
	}
	in.ObservedState.Migrations = append([]workload.MigrationRecord(nil), f.records...)
	in.MutateMigration = func(_ context.Context, uuid string, mutate func(*workload.MigrationRecord) bool) error {
		for i := range f.records {
			if f.records[i].RequestUUID == uuid {
				r := f.records[i]
				if mutate(&r) {
					f.records[i] = r
				}
				return nil
			}
		}
		return nil
	}
	return in
}

func (f *migFixture) applyInstanceMutationsWithRetryBlock(
	ctx context.Context,
	mutations []workload.InstanceMutation,
	_ string,
	mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition,
) error {
	if mutateRetryBlock != nil {
		return fmt.Errorf("migration fixture does not support RetryBlock mutations")
	}
	ir := &v1beta1.InferenceReplica{}
	if err := f.c.Get(ctx, f.irKey(), ir); err != nil {
		return err
	}
	slots := make(map[int32]workload.InstanceStatus, len(ir.Status.InstanceStatuses))
	for _, status := range ir.Status.InstanceStatuses {
		slots[status.Index] = migInstStatusToWorkload(status)
	}
	snapshot := workload.InstanceMutationSnapshot{
		OwnerUID:  f.isvc.UID,
		Instances: slots,
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return workload.ErrStatusMutationPrecondition
		}
	}

	type committedMutation struct {
		callback func(*workload.InstanceStatus, *workload.InstanceStatus)
		previous *workload.InstanceStatus
		current  *workload.InstanceStatus
	}
	var committed []committedMutation
	changed := false
	for _, mutation := range mutations {
		status, found := slots[mutation.Index]
		if mutation.Remove {
			if !found || mutation.Precondition != nil && !mutation.Precondition(&status) {
				continue
			}
			previous := status
			delete(slots, mutation.Index)
			changed = true
			if mutation.OnCommit != nil {
				committed = append(committed, committedMutation{callback: mutation.OnCommit, previous: &previous})
			}
			continue
		}
		if !found {
			status = workload.InstanceStatus{Index: mutation.Index}
		}
		if mutation.Precondition != nil && !mutation.Precondition(&status) || mutation.Mutate == nil {
			continue
		}
		previous := status
		if !mutation.Mutate(&status) {
			continue
		}
		slots[mutation.Index] = status
		changed = true
		if mutation.OnCommit != nil {
			current := status
			committed = append(committed, committedMutation{callback: mutation.OnCommit, previous: &previous, current: &current})
		}
	}
	if changed {
		persisted := make([]v1beta1.OMENativeInstanceStatus, 0, len(slots))
		for _, original := range ir.Status.InstanceStatuses {
			if status, found := slots[original.Index]; found {
				persisted = append(persisted, migInstStatusFromWorkload(status))
				delete(slots, original.Index)
			}
		}
		for _, status := range slots {
			persisted = append(persisted, migInstStatusFromWorkload(status))
		}
		ir.Status.InstanceStatuses = persisted
		if err := f.c.Status().Update(ctx, ir); err != nil {
			return err
		}
	}
	for _, mutation := range committed {
		mutation.callback(mutation.previous, mutation.current)
	}
	return nil
}

// mutateInstance is a full-fidelity (v1beta1convert) round-trip writer
// so counter fields survive per-op writes.
func (f *migFixture) mutateInstance() func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		ir := &v1beta1.InferenceReplica{}
		create := false
		if err := f.c.Get(ctx, f.irKey(), ir); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			ir = &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: f.irKey().Namespace, Name: f.irKey().Name}}
			create = true
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		slot := v1beta1.OMENativeInstanceStatus{Index: idx}
		if pos != -1 {
			slot = ir.Status.InstanceStatuses[pos]
		}
		w := migInstStatusToWorkload(slot)
		if !mutate(&w) {
			return nil
		}
		updated := migInstStatusFromWorkload(w)
		if pos == -1 {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, updated)
		} else {
			ir.Status.InstanceStatuses[pos] = updated
		}
		if create {
			bare := &v1beta1.InferenceReplica{ObjectMeta: ir.ObjectMeta}
			if err := f.c.Create(ctx, bare); err != nil {
				return err
			}
			bare.Status = ir.Status
			ir = bare
		}
		return f.c.Status().Update(ctx, ir)
	}
}

// pass runs one Migrate pass the way the dispatcher would: request
// reconstructed from the record, expectations reset (informer caught
// up).
func (f *migFixture) pass(t *testing.T, uuid string) (done, accepted bool) {
	t.Helper()
	done, accepted, err := f.passResult(t, uuid)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return done, accepted
}

func (f *migFixture) passResult(t *testing.T, uuid string) (done, accepted bool, err error) {
	t.Helper()
	legacyResetExpectations(t)
	rec := f.record(t, uuid)
	req := &audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(f.component),
		Instance:        rec.SourceInstance,
		FromNode:        rec.FromNode,
		HintTargetNodes: append([]string(nil), rec.HintTargetNodes...),
		Reason:          rec.Reason,
	}
	in := f.input(t)
	return Migrate(context.Background(), f.deps(), in, f.plan, rec.SourceInstance, uuid, req)
}

func (f *migFixture) listPods(t *testing.T) []*corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	if err := f.c.List(context.Background(), list, client.InNamespace(f.isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	return out
}

func containersReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// react simulates the environment settling between passes: kubelet
// (ContainersReady on every pod), the endpoint controller (per-revision
// EndpointSlices reflect serving state), and the status aggregator
// (per-Instance pod counters on the IR).
func (f *migFixture) react(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	// Kubelet: every pod's containers come up.
	for _, pod := range f.listPods(t) {
		if containersReady(pod) {
			continue
		}
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
		if err := f.c.Status().Update(ctx, pod); err != nil {
			t.Fatalf("mark pod ready: %v", err)
		}
	}

	// Endpoint controller: one slice per routable per-revision Service;
	// endpoint Ready mirrors serving && containers-ready; no members →
	// slice deleted.
	byService := map[string][]*corev1.Pod{}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelRunner] == "worker" {
			continue
		}
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" {
			continue
		}
		svc := query.PerRevisionServiceName(f.isvc.Name, f.component, hash)
		byService[svc] = append(byService[svc], pod)
	}
	slices := &discoveryv1.EndpointSliceList{}
	if err := f.c.List(ctx, slices, client.InNamespace(f.isvc.Namespace)); err != nil {
		t.Fatalf("list slices: %v", err)
	}
	seen := map[string]bool{}
	for svc, pods := range byService {
		endpoints := make([]discoveryv1.Endpoint, 0, len(pods))
		for _, pod := range pods {
			ready := podreadiness.IsServing(pod) && containersReady(pod)
			r := ready
			endpoints = append(endpoints, discoveryv1.Endpoint{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: &r},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
			})
		}
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: svc + "-slice", Namespace: f.isvc.Namespace,
				Labels: map[string]string{discoveryv1.LabelServiceName: svc},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints:   endpoints,
		}
		seen[slice.Name] = true
		existing := &discoveryv1.EndpointSlice{}
		err := f.c.Get(ctx, types.NamespacedName{Namespace: slice.Namespace, Name: slice.Name}, existing)
		switch {
		case err == nil:
			existing.Endpoints = endpoints
			if uerr := f.c.Update(ctx, existing); uerr != nil {
				t.Fatalf("update slice: %v", uerr)
			}
		case apierrors.IsNotFound(err):
			if cerr := f.c.Create(ctx, slice); cerr != nil {
				t.Fatalf("create slice: %v", cerr)
			}
		default:
			t.Fatalf("get slice: %v", err)
		}
	}
	for i := range slices.Items {
		if !seen[slices.Items[i].Name] {
			if err := f.c.Delete(ctx, &slices.Items[i]); err != nil {
				t.Fatalf("delete slice: %v", err)
			}
		}
	}

	// Aggregator: refresh per-Instance pod counters on the IR.
	ir := f.getIR(t)
	pods := f.listPods(t)
	changed := false
	for i := range ir.Status.InstanceStatuses {
		s := &ir.Status.InstanceStatuses[i]
		var podCount, readyCount, availCount int32
		for _, pod := range pods {
			if pod.Labels[query.LabelInstanceIdx] != fmt.Sprintf("%d", s.Index) {
				continue
			}
			podCount++
			if containersReady(pod) {
				readyCount++
				if podreadiness.IsServing(pod) {
					availCount++
				}
			}
		}
		if s.PodCount != podCount || s.ReadyPodCount != readyCount || s.AvailablePodCount != availCount {
			s.PodCount, s.ReadyPodCount, s.AvailablePodCount = podCount, readyCount, availCount
			changed = true
		}
	}
	if changed {
		if err := f.c.Status().Update(context.Background(), ir); err != nil {
			t.Fatalf("update IR counters: %v", err)
		}
	}
}

// drive runs Migrate passes (react between them) until done.
func (f *migFixture) drive(t *testing.T, uuid string, maxPasses int) {
	t.Helper()
	for i := 0; i < maxPasses; i++ {
		done, accepted := f.pass(t, uuid)
		if done {
			return
		}
		if !accepted {
			t.Fatalf("pass %d: unexpected defer-without-ownership (record=%+v)", i, *f.record(t, uuid))
		}
		f.react(t)
	}
	t.Fatalf("migration did not complete in %d passes; record=%+v", maxPasses, *f.record(t, uuid))
}

// newSinglePodMigFixture: instance 0 Ready on node-a with a seeded
// RunningRevision and a serving pod.
func newSinglePodMigFixture(t *testing.T) *migFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	sourcePod := legacyPodForInstance(isvc, 0, true, true)
	sourcePod.Spec.NodeName = "node-a"
	c := legacyNewFakeClient(t, isvc, ir, sourcePod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	})
	return &migFixture{
		c: c, isvc: isvc, component: workload.ComponentEngine,
		plan: legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil),
	}
}

// seedMigrationCompletionTailWindow reconstructs the durable state after the
// source status was removed and the surge was promoted, while the migration
// record is still Draining. The caller controls whether authoritative source
// pods remain and may weaken the surge evidence for negative cases.
func seedMigrationCompletionTailWindow(
	t *testing.T,
	f *migFixture,
	deleteSourcePods bool,
	mutateSurge func(*v1beta1.OMENativeInstanceStatus),
) v1beta1.OMENativeInstanceStatus {
	t.Helper()
	ir := f.getIR(t)
	if len(ir.Status.InstanceStatuses) != 1 || ir.Status.InstanceStatuses[0].Index != 0 {
		t.Fatalf("source fixture statuses = %+v, want only instance 0", ir.Status.InstanceStatuses)
	}
	source := ir.Status.InstanceStatuses[0]
	if source.RunningRevision == "" {
		t.Fatal("source fixture has no RunningRevision")
	}
	surge := v1beta1.OMENativeInstanceStatus{
		Index:           1,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: source.RunningRevision,
	}
	if mutateSurge != nil {
		mutateSurge(&surge)
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{surge}
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed completion-tail status: %v", err)
	}
	if deleteSourcePods {
		deleted := 0
		for _, pod := range f.listPods(t) {
			if pod.Labels[query.LabelInstanceIdx] != "0" {
				continue
			}
			if err := f.c.Delete(context.Background(), pod); err != nil {
				t.Fatalf("delete source pod %s: %v", pod.Name, err)
			}
			deleted++
		}
		if deleted == 0 {
			t.Fatal("source fixture has no pod to delete")
		}
	}
	return source
}

// newMultiInstanceMigFixture: n single-pod instances (0..n-1), each
// Ready on node-a with a seeded RunningRevision and a serving pod —
// the shape a batch of migration requests lands on.
func newMultiInstanceMigFixture(t *testing.T, n int32) *migFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc := legacyMinimalISVC("llama-70b", "prod", int(n))
	statuses := make([]v1beta1.OMENativeInstanceStatus, 0, n)
	for i := int32(0); i < n; i++ {
		statuses = append(statuses, v1beta1.OMENativeInstanceStatus{
			Index: i, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
		})
	}
	ir := legacyInstanceIR(isvc, workload.ComponentEngine, statuses...)
	objs := []client.Object{isvc, ir}
	for i := int32(0); i < n; i++ {
		pod := legacyPodForInstance(isvc, i, true, true)
		pod.Spec.NodeName = "node-a"
		objs = append(objs, pod)
	}
	c := legacyNewFakeClient(t, objs...)
	for i := int32(0); i < n; i++ {
		legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, i, &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
		})
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	plan.Replicas = n
	plan.Instances = nil
	for i := int32(0); i < n; i++ {
		plan.Instances = append(plan.Instances, workload.InstancePlan{
			Index: i, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		})
	}
	return &migFixture{c: c, isvc: isvc, component: workload.ComponentEngine, plan: plan}
}

// newGangMigFixture: instance 0 is a 3-pod gang (leader + 2 workers)
// spanning nodes, RunningRevision carries leader + worker templates.
func newGangMigFixture(t *testing.T) *migFixture {
	t.Helper()
	return newGangMigFixtureWithWorkerSpec(t, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}},
	})
}

// newGangMigFixtureWithWorkerSpec is newGangMigFixture with the
// RunningRevision's WORKER template supplied by the caller — the
// worker-pinned-affinity rejection test needs a worker spec whose hard
// NodeAffinity collides with the migration overlay.
func newGangMigFixtureWithWorkerSpec(t *testing.T, workerSpec *corev1.PodSpec) *migFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	mkGangPod := func(runner string, ordinal int32, node string) *corev1.Pod {
		labels := legacyTestPodLabels(isvc.Name, workload.ComponentEngine, 0, runner, 1, ordinal)
		labels[query.LabelRevisionHash] = testRevisionHashLegacy
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      query.PodName(isvc.Name, workload.ComponentEngine, 0, runner, ordinal),
				Namespace: isvc.Namespace,
				Labels:    labels,
			},
			Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}},
		}
		now := metav1.Now()
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: now},
		}
		return pod
	}
	leader := mkGangPod("leader", 0, "node-a")
	worker0 := mkGangPod("worker", 0, "node-b")
	worker1 := mkGangPod("worker", 1, "node-c")
	c := legacyNewFakeClient(t, isvc, ir, leader, worker0, worker1)

	leaderSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "llama:v1"}}}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		leaderSpec, workerSpec, nil, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("EnsureControllerRevisionWithWorker: %v", err)
	}
	freshIR := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}, freshIR); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	freshIR.Status.InstanceStatuses[0].RunningRevision = cr.Name
	if err := c.Status().Update(context.Background(), freshIR); err != nil {
		t.Fatalf("seed gang RunningRevision: %v", err)
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners: []workload.RunnerPlan{
				{Name: "leader", Size: 1},
				{Name: "worker", Size: 2},
			},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy:       workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain},
	}
	return &migFixture{
		c: c, isvc: isvc, component: workload.ComponentEngine,
		plan: plan, gangSched: true,
	}
}

// TestMigrate_EntryLifecycle_SinglePod walks the full Manual chain for
// a single-pod Instance: Accepted -> allocation (SurgeInstance +
// SurgePending recorded, pair stamped, ledger Started upserted with the
// real index) -> surge pod created -> SurgeReady -> Draining ->
// Completed (source pod deleted, source status removed, surge
// promoted, record terminal with CompletedAt).
func TestMigrate_EntryLifecycle_SinglePod(t *testing.T) {
	f := newSinglePodMigFixture(t)
	finalizations := 0
	f.finalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
		finalizations++
		if index != 0 || findInstanceStatusOnIRForFixture(t, f, index) == nil {
			t.Fatalf("finalization ran without the owned source status: index=%d", index)
		}
		return true, nil
	}
	const uuid = "mig-single-1"
	rec0 := mkMigRecord(uuid, 0, "node-a")
	rec0.HintTargetNodes = []string{"node-hint-1", "node-hint-2"}
	f.records = []workload.MigrationRecord{rec0}

	// Pass 1: fresh record -> allocation + stamps + surge pod.
	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("pass 1: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseSurgePending {
		t.Errorf("pass 1: record phase = %s, want SurgePending", rec.Phase)
	}
	if rec.SurgeInstance == nil || *rec.SurgeInstance != 1 {
		t.Fatalf("pass 1: record SurgeInstance = %v, want 1", rec.SurgeInstance)
	}
	if rec.AllocatedAt == nil {
		t.Errorf("pass 1: AllocatedAt must be stamped in the same write as SurgeInstance")
	}
	surgePodName := query.PodName(f.isvc.Name, f.component, 1, "default", 0)
	surgePod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: surgePodName}, surgePod); err != nil {
		t.Fatalf("surge pod must exist after pass 1: %v", err)
	}
	// The record's hints flow through the reconstructed request +
	// MigrationOverlay into the rendered surge (preferred affinity).
	if !surgePodHasHintTerm(surgePod, rec0.HintTargetNodes) {
		t.Errorf("surge pod must carry the record's HintTargetNodes as preferred node affinity; affinity=%+v", surgePod.Spec.Affinity)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if e := ledger.InFlightEntry(uuid); e == nil || e.SurgeInstance != 1 {
		t.Errorf("ledger Started row must carry the real surge index; got %+v", e)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating {
		t.Errorf("source must be stamped Migrating; got %+v", src)
	}

	// Drive the rest; the record must pass through SurgeReady/Draining
	// before Completed (observed transitively via the terminal state).
	f.react(t)
	f.drive(t, uuid, 10)

	rec = f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
		t.Fatalf("record must finish Completed with CompletedAt; got %+v", *rec)
	}
	// Source status removed; surge promoted Ready at the source's revision.
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src != nil {
		t.Errorf("source InstanceStatus must be removed on completion; got %+v", src)
	}
	if finalizations != 1 {
		t.Errorf("source resource finalizations = %d, want 1", finalizations)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceReady || surge.RunningRevision == "" || surge.Operation != nil {
		t.Errorf("surge must be promoted Ready on the source revision with the op cleared; got %+v", surge)
	}
	// Source pod gone.
	srcPod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: query.PodName(f.isvc.Name, f.component, 0, "default", 0)}, srcPod); !apierrors.IsNotFound(err) {
		t.Errorf("source pod must be deleted; get returned %v", err)
	}
	// Ledger terminal.
	ledger, _ = audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger must record the terminal Completed row")
	}
	// Terminal record is never picked again.
	if picked := workload.NextManualMigration(f.records); picked != nil {
		t.Errorf("completed record must not be re-selected; picked %q", picked.RequestUUID)
	}
}

func TestMigrate_FinalizationFailureRetainsSourceAndRecord(t *testing.T) {
	f := newSinglePodMigFixture(t)
	finalizeErr := fmt.Errorf("PodGroup delete failed")
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return false, finalizeErr
	}
	const uuid = "mig-finalize-failure"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	for pass := 0; pass < 12; pass++ {
		done, accepted, err := f.passResult(t, uuid)
		if err != nil {
			if !strings.Contains(err.Error(), finalizeErr.Error()) {
				t.Fatalf("finalization error = %v", err)
			}
			if finalizations != 1 {
				t.Fatalf("finalizations = %d, want 1", finalizations)
			}
			if source := findInstanceStatusOnIRForFixture(t, f, 0); source == nil {
				t.Fatal("finalization failure removed the source status")
			}
			if record := f.record(t, uuid); record.Phase.Terminal() || record.CompletedAt != nil {
				t.Fatalf("finalization failure stamped a terminal migration record: %+v", *record)
			}
			return
		}
		if done || !accepted {
			t.Fatalf("pass %d completed or deferred before finalization failure: done=%v accepted=%v", pass, done, accepted)
		}
		f.react(t)
	}
	t.Fatal("migration never reached the finalization failure")
}

func TestMigrate_FinalizationWaitsForResourceAbsence(t *testing.T) {
	f := newSinglePodMigFixture(t)
	resourcesAbsent := false
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return resourcesAbsent, nil
	}
	const uuid = "mig-finalize-pending"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	for pass := 0; pass < 12; pass++ {
		done, accepted, err := f.passResult(t, uuid)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if finalizations == 0 {
			if done || !accepted {
				t.Fatalf("pass %d completed or deferred before finalization: done=%v accepted=%v", pass, done, accepted)
			}
			f.react(t)
			continue
		}
		if done || !accepted {
			t.Fatalf("delete accepted completed migration: done=%v accepted=%v", done, accepted)
		}
		if source := findInstanceStatusOnIRForFixture(t, f, 0); source == nil {
			t.Fatal("delete accepted removed the source status")
		}
		if record := f.record(t, uuid); record.Phase.Terminal() || record.CompletedAt != nil {
			t.Fatalf("delete accepted stamped a terminal migration record: %+v", *record)
		}

		record := f.record(t, uuid)
		if record.SurgeInstance == nil {
			t.Fatal("promoted migration has no surge instance")
		}
		promoted := f.plan.Instances[0]
		promoted.Index = *record.SurgeInstance
		f.plan.Instances = []workload.InstancePlan{promoted}
		resourcesAbsent = true
		done, accepted, err = f.passResult(t, uuid)
		if err != nil || !done || !accepted {
			t.Fatalf("absence pass: done=%v accepted=%v err=%v", done, accepted, err)
		}
		if source := findInstanceStatusOnIRForFixture(t, f, 0); source != nil {
			t.Fatalf("absence pass retained source status: %+v", source)
		}
		if finalizations != 2 {
			t.Fatalf("finalizations=%d want 2", finalizations)
		}
		return
	}
	t.Fatal("migration never reached resource finalization")
}

func TestMigrate_GangPromotionTailCompletesFromRetainedSource(t *testing.T) {
	f := newGangMigFixture(t)
	resourcesAbsent := false
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return resourcesAbsent, nil
	}
	const uuid = "mig-gang-promoted-tail"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	surgeIndex := driveGangMigrationToPendingFinalization(t, f, uuid)
	if finalizations != 1 {
		t.Fatalf("pending finalizations=%d want 1", finalizations)
	}

	// The promoted surge is the steady plan member while the source status
	// remains as the terminal-cleanup ownership token.
	promoted := f.plan.Instances[0]
	promoted.Index = surgeIndex
	f.plan.Instances = []workload.InstancePlan{promoted}
	resourcesAbsent = true

	done, accepted, err := f.passResult(t, uuid)
	defaultPod := &corev1.Pod{}
	defaultPodName := query.PodName(f.isvc.Name, f.component, surgeIndex, "default", 0)
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: defaultPodName}, defaultPod); !apierrors.IsNotFound(err) {
		t.Fatalf("completion tail synthesized a single-pod runner: get %s returned %v", defaultPodName, err)
	}
	if err != nil || !done || !accepted {
		t.Fatalf("completion pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if finalizations != 2 {
		t.Fatalf("finalizations=%d want 2", finalizations)
	}
	if source := findInstanceStatusOnIRForFixture(t, f, 0); source != nil {
		t.Fatalf("completion retained source status: %+v", source)
	}
	record := f.record(t, uuid)
	if record.Phase != workload.MigrationPhaseCompleted || record.CompletedAt == nil {
		t.Fatalf("completion record = %+v", *record)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("completion ledger: entries=%+v err=%v", ledger.Entries, err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != uuid ||
		ledger.Entries[0].Phase != audit.PhaseCompleted || ledger.Entries[0].Outcome != "migrated" {
		t.Fatalf("completion ledger=%+v want one Completed migrated entry", ledger.Entries)
	}
	runnerCounts := map[string]int{}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == fmt.Sprintf("%d", surgeIndex) {
			runnerCounts[pod.Labels[query.LabelRunner]]++
		}
	}
	if runnerCounts["leader"] != 1 || runnerCounts["worker"] != 2 || len(runnerCounts) != 2 {
		t.Fatalf("promoted gang runners=%v want leader=1 worker=2", runnerCounts)
	}
}

func TestMigrate_GangPromotionTailRejectsDifferentRevision(t *testing.T) {
	f := newGangMigFixture(t)
	resourcesAbsent := false
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return resourcesAbsent, nil
	}
	const uuid = "mig-gang-promoted-tail-revision"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	surgeIndex := driveGangMigrationToPendingFinalization(t, f, uuid)

	ir := f.getIR(t)
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == surgeIndex {
			ir.Status.InstanceStatuses[i].RunningRevision = "different-revision"
		}
	}
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("change promoted revision: %v", err)
	}
	promoted := f.plan.Instances[0]
	promoted.Index = surgeIndex
	f.plan.Instances = []workload.InstancePlan{promoted}
	resourcesAbsent = true

	done, accepted, err := f.passResult(t, uuid)
	if err != nil || done || !accepted {
		t.Fatalf("guarded pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if finalizations != 1 {
		t.Fatalf("revision mismatch ran finalization: calls=%d want 1", finalizations)
	}
	if source := findInstanceStatusOnIRForFixture(t, f, 0); source == nil {
		t.Fatal("revision mismatch removed the source status")
	}
	record := f.record(t, uuid)
	if record.Phase != workload.MigrationPhaseDraining || record.CompletedAt != nil {
		t.Fatalf("revision mismatch closed the record: %+v", *record)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if ledger.HasCompletedOrFailedRequest(uuid) {
		t.Fatalf("revision mismatch wrote a terminal ledger entry: %+v", ledger.Entries)
	}
}

func driveGangMigrationToPendingFinalization(t *testing.T, f *migFixture, uuid string) int32 {
	t.Helper()
	for pass := 0; pass < 12; pass++ {
		done, accepted, err := f.passResult(t, uuid)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if done || !accepted {
			t.Fatalf("pass %d completed or deferred before finalization: done=%v accepted=%v", pass, done, accepted)
		}
		record := f.record(t, uuid)
		if record.SurgeInstance != nil && record.Phase == workload.MigrationPhaseDraining {
			surge := findInstanceStatusOnIRForFixture(t, f, *record.SurgeInstance)
			source := findInstanceStatusOnIRForFixture(t, f, record.SourceInstance)
			sourcePods := 0
			for _, pod := range f.listPods(t) {
				if pod.Labels[query.LabelInstanceIdx] == fmt.Sprintf("%d", record.SourceInstance) {
					sourcePods++
				}
			}
			if source != nil && sourcePods == 0 && surge != nil &&
				surge.Phase == v1beta1.OMENativeInstanceReady && surge.Operation == nil && surge.RunningRevision != "" {
				return *record.SurgeInstance
			}
		}
		f.react(t)
	}
	t.Fatal("gang migration never reached pending resource finalization")
	return -1
}

func TestMigrate_CompletionTailCrashRecoversBeforeDeadline(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-tail-recovery"
	source := seedMigrationCompletionTailWindow(t, f, true, nil)
	surgeIdx := int32(1)
	record := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute))
	record.SurgeInstance = &surgeIdx
	record.Phase = workload.MigrationPhaseDraining
	f.records = []workload.MigrationRecord{record}

	finalizations := 0
	f.finalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
		finalizations++
		if index != 0 {
			t.Fatalf("finalized instance %d, want source 0", index)
		}
		if status := findInstanceStatusOnIRForFixture(t, f, index); status != nil {
			t.Fatalf("residual resource finalization ran while source status was live: %+v", status)
		}
		return true, nil
	}

	done, accepted, err := f.passResult(t, uuid)
	if err != nil || !done || !accepted {
		t.Fatalf("recovery pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if !clk.Now().Before(record.Deadline.Time) {
		t.Fatalf("test record expired before recovery: now=%v deadline=%v", clk.Now(), record.Deadline.Time)
	}
	if finalizations != 1 {
		t.Fatalf("residual resource finalizations = %d, want 1", finalizations)
	}
	completed := f.record(t, uuid)
	if completed.Phase != workload.MigrationPhaseCompleted || completed.CompletedAt == nil ||
		!strings.Contains(completed.Message, "migrated to instance=1") {
		t.Fatalf("recovered record = %+v, want Completed migration to instance 1", *completed)
	}
	if sourceStatus := findInstanceStatusOnIRForFixture(t, f, 0); sourceStatus != nil {
		t.Fatalf("recovery resurrected source status: %+v", sourceStatus)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, surgeIdx)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceReady ||
		surge.Operation != nil || surge.RunningRevision != source.RunningRevision {
		t.Fatalf("recovery changed promoted surge: %+v", surge)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load recovery ledger: %v", err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != uuid ||
		ledger.Entries[0].Phase != audit.PhaseCompleted || ledger.Entries[0].Outcome != "migrated" {
		t.Fatalf("recovery ledger = %+v, want one Completed migrated entry", ledger.Entries)
	}
	if picked := workload.NextManualMigration(f.records); picked != nil {
		t.Fatalf("recovered record was selected again: %+v", *picked)
	}

	done, accepted, err = f.passResult(t, uuid)
	if err != nil || !done || !accepted || finalizations != 1 {
		t.Fatalf("terminal replay: done=%v accepted=%v err=%v finalizations=%d", done, accepted, err, finalizations)
	}
}

func TestMigrate_CompletionTailRecoveryRequiresCompleteEvidence(t *testing.T) {
	tests := []struct {
		name              string
		deleteSourcePods  bool
		mutateSurgeStatus func(*v1beta1.OMENativeInstanceStatus)
	}{
		{
			name:             "source pod remains",
			deleteSourcePods: false,
		},
		{
			name:             "surge is not Ready",
			deleteSourcePods: true,
			mutateSurgeStatus: func(status *v1beta1.OMENativeInstanceStatus) {
				status.Phase = v1beta1.OMENativeInstanceCreating
			},
		},
		{
			name:             "surge still owns an operation",
			deleteSourcePods: true,
			mutateSurgeStatus: func(status *v1beta1.OMENativeInstanceStatus) {
				status.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationType(workload.InstanceOperationMigrate)}
			},
		},
		{
			name:             "surge has no running revision",
			deleteSourcePods: true,
			mutateSurgeStatus: func(status *v1beta1.OMENativeInstanceStatus) {
				status.RunningRevision = ""
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newSinglePodMigFixture(t)
			clk := f.withFakeClock()
			const uuid = "mig-tail-incomplete"
			seedMigrationCompletionTailWindow(t, f, test.deleteSourcePods, test.mutateSurgeStatus)
			surgeIdx := int32(1)
			record := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute))
			record.SurgeInstance = &surgeIdx
			record.Phase = workload.MigrationPhaseDraining
			f.records = []workload.MigrationRecord{record}
			finalizations := 0
			f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
				finalizations++
				return true, nil
			}

			done, accepted, err := f.passResult(t, uuid)
			if err != nil || done || !accepted {
				t.Fatalf("guarded pass: done=%v accepted=%v err=%v", done, accepted, err)
			}
			if finalizations != 0 {
				t.Fatalf("incomplete evidence triggered %d resource finalizations", finalizations)
			}
			got := f.record(t, uuid)
			if got.Phase != workload.MigrationPhaseDraining || got.CompletedAt != nil {
				t.Fatalf("incomplete evidence closed record: %+v", *got)
			}
			ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
			if err != nil {
				t.Fatalf("load ledger: %v", err)
			}
			if ledger.HasCompletedOrFailedRequest(uuid) {
				t.Fatalf("incomplete evidence wrote a terminal ledger row: %+v", ledger.Entries)
			}
		})
	}
}

func TestMigrate_CompletionTailRecoveryRechecksAuthoritativeAbsence(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-tail-status-drift"
	source := seedMigrationCompletionTailWindow(t, f, true, nil)
	surgeIdx := int32(1)
	record := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute))
	record.SurgeInstance = &surgeIdx
	record.Phase = workload.MigrationPhaseDraining
	f.records = []workload.MigrationRecord{record}
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return true, nil
	}

	input := f.input(t)
	liveIR := f.getIR(t)
	liveIR.Status.InstanceStatuses = append(liveIR.Status.InstanceStatuses, source)
	if err := f.c.Status().Update(context.Background(), liveIR); err != nil {
		t.Fatalf("restore live source status: %v", err)
	}
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      record.SourceInstance,
		FromNode:      record.FromNode,
		Reason:        record.Reason,
	}
	done, accepted, err := Migrate(context.Background(), f.deps(), input, f.plan, record.SourceInstance, uuid, req)
	if err != nil || done || !accepted {
		t.Fatalf("drift pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if finalizations != 0 {
		t.Fatalf("live source status triggered %d residual resource finalizations", finalizations)
	}
	if status := findInstanceStatusOnIRForFixture(t, f, 0); status == nil {
		t.Fatal("authoritative source status was removed")
	}
	if got := f.record(t, uuid); got.Phase != workload.MigrationPhaseDraining || got.CompletedAt != nil {
		t.Fatalf("authoritative status drift closed record: %+v", *got)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if ledger.HasCompletedOrFailedRequest(uuid) {
		t.Fatalf("authoritative status drift wrote a terminal ledger row: %+v", ledger.Entries)
	}
}

// TestMigrate_EntryLifecycle_Gang walks the same chain for a 3-pod
// gang (leader + 2 workers). Gang-specific assertions: the fresh-stamp
// pass requeues BEFORE creating pods (PodGroup ordering under gang
// scheduling), the surge materializes all 3 pods with the worker
// template from the RunningRevision, and completion tears down all 3
// source pods.
func TestMigrate_EntryLifecycle_Gang(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-gang-1"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-b")} // a worker's node — gang-aware validateFromNode accepts any member's node

	// Pass 1: allocation + stamps, then the gang PodGroup requeue —
	// NO surge pods yet (EnsurePodGroups must see the pinned index
	// before the gang's pods render).
	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("pass 1: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseSurgePending || rec.SurgeInstance == nil || *rec.SurgeInstance != 1 {
		t.Fatalf("pass 1: record must be SurgePending with SurgeInstance=1; got %+v", *rec)
	}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == "1" {
			t.Fatalf("gang fresh-stamp pass must requeue before creating surge pods; found %s", pod.Name)
		}
	}

	// Pass 2: surge gang pods render — 3 of them, worker template from
	// the RunningRevision's worker payload.
	done, accepted = f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("pass 2: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	var surgeLeaders, surgeWorkers int
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] != "1" {
			continue
		}
		switch pod.Labels[query.LabelRunner] {
		case "leader":
			surgeLeaders++
			if got := pod.Spec.Containers[0].Name; got != "leader" {
				t.Errorf("surge leader must render from the revision's leader template; container=%q", got)
			}
		case "worker":
			surgeWorkers++
			if got := pod.Spec.Containers[0].Name; got != "worker" {
				t.Errorf("surge worker must render from the revision's WORKER template; container=%q", got)
			}
		}
	}
	if surgeLeaders != 1 || surgeWorkers != 2 {
		t.Fatalf("surge gang shape: got %d leaders + %d workers, want 1 + 2", surgeLeaders, surgeWorkers)
	}

	f.react(t)
	f.drive(t, uuid, 12)

	rec = f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
		t.Fatalf("gang record must finish Completed with CompletedAt; got %+v", *rec)
	}
	// All 3 source pods gone; the 3 surge pods remain; source status
	// removed; surge promoted.
	var sourceLeft, surgeLeft int
	for _, pod := range f.listPods(t) {
		switch pod.Labels[query.LabelInstanceIdx] {
		case "0":
			sourceLeft++
		case "1":
			surgeLeft++
		}
	}
	if sourceLeft != 0 {
		t.Errorf("all 3 gang source pods must be deleted; %d remain", sourceLeft)
	}
	if surgeLeft != 3 {
		t.Errorf("surge gang must survive completion; got %d pods", surgeLeft)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src != nil {
		t.Errorf("gang source InstanceStatus must be removed; got %+v", src)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("gang surge must be promoted Ready; got %+v", surge)
	}
}

// TestMigrate_ResumeAfterCrash_PreStamp pins the record-first crash
// anchor: SurgeInstance + SurgePending were committed but the process
// died before the pair stamps landed. The resume pass must NOT re-run
// the fresh-request guards (the source shows no steady-Ready violation
// here, but the point is the branch), must re-ensure both stamps at
// the recorded index, and the migration must run to completion.
func TestMigrate_ResumeAfterCrash_PreStamp(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-crash-prestamp"
	rec := mkMigRecord(uuid, 0, "node-a")
	idx := int32(1)
	rec.SurgeInstance = &idx
	rec.Phase = workload.MigrationPhaseSurgePending
	f.records = []workload.MigrationRecord{rec}

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("resume pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating ||
		src.Operation == nil || src.Operation.RequestUUID != uuid {
		t.Fatalf("resume must re-ensure the source stamp; got %+v", src)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceCreating ||
		surge.Operation == nil || surge.Operation.SurgeIndex == nil || *surge.Operation.SurgeIndex != 0 {
		t.Fatalf("resume must re-ensure the surge stamp (sibling pointer at source); got %+v", surge)
	}

	f.react(t)
	f.drive(t, uuid, 10)
	if got := f.record(t, uuid); got.Phase != workload.MigrationPhaseCompleted {
		t.Fatalf("crash-resume must complete; got %+v", *got)
	}
}

// TestMigrate_ResumeAfterCrash_Draining pins resume from the Draining
// phase: source pods already deleted, surge serving and in rotation,
// process died before the promote/remove/Completed tail. The resume
// pass must finish the tail without touching the (gone) source pods
// and without regressing the record's phase.
func TestMigrate_ResumeAfterCrash_Draining(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-crash-draining"

	// Walk the real machinery to the Draining state first (passes 1-3),
	// then simulate the crash by rebuilding fixture inputs from
	// persisted state only (records slice survives — it models the
	// re-read status.migrations).
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	for i := 0; i < 6; i++ {
		if f.record(t, uuid).Phase == workload.MigrationPhaseDraining {
			break
		}
		done, _ := f.pass(t, uuid)
		if done {
			t.Fatalf("reached terminal before Draining checkpoint")
		}
		f.react(t)
	}
	if f.record(t, uuid).Phase != workload.MigrationPhaseDraining {
		t.Fatalf("fixture never reached Draining; record=%+v", *f.record(t, uuid))
	}

	// Crash + resume: drive to completion from the persisted state.
	f.drive(t, uuid, 10)
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
		t.Fatalf("resume-from-Draining must complete; got %+v", *rec)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src != nil {
		t.Errorf("source InstanceStatus must be removed; got %+v", src)
	}
}

// TestMigrate_Rejections_RecordFailed drives every fresh-request
// rejection path and asserts the record lands Phase=Failed with the
// rejection reason and CompletedAt, plus the terminal ledger row.
func TestMigrate_Rejections_RecordFailed(t *testing.T) {
	t.Run("from-node-mismatch", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-reject-node"
		f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-WRONG")}
		done, accepted := f.pass(t, uuid)
		if !done || !accepted {
			t.Fatalf("rejection must be terminal: done=%v accepted=%v", done, accepted)
		}
		assertRecordFailed(t, f, uuid, "does not match observed source node")
	})

	t.Run("source-missing", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-reject-missing"
		f.records = []workload.MigrationRecord{mkMigRecord(uuid, 7, "node-a")}
		done, accepted := f.pass(t, uuid)
		if !done || !accepted {
			t.Fatalf("rejection must be terminal: done=%v accepted=%v", done, accepted)
		}
		assertRecordFailed(t, f, uuid, "source InstanceStatus missing")
	})

	t.Run("capacity-in-flight-cap", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-reject-capacity"
		// Three other ALLOCATED non-terminal records exhaust the in-flight
		// cap; the 4th request is rejected. Three concurrent executions on
		// one IR can't occur under the serial dispatcher — records are
		// constructed directly to pin the cap.
		busy := func(u string, phase workload.MigrationPhase, surge int32) workload.MigrationRecord {
			at := metav1.Now()
			return workload.MigrationRecord{
				RequestUUID: u, Trigger: workload.MigrationTriggerManual, Phase: phase,
				SurgeInstance: &surge, AllocatedAt: &at, StartedAt: metav1.Now(),
			}
		}
		f.records = []workload.MigrationRecord{
			busy("busy-1", workload.MigrationPhaseSurgePending, 5),
			busy("busy-2", workload.MigrationPhaseDraining, 6),
			busy("busy-3", workload.MigrationPhaseSurgeReady, 7),
			mkMigRecord(uuid, 0, "node-a"),
		}
		done, accepted := f.pass(t, uuid)
		if !done || !accepted {
			t.Fatalf("rejection must be terminal: done=%v accepted=%v", done, accepted)
		}
		assertRecordFailed(t, f, uuid, "in-flight migration cap reached")
		// The busy records are untouched.
		for _, u := range []string{"busy-1", "busy-2", "busy-3"} {
			if r := f.record(t, u); r.Phase.Terminal() {
				t.Errorf("capacity rejection must not touch other records; %s became %s", u, r.Phase)
			}
		}
	})
}

// TestMigrate_QueuedBatch_NotCapacityPoisoned_SerialCompletion pins the
// queued-batch guard: five requests accepted in one burst sit queued
// (Accepted, no surge allocated). Capacity counts EXECUTION — allocated
// surges and AllocatedAt in the window — never queued intent, so the
// oldest record's fresh-path gate admits (counting the 4 queued siblings
// against the in-flight cap would terminally Fail the whole batch), and
// serial dispatch completes all five.
func TestMigrate_QueuedBatch_NotCapacityPoisoned_SerialCompletion(t *testing.T) {
	const n = 5
	f := newMultiInstanceMigFixture(t, n)
	uuids := make([]string, 0, n)
	for i := int32(0); i < n; i++ {
		rec := mkMigRecord(fmt.Sprintf("mig-batch-%d", i), i, "node-a")
		// Deterministic dispatch order: oldest first.
		rec.StartedAt = metav1.NewTime(time.Now().Add(time.Duration(i-n) * time.Minute))
		f.records = append(f.records, rec)
		uuids = append(uuids, rec.RequestUUID)
	}

	// The oldest queued record must not be capacity-rejected by its four
	// queued siblings.
	if picked := workload.NextManualMigration(f.records); picked == nil || picked.RequestUUID != uuids[0] {
		t.Fatalf("dispatcher must pick the oldest record first; picked %+v", picked)
	}
	done, accepted := f.pass(t, uuids[0])
	if done || !accepted {
		t.Fatalf("oldest queued record must start executing, not terminally fail: done=%v accepted=%v record=%+v",
			done, accepted, *f.record(t, uuids[0]))
	}

	// Serial completion of the whole batch, dispatcher order.
	f.react(t)
	for _, uuid := range uuids {
		f.drive(t, uuid, 12)
		rec := f.record(t, uuid)
		if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
			t.Fatalf("batch record %s must complete; got %+v", uuid, *rec)
		}
	}
}

// TestMigrate_GangWorkerPinnedToFromNode_RejectedPreStamp pins the
// fresh-path overlay pre-check on BOTH gang templates: a WORKER spec
// whose hard NodeAffinity pins hostname=FromNode makes the surge
// unschedulable, so the request must fail terminally BEFORE the pair is
// stamped — no surge InstanceStatus, source untouched. (A pre-check that
// resolved only the leader spec would pass the gang, stamp the pair, then
// terminally fail on resume, orphaning the stamped pair until the
// deadline.)
func TestMigrate_GangWorkerPinnedToFromNode_RejectedPreStamp(t *testing.T) {
	f := newGangMigFixtureWithWorkerSpec(t, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}},
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"node-b"},
					}},
				}},
			},
		}},
	})
	const uuid = "mig-gang-worker-pinned"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-b")}

	done, accepted := f.pass(t, uuid)
	if !done || !accepted {
		t.Fatalf("worker-pinned gang must be rejected terminally on the fresh pass: done=%v accepted=%v record=%+v",
			done, accepted, *f.record(t, uuid))
	}
	assertRecordFailed(t, f, uuid, "NodeAffinity requires")
	if s := findInstanceStatusOnIRForFixture(t, f, 1); s != nil {
		t.Errorf("rejection must land before the surge stamp; found surge status %+v", s)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Errorf("source must be untouched (Ready, no op); got %+v", src)
	}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == "1" {
			t.Errorf("no surge pod may exist after a pre-stamp rejection; found %s", pod.Name)
		}
	}
}

// surgePodHasHintTerm reports whether pod carries a preferred
// node-affinity term listing exactly the hint nodes.
func surgePodHasHintTerm(pod *corev1.Pod, hints []string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return false
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		for _, expr := range term.Preference.MatchExpressions {
			if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpIn &&
				fmt.Sprintf("%v", expr.Values) == fmt.Sprintf("%v", hints) {
				return true
			}
		}
	}
	return false
}

// TestMigrate_DeferWithoutOwnership_LeavesRecordAccepted pins the
// mid-op defer contract: a source that is not steady-Ready (in-flight
// Update) defers (done=false, accepted=false) and the record stays
// Accepted for the next pass.
func TestMigrate_DeferWithoutOwnership_LeavesRecordAccepted(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-defer"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	ir := f.getIR(t)
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Updating source: %v", err)
	}

	done, accepted := f.pass(t, uuid)
	if done || accepted {
		t.Fatalf("mid-Update source must defer without ownership: done=%v accepted=%v", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseAccepted || rec.SurgeInstance != nil {
		t.Errorf("deferred record must stay Accepted with no surge; got %+v", *rec)
	}
}

// assertRecordFailed asserts the record is terminal-Failed carrying the
// rejection reason + CompletedAt and the ledger holds a terminal row.
func assertRecordFailed(t *testing.T, f *migFixture, uuid, wantMsg string) {
	t.Helper()
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record phase = %s, want Failed", rec.Phase)
	}
	if rec.CompletedAt == nil {
		t.Errorf("Failed record must carry CompletedAt")
	}
	if !strings.Contains(rec.Message, wantMsg) {
		t.Errorf("record Message = %q, want substring %q", rec.Message, wantMsg)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("terminal Failed ledger row must exist for %s", uuid)
	}
}

// findInstanceStatusOnIRForFixture reads the persisted InstanceStatus
// for idx off the fixture's IR, or nil.
func findInstanceStatusOnIRForFixture(t *testing.T, f *migFixture, idx int32) *v1beta1.OMENativeInstanceStatus {
	t.Helper()
	ir := f.getIR(t)
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == idx {
			return &ir.Status.InstanceStatuses[i]
		}
	}
	return nil
}

// TestStampMigrationStatus_SourceSlotMissing_NotResurrected pins the
// fresh-empty-slot guard on the SOURCE side of the pair stamp: writing
// the Migrating stamp for an index whose InstanceStatus is gone must
// not append a phantom slot (the append path seeds Phase==""). The
// surge side legitimately seeds its slot and must keep doing so.
func TestStampMigrationStatus_SourceSlotMissing_NotResurrected(t *testing.T) {
	f := newSinglePodMigFixture(t)
	in := f.input(t)

	if err := patchInstanceStatusMigrating(context.Background(), in, 7, 8, "uuid-guard", time.Minute); err != nil {
		t.Fatalf("patchInstanceStatusMigrating: %v", err)
	}
	if s := findInstanceStatusOnIRForFixture(t, f, 7); s != nil {
		t.Fatalf("source stamp resurrected a removed slot: %+v", s)
	}

	if err := patchInstanceStatusMigrationSurge(context.Background(), in, 8, 7, "uuid-guard", time.Minute); err != nil {
		t.Fatalf("patchInstanceStatusMigrationSurge: %v", err)
	}
	s := findInstanceStatusOnIRForFixture(t, f, 8)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating {
		t.Fatalf("surge stamp must still seed its slot; got %+v", s)
	}
}

// TestMigrate_GangResume_SurgeSlotUnstamped_RequeuesBeforePods pins the
// PodGroup-before-pods ordering on RESUME: crash window after the
// record's SurgeInstance write but before the pair stamps. The resume
// pass re-ensures the stamps and must requeue — EnsurePodGroups ran
// this pass without the surge index in the plan, so creating the surge
// gang's pods in the same pass would beat their PodGroup and park the
// gang at the coscheduler.
func TestMigrate_GangResume_SurgeSlotUnstamped_RequeuesBeforePods(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-gang-resume"
	rec := mkMigRecord(uuid, 0, "node-a")
	si := int32(1)
	rec.SurgeInstance = &si
	rec.Phase = workload.MigrationPhaseSurgePending
	f.records = []workload.MigrationRecord{rec}

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("resume pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	if s := findInstanceStatusOnIRForFixture(t, f, 1); s == nil {
		t.Fatalf("resume pass must re-ensure the surge stamp")
	}
	surgePods, err := query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("list surge pods: %v", err)
	}
	if len(surgePods) != 0 {
		t.Fatalf("surge pods created in the same pass as the re-ensured stamp: got %d, want 0 (requeue first)", len(surgePods))
	}

	// The next pass observes the stamped slot and creates the gang.
	done, accepted = f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("post-requeue pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	surgePods, err = query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("re-list surge pods: %v", err)
	}
	if len(surgePods) == 0 {
		t.Fatalf("surge gang must be created on the pass after the requeue")
	}
}

// TestMigrate_SourceStuckTerminating_EscalationRuns pins the exit path
// for the migration's primary use case: the drained source pod wedges
// Terminating (here finalizer-pinned) and the drive tail must run the
// stuck-teardown escalation instead of silently requeuing forever.
// Finalizer-pinned evidence is report-only, so the observable is the
// once-per-UID ForceDelete ledger marker.
func TestMigrate_SourceStuckTerminating_EscalationRuns(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	f.forceDelete = fdPolicy()
	const uuid = "mig-stuck-term"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	driveToDrainingDrainIncomplete(t, f, uuid)
	f.react(t) // drain settles — the tail is one delete away

	srcPodName := query.PodName(f.isvc.Name, f.component, 0, "default", 0)
	srcPod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	srcPod.Finalizers = append(srcPod.Finalizers, "test.ome.io/block")
	if err := f.c.Update(context.Background(), srcPod); err != nil {
		t.Fatalf("pin source pod: %v", err)
	}
	if err := f.c.Delete(context.Background(), srcPod); err != nil {
		t.Fatalf("delete source pod: %v", err)
	}
	// Past the pod's own deletion deadline plus the policy's slack.
	clk.Step(10 * time.Minute)

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("stuck pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}

	found := false
	for _, e := range fdLedgerEntries(t, f.c, f.isvc) {
		if e.Reason == audit.ReasonForceDelete && e.Outcome == audit.OutcomeForceDeleteFinalizerReport {
			found = true
		}
	}
	if !found {
		t.Fatalf("Migrate's delete tail must run the stuck-Terminating escalation (no ForceDelete report ledger row)")
	}
}
