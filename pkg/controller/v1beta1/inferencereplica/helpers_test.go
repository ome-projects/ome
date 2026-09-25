package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// newCountingStatusClient builds a fake client over objs that counts
// status-subresource Update calls, optionally failing the first
// failFirst of them with a Conflict.
func newCountingStatusClient(t *testing.T, failFirst int, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	writes := new(int)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			assertCompactInferenceReplicaWrite(t, obj)
			*writes++
			if *writes <= failFirst {
				return apierrors.NewConflict(
					schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
					obj.GetName(), fmt.Errorf("the object has been modified"))
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build(), writes
}

func assertCompactInferenceReplicaWrite(t *testing.T, obj client.Object) {
	t.Helper()
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return
	}
	for _, status := range ir.Status.InstanceStatuses {
		if status.ReadyPodCount != 0 || status.ScheduledPodCount != 0 || status.NodesOccupied != nil {
			t.Fatalf("status write for Instance %d persisted Pod-derived observations: %+v", status.Index, status)
		}
	}
	body, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal status write: %v", err)
	}
	for _, field := range []string{"readyPodCount", "scheduledPodCount", "nodesOccupied"} {
		if strings.Contains(string(body), `"`+field+`"`) {
			t.Fatalf("status write serialized compatibility field %q", field)
		}
	}
}

type staleReadingClient struct {
	client.Client
	reader client.Reader
}

func (c *staleReadingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reader.Get(ctx, key, obj, opts...)
}

func fullyPopulatedInstanceStatus(idx int32) v1beta1.OMENativeInstanceStatus {
	started := metav1.NewTime(time.Date(2026, time.February, 1, 10, 0, 0, 0, time.UTC))
	progress := metav1.NewTime(started.Add(2 * time.Minute))
	deadline := metav1.NewTime(started.Add(30 * time.Minute))
	transition := metav1.NewTime(started.Add(time.Minute))
	surge := idx + 100
	exitCode := int32(137)
	return v1beta1.OMENativeInstanceStatus{
		Index:             idx,
		Incarnation:       42,
		Phase:             v1beta1.OMENativeInstanceRestarting,
		RunningRevision:   "rev-running",
		TargetRevision:    "rev-target",
		PodCount:          8,
		ReadyPodCount:     7,
		ServingPodCount:   6,
		AvailablePodCount: 5,
		ScheduledPodCount: 8,
		Admitted:          true,
		NodesOccupied:     []string{"node-a", "node-b"},
		Conditions: []metav1.Condition{{
			Type:               "AllPodsReady",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: 17,
			LastTransitionTime: transition,
			Reason:             "PodPending",
			Message:            "one worker is still starting",
		}},
		Operation: &v1beta1.InstanceOperation{
			ID:              fmt.Sprintf("migrate-%d", idx),
			Type:            v1beta1.InstanceOperationMigrate,
			Step:            "WaitReady",
			StartedAt:       started,
			LastProgressAt:  progress,
			Deadline:        deadline,
			RetryCount:      3,
			TargetRevision:  "rev-target",
			Reason:          "node-recovery",
			SurgeIndex:      &surge,
			FromNode:        "node-old",
			HintTargetNodes: []string{"node-new-a", "node-new-b"},
			RequestUUID:     fmt.Sprintf("request-%d", idx),
		},
		ActiveOrdinal: 1,
		LastFailure: &v1beta1.InstanceTermination{
			PodName:       fmt.Sprintf("engine-%d-worker-6", idx),
			ContainerName: "main",
			Reason:        "OOMKilled",
			ExitCode:      &exitCode,
			Message:       "out of memory",
			Time:          transition,
		},
	}
}

func instanceStatusAt(t *testing.T, ir *v1beta1.InferenceReplica, idx int32) v1beta1.OMENativeInstanceStatus {
	t.Helper()
	for _, status := range ir.Status.InstanceStatuses {
		if status.Index == idx {
			return status
		}
	}
	t.Fatalf("InstanceStatus index %d not found", idx)
	return v1beta1.OMENativeInstanceStatus{}
}

func irStatusUpdateMetric(t *testing.T, result string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "ome_isvc_status_update_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricLabelsMatch(metric, map[string]string{"controller": obsmetrics.ControllerIR, "result": result}) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func metricLabelsMatch(metric *dto.Metric, expected map[string]string) bool {
	if len(metric.GetLabel()) != len(expected) {
		return false
	}
	for _, label := range metric.GetLabel() {
		if expected[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}

// countingReader records every Get so a test can assert which reader a
// conflict-retry closure re-read through.
type countingReader struct {
	client.Reader
	gets int
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.gets++
	return r.Reader.Get(ctx, key, obj, opts...)
}

// columnarTwin stores the same logical rows as ir in the ColumnarV2
// representation, exactly as a ColumnarV2-target writer would persist them.
func columnarTwin(t *testing.T, ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	t.Helper()
	twin := ir.DeepCopy()
	columns, err := irstatus.EncodeColumns(twin.Status.InstanceStatuses, uint64(len(twin.Status.InstanceStatuses)))
	if err != nil {
		t.Fatalf("encode columns: %v", err)
	}
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	twin.Status.InstanceStatuses = nil
	twin.Status.InstanceStatusEncoding = &encoding
	twin.Status.InstanceStatusColumns = columns
	return twin
}

// decodeFixtureIR is a two-replica IR with one Ready Instance on a full
// revision name and one Failed Instance carrying a failure record, the mix
// the reader-side decisions branch on.
func decodeFixtureIR() *v1beta1.InferenceReplica {
	ir := baselineIR("llama-engine", "default", 2)
	now := metav1.NewTime(metav1.Now().Truncate(1e9))
	ir.Status.Replicas = 2
	ir.Status.ReadyReplicas = 1
	ir.Status.CurrentRevision = "llama-engine-aaaaaaaa"
	ir.Status.UpdateRevision = "llama-engine-bbbbbbbb"
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "llama-engine-aaaaaaaa", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true, ReadySince: &now},
		{Index: 1, Incarnation: 2, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "llama-engine-aaaaaaaa", TargetRevision: "llama-engine-bbbbbbbb", PodCount: 1,
			LastFailure: &v1beta1.InstanceTermination{PodName: "llama-engine-1", ContainerName: "ome-container", Reason: "Error", Time: now}},
	}
	return ir
}

// readerDecisions collects every decision the InferenceReplica reconciler
// derives from decoded rows without performing an effect.
type readerDecisions struct {
	observed       any
	anyFailed      bool
	ready          metav1.Condition
	stalled        metav1.Condition
	liveRevisions  []string
	stagedAtPartit bool
}

func decisionsFor(t *testing.T, r *Reconciler, key client.ObjectKey) readerDecisions {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), r.cachedReader(), key, ir); err != nil {
		t.Fatalf("GetDecoded: %v", err)
	}
	partition := int32(1)
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: &partition}
	ready := computeReadyCondition(&ir.Status, ir.Spec.Replicas, ir.Spec.Lifecycle, ir.Spec.Pacing)
	stalled := computeRolloutStalledCondition(&ir.Status)
	ready.LastTransitionTime, stalled.LastTransitionTime = metav1.Time{}, metav1.Time{}
	live := revision.CollectLiveRevisionNames(ir.Status.CurrentRevision, ir.Status.UpdateRevision, observedFromIR(ir).InstanceStatuses)
	sort.Strings(live)
	return readerDecisions{
		observed:       observedFromIR(ir),
		anyFailed:      hasFailedInstance(ir.Status.InstanceStatuses),
		ready:          ready,
		stalled:        stalled,
		liveRevisions:  live,
		stagedAtPartit: stagedAtPartition(&ir.Status, ir.Spec.Lifecycle, ir.Spec.Pacing),
	}
}

// migrationTestNow is the fixed instant every consume test's fake
// clock reads. Arbitrary but stable so Deadline assertions are exact.
var migrationTestNow = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

const migrationTestTimeout = 42 * time.Minute

// withMigrationCapacityConfig wires a fake clientset serving an
// inferenceservice-config ConfigMap whose lifecycle block carries the
// migration capacity caps, so a reconcile-level test admits migrations
// the way a chart-configured deployment does.
func withMigrationCapacityConfig(r *Reconciler) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inferenceservice-config",
			Namespace: "ome",
		},
		Data: map[string]string{
			"lifecycle": `{"audit":{"maxInFlightMigrations":3,"maxMigrationsPerWindow":10,"window":"1h"}}`,
		},
	}
	r.Clientset = kubefake.NewSimpleClientset(cm)
}

// migrationParent builds the parent ISVC ("llama", matching baselineIR's
// ParentRef) carrying the given annotations. withDecoder adds a Decoder
// block so sibling-component routing can be exercised.
func migrationParent(annotations map[string]string, withDecoder bool) *v1beta1.InferenceService {
	parent := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "llama",
			Namespace:   "default",
			UID:         types.UID("llama-isvc-uid"),
			Annotations: annotations,
		},
	}
	if withDecoder {
		parent.Spec.Decoder = &v1beta1.DecoderSpec{}
	}
	return parent
}

// acceptedEntry is the shape the accept pass persists.
func acceptedEntry(uuid string) v1beta1.MigrationStatus {
	return v1beta1.MigrationStatus{
		RequestUUID:    uuid,
		Trigger:        v1beta1.MigrationTriggerManual,
		Phase:          v1beta1.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       "node-a",
		Reason:         "maintenance",
		StartedAt:      metav1.NewTime(migrationTestNow),
		Deadline:       metav1.NewTime(migrationTestNow.Add(migrationTestTimeout)),
	}
}

// testScheme returns a k8sruntime.Scheme with the types the IR reconciler
// exercises registered: v1beta1 (with status subresource for IR),
// corev1 (Pods, EndpointSlices), appsv1 (ControllerRevisions),
// discoveryv1 (EndpointSlices), and schedulingv1alpha1 (PodGroups).
func testScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	s := k8sruntime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("v1beta1.AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("appsv1.AddToScheme: %v", err)
	}
	if err := discoveryv1.AddToScheme(s); err != nil {
		t.Fatalf("discoveryv1.AddToScheme: %v", err)
	}
	if err := schedulingv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("schedulingv1alpha1.AddToScheme: %v", err)
	}
	return s
}

// baselineIR returns a controller-write-stamped, single-pod
// InferenceReplica matching the shape the ISVC controller will project.
// Single Runner named "default" with size 1, one container, no
// lifecycle defaults (workload.BuildPlan applies them).
func baselineIR(name, namespace string, replicas int32) *v1beta1.InferenceReplica {
	r := replicas
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			Generation: 1,
			// Controller-owner = parent ISVC. The IR reconciler reads
			// this to derive scopeUID for revision partitioning (parent
			// ISVC UID). The projector stamps the live ISVC; here we
			// stamp a synthetic UID matching the well-known parent name.
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       "llama",
				UID:        types.UID("llama-isvc-uid"),
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{
				Name: "llama",
			},
			Component: v1beta1.EngineComponent,
			Replicas:  &r,
			Runners: []v1beta1.Runner{
				{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
						},
					},
				},
			},
		},
	}
}

// newReconciler builds a fake-client-backed Reconciler with the IR
// status subresource wired and a fresh Expectations cache so per-test
// create attempts aren't blocked by entries from a previous test.
const testScaleDownRequeueInterval = 37 * time.Second

func newReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithIndex(&schedulingv1alpha1.PodGroup{}, workloadgang.PodGroupControllerUIDIndexField, workloadgang.PodGroupControllerUIDIndexExtractor).
		Build()
	return &Reconciler{
		Client:                   c,
		APIReader:                c,
		Log:                      logf.Log.WithName("test"),
		InstanceStatusTarget:     irstatus.EncodingDenseV1,
		Expectations:             workloadtypes.NewExpectations(),
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
	}, c
}

type podListFailingReader struct {
	client.Reader
}

func (r podListFailingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return errors.New("injected live pod list failure")
	}
	return r.Reader.List(ctx, list, opts...)
}

// podForIR builds a fake pod matching what workload/ops/render would
// produce for a given (IR, instance, runner, ordinal) tuple. The
// label set mirrors render.go's podLabels(); the pod's
// ContainersReady + ome.io/serving conditions are toggled by the
// (ready, serving) booleans.
//
// Owner ref points at the IR (Kind=InferenceReplica) so the
// expectation cache + workload-side query.ListOMENativePods see this
// as a real workload pod, not a foreign one.
func podForIR(ir *v1beta1.InferenceReplica, instanceIdx int32, runnerName string, ordinal int32, ready, serving bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(ir.Spec.ParentRef.Name, v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), instanceIdx, runnerName, ordinal),
			Namespace: ir.Namespace,
			UID:       types.UID(fmt.Sprintf("%s-%d-%s-%d-uid", ir.Name, instanceIdx, runnerName, ordinal)),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
				constants.OMEComponentLabel:           string(ir.Spec.Component),
				query.LabelInstanceIdx:                intToLabel(int64(instanceIdx)),
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     runnerName,
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 intToLabel(int64(ordinal)),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       ir.Name,
				UID:        ir.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
		},
	}
	now := metav1.Now()
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.ContainersReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               query.ServingConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	return pod
}

// sliceForIRPod constructs an EndpointSlice carrying one endpoint for
// pod against the IR's per-Component headless Service. Used by status
// tests that need AvailableReplicas to mirror ReadyReplicas — the
// aggregator reads availability off the EndpointSlice (same
// as the omenative direct path), so without a slice every pod is
// invisible to the availability counter regardless of ContainersReady.
//
// Returns a slice with Endpoints[0].Conditions.Ready set to ready —
// the same toggle the omenative sliceWithEndpoint helper exposes.
// AddressType=IPv4 + a fixed bogus address keep the fake-client
// validation happy; the controller's availability counter only reads
// TargetRef.Name + Ready, not the IP.
func sliceForIRPod(ir *v1beta1.InferenceReplica, pod *corev1.Pod, ready bool) *discoveryv1.EndpointSlice {
	serviceName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name + "-slice",
			Namespace: ir.Namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(ready),
				},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			},
		},
	}
}

// intToLabel formats a non-negative int64 as the label-safe ASCII
// string the workload-side pod label helpers produce.
func intToLabel(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// newReconcilerWithGrace returns a reconciler with a fake clientset that
// supplies the given stuck-pod grace period, so fast-escalation tests work
// without wiring a real config cache.
func newReconcilerWithGrace(t *testing.T, grace time.Duration, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	r, c := newReconciler(t, objs...)

	// Wire a fake clientset + config cache that resolves the grace. The
	// readiness window rides along: it has no in-code default, so a fixture
	// that expects operation deadlines must supply one as a deployment does.
	lifecycleCfg := fmt.Sprintf(`{"stuckPodGracePeriod":"%s","instanceReadyTimeout":"30m"}`, grace.String())
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inferenceservice-config",
			Namespace: "ome",
		},
		Data: map[string]string{
			"lifecycle": lifecycleCfg,
		},
	}
	fakeCS := kubefake.NewSimpleClientset(cm)
	r.Clientset = fakeCS
	r.ConfigCache = controllerconfig.NewConfigCache(0) // zero TTL = always refetch

	return r, c
}

// targetRevisionNameFor returns the ControllerRevision name the reconciler
// derives from ir.Spec, by running one pass over a scratch copy that owns
// no pods. Pod fixtures use it to carry the revision-hash label the
// renderer stamps on every pod the engine writes.
func targetRevisionNameFor(t *testing.T, ir *v1beta1.InferenceReplica) string {
	t.Helper()
	scratch := ir.DeepCopy()
	r, c := newReconciler(t, scratch)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: scratch.Name, Namespace: scratch.Namespace},
	}); err != nil {
		t.Fatalf("scratch reconcile to materialize the target revision: %v", err)
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: scratch.Name, Namespace: scratch.Namespace}, got); err != nil {
		t.Fatalf("read back the scratch IR: %v", err)
	}
	if got.Status.UpdateRevision == "" {
		t.Fatalf("scratch reconcile did not record an UpdateRevision")
	}
	return got.Status.UpdateRevision
}

// testRevisionRetention is the retention cap the tests configure —
// via the fake operator ConfigMap (config-default path) or the IR spec
// (annotation-projection path). The binary itself carries no default.
const testRevisionRetention = 10

// seedControllerRevision builds a ControllerRevision carrying the IR's
// revision Key label set and a monotonic .Revision number, named like a
// real CR (`<parent>-<component>-<suffix>`). Seeded directly into the
// fake client so the retention sweep has history to trim. Data is left
// empty — retention keys only on name + .Revision + the live-name union,
// never on the payload.
func seedControllerRevision(ir *v1beta1.InferenceReplica, suffix string, rev int64) *appsv1.ControllerRevision {
	key := irRevisionKey(ir)
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      revision.Name(key, suffix),
			Namespace: ir.Namespace,
			Labels:    revision.Labels(key),
		},
		Revision: rev,
	}
}

// listRevisionNames returns the surviving CR names in the IR's namespace
// for assertion readability.
func listRevisionNames(t *testing.T, c client.Client, namespace string) map[string]struct{} {
	t.Helper()
	list := &appsv1.ControllerRevisionList{}
	if err := c.List(context.Background(), list, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list ControllerRevisions: %v", err)
	}
	out := make(map[string]struct{}, len(list.Items))
	for _, cr := range list.Items {
		out[cr.Name] = struct{}{}
	}
	return out
}

// mt returns a whole-second, local-zone metav1.Time so values compare
// DeepEqual across the fake client's serialization round-trip (metav1
// truncates sub-second precision and unmarshals into the local zone).
func mt(hour, min int) *metav1.Time {
	t := metav1.NewTime(time.Date(2026, 7, 22, hour, min, 0, 0, time.UTC).Local())
	return &t
}

const (
	selectionFixtureRevision      = "example-engine-2f32f6fe"
	selectionFixtureOtherRevision = "example-engine-9b1c0d2e"
)

var selectionFixtureTime = metav1.NewTime(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))

// uniformFixtureRows mirrors the codec package's steady-state fixture: every
// row Ready on one revision at incarnation 1 with single-pod counts and
// admission.
func uniformFixtureRows(n int) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, n)
	for i := range rows {
		rows[i] = v1beta1.OMENativeInstanceStatus{
			Index:             int32(i),
			Incarnation:       1,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   selectionFixtureRevision,
			PodCount:          1,
			ServingPodCount:   1,
			AvailablePodCount: 1,
			Admitted:          true,
		}
	}
	return rows
}

// fixtureIRWithRows is a baseline IR whose status carries rows.
func fixtureIRWithRows(name string, rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	ir := baselineIR(name, "default", int32(len(rows)))
	ir.Status.Replicas = int32(len(rows))
	ir.Status.CurrentRevision = selectionFixtureRevision
	ir.Status.UpdateRevision = selectionFixtureRevision
	ir.Status.InstanceStatuses = rows
	return ir
}

func writeLabels(encoding, result string) map[string]string {
	return map[string]string{"encoding": encoding, "result": result}
}

// storedEncoding reads the raw stored object and classifies its representation.
func storedEncoding(t *testing.T, c client.Client, key client.ObjectKey) (*v1beta1.InferenceReplica, irstatus.Encoding) {
	t.Helper()
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), key, stored); err != nil {
		t.Fatalf("get stored IR: %v", err)
	}
	encoding, err := irstatus.ObservedEncoding(&stored.Status)
	if err != nil {
		t.Fatalf("stored object does not carry exactly one representation: %v", err)
	}
	return stored, encoding
}

func listPods(t *testing.T, c client.Client, namespace string) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

// newColumnarReconciler is newReconciler under the ColumnarV2 target with the
// package's test row bound.
func newColumnarReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	r, c := newReconciler(t, objs...)
	r.InstanceStatusTarget = irstatus.EncodingColumnarV2
	r.InstanceStatusDecoder = irstatus.NewDecoder(testColumnarBound)
	return r, c
}

// capturingRecorder records every event with the object it was attached to.
type capturingRecorder struct {
	events []capturedEvent
}

type capturedEvent struct {
	object  k8sruntime.Object
	kind    string
	reason  string
	message string
}

func (r *capturingRecorder) Event(object k8sruntime.Object, eventtype, reason, message string) {
	r.events = append(r.events, capturedEvent{object: object, kind: eventtype, reason: reason, message: message})
}

func (r *capturingRecorder) Eventf(object k8sruntime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *capturingRecorder) AnnotatedEventf(object k8sruntime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Eventf(object, eventtype, reason, messageFmt, args...)
}

// irStatusMetric reads one sample of a write-boundary metric from the
// controller-runtime registry; zero when the series does not exist.
func irStatusMetric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !metricLabelsMatch(metric, labels) {
				continue
			}
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}
			if metric.Gauge != nil {
				return metric.Gauge.GetValue()
			}
		}
	}
	return 0
}

func writeResultLabels(result string) map[string]string {
	return map[string]string{"encoding": obsmetrics.IRStatusEncodingDenseV1, "result": result}
}

// testStatusWriter is the persistence boundary over a bare fake client under
// the DenseV1 target: no row bound (every object under test is DenseV1) and
// no recorder.
func testStatusWriter(c client.Client) statusWriter {
	return statusWriter{Client: c, target: irstatus.EncodingDenseV1}
}

// columnarStatusWriter is the persistence boundary under the ColumnarV2
// target with a row bound large enough for every fixture in this package.
func columnarStatusWriter(c client.Client) statusWriter {
	return statusWriter{Client: c, decoder: irstatus.NewDecoder(testColumnarBound), target: irstatus.EncodingColumnarV2}
}

// testColumnarBound is the ColumnarV2 decode bound the ColumnarV2-target
// fixtures in this package run under; it exceeds the largest fixture row
// count so the bound never decides a selection here.
const testColumnarBound = 8192

// withLifecycleConfig wires a fake clientset + zero-TTL config cache
// serving the given lifecycle JSON so the teardown/force-delete config
// resolvers see it.
func withLifecycleConfig(r *Reconciler, lifecycleJSON string) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-config", Namespace: "ome"},
		Data:       map[string]string{"lifecycle": lifecycleJSON},
	}
	r.Clientset = kubefake.NewSimpleClientset(cm)
	r.ConfigCache = controllerconfig.NewConfigCache(0)
}

// drainEvents empties a FakeRecorder's channel.
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

func eventsContaining(events []string, substr string) []string {
	var out []string
	for _, e := range events {
		if strings.Contains(e, substr) {
			out = append(out, e)
		}
	}
	return out
}

type teardownListReader struct {
	client.Reader
	podLists      int
	podGroupLists int
	podError      error
	podGroupError error
}

func (r *teardownListReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	switch list.(type) {
	case *corev1.PodList:
		r.podLists++
		if r.podError != nil {
			return r.podError
		}
	case *schedulingv1alpha1.PodGroupList:
		r.podGroupLists++
		if r.podGroupError != nil {
			return r.podGroupError
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

func ownedPodGroupForIR(ir *v1beta1.InferenceReplica, name string, index int32) *schedulingv1alpha1.PodGroup {
	return &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: ir.Namespace,
		Labels: map[string]string{
			query.LabelInstanceIdx: intToLabel(int64(index)),
		},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(
			ir, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))},
	}}
}

func scaleDownGaugeSeriesExists(t *testing.T, metricName, namespace, isvc, component string) bool {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather controller metrics: %v", err)
	}
	want := map[string]string{"namespace": namespace, "isvc": isvc, "component": component}
	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.Metric {
			if len(metric.Label) != len(want) {
				continue
			}
			matches := true
			for _, label := range metric.Label {
				if want[label.GetName()] != label.GetValue() {
					matches = false
					break
				}
			}
			if matches {
				return true
			}
		}
	}
	return false
}

func assertScaleDownGaugeSeries(t *testing.T, want bool, namespace, isvc, component string) {
	t.Helper()
	for _, name := range []string{
		"ome_omenative_scale_down_active_pods",
		"ome_omenative_scale_down_deferred_instances",
	} {
		if got := scaleDownGaugeSeriesExists(t, name, namespace, isvc, component); got != want {
			t.Errorf("metric %s series existence: got %t want %t", name, got, want)
		}
	}
}

// ledgerCMForOwner materializes a migration audit ConfigMap for the
// named ledger owner, the shape audit.PersistLedgerForOwner writes.
func ledgerCMForOwner(t *testing.T, ownerName, namespace string, ledger *audit.Ledger) *corev1.ConfigMap {
	t.Helper()
	raw, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ownerName + audit.ConfigMapNameSuffix, Namespace: namespace},
		Data:       map[string]string{audit.LedgerKey: string(raw)},
	}
}
