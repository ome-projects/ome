package irprojector

import (
	"context"
	"strings"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// baselineISVC returns a controller-write-ready ISVC carrying an
// EngineSpec with MinReplicas=2 and a Lifecycle block. The default
// per-Component shape is single-pod; tests that need multi-pod
// override Leader / Worker before calling the projector.
func baselineISVC(name, namespace string) *v1beta1.InferenceService {
	min := 2
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			UID:        types.UID(name + "-uid"),
			Generation: 1,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &min,
					Lifecycle: &v1beta1.LifecycleSpec{
						RestartPolicy: ptr.To(v1beta1.InstanceRestartPolicyNone),
					},
				},
			},
		},
	}
}

// minimalParams returns a Params bag matching the ISVC shape above.
// PodSpec carries a single container named "ome-container"; the
// per-Component ObjectMeta carries the legacy OMENative pod label trio
// so the IR's Spec.Runners[0].Template.ObjectMeta inherits it.
func minimalParams(t *testing.T, isvc *v1beta1.InferenceService, c client.Client) Params {
	t.Helper()
	objMeta := metav1.ObjectMeta{
		Name:      isvc.Name + "-engine",
		Namespace: isvc.Namespace,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			"managed-by":                          "OMENative",
		},
		Annotations: map[string]string{
			"ome.io/some-annotation": "value",
		},
	}
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  "ome-container",
			Image: "sgl:1.0",
		}},
	}
	return Params{
		ISVC:         isvc,
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		ObjectMeta:   objMeta,
		PodSpec:      podSpec,
		MultiPod:     false,
		Client:       c,
	}
}

// TestIsIRManagedComponent_OMENativeMode_True pins the default state
// post-cutover: an OMENative-mode Component routes through the
// IR-managed path automatically (no per-Component opt-in required).
func TestIsIRManagedComponent_OMENativeMode_True(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(IsIRManagedComponent(constants.OMENative)).To(gomega.BeTrue(),
		"OMENative-mode Component must route through the IR-managed path by default")
}

// TestIsIRManagedComponent_NonOMENativeMode_False pins the routing
// boundary: only OMENative-mode Components flow through the
// IR-managed path. RawDeployment, MultiNode, PDDisaggregated,
// VirtualDeployment all fall through to their own dispatch branches.
func TestIsIRManagedComponent_NonOMENativeMode_False(t *testing.T) {
	g := gomega.NewWithT(t)
	for _, mode := range []constants.DeploymentModeType{
		constants.RawDeployment,
		constants.MultiNode,
		constants.PDDisaggregated,
		constants.VirtualDeployment,
		constants.DeploymentModeType(""), // unresolved / unknown
	} {
		g.Expect(IsIRManagedComponent(mode)).To(gomega.BeFalse(),
			"non-OMENative mode %q must NOT route through the IR-managed path", mode)
	}
}

// projection is the world one row of TestEnsureInferenceReplica runs in: the
// parent InferenceService, the client whose store holds the replica, the key
// of the projected replica, the input of the first projection, and counters of
// the writes the projector issues.
type projection struct {
	*gomega.WithT
	t      *testing.T
	isvc   *v1beta1.InferenceService
	client client.WithWatch
	key    types.NamespacedName
	// input is what the first projection received, for a row that edits it
	// after the fact and checks the store did not follow.
	input   Params
	creates *fakeclient.Calls
	updates *fakeclient.Calls
	patches *fakeclient.Calls
}

// params is the projector's input for the parent as it is now, so a row edits
// the InferenceService between projections and projects it again.
func (s *projection) params() Params {
	s.t.Helper()
	return minimalParams(s.t, s.isvc, s.client)
}

// project runs the projector on p, requires it to succeed, and returns the
// replica read back from the store.
func (s *projection) project(p Params) *v1beta1.InferenceReplica {
	s.t.Helper()
	ir, err := EnsureInferenceReplica(context.Background(), p)
	s.Expect(err).NotTo(gomega.HaveOccurred())
	s.Expect(ir).NotTo(gomega.BeNil())
	return s.live()
}

// live reads the projected replica from the store.
func (s *projection) live() *v1beta1.InferenceReplica {
	s.t.Helper()
	got := &v1beta1.InferenceReplica{}
	s.Expect(s.client.Get(context.Background(), s.key, got)).To(gomega.Succeed())
	return got
}

// writes is the number of Create, Update and Patch calls the store has seen.
func (s *projection) writes() int {
	return s.creates.Count() + s.updates.Count() + s.patches.Count()
}

// ensureCase is one row of TestEnsureInferenceReplica: how the parent and the
// store look before the first projection, what that projection receives, and
// either the error it must return or the checks after it succeeds.
type ensureCase struct {
	name string
	// isvc edits the baseline parent before the first projection.
	isvc func(isvc *v1beta1.InferenceService)
	// objects are replicas already in the store.
	objects []client.Object
	// params edits the first projection's input.
	params func(p *Params)
	// wantErr is text the first projection's error must contain; empty means
	// the projection must succeed and check runs.
	wantErr string
	// check holds the row's assertions over the replica the first projection
	// left in the store; it projects again through s where the situation
	// moves on.
	check func(s *projection, ir *v1beta1.InferenceReplica)
}

// runEnsureCase builds the parent, the store and the counters, projects once
// with the row's input, checks the error, and hands the live replica to the
// row's check.
func runEnsureCase(t *testing.T, row ensureCase) {
	t.Helper()
	isvc := baselineISVC("llama", "prod")
	if row.isvc != nil {
		row.isvc(isvc)
	}
	s := &projection{
		WithT:   gomega.NewWithT(t),
		t:       t,
		isvc:    isvc,
		key:     types.NamespacedName{Name: "llama-engine", Namespace: "prod"},
		creates: fakeclient.CountingVerb(fakeclient.Create),
		updates: fakeclient.CountingVerb(fakeclient.Update),
		patches: fakeclient.CountingVerb(fakeclient.Patch),
	}
	s.client = fakeclient.New(t, fakeclient.WithObjects(row.objects...), s.creates, s.updates, s.patches)
	s.input = s.params()
	if row.params != nil {
		row.params(&s.input)
	}
	ir, err := EnsureInferenceReplica(context.Background(), s.input)
	if row.wantErr != "" {
		s.Expect(err).To(gomega.HaveOccurred())
		s.Expect(err.Error()).To(gomega.ContainSubstring(row.wantErr))
		return
	}
	s.Expect(err).NotTo(gomega.HaveOccurred())
	s.Expect(ir).NotTo(gomega.BeNil())
	row.check(s, s.live())
}

// threeWorkers makes the projection multi-pod: a leader from the primary
// PodSpec and three workers from their own template.
func threeWorkers(p *Params) {
	p.MultiPod = true
	p.WorkerSize = 3
	p.WorkerPodSpec = &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "worker:1.0"}},
	}
}

// autoscalerBlockA returns a distinctive KEDA-class ComponentAutoscaler
// for the projector autoscaler-propagation rows. The block carries a
// non-empty Triggers list + a non-default PollingInterval so a buggy
// shallow projection trips the deep-equality assertions.
func autoscalerBlockA() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaAutoscaler{
			Triggers: []kedav1.ScaleTriggers{
				{Type: "prometheus", Metadata: map[string]string{"v": "A"}},
			},
			PollingInterval: ptr.To(int32(15)),
		},
	}
}

// autoscalerBlockB returns a second, distinct ComponentAutoscaler so
// the update-replaces-whole-block row can distinguish "no projection"
// from "projection took". Differs from autoscalerBlockA in Class, Keda
// shape (nil), and HPA shape (non-nil) — the swap exercises every
// pointer in ComponentAutoscaler.
func autoscalerBlockB() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerHPA,
		HPA: &v1beta1.HPAAutoscaler{
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "cpu",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: ptr.To(int32(60)),
						},
					},
				},
			},
		},
	}
}

// hpaAutoscalerBlock returns a minimal HPA-class ComponentAutoscaler for
// the replica-preservation rows. The exact metric shape is irrelevant —
// only Class drives the projector's "is this Component autoscaler-managed"
// decision — so a bare {Class: HPA} is enough.
func hpaAutoscalerBlock() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
}

// TestEnsureInferenceReplica drives the projector over one parent
// InferenceService. Each row is a former single test, named for it, and keeps
// its assertions: the row seeds the parent and the store, projects once, and
// checks the live replica, projecting again where the situation moves on.
func TestEnsureInferenceReplica(t *testing.T) {
	rows := []ensureCase{
		// A first projection writes the whole replica: name, owner, parent,
		// component, replicas from MinReplicas, one default runner with the pod
		// template as given, and the lifecycle.
		{
			name: "Create_NewIR",
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Name).To(gomega.Equal("llama-engine"),
					"IR name must be <isvc>-<component>")
				s.Expect(got.Namespace).To(gomega.Equal("prod"))
				s.Expect(got.Annotations).To(gomega.HaveKeyWithValue(
					constants.InferenceReplicaControllerWriteAnnotationKey,
					constants.InferenceReplicaControllerWriteAnnotationVal),
					"controller-write annotation must be stamped so the IR webhook accepts the create")

				s.Expect(got.OwnerReferences).To(gomega.HaveLen(1))
				ref := got.OwnerReferences[0]
				s.Expect(ref.Kind).To(gomega.Equal("InferenceService"),
					"IR owner ref must point at the parent ISVC for cascade GC")
				s.Expect(ref.Name).To(gomega.Equal(s.isvc.Name))
				s.Expect(ref.UID).To(gomega.Equal(s.isvc.UID))
				s.Expect(ref.Controller).NotTo(gomega.BeNil())
				s.Expect(*ref.Controller).To(gomega.BeTrue())

				s.Expect(got.Spec.ParentRef.Name).To(gomega.Equal(s.isvc.Name))
				s.Expect(got.Spec.Component).To(gomega.Equal(v1beta1.EngineComponent))

				s.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
				s.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(2)),
					"Replicas must default from MinReplicas")

				s.Expect(got.Spec.Runners).To(gomega.HaveLen(1),
					"single-pod Component must emit one Runner")
				r0 := got.Spec.Runners[0]
				s.Expect(r0.Name).To(gomega.Equal(v1beta1.RunnerNameDefault))
				s.Expect(r0.Size).To(gomega.Equal(int32(1)))
				s.Expect(r0.Template.Spec.Containers).To(gomega.HaveLen(1))
				s.Expect(r0.Template.Spec.Containers[0].Image).To(gomega.Equal("sgl:1.0"),
					"PodSpec must be threaded through verbatim — no re-rendering")
				s.Expect(r0.Template.ObjectMeta.Labels).To(gomega.HaveKeyWithValue(
					constants.InferenceServicePodLabelKey, s.isvc.Name),
					"PodTemplate ObjectMeta labels must inherit from the projected ObjectMeta")

				s.Expect(got.Spec.Lifecycle).NotTo(gomega.BeNil())
				s.Expect(got.Spec.Lifecycle.RestartPolicy).NotTo(gomega.BeNil())
				s.Expect(*got.Spec.Lifecycle.RestartPolicy).To(gomega.Equal(
					v1beta1.InstanceRestartPolicyNone),
					"Lifecycle must be threaded through")
			},
		},
		// The parent's pause annotation projects as spec.paused with its
		// depth; a plain value clears the depth and removal clears the pause.
		{
			name: "ProjectsPauseAnnotationAndClearsOnRemoval",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
			},
			check: func(s *projection, paused *v1beta1.InferenceReplica) {
				s.Expect(paused.Spec.Paused).To(gomega.BeTrue(),
					"the controller-only IR must receive pause intent from its parent ISVC")

				s.isvc.Annotations[constants.PausedRolloutAnnotation] = constants.PausedRolloutFreezeValue
				frozen := s.project(s.params())
				s.Expect(frozen.Spec.Paused).To(gomega.BeTrue(),
					"the freeze value must still project as paused")
				s.Expect(frozen.Spec.PauseMode).To(gomega.Equal(v1beta1.PauseModeFreeze),
					"the freeze value must project the full-stop depth")

				s.isvc.Annotations[constants.PausedRolloutAnnotation] = "true"
				replain := s.project(s.params())
				s.Expect(replain.Spec.Paused).To(gomega.BeTrue())
				s.Expect(replain.Spec.PauseMode).To(gomega.BeEmpty(),
					"downgrading freeze to plain pause must clear the projected depth")

				delete(s.isvc.Annotations, constants.PausedRolloutAnnotation)
				unpaused := s.project(s.params())
				s.Expect(unpaused.Spec.Paused).To(gomega.BeFalse(),
					"removing the parent annotation must clear the projected circuit breaker")
				s.Expect(unpaused.Spec.PauseMode).To(gomega.BeEmpty())
			},
		},
		// A positive revision-history-limit annotation lands on the spec,
		// removal clears it (the operator config default applies), and a
		// malformed or non-positive value projects as nil.
		{
			name: "ProjectsRevisionHistoryLimit",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{constants.RevisionHistoryLimitAnnotation: "3"}
			},
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.RevisionHistoryLimit).To(gomega.HaveValue(gomega.Equal(int32(3))),
					"a positive annotation value must project onto Spec.RevisionHistoryLimit")

				delete(s.isvc.Annotations, constants.RevisionHistoryLimitAnnotation)
				cleared := s.project(s.params())
				s.Expect(cleared.Spec.RevisionHistoryLimit).To(gomega.BeNil(),
					"removing the annotation must clear the projection (config default applies)")

				for _, bad := range []string{"0", "-1", "many"} {
					s.isvc.Annotations = map[string]string{constants.RevisionHistoryLimitAnnotation: bad}
					out := s.project(s.params())
					s.Expect(out.Spec.RevisionHistoryLimit).To(gomega.BeNil(),
						"malformed or non-positive annotation value %q must project as nil, never be honored", bad)
				}
			},
		},
		// A second projection with the same input changes nothing the
		// projector owns, so the live replica keeps its ResourceVersion.
		{
			name: "Update_PreservesGeneration",
			check: func(s *projection, first *v1beta1.InferenceReplica) {
				firstRV := first.ResourceVersion
				second := s.project(s.params())
				s.Expect(second.Spec.Replicas).To(gomega.Equal(first.Spec.Replicas))
				s.Expect(second.Spec.Component).To(gomega.Equal(first.Spec.Component))
				s.Expect(second.ParentName()).To(gomega.Equal(s.isvc.Name))
				s.Expect(second.ResourceVersion).To(gomega.Equal(firstRV),
					"identical re-projection must not bump ResourceVersion (no-op guard)")
			},
		},
		// The first projection is one Create; a second with the same input
		// issues no Create, Update or Patch at all, so the replica's status
		// writer never meets a conflict the projector caused.
		{
			name: "NoChange_IssuesNoWrite",
			check: func(s *projection, _ *v1beta1.InferenceReplica) {
				s.Expect(s.writes()).To(gomega.Equal(1), "first projection should Create once")

				before := s.writes()
				s.project(s.params())
				s.Expect(s.writes()-before).To(gomega.Equal(0), "identical re-projection must issue no writes")
			},
		},
		// The replica carries the parent's generation, and a generation that
		// moves with no spec change still advances the annotation: the
		// Sequential gate reads it to tell an idle earlier Component from a
		// lagging one.
		{
			name: "StampsParentGeneration",
			isvc: func(isvc *v1beta1.InferenceService) { isvc.Generation = 3 },
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Annotations).To(gomega.HaveKeyWithValue(
					constants.InferenceReplicaParentGenerationAnnotationKey, "3"),
					"created IR must carry the parent ISVC generation")

				s.isvc.Generation = 4
				got = s.project(s.params())
				s.Expect(got.Annotations).To(gomega.HaveKeyWithValue(
					constants.InferenceReplicaParentGenerationAnnotationKey, "4"),
					"re-projection must advance the stamp even with no spec diff")
			},
		},
		// A changed desired spec is written as exactly one merge Patch, never
		// an Update, so a concurrent status write cannot turn it into an
		// optimistic-lock conflict.
		{
			name: "SpecChange_IssuesMergePatch",
			check: func(s *projection, _ *v1beta1.InferenceReplica) {
				patchesBefore, updatesBefore := s.patches.Count(), s.updates.Count()
				changed := s.params()
				changed.ComponentExt = &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(7)}
				got := s.project(changed)
				s.Expect(s.updates.Count()-updatesBefore).To(gomega.Equal(0), "projector must not use Update")
				s.Expect(s.patches.Count()-patchesBefore).To(gomega.Equal(1), "a real spec change must issue exactly one merge Patch")

				s.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
				s.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(7)))
			},
		},
		// A multi-pod Component projects a leader runner of size one from the
		// primary PodSpec and a worker runner of WorkerSize from the worker
		// template.
		{
			name:   "MultiPod_LeaderAndWorker",
			params: threeWorkers,
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Runners).To(gomega.HaveLen(2),
					"multi-pod Component must emit leader + worker Runners")
				s.Expect(got.Spec.Runners[0].Name).To(gomega.Equal(v1beta1.RunnerNameLeader))
				s.Expect(got.Spec.Runners[0].Size).To(gomega.Equal(int32(1)))
				s.Expect(got.Spec.Runners[0].Template.Spec.Containers[0].Image).To(gomega.Equal("sgl:1.0"),
					"leader Template must use the primary PodSpec")
				s.Expect(got.Spec.Runners[1].Name).To(gomega.Equal(v1beta1.RunnerNameWorker))
				s.Expect(got.Spec.Runners[1].Size).To(gomega.Equal(int32(3)),
					"worker Size must equal WorkerSize")
				s.Expect(got.Spec.Runners[1].Template.Spec.Containers[0].Image).To(gomega.Equal("worker:1.0"),
					"worker Template must use the WorkerPodSpec")
			},
		},
		// Multi-pod without a worker template is a wiring error at the
		// dispatch site; the projector refuses it instead of writing a
		// malformed replica.
		{
			name: "MultiPod_RejectsNilWorkerSpec",
			params: func(p *Params) {
				p.MultiPod = true
				p.WorkerSize = 3
			},
			wantErr: "WorkerPodSpec",
		},
		// A nil PodSpec is refused: the dispatch site must always render one.
		{
			name:    "NilPodSpec_Rejected",
			params:  func(p *Params) { p.PodSpec = nil },
			wantErr: "nil PodSpec",
		},
		// A nil ComponentExt is refused rather than letting Lifecycle and
		// Replicas fall back to defaults with no error for the dispatch site.
		{
			name:    "NilComponentExt_Rejected",
			params:  func(p *Params) { p.ComponentExt = nil },
			wantErr: "ComponentExtensionSpec",
		},
		// A nil client means the dispatch site dropped its client; the
		// projector names it.
		{
			name:    "NilClient_Rejected",
			params:  func(p *Params) { p.Client = nil },
			wantErr: "nil client",
		},
		// No MinReplicas projects one replica.
		{
			name: "DefaultsReplicasToOne",
			isvc: func(isvc *v1beta1.InferenceService) { isvc.Spec.Engine.MinReplicas = nil },
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
				s.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(1)),
					"nil MinReplicas must default to 1")
			},
		},
		// A replica another pass created under the projected name is read as
		// the live object and brought to the desired spec, not reported as a
		// failed create.
		{
			name: "AlreadyExists_PropagatedAsUpdate",
			objects: []client.Object{&v1beta1.InferenceReplica{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-engine",
					Namespace: "prod",
					Annotations: map[string]string{
						constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
					},
				},
				Spec: v1beta1.InferenceReplicaSpec{
					ParentRef: &v1beta1.ParentReference{Name: "llama"},
					Component: v1beta1.EngineComponent,
				},
			}},
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
				s.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(2)),
					"Update must propagate the projector's desired Replicas onto the live IR")

				// Sanity: not an apierror NotFound on the post-write Get.
				_, getErr := s.client.RESTMapper().RESTMapping(got.GroupVersionKind().GroupKind())
				s.Expect(apierrors.IsNotFound(getErr)).To(gomega.BeFalse(),
					"sanity: post-create Get must succeed")
			},
		},
		// The Component's own annotations reach the runner template even when
		// the dispatch site did not merge them into ObjectMeta: the revision
		// hash reads the template, so a missing annotation means no rollout.
		{
			name: "PropagatesComponentAnnotations",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Annotations = map[string]string{
					"test.ome.io/rollout-trigger": "2026-05-29T01:02:03.456789Z",
					"linkerd.io/inject":           "enabled",
				}
			},
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
				tmplAnns := got.Spec.Runners[0].Template.ObjectMeta.Annotations
				s.Expect(tmplAnns).To(gomega.HaveKeyWithValue(
					"test.ome.io/rollout-trigger", "2026-05-29T01:02:03.456789Z"),
					"rollout-trigger annotation must reach the Runner template — "+
						"the revision hash flips off this key, no propagation means no rollout")
				s.Expect(tmplAnns).To(gomega.HaveKeyWithValue(
					"linkerd.io/inject", "enabled"),
					"all per-Component annotations must propagate, not just the rollout-trigger")
				// Baseline ObjectMeta annotation also propagates (no regression).
				s.Expect(tmplAnns).To(gomega.HaveKeyWithValue(
					"ome.io/some-annotation", "value"),
					"existing ObjectMeta annotations must still propagate alongside ComponentExt")
			},
		},
		// The Component's own labels reach the runner template the same way,
		// for affinity, NetworkPolicy and kubectl selectors.
		{
			name: "PropagatesComponentLabels",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Labels = map[string]string{
					"team":        "ml-platform",
					"cost-center": "1234",
				}
			},
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
				tmplLabels := got.Spec.Runners[0].Template.ObjectMeta.Labels
				s.Expect(tmplLabels).To(gomega.HaveKeyWithValue("team", "ml-platform"),
					"per-Component labels must reach the Runner template for selector matching")
				s.Expect(tmplLabels).To(gomega.HaveKeyWithValue("cost-center", "1234"))
				// Baseline ObjectMeta labels also propagate.
				s.Expect(tmplLabels).To(gomega.HaveKeyWithValue(
					constants.InferenceServicePodLabelKey, s.isvc.Name),
					"existing ObjectMeta labels must still propagate alongside ComponentExt")
			},
		},
		// With no Component annotations or labels the template carries only
		// the ObjectMeta content: no empty maps, no nil dereference in the
		// merge.
		{
			name: "NilComponentAnnotationsAndLabels_NoRegression",
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(s.isvc.Spec.Engine.Annotations).To(gomega.BeNil(),
					"sanity: baseline fixture has no per-Component annotations")
				s.Expect(s.isvc.Spec.Engine.Labels).To(gomega.BeNil(),
					"sanity: baseline fixture has no per-Component labels")

				s.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
				// Only the minimalParams ObjectMeta content lands on the template.
				s.Expect(got.Spec.Runners[0].Template.ObjectMeta.Labels).To(gomega.HaveKeyWithValue(
					constants.InferenceServicePodLabelKey, s.isvc.Name),
					"baseline ObjectMeta labels must still propagate when ComponentExt is empty")
				s.Expect(got.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
					"ome.io/some-annotation", "value"),
					"baseline ObjectMeta annotations must still propagate when ComponentExt is empty")
			},
		},
		// On a multi-pod Component the annotations and labels reach both the
		// leader and the worker template; the renderer reads each for its own
		// pods.
		{
			name: "ComponentExt_PropagatesToBothRunners",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Annotations = map[string]string{
					"test.ome.io/rollout-trigger": "2026-05-29T04:05:06Z",
				}
				isvc.Spec.Engine.Labels = map[string]string{
					"team": "ml-platform",
				}
			},
			params: threeWorkers,
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Runners).To(gomega.HaveLen(2))
				for _, r := range got.Spec.Runners {
					s.Expect(r.Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
						"test.ome.io/rollout-trigger", "2026-05-29T04:05:06Z"),
						"runner %q template must carry per-Component rollout-trigger annotation", r.Name)
					s.Expect(r.Template.ObjectMeta.Labels).To(gomega.HaveKeyWithValue(
						"team", "ml-platform"),
						"runner %q template must carry per-Component team label", r.Name)
				}
			},
		},
		// When ObjectMeta and the Component declare the same key, the
		// Component's value wins, the same order the dispatch site merges in.
		{
			name: "ComponentExt_OverridesObjectMetaOnCollision",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Annotations = map[string]string{
					"ome.io/some-annotation": "from-component-ext",
				}
			},
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				// minimalParams sets ObjectMeta.Annotations["ome.io/some-annotation"] to "value".
				s.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
				s.Expect(got.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
					"ome.io/some-annotation", "from-component-ext"),
					"ComponentExt annotation MUST win over ObjectMeta on key collision (matches engine.go merge order)")
			},
		},
		// A changed Component annotation reaches the live replica's template
		// on the next projection: this is the rollout trigger that needs no
		// image change.
		{
			name: "ComponentAnnotationUpdate_RepatchesTemplate",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Annotations = map[string]string{
					"test.ome.io/rollout-trigger": "initial",
				}
			},
			check: func(s *projection, first *v1beta1.InferenceReplica) {
				s.Expect(first.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
					"test.ome.io/rollout-trigger", "initial"),
					"sanity: first projection lands the initial value")

				s.isvc.Spec.Engine.Annotations["test.ome.io/rollout-trigger"] = "bumped"
				second := s.project(s.params())
				s.Expect(second.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
					"test.ome.io/rollout-trigger", "bumped"),
					"annotation bump must repatch the IR runner template — "+
						"this is the canonical no-image-bump rollout trigger")
			},
		},
		// The resolved autoscaler lands on the replica whole at create time;
		// the autoscaler dispatch reads it from there.
		{
			name:   "PropagatesResolvedAutoscaler",
			params: func(p *Params) { p.ResolvedAutoscaler = autoscalerBlockA() },
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Autoscaler).NotTo(gomega.BeNil(),
					"non-nil p.ResolvedAutoscaler must land on ir.Spec.Autoscaler at Create — "+
						"the autoscaler dispatch reads this field")
				s.Expect(got.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockA()),
					"projection must be deep-equal — Class + Keda + HPA all preserved verbatim")
			},
		},
		// A different resolved autoscaler replaces the whole block, never
		// merges into it, so the replica is never half of each.
		{
			name:   "UpdateAutoscalerRepatchesIR",
			params: func(p *Params) { p.ResolvedAutoscaler = autoscalerBlockA() },
			check: func(s *projection, first *v1beta1.InferenceReplica) {
				s.Expect(first.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockA()),
					"sanity: first projection lands block A")

				p2 := s.params()
				p2.ResolvedAutoscaler = autoscalerBlockB()
				second := s.project(p2)
				s.Expect(second.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockB()),
					"second projection must REPLACE the whole block — not merge — so block B fully wins")
				s.Expect(second.Spec.Autoscaler.Class).To(gomega.Equal(v1beta1.AutoscalerHPA),
					"Class must flip from KEDA to HPA")
				s.Expect(second.Spec.Autoscaler.Keda).To(gomega.BeNil(),
					"Keda block must be cleared — block B doesn't have one")
				s.Expect(second.Spec.Autoscaler.HPA).NotTo(gomega.BeNil(),
					"HPA block must be set — block B has one")
			},
		},
		// No resolved autoscaler projects no block: no panic, no empty
		// {Class: ""} the dispatch cannot read.
		{
			name: "NilResolvedAutoscaler_NoRegression",
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Autoscaler).To(gomega.BeNil(),
					"nil ResolvedAutoscaler must project to nil ir.Spec.Autoscaler — "+
						"no spurious empty block")
			},
		},
		// Under an autoscaler the replica's spec.replicas belongs to the
		// autoscaler: a re-projection keeps the count it wrote through the
		// scale subresource instead of putting MinReplicas back.
		{
			name:   "AutoscalerManaged_PreservesLiveReplicas",
			params: func(p *Params) { p.ResolvedAutoscaler = hpaAutoscalerBlock() },
			check: func(s *projection, created *v1beta1.InferenceReplica) {
				s.Expect(created.Spec.Replicas).NotTo(gomega.BeNil())
				s.Expect(*created.Spec.Replicas).To(gomega.Equal(int32(2)),
					"create must stamp MinReplicas as the initial desired count")

				// The autoscaler scales the replica to 8 through /scale.
				const scaledReplicas int32 = 8
				created.Spec.Replicas = ptr.To(scaledReplicas)
				s.Expect(s.client.Update(context.Background(), created)).To(gomega.Succeed())

				p2 := s.params()
				p2.ResolvedAutoscaler = hpaAutoscalerBlock()
				after := s.project(p2)
				s.Expect(after.Spec.Replicas).NotTo(gomega.BeNil())
				s.Expect(*after.Spec.Replicas).To(gomega.Equal(scaledReplicas),
					"autoscaler-managed Component must PRESERVE the live (autoscaler-written) "+
						"spec.replicas across reconciles -- re-stamping MinReplicas clobbers HPA/KEDA")
			},
		},
		// With autoscaling off the InferenceService owns the count: a drifted
		// spec.replicas goes back to MinReplicas on the next projection.
		{
			name: "AutoscalingOff_AppliesMinReplicas",
			params: func(p *Params) {
				p.ResolvedAutoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
			},
			check: func(s *projection, created *v1beta1.InferenceReplica) {
				s.Expect(*created.Spec.Replicas).To(gomega.Equal(int32(2)),
					"create must stamp MinReplicas")

				// A stale manual edit moves the count with no autoscaler running.
				created.Spec.Replicas = ptr.To(int32(9))
				s.Expect(s.client.Update(context.Background(), created)).To(gomega.Succeed())

				p2 := s.params()
				p2.ResolvedAutoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
				after := s.project(p2)
				s.Expect(*after.Spec.Replicas).To(gomega.Equal(int32(2)),
					"autoscaling-off Component must re-apply MinReplicas -- the ISVC controller "+
						"owns the count when no autoscaler drives /scale")
			},
		},
		// A nil resolved block counts as autoscaling off: an update puts
		// MinReplicas back, since no autoscaler could have written /scale.
		{
			name: "NilResolvedAutoscaler_AppliesMinReplicasOnUpdate",
			check: func(s *projection, created *v1beta1.InferenceReplica) {
				created.Spec.Replicas = ptr.To(int32(5))
				s.Expect(s.client.Update(context.Background(), created)).To(gomega.Succeed())

				after := s.project(s.params())
				s.Expect(*after.Spec.Replicas).To(gomega.Equal(int32(2)),
					"nil ResolvedAutoscaler is autoscaling-off -- update must re-apply MinReplicas")
			},
		},
		// Even under an autoscaler the first projection writes MinReplicas:
		// nothing could have scaled a replica that did not exist.
		{
			name:   "AutoscalerManaged_CreateStampsMinReplicas",
			params: func(p *Params) { p.ResolvedAutoscaler = hpaAutoscalerBlock() },
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
				s.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(2)),
					"autoscaler-managed create must still stamp MinReplicas as the initial count")
			},
		},
		// The stored block is a copy of the resolved one: a later edit of the
		// input does not show in the store.
		{
			name:   "AutoscalerProjection_DeepCopies",
			params: func(p *Params) { p.ResolvedAutoscaler = autoscalerBlockA() },
			check: func(s *projection, _ *v1beta1.InferenceReplica) {
				s.input.ResolvedAutoscaler.Class = v1beta1.AutoscalerNone
				s.input.ResolvedAutoscaler.Keda.Triggers[0].Type = "mutated"

				got2 := s.live()
				s.Expect(got2.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockA()),
					"post-projection input mutation must NOT leak into the stored IR — "+
						"ir.Spec.Autoscaler must be a deep copy of p.ResolvedAutoscaler")
			},
		},
		// The replica records the parent's object-level annotation keys that
		// are not revision inputs, minus any key the Component declares too.
		{
			name: "StampsRevisionExcludedAnnotations",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{
					"editor.example.com/resource-version": "v1beta1",
					"delivery.example.com/tracking-id":    "app:ome",
					"ome.io/rollout-nonce":                "7", // declared on component -> excluded from the list
				}
				isvc.Spec.Engine.Annotations = map[string]string{
					"ome.io/rollout-nonce": "7",
				}
			},
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Annotations).To(gomega.HaveKeyWithValue(
					constants.RevisionExcludedAnnotationKeysAnnotationKey,
					"delivery.example.com/tracking-id,editor.example.com/resource-version"),
					"projector records inherited ISVC annotation keys but not the "+
						"component-declared ome.io/rollout-nonce")
			},
		},
		// An annotation another writer put on the replica survives the next
		// projection beside the projector's own.
		{
			name: "PreservesReleaseHeldRevisionAnnotation",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{"editor.example.com/resource-version": "v1beta1"}
			},
			check: func(s *projection, ir *v1beta1.InferenceReplica) {
				ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "blocked-revision"
				s.Expect(s.client.Update(context.Background(), ir)).To(gomega.Succeed())

				updated := s.project(s.params())
				s.Expect(updated.Annotations).To(gomega.HaveKeyWithValue(
					constants.ReleaseHeldRevisionAnnotationKey, "blocked-revision"))
				s.Expect(updated.Annotations).To(gomega.HaveKeyWithValue(
					constants.RevisionExcludedAnnotationKeysAnnotationKey,
					"editor.example.com/resource-version"))
			},
		},
		// A parent with no object annotations gets no excluded-keys
		// annotation at all.
		{
			name: "NoISVCAnnotations_NoExcludeStamp",
			isvc: func(isvc *v1beta1.InferenceService) { isvc.Annotations = nil },
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Annotations).NotTo(gomega.HaveKey(
					constants.RevisionExcludedAnnotationKeysAnnotationKey),
					"no ISVC object annotations -> no exclude stamp")
			},
		},
		// lifecycle.minReadySeconds projects onto the top-level field the
		// workload engine reads: unset is 0, a value is copied, and clearing
		// it projects 0 again.
		{
			name: "ProjectsMinReadySeconds",
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.MinReadySeconds).To(gomega.Equal(int32(0)),
					"unset lifecycle.minReadySeconds must project to 0 (Available as soon as Ready)")

				s.isvc.Spec.Engine.Lifecycle.MinReadySeconds = ptr.To(int32(20))
				got = s.project(s.params())
				s.Expect(got.Spec.MinReadySeconds).To(gomega.Equal(int32(20)),
					"authored lifecycle.minReadySeconds must project onto IR spec.minReadySeconds")
				s.Expect(got.Spec.Lifecycle).NotTo(gomega.BeNil())
				s.Expect(got.Spec.Lifecycle.MinReadySeconds).To(gomega.HaveValue(gomega.Equal(int32(20))),
					"the lifecycle block keeps the field too")

				s.isvc.Spec.Engine.Lifecycle.MinReadySeconds = nil
				got = s.project(s.params())
				s.Expect(got.Spec.MinReadySeconds).To(gomega.Equal(int32(0)),
					"clearing the field must re-project 0 rather than keep the stale window")
			},
		},
		// The canary partition travels on spec.pacing.partition while the
		// user's rollingUpdate partition is copied as written; no canary
		// means no pacing block, and an explicit 0 is still a projected value.
		{
			name: "ProjectsPacingPartition",
			isvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Lifecycle.UpdateStrategy = &v1beta1.UpdateStrategy{
					RollingUpdate: &v1beta1.RollingUpdate{Partition: ptr.To(int32(1))},
				}
			},
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Pacing).To(gomega.BeNil(), "no canary must project no pacing block")
				s.Expect(got.Spec.Lifecycle.UpdateStrategy.RollingUpdate.Partition).To(gomega.HaveValue(gomega.Equal(int32(1))))

				// A canary step: partition 2 (of MinReplicas=2 the step holds both).
				step := s.params()
				step.PacingPartition = fixedPartition(2)
				got = s.project(step)
				s.Expect(got.Spec.Pacing).NotTo(gomega.BeNil())
				s.Expect(got.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(2))),
					"the canary partition must travel on spec.pacing.partition")
				s.Expect(got.Spec.Lifecycle.UpdateStrategy.RollingUpdate.Partition).To(gomega.HaveValue(gomega.Equal(int32(1))),
					"the user's rollingUpdate.partition must survive the canary untouched")

				// The final step releases with an explicit 0, still on the pacing block.
				release := s.params()
				release.PacingPartition = fixedPartition(0)
				got = s.project(release)
				s.Expect(got.Spec.Pacing).NotTo(gomega.BeNil())
				s.Expect(got.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(0))),
					"an explicit 0 is a projected value, distinct from no canary")

				// Canary gone: the pacing block is removed, the user partition stays.
				got = s.project(s.params())
				s.Expect(got.Spec.Pacing).To(gomega.BeNil(), "no canary must clear the projected partition")
				s.Expect(got.Spec.Lifecycle.UpdateStrategy.RollingUpdate.Partition).To(gomega.HaveValue(gomega.Equal(int32(1))))
			},
		},
		// The rollback target the canary executor writes on the pacing block
		// outlives every re-projection of the partition, a partition of 0
		// included; only a projection with no canary at all drops it with the
		// block.
		{
			name:   "RollbackTargetLivesWhileACanaryGoverns",
			params: func(p *Params) { p.PacingPartition = fixedPartition(1) },
			check: func(s *projection, live *v1beta1.InferenceReplica) {
				// The executor writes a rollback target on the live replica.
				live.Spec.Pacing.RollbackToRevision = ptr.To("llama-engine-stable")
				s.Expect(s.client.Update(context.Background(), live)).To(gomega.Succeed())

				// Rollback releases the hold (partition 0); the target must remain.
				release := s.params()
				release.PacingPartition = fixedPartition(0)
				got := s.project(release)
				s.Expect(got.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(0))))
				s.Expect(got.Spec.Pacing.RollbackToRevision).To(gomega.HaveValue(gomega.Equal("llama-engine-stable")),
					"while a canary governs the Component the rollback target is the executor's to clear")

				// No canary at all: the target goes with the partition and the
				// block collapses, so the Component follows its own spec.
				got = s.project(s.params())
				s.Expect(got.Spec.Pacing).To(gomega.BeNil(),
					"a Component no canary governs must not keep a rollback target left behind by one")
			},
		},
		// A partition above the replica count is written as the count:
		// holding every Instance is the same hold, and the webhook refuses
		// more.
		{
			name:   "PacingPartitionCappedAtReplicas",
			params: func(p *Params) { p.PacingPartition = fixedPartition(5) },
			check: func(s *projection, got *v1beta1.InferenceReplica) {
				s.Expect(got.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(2))),
					"partition must be capped at the projected replica count")
			},
		},
		// Re-projecting the same pacing partition writes nothing.
		{
			name:   "UnchangedPacingPartition_NoWrite",
			params: func(p *Params) { p.PacingPartition = fixedPartition(1) },
			check: func(s *projection, _ *v1beta1.InferenceReplica) {
				before := s.patches.Count()
				again := s.params()
				again.PacingPartition = fixedPartition(1)
				s.project(again)
				s.Expect(s.patches.Count()-before).To(gomega.Equal(0), "an unchanged pacing partition must not write")
			},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			runEnsureCase(t, row)
		})
	}
}

// TestInferenceReplicaName_FormatPins pins the canonical IR name
// shape used by the projector + the IR controller for revision /
// service / pod naming.
func TestInferenceReplicaName_FormatPins(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(InferenceReplicaName("llama", v1beta1.EngineComponent)).To(gomega.Equal("llama-engine"))
	g.Expect(InferenceReplicaName("llama", v1beta1.DecoderComponent)).To(gomega.Equal("llama-decoder"))
	g.Expect(InferenceReplicaName("llama", v1beta1.RouterComponent)).To(gomega.Equal("llama-router"))
}

// TestObjectScopedAnnotationKeys verifies that component annotations remain revision inputs.
func TestObjectScopedAnnotationKeys(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Annotations = map[string]string{
		"editor.example.com/resource-version": "v1beta1",
		"apply.example.com/state":             "{}",
		"ome.io/rollout-nonce":                "7", // also declared below
	}
	// Component metadata remains part of revision identity.
	isvc.Spec.Engine.Annotations = map[string]string{
		"ome.io/rollout-nonce": "7",
	}

	got := objectScopedAnnotationKeys(minimalParams(t, isvc, nil))
	g.Expect(got).To(gomega.Equal([]string{
		"apply.example.com/state",
		"editor.example.com/resource-version",
	}), "object-scoped keys = ISVC-object annotation keys minus component-declared keys, sorted")
}

func TestObjectScopedAnnotationKeys_NoISVCAnnotations_Nil(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Annotations = nil
	g.Expect(objectScopedAnnotationKeys(minimalParams(t, isvc, nil))).To(gomega.BeNil())
}

// TestProjectedReplicaCarriesItsParentRef pins that a projected replica
// names the InferenceService that projects it.
func TestProjectedReplicaCarriesItsParentRef(t *testing.T) {
	isvc := baselineISVC("svc", "team-a")
	p := minimalParams(t, isvc, nil)
	ir := newInferenceReplica(p, InferenceReplicaName(isvc.Name, p.Component))
	if ir.Spec.ParentRef == nil || ir.Spec.ParentRef.Name != "svc" {
		t.Fatalf("ParentRef = %+v, want &{Name: svc}", ir.Spec.ParentRef)
	}
	if got := ir.ParentName(); got != "svc" {
		t.Fatalf("ParentName = %q, want svc", got)
	}
}

// TestEnsureInferenceReplicaRefusesAForeignReplica pins that a standalone
// replica holding the projected name is reported and left untouched, never
// adopted.
func TestEnsureInferenceReplicaRefusesAForeignReplica(t *testing.T) {
	isvc := baselineISVC("svc", "team-a")
	standalone := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine", Namespace: "team-a", UID: "replica-uid"},
		Spec:       v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent},
	}
	p := minimalParams(t, isvc, fakeclient.New(t, fakeclient.WithObjects(standalone)))
	_, err := EnsureInferenceReplica(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "not projected by InferenceService") {
		t.Fatalf("expected a foreign-replica error, got %v", err)
	}
	fresh := &v1beta1.InferenceReplica{}
	if gerr := p.Client.Get(context.Background(), client.ObjectKeyFromObject(standalone), fresh); gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	if fresh.Spec.ParentRef != nil || len(fresh.OwnerReferences) != 0 {
		t.Fatalf("the standalone replica must be left untouched, got parentRef=%v owners=%v", fresh.Spec.ParentRef, fresh.OwnerReferences)
	}
}

// TestProjectedBy pins which live replicas the projector treats as its own:
// one this InferenceService controls, or one with no controller that names
// it as parent. Any other controller, including a same-named
// InferenceService with another UID, keeps the replica out of reach.
func TestProjectedBy(t *testing.T) {
	isvc := baselineISVC("svc", "team-a")
	parent := &v1beta1.ParentReference{Name: "svc"}
	cases := []struct {
		name   string
		owner  *metav1.OwnerReference
		parent *v1beta1.ParentReference
		want   bool
	}{
		{name: "controlled by this InferenceService", owner: metav1.NewControllerRef(isvc, isvcGVK), parent: parent, want: true},
		{name: "no controller and a matching parentRef", parent: parent, want: true},
		{name: "no controller and no parentRef", want: false},
		{name: "no controller and a parentRef naming another InferenceService", parent: &v1beta1.ParentReference{Name: "other"}, want: false},
		{
			name:   "controlled by a Deployment",
			owner:  &metav1.OwnerReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "svc", UID: "deployment-uid", Controller: ptr.To(true)},
			parent: parent,
			want:   false,
		},
		{
			name:   "controlled by a same-named InferenceService with another UID",
			owner:  &metav1.OwnerReference{APIVersion: isvcGVK.GroupVersion().String(), Kind: isvcGVK.Kind, Name: "svc", UID: "old-uid", Controller: ptr.To(true)},
			parent: parent,
			want:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ir := &v1beta1.InferenceReplica{
				ObjectMeta: metav1.ObjectMeta{Name: "svc-engine", Namespace: "team-a"},
				Spec:       v1beta1.InferenceReplicaSpec{ParentRef: tc.parent, Component: v1beta1.EngineComponent},
			}
			if tc.owner != nil {
				ir.OwnerReferences = []metav1.OwnerReference{*tc.owner}
			}
			if got := projectedBy(ir, isvc); got != tc.want {
				t.Fatalf("projectedBy = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnsureInferenceReplicaWaitsForAPreviousIncarnation pins that a replica
// still controlled by a deleted InferenceService of the same name is
// reported as awaiting removal, not as a name to change, and left untouched.
func TestEnsureInferenceReplicaWaitsForAPreviousIncarnation(t *testing.T) {
	isvc := baselineISVC("svc", "team-a")
	isvc.UID = "new-uid"
	previous := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-engine",
			Namespace: "team-a",
			UID:       "replica-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: isvcGVK.GroupVersion().String(),
				Kind:       isvcGVK.Kind,
				Name:       "svc",
				UID:        "old-uid",
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: "svc"}, Component: v1beta1.EngineComponent},
	}
	p := minimalParams(t, isvc, fakeclient.New(t, fakeclient.WithObjects(previous)))
	before := &v1beta1.InferenceReplica{}
	if gerr := p.Client.Get(context.Background(), client.ObjectKeyFromObject(previous), before); gerr != nil {
		t.Fatalf("get: %v", gerr)
	}

	_, err := EnsureInferenceReplica(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "still belongs to a previous InferenceService") || !strings.Contains(err.Error(), "old-uid") {
		t.Fatalf("expected a previous-incarnation error naming old-uid, got %v", err)
	}
	fresh := &v1beta1.InferenceReplica{}
	if gerr := p.Client.Get(context.Background(), client.ObjectKeyFromObject(previous), fresh); gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	if fresh.ResourceVersion != before.ResourceVersion || fresh.OwnerReferences[0].UID != "old-uid" {
		t.Fatalf("the previous incarnation's replica must be left untouched, got resourceVersion %s (was %s) owners=%v",
			fresh.ResourceVersion, before.ResourceVersion, fresh.OwnerReferences)
	}
}

// TestEnsureInferenceReplicaWaitsForADeletingReplica pins that a replica
// already being deleted is reported as awaiting removal, not as a name to
// change, and left untouched.
func TestEnsureInferenceReplicaWaitsForADeletingReplica(t *testing.T) {
	isvc := baselineISVC("svc", "team-a")
	deletedAt := metav1.Now()
	deleting := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "svc-engine",
			Namespace:         "team-a",
			UID:               "replica-uid",
			DeletionTimestamp: &deletedAt,
			// The fake client stores a deleting object only while a finalizer holds it.
			Finalizers: []string{"ome.io/ir-teardown"},
		},
		Spec: v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent},
	}
	p := minimalParams(t, isvc, fakeclient.New(t, fakeclient.WithObjects(deleting)))
	before := &v1beta1.InferenceReplica{}
	if gerr := p.Client.Get(context.Background(), client.ObjectKeyFromObject(deleting), before); gerr != nil {
		t.Fatalf("get: %v", gerr)
	}

	_, err := EnsureInferenceReplica(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "is being deleted") || strings.Contains(err.Error(), "rename") ||
		strings.Contains(err.Error(), "previous InferenceService") {
		t.Fatalf("expected an awaiting-deletion error, got %v", err)
	}
	fresh := &v1beta1.InferenceReplica{}
	if gerr := p.Client.Get(context.Background(), client.ObjectKeyFromObject(deleting), fresh); gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	if fresh.ResourceVersion != before.ResourceVersion {
		t.Fatalf("the deleting replica must be left untouched, got resourceVersion %s (was %s)", fresh.ResourceVersion, before.ResourceVersion)
	}
}

// fixedPartition is a partition resolver that projects n whatever replica
// count the projection stamps.
func fixedPartition(n int32) PartitionResolver {
	return func(int32) *int32 { return ptr.To(n) }
}
