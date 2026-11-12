package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDeriveISVC(t *testing.T) {
	src := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "uid-123",
			ResourceVersion: "999",
			Annotations: map[string]string{
				LocalQueueAnnotation:                    "serving-lq",
				AcceleratorRequirementsAnnotation:       "gpu=gb300",
				ClusterSelectorAnnotation:               "provider=cloud-a",
				constants.TrafficDrainAnnotation:        `{"drain":{"cluster":"cluster-a","reason":"mitigation"}}`,
				constants.RolloutPromoteAnnotation:      "abc123def",
				constants.RolloutPromoteForceAnnotation: "abc123def",
				constants.RolloutRollbackAnnotation:     "true",
				constants.NetworkVisibility:             "cluster-local", // an ingress override that SHOULD ride along
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}},
			},
			Decoder: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}},
			},
		},
	}

	src.Annotations[constants.RoutingPublisherOptionAnnotationPrefix+"routingClass"] = "premium"

	d := DeriveISVC(src, "cp-east", "")

	// identity preserved (worker addresses it by the same name/ns).
	assert.Equal(t, "svc", d.Name)
	assert.Equal(t, "prod", d.Namespace)
	// server-side fields cleared.
	assert.Empty(t, d.ResourceVersion)
	assert.Empty(t, string(d.UID))
	assert.True(t, d.Status.DeepCopy() != nil) // status is a fresh zero value (compiles regardless)
	// origin markers.
	assert.NotEmpty(t, d.Labels[PlacementOriginLabel])
	assert.Equal(t, "uid-123", d.Annotations[PlacementOriginUIDAnnotation])
	// control-plane identity stamped from the supplied id.
	assert.Equal(t, "cp-east", d.Labels[PlacementControlPlaneLabel])
	// Kueue queue-name label stamped on the engine component pod metadata
	// (the sole Kueue stamp).
	require.NotNil(t, d.Spec.Engine)
	assert.Equal(t, "serving-lq", d.Spec.Engine.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey])
	// ...and on the decoder component too.
	require.NotNil(t, d.Spec.Decoder)
	assert.Equal(t, "serving-lq", d.Spec.Decoder.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey])
	// control-plane-only directives are dropped on the derived object: the
	// placement selectors and the rollout operator verbs.
	for _, k := range []string{
		AcceleratorRequirementsAnnotation, ClusterSelectorAnnotation,
		constants.TrafficDrainAnnotation, constants.RoutingPublisherOptionAnnotationPrefix + "routingClass",
		constants.RolloutPromoteAnnotation, constants.RolloutPromoteForceAnnotation, constants.RolloutRollbackAnnotation,
	} {
		_, has := d.Annotations[k]
		assert.Falsef(t, has, "control-plane-only annotation %q must be stripped", k)
	}
	// ...but a legitimate worker-side serving directive (ingress visibility)
	// rides along untouched.
	assert.Equal(t, "cluster-local", d.Annotations[constants.NetworkVisibility])

	// no LocalQueueAnnotation and no configured queue -> no queue label.
	src2 := src.DeepCopy()
	delete(src2.Annotations, LocalQueueAnnotation)
	d2 := DeriveISVC(src2, "", "")
	_, hasQueue := d2.Spec.Engine.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey]
	assert.False(t, hasQueue, "no queue configured -> nothing stamped")
	// empty control-plane id leaves the identity label unset (single-CP behavior).
	_, hasCP := d2.Labels[PlacementControlPlaneLabel]
	assert.False(t, hasCP)

	// source must not be mutated (deep copy).
	assert.Nil(t, src.Spec.Engine.ComponentExtensionSpec.Labels, "source ISVC must be untouched")
}

// Leaving it on overwrites the member object's own tracking-id, which is what
// ArgoCD reads to decide ownership — reassigning it to an application on
// another cluster that does not manage it.
func TestDeriveISVC_StripsGitOpsAnnotations(t *testing.T) {
	src := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "uid-123",
			Annotations: map[string]string{
				"argocd.argoproj.io/tracking-id":     "source-app:ome.io/InferenceService:prod/svc",
				"argocd.argoproj.io/sync-options":    "Prune=false",
				"argocd.argoproj.io/compare-options": "IgnoreExtraneous",
				"argocd.argoproj.io/sync-wave":       "1",
				// Not the control plane's tooling: a user annotation on a
				// lookalike-but-different domain must survive.
				"argocd.argoproj.io.example.com/keep": "yes",
				constants.NetworkVisibility:           "cluster-local",
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}},
			},
		},
	}

	d := DeriveISVC(src, "cp-east", "")

	for _, k := range []string{
		"argocd.argoproj.io/tracking-id",
		"argocd.argoproj.io/sync-options",
		"argocd.argoproj.io/compare-options",
		"argocd.argoproj.io/sync-wave",
	} {
		_, present := d.Annotations[k]
		assert.False(t, present, "%s must not ride along to the derived copy", k)
	}
	assert.Equal(t, "yes", d.Annotations["argocd.argoproj.io.example.com/keep"],
		"the prefix must match the family, not merely a leading substring")
	assert.Equal(t, "cluster-local", d.Annotations[constants.NetworkVisibility],
		"unrelated annotations still ride along")
	// The source is untouched: DeriveISVC works on a deep copy.
	assert.Equal(t, "source-app:ome.io/InferenceService:prod/svc",
		src.Annotations["argocd.argoproj.io/tracking-id"])
}

func TestDeriveISVC_StripsRoutingDirectives(t *testing.T) {
	enabled := true
	src := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-123"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
			Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity,
				Mode: v1beta1.PlacementModeAll,
			},
			Routing: &v1beta1.RoutingSpec{
				Enabled:         &enabled,
				CapacityFactors: map[string]resource.Quantity{"cluster-a": resource.MustParse("2")},
				Publisher: &v1beta1.RoutingPublisherSpec{
					Options: map[string]string{"routingClass": "premium"},
				},
			},
		},
	}
	wantSource := src.DeepCopy()

	d := DeriveISVC(src, "cp-east", "")

	assert.Nil(t, d.Spec.Routing, "routing is reconciled only by the control plane")
	assert.Nil(t, d.Spec.Placement, "placement is reconciled only by the control plane")
	assert.Equal(t, wantSource, src, "derivation must not mutate the source ISVC")
}

func TestSetPlannedReplicas(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		floor, cap           int32
		maximum, wantMaximum int
	}{
		{name: "explicit cap", floor: 1, cap: 2, maximum: 9, wantMaximum: 2},
		{name: "source maximum is bounded by allocation", floor: 2, maximum: 5, wantMaximum: 2},
		{name: "floor exceeds explicit maximum", floor: 3, maximum: 1, wantMaximum: 3},
		{name: "omitted source maximum is bounded by allocation", floor: 2, wantMaximum: 2},
		{name: "retained floor exceeds reduced cap", floor: 3, cap: 1, maximum: 5, wantMaximum: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			member := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: tt.maximum}},
				Decoder: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: tt.maximum}},
				Router:  &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: 7}},
			}}
			router := member.Spec.Router.DeepCopy()
			bound := plannedBound{Floor: tt.floor, Ceiling: tt.cap}
			setPlannedReplicas(member, map[v1beta1.ComponentType]plannedBound{v1beta1.EngineComponent: bound, v1beta1.DecoderComponent: bound})
			want := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(tt.floor)), MaxReplicas: tt.wantMaximum}
			for _, got := range []v1beta1.ComponentExtensionSpec{member.Spec.Engine.ComponentExtensionSpec, member.Spec.Decoder.ComponentExtensionSpec} {
				if diff := cmp.Diff(want, got); diff != "" {
					t.Error(diff)
				}
			}
			if diff := cmp.Diff(router, member.Spec.Router); diff != "" {
				t.Error(diff)
			}
			runtime := &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: 9}}
			merged, err := isvcutils.MergeEngineSpec(runtime, member.Spec.Engine)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantMaximum, merged.MaxReplicas); diff != "" {
				t.Error(diff)
			}
		})
	}
}

// The queue a derived workload joins is operator-configurable: a per-ISVC
// annotation wins, then the configured queue. The queue names a resource the
// operator provisioned, so an unconfigured fleet gets no label rather than a
// guessed name Kueue would never match.
func TestDeriveISVC_LocalQueuePrecedence(t *testing.T) {
	base := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc", UID: "uid-1"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
		},
	}
	queueOf := func(d *v1beta1.InferenceService) string {
		return d.Spec.Engine.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey]
	}

	assert.Empty(t, queueOf(DeriveISVC(base, "", "")),
		"nothing configured -> no queue label")
	assert.Equal(t, "gpu-queue", queueOf(DeriveISVC(base, "", "gpu-queue")),
		"configured queue is stamped when no annotation is set")

	annotated := base.DeepCopy()
	annotated.Annotations = map[string]string{LocalQueueAnnotation: "per-isvc"}
	assert.Equal(t, "per-isvc", queueOf(DeriveISVC(annotated, "", "gpu-queue")),
		"per-ISVC annotation must beat the configured queue")
}

// TestDeriveISVC_RoutingPublisherOption_MemberUnchanged derives a member from
// a source with and without the option. The option stays on the source, and
// both members are identical.
func TestDeriveISVC_RoutingPublisherOption_MemberUnchanged(t *testing.T) {
	source := srcISVCMode(v1beta1.PlacementModeSplit, "")
	source.Annotations = map[string]string{constants.NetworkVisibility: "cluster-local"}
	withOption := withRoutingPublisherOption(source)
	member := DeriveISVC(withOption, "cp-east", "serving-lq")
	if value, present := member.Annotations[routingPublisherOptionKey]; present {
		t.Fatalf("the option %q reached the member", value)
	}
	if diff := cmp.Diff(DeriveISVC(source, "cp-east", "serving-lq"), member); diff != "" {
		t.Fatalf("the option changed the member (-without +with):\n%s", diff)
	}
	if withOption.Annotations[routingPublisherOptionKey] != "premium" {
		t.Fatal("deriving a member removed the option from the source")
	}
}

// A derived copy carries none of the source's server-side state, status or
// control-plane-only spec; the worker cluster's apiserver assigns its own.
func TestDeriveISVC_CarriesNoServerSideStateOrStatus(t *testing.T) {
	now := metav1.Now()
	src := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "uid-123", ResourceVersion: "999", Generation: 7,
			CreationTimestamp: now, DeletionTimestamp: &now,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "owner", UID: "owner-uid"}},
			Finalizers:      []string{"ome.io/placement"},
			ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "ome", Operation: metav1.ManagedFieldsOperationApply}},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine:    &v1beta1.EngineSpec{},
			Placement: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeAll},
			Routing:   &v1beta1.RoutingSpec{},
		},
	}
	src.Status.ObservedGeneration = 7

	want := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod",
			Labels:      map[string]string{PlacementOriginLabel: "uid-123"},
			Annotations: map[string]string{PlacementOriginUIDAnnotation: "uid-123"},
		},
		Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	if diff := cmp.Diff(want, DeriveISVC(src, "", "")); diff != "" {
		t.Fatalf("derived copy (-want +got):\n%s", diff)
	}
}

// The placement-policy annotation of a derived copy says what the source spec
// says: written when the spec uses cluster affinity, absent otherwise, whatever
// the source's own annotation held.
func TestDeriveISVC_PlacementPolicyAnnotationFollowsTheSpec(t *testing.T) {
	for _, tt := range []struct {
		name      string
		placement *v1beta1.PlacementSpec
		annotated string
		want      string
	}{
		{name: "cluster affinity, no annotation", placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, want: "ClusterAffinity"},
		{name: "cluster affinity, annotation of another value", placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, annotated: "other", want: "ClusterAffinity"},
		{name: "another policy, stale annotation", placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicy("Other")}, annotated: "ClusterAffinity", want: ""},
		{name: "no placement, stale annotation", annotated: "ClusterAffinity", want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
				Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Placement: tt.placement},
			}
			if tt.annotated != "" {
				src.Annotations = map[string]string{constants.PlacementPolicy: tt.annotated}
			}
			got, present := DeriveISVC(src, "", "").Annotations[constants.PlacementPolicy]
			if got != tt.want || present != (tt.want != "") {
				t.Fatalf("placement-policy annotation = %q (present %t), want %q", got, present, tt.want)
			}
		})
	}
}

// The queue label reaches the pods of every component the derived copy
// declares: the engine, the decoder and the router.
func TestDeriveISVC_QueueLabelReachesTheRouter(t *testing.T) {
	src := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{}, Router: &v1beta1.RouterSpec{},
		},
	}
	d := DeriveISVC(src, "", "gpu-queue")
	for name, labels := range map[string]map[string]string{
		"engine":  d.Spec.Engine.Labels,
		"decoder": d.Spec.Decoder.Labels,
		"router":  d.Spec.Router.Labels,
	} {
		if got := labels[constants.KueueQueueLabelKey]; got != "gpu-queue" {
			t.Errorf("%s queue label = %q, want gpu-queue", name, got)
		}
	}
}
