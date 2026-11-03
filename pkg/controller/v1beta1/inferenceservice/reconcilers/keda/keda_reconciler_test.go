package keda

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ownedscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestCalculateMinMaxReplicas(t *testing.T) {
	testCases := []struct {
		name               string
		componentExt       *v1beta1.ComponentExtensionSpec
		expectedMinReplica int32
		expectedMaxReplica int32
	}{
		{
			name: "Default min/max replicas",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: nil,
				MaxReplicas: 0,
			},
			expectedMinReplica: 1,
			expectedMaxReplica: 1,
		},
		{
			name: "Custom min/max replicas",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: ptr.To(2),
				MaxReplicas: 10,
			},
			expectedMinReplica: 2,
			expectedMaxReplica: 10,
		},
		{
			name: "Max replicas less than min - should use min",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: ptr.To(5),
				MaxReplicas: 3,
			},
			expectedMinReplica: 5,
			expectedMaxReplica: 5,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			minReplicas := calculateMinReplicas(tt.componentExt)
			maxReplicas := calculateMaxReplicas(tt.componentExt, minReplicas)

			if minReplicas != tt.expectedMinReplica {
				t.Errorf("Expected minReplicas %d, got %d", tt.expectedMinReplica, minReplicas)
			}
			if maxReplicas != tt.expectedMaxReplica {
				t.Errorf("Expected maxReplicas %d, got %d", tt.expectedMaxReplica, maxReplicas)
			}
		})
	}
}

// TestCreateScaledObject_IRScaleTarget verifies the IR-managed wiring:
// when the caller passes an InferenceReplica ScaleTarget and a KedaAutoscaler
// with a single trigger, the generator emits a ScaledObject targeting the IR
// with the trigger forwarded verbatim and the default min/max bounds applied.
func TestCreateScaledObject_IRScaleTarget(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "foo-engine",
		Namespace: "ns",
	}
	scaleTargetRef := kedav1.ScaleTarget{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	kedaSpec := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{
				Type: "prometheus",
				Metadata: map[string]string{
					"serverAddress": "http://prom:9090",
					"metricName":    "tps",
					"query":         "sum(rate(http_requests_total[1m]))",
					"threshold":     "10",
				},
			},
		},
	}

	got := createScaledObject(componentMeta, scaleTargetRef, kedaSpec, 1, 5)

	if got.Name != utils.GetScaledObjectName("foo-engine") {
		t.Errorf("Name mismatch: want %s, got %s", utils.GetScaledObjectName("foo-engine"), got.Name)
	}
	if got.Namespace != "ns" {
		t.Errorf("Namespace mismatch: want ns, got %s", got.Namespace)
	}
	if got.Spec.ScaleTargetRef == nil {
		t.Fatal("Expected ScaleTargetRef to be set, got nil")
	}
	if diff := cmp.Diff(scaleTargetRef, *got.Spec.ScaleTargetRef); diff != "" {
		t.Errorf("ScaleTargetRef mismatch (-want +got): %s", diff)
	}
	if got.Spec.MinReplicaCount == nil || *got.Spec.MinReplicaCount != 1 {
		t.Errorf("MinReplicaCount: want 1, got %v", got.Spec.MinReplicaCount)
	}
	if got.Spec.MaxReplicaCount == nil || *got.Spec.MaxReplicaCount != 5 {
		t.Errorf("MaxReplicaCount: want 5, got %v", got.Spec.MaxReplicaCount)
	}
	if diff := cmp.Diff(kedaSpec.Triggers, got.Spec.Triggers); diff != "" {
		t.Errorf("Triggers not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateScaledObject_AdvancedPassThrough pins the verbatim contract for
// the KEDA Advanced block: the entire AdvancedConfig struct
// (HorizontalPodAutoscalerConfig, RestoreToOriginalReplicaCount) must land
// on the ScaledObject without any field-level filtering.
func TestCreateScaledObject_AdvancedPassThrough(t *testing.T) {
	componentMeta := metav1.ObjectMeta{Name: "foo-engine", Namespace: "ns"}
	scaleTargetRef := kedav1.ScaleTarget{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	restoreOriginal := true
	advanced := &kedav1.AdvancedConfig{
		HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{
			Name: "custom-hpa-name",
		},
		RestoreToOriginalReplicaCount: restoreOriginal,
	}
	kedaSpec := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{Type: "cpu", Metadata: map[string]string{"value": "50"}},
		},
		Advanced: advanced,
	}

	got := createScaledObject(componentMeta, scaleTargetRef, kedaSpec, 1, 3)

	if diff := cmp.Diff(advanced, got.Spec.Advanced); diff != "" {
		t.Errorf("Advanced not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateScaledObject_PollingCooldownFallback pins the verbatim contract
// for the scalar pass-through fields: PollingInterval, CooldownPeriod,
// IdleReplicaCount, Fallback. Each must
// land on the ScaledObject as the same pointer/struct.
func TestCreateScaledObject_PollingCooldownFallback(t *testing.T) {
	componentMeta := metav1.ObjectMeta{Name: "foo-engine", Namespace: "ns"}
	scaleTargetRef := kedav1.ScaleTarget{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	polling := int32(15)
	cooldown := int32(120)
	idle := int32(0)
	fallback := &kedav1.Fallback{
		FailureThreshold: 3,
		Replicas:         2,
	}
	kedaSpec := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{Type: "cpu", Metadata: map[string]string{"value": "50"}},
		},
		PollingInterval:  &polling,
		CooldownPeriod:   &cooldown,
		IdleReplicaCount: &idle,
		Fallback:         fallback,
	}

	got := createScaledObject(componentMeta, scaleTargetRef, kedaSpec, 1, 3)

	if got.Spec.PollingInterval == nil || *got.Spec.PollingInterval != polling {
		t.Errorf("PollingInterval: want %d, got %v", polling, got.Spec.PollingInterval)
	}
	if got.Spec.CooldownPeriod == nil || *got.Spec.CooldownPeriod != cooldown {
		t.Errorf("CooldownPeriod: want %d, got %v", cooldown, got.Spec.CooldownPeriod)
	}
	if got.Spec.IdleReplicaCount == nil || *got.Spec.IdleReplicaCount != idle {
		t.Errorf("IdleReplicaCount: want %d, got %v", idle, got.Spec.IdleReplicaCount)
	}
	if diff := cmp.Diff(fallback, got.Spec.Fallback); diff != "" {
		t.Errorf("Fallback not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateScaledObject_ComponentExtShim_Identical pins the byte-identical
// contract for the Raw Deployment dispatch path: the shim must emit the
// same ScaledObject as createScaledObject when given a componentExt with
// an Autoscaler.Keda block (the canonical authoring location). Also covers
// the `k8slens-edit-resource-version` label filtering quirk.
func TestCreateScaledObject_ComponentExtShim_Identical(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "raw-engine",
		Namespace: "ns",
		Labels: map[string]string{
			"app":                           "raw-engine",
			"k8slens-edit-resource-version": "skip-me",
		},
		Annotations: map[string]string{"foo": "bar"},
	}
	polling := int32(30)
	keda := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{
				Type: "prometheus",
				Metadata: map[string]string{
					"serverAddress": "http://prom:9090",
					"metricName":    "tps",
					"query":         "sum(rate(http_requests_total[1m]))",
					"threshold":     "10",
				},
			},
		},
		PollingInterval: &polling,
	}
	componentExt := &v1beta1.ComponentExtensionSpec{
		MinReplicas: ptr.To(2),
		MaxReplicas: 6,
		Autoscaler: &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerKEDA,
			Keda:  keda,
		},
	}

	minReplicas := int32(2)
	maxReplicas := int32(6)
	expected := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:        utils.GetScaledObjectName("raw-engine"),
			Namespace:   "ns",
			Labels:      map[string]string{"app": "raw-engine"},
			Annotations: map[string]string{"foo": "bar"},
		},
		Spec: kedav1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1.ScaleTarget{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "raw-engine",
			},
			MinReplicaCount: &minReplicas,
			MaxReplicaCount: &maxReplicas,
			Triggers:        keda.Triggers,
			PollingInterval: &polling,
		},
	}

	got := createScaledObjectFromComponentExt(componentMeta, componentExt)

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("ComponentExt shim is not byte-identical (-want +got): %s", diff)
	}
}

func TestMatchesComparesTheControllerOwner(t *testing.T) {
	controller := true
	desiredOwner := metav1.OwnerReference{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceService",
		Name:       "demo",
		UID:        "isvc-uid",
		Controller: &controller,
	}
	staleOwner := desiredOwner
	staleOwner.Name = "stale"

	desired := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{desiredOwner}}}
	existing := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{staleOwner}}}
	if kind.Matches(desired, existing) {
		t.Fatal("ScaledObjects with different controller metadata must not be equal")
	}

	nonControllerOwner := desiredOwner
	nonControllerOwner.Controller = nil
	existing.OwnerReferences = []metav1.OwnerReference{desiredOwner, nonControllerOwner}
	if !kind.Matches(desired, existing) {
		t.Fatal("unrelated non-controller owner references must not force an update")
	}
}

func TestMatchesComparesManagedMetadata(t *testing.T) {
	desired := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"component": "engine"},
			Annotations: map[string]string{"example.com/control": "enabled"},
		},
	}
	existing := desired.DeepCopy()
	if !kind.Matches(desired, existing) {
		t.Fatal("identical ScaledObject metadata must compare equal")
	}

	existing.Labels["component"] = "decoder"
	if kind.Matches(desired, existing) {
		t.Fatal("different ScaledObject labels must force an update")
	}

	existing = desired.DeepCopy()
	existing.Annotations["example.com/control"] = "disabled"
	if kind.Matches(desired, existing) {
		t.Fatal("different ScaledObject annotations must force an update")
	}

	existing = desired.DeepCopy()
	existing.Labels["example.com/injected"] = "keep"
	existing.Annotations["example.com/injected"] = "keep"
	if !kind.Matches(desired, existing) {
		t.Fatal("injected ScaledObject metadata must not force an update")
	}
}

func TestKEDAReconcilerUpdatePreservesUnmanagedMetadata(t *testing.T) {
	controller := true
	owner := metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceService",
		Name:       "demo",
		UID:        "isvc-uid",
		Controller: &controller,
	}
	nonController := owner
	nonController.Name = "observer"
	nonController.UID = "observer-uid"
	nonController.Controller = nil
	maxExisting := int32(3)
	maxDesired := int32(5)

	existing := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "scaledobject-demo-engine",
			Namespace:   "test-ns",
			UID:         "scaledobject-uid",
			Labels:      map[string]string{"component": "decoder", "example.com/injected": "keep"},
			Annotations: map[string]string{"example.com/managed": "old", "example.com/injected": "keep"},
			Finalizers:  []string{"example.com/finalizer"},
			OwnerReferences: []metav1.OwnerReference{
				owner,
				nonController,
			},
		},
		Spec: kedav1.ScaledObjectSpec{
			MaxReplicaCount: &maxExisting,
			Triggers:        []kedav1.ScaleTriggers{{Type: "cron"}},
		},
	}
	desired := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:            existing.Name,
			Namespace:       existing.Namespace,
			Labels:          map[string]string{"component": "engine"},
			Annotations:     map[string]string{"example.com/managed": "new"},
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: kedav1.ScaledObjectSpec{
			MaxReplicaCount: &maxDesired,
			Triggers:        []kedav1.ScaleTriggers{{Type: "cron"}},
		},
	}

	cl := fakeclient.New(t, fakeclient.WithTypes(kedav1.AddToScheme), fakeclient.WithObjects(existing))
	r := ownedscaler.New(cl, kind, desired)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := &kedav1.ScaledObject{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: existing.Namespace, Name: existing.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.MaxReplicaCount == nil || *got.Spec.MaxReplicaCount != maxDesired {
		t.Fatalf("MaxReplicaCount = %v, want %d", got.Spec.MaxReplicaCount, maxDesired)
	}
	if got.Labels["component"] != "engine" || got.Labels["example.com/injected"] != "keep" {
		t.Fatalf("labels = %#v", got.Labels)
	}
	if got.Annotations["example.com/managed"] != "new" || got.Annotations["example.com/injected"] != "keep" {
		t.Fatalf("annotations = %#v", got.Annotations)
	}
	if diff := cmp.Diff([]string{"example.com/finalizer"}, got.Finalizers); diff != "" {
		t.Fatalf("finalizers mismatch (-want +got): %s", diff)
	}
	if !containsOwnerReference(got.OwnerReferences, nonController) {
		t.Fatalf("non-controller owner reference was dropped: %#v", got.OwnerReferences)
	}
}

func containsOwnerReference(refs []metav1.OwnerReference, want metav1.OwnerReference) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func TestKEDAReconcilerRejectsForeignController(t *testing.T) {
	controller := true
	expectedOwner := metav1.OwnerReference{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceService",
		Name:       "demo",
		UID:        "expected-uid",
		Controller: &controller,
	}
	foreignOwner := expectedOwner
	foreignOwner.Name = "other"
	foreignOwner.UID = "foreign-uid"

	tests := []struct {
		name   string
		owners []metav1.OwnerReference
	}{
		{name: "ownerless"},
		{name: "different controller", owners: []metav1.OwnerReference{foreignOwner}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desired := &kedav1.ScaledObject{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       "test-ns",
					Name:            "scaledobject-demo-engine",
					OwnerReferences: []metav1.OwnerReference{expectedOwner},
				},
			}
			existing := desired.DeepCopy()
			existing.UID = types.UID("scaledobject-uid")
			existing.OwnerReferences = tt.owners

			cl := fakeclient.New(t, fakeclient.WithTypes(kedav1.AddToScheme), fakeclient.WithObjects(existing))
			r := ownedscaler.New(cl, kind, desired)

			err := r.Reconcile(context.Background())
			if err == nil || !strings.Contains(err.Error(), "not controlled by expected owner") {
				t.Fatalf("expected ownership error, got %v", err)
			}

			got := &kedav1.ScaledObject{}
			if err := cl.Get(context.Background(), types.NamespacedName{Namespace: desired.Namespace, Name: desired.Name}, got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.owners, got.OwnerReferences); diff != "" {
				t.Errorf("owner references changed (-want +got): %s", diff)
			}
		})
	}
}

func TestConfiguredHPAName(t *testing.T) {
	testCases := []struct {
		name     string
		advanced *kedav1.AdvancedConfig
		want     string
	}{
		{name: "no advanced block"},
		{name: "no HPA config", advanced: &kedav1.AdvancedConfig{RestoreToOriginalReplicaCount: true}},
		{name: "empty name", advanced: &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{}}},
		{
			name:     "configured name",
			advanced: &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{Name: "demo-engine"}},
			want:     "demo-engine",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ConfiguredHPAName(tc.advanced); got != tc.want {
				t.Errorf("ConfiguredHPAName() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWithHPAName(t *testing.T) {
	hpaConfig := &kedav1.HorizontalPodAutoscalerConfig{Name: "custom"}
	requested := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{{Type: "cron", Metadata: map[string]string{"timezone": "UTC"}}},
		Advanced: &kedav1.AdvancedConfig{RestoreToOriginalReplicaCount: true, HorizontalPodAutoscalerConfig: hpaConfig},
	}
	original := requested.DeepCopy()

	got := WithHPAName(requested, "demo-engine")
	if got == requested {
		t.Fatal("WithHPAName must copy the block when it changes the name")
	}
	if diff := cmp.Diff(original, requested); diff != "" {
		t.Errorf("WithHPAName changed the caller's block (-want +got):\n%s", diff)
	}
	if name := ConfiguredHPAName(got.Advanced); name != "demo-engine" {
		t.Errorf("configured name = %q, want %q", name, "demo-engine")
	}
	if !got.Advanced.RestoreToOriginalReplicaCount || !cmp.Equal(original.Triggers, got.Triggers) {
		t.Error("WithHPAName must keep every other field")
	}

	if same := WithHPAName(requested, "custom"); same != requested {
		t.Error("WithHPAName must return the block itself when the name already matches")
	}
	if WithHPAName(nil, "demo-engine") != nil {
		t.Error("WithHPAName(nil) must stay nil")
	}
	bare := WithHPAName(&v1beta1.KedaAutoscaler{}, "demo-engine")
	if name := ConfiguredHPAName(bare.Advanced); name != "demo-engine" {
		t.Errorf("configured name on a block without advanced = %q, want %q", name, "demo-engine")
	}
}

func TestShortenedHPAName(t *testing.T) {
	// KEDA names the HPA "keda-hpa-" plus the ScaledObject name, 9 characters
	// longer than the ScaledObject name.
	fits := "scaledobject-" + strings.Repeat("a", 41)
	if got := len(kedaHPANamePrefix + fits); got != validation.DNS1123LabelMaxLength {
		t.Fatalf("test setup: KEDA's name is %d characters, want exactly %d", got, validation.DNS1123LabelMaxLength)
	}
	if got := ShortenedHPAName(fits); got != "" {
		t.Errorf("ShortenedHPAName(%q) = %q, want \"\" so KEDA keeps its own name", fits, got)
	}

	tooLong := "scaledobject-team-a-example-model-shared-shadow-decoder"
	got := ShortenedHPAName(tooLong)
	if len(got) != validation.DNS1123LabelMaxLength {
		t.Errorf("ShortenedHPAName(%q) = %q, %d characters, want %d", tooLong, got, len(got), validation.DNS1123LabelMaxLength)
	}
	if errs := validation.IsDNS1123Label(got); len(errs) > 0 {
		t.Errorf("ShortenedHPAName(%q) = %q, not a DNS label: %v", tooLong, got, errs)
	}
	if !strings.HasSuffix(got, "example-model-shared-shadow-decoder") {
		t.Errorf("ShortenedHPAName(%q) = %q, want it to keep the end of the name", tooLong, got)
	}
	if got != ShortenedHPAName(tooLong) {
		t.Error("ShortenedHPAName must return the same name on every call")
	}
	// Two names that differ only in their first characters keep the same end,
	// so only the hash keeps their HPAs apart.
	other := ShortenedHPAName("scaledobject-team-b-example-model-shared-shadow-decoder")
	if other == got {
		t.Errorf("two ScaledObjects got the same HPA name %q", got)
	}
}
