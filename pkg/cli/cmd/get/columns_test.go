package get

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestISVCColumns(t *testing.T) {
	e, err := resolve("isvc")
	require.NoError(t, err)
	u, _ := apis.ParseURL("https://llama.team-a.example.com")
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b"},
		Spec: v1beta1.InferenceServiceSpec{
			Model:   &v1beta1.ModelRef{Name: "llama-3-3-70b"},
			Runtime: &v1beta1.ServingRuntimeRef{Name: "srt-llama"},
		},
		Status: v1beta1.InferenceServiceStatus{
			URL: u,
			Status: duckv1.Status{Conditions: duckv1.Conditions{{
				Type: apis.ConditionReady, Status: "True",
			}}},
		},
	}
	got := map[string]string{}
	for _, c := range e.Columns {
		got[c.Name] = c.Extract(isvc)
	}
	assert.Equal(t, "llama-70b", got["NAME"])
	assert.Equal(t, "llama-3-3-70b", got["MODEL"])
	assert.Equal(t, "srt-llama", got["RUNTIME"])
	assert.Equal(t, "True", got["READY"])
	assert.Equal(t, "https://llama.team-a.example.com", got["URL"])
}

func TestISVCColumnsNilRefs(t *testing.T) {
	e, _ := resolve("isvc")
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "bare"}}
	for _, c := range e.Columns {
		assert.NotPanics(t, func() { c.Extract(isvc) }, c.Name)
	}
}

// TestRolloutPolicyColumns pins the inventory contract for a current reusable
// rollout policy. Stale and unobserved status are covered separately below.
func TestRolloutPolicyColumns(t *testing.T) {
	e, err := resolve("rp")
	require.NoError(t, err)
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "guarded", Generation: 7},
		Spec:       v1beta1.RolloutPolicySpec{Canary: &v1beta1.GroupCanary{}},
		Status: v1beta1.RolloutPolicyStatus{
			ObservedGeneration: 7,
			PortableDigest:     "rp1:1234567890ab",
			AttachedGroups:     3,
			Conditions: []metav1.Condition{
				{Type: v1beta1.RolloutPolicyReadyCondition, Status: metav1.ConditionTrue, Reason: v1beta1.RolloutPolicyReasonBodyValid},
				{Type: v1beta1.RolloutPolicyInUseCondition, Status: metav1.ConditionTrue},
			},
		},
	}
	got := map[string]string{}
	for _, c := range e.Columns {
		got[c.Name] = c.Extract(policy)
	}
	assert.Equal(t, "guarded", got["NAME"])
	assert.Equal(t, "canary", got["PROGRESSION"])
	assert.Equal(t, "True", got["READY"])
	assert.Equal(t, "rp1:1234567890ab", got["DIGEST"])
	assert.Equal(t, "3", got["REFS"])
	assert.Equal(t, v1beta1.RolloutPolicyReasonBodyValid, got["REASON"])
	assert.Equal(t, "True", got["IN-USE"])
	assert.Equal(t, "Current", got["STATUS-FRESHNESS"])
}

func TestRolloutPolicyColumnsMissingStatusAndWrongType(t *testing.T) {
	e, err := resolve("rolloutpolicy")
	require.NoError(t, err)

	missing := &v1beta1.RolloutPolicy{}
	got := map[string]string{}
	for _, c := range e.Columns {
		got[c.Name] = c.Extract(missing)
	}
	assert.Equal(t, "-", got["PROGRESSION"])
	assert.Equal(t, "Unknown", got["READY"])
	assert.Equal(t, "-", got["DIGEST"])
	assert.Equal(t, "-", got["REFS"])
	assert.Equal(t, "-", got["REASON"])
	assert.Equal(t, "Unknown", got["IN-USE"])
	assert.Equal(t, "Unobserved", got["STATUS-FRESHNESS"])

	wrong := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "wrong-kind"}}
	for _, c := range e.Columns {
		var value string
		assert.NotPanics(t, func() { value = c.Extract(wrong) }, c.Name)
		assert.Equal(t, "?", value, c.Name)
	}
}

func TestRolloutPolicyColumnsHideStaleStatus(t *testing.T) {
	e, err := resolve("rolloutpolicy")
	require.NoError(t, err)

	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "stale", Generation: 7},
		Status: v1beta1.RolloutPolicyStatus{
			ObservedGeneration: 6,
			PortableDigest:     "rp1:stale",
			AttachedGroups:     3,
			Conditions: []metav1.Condition{
				{Type: v1beta1.RolloutPolicyReadyCondition, Status: metav1.ConditionTrue, Reason: v1beta1.RolloutPolicyReasonBodyValid},
				{Type: v1beta1.RolloutPolicyInUseCondition, Status: metav1.ConditionTrue},
			},
		},
	}
	got := map[string]string{}
	for _, c := range e.Columns {
		got[c.Name] = c.Extract(policy)
	}
	assert.Equal(t, "Unknown", got["READY"])
	assert.Equal(t, "-", got["DIGEST"])
	assert.Equal(t, "-", got["REFS"])
	assert.Equal(t, "-", got["REASON"])
	assert.Equal(t, "Unknown", got["IN-USE"])
	assert.Equal(t, "Stale", got["STATUS-FRESHNESS"])
}

func TestModelColumnsMergedScope(t *testing.T) {
	e, err := resolve("models")
	require.NoError(t, err)
	arch, params := "LlamaForCausalLM", "70B"
	bm := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "team-a"},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat:        v1beta1.ModelFormat{Name: "safetensors"},
			ModelArchitecture:  &arch,
			ModelParameterSize: &params,
		},
		Status: v1beta1.ModelStatusSpec{State: v1beta1.LifeCycleStateReady},
	}
	got := map[string]string{}
	for _, c := range e.Columns {
		got[c.Name] = c.Extract(bm)
	}
	assert.Equal(t, "Namespaced", got["SCOPE"])
	assert.Equal(t, "LlamaForCausalLM", got["ARCH"])
	assert.Equal(t, "70B", got["PARAMS"])
	assert.Equal(t, "safetensors", got["FORMAT"])
	assert.Equal(t, "Ready", got["STATE"])

	cbm := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "m2"}}
	for _, c := range e.Columns {
		if c.Name == "SCOPE" {
			assert.Equal(t, "Cluster", c.Extract(cbm))
		}
	}
}

// TestModelAndRuntimeColumnsWrongType pins a fix for a nil-pointer-deref
// panic: baseModelColumns'/runtimeColumns' internal spec()/status()
// type-switch helpers used to return a nil pointer for any concrete type
// outside {BaseModel,ClusterBaseModel} / {ServingRuntime,ClusterServingRuntime},
// and several extractors dereferenced that nil unconditionally. Every
// extractor must be nil-safe -- including against an object of the wrong
// resource kind entirely, which should never reach these columns in
// practice but must never crash the CLI if it does.
//
// Every column must survive (NotPanics). Columns that guard on the object's
// *domain* type (a BaseModel/ClusterBaseModel or ServingRuntime/
// ClusterServingRuntime type-switch, directly or via spec()/status()) report
// "?" for a foreign type. AGE and runtimes' NAME instead guard on the
// broader `metav1.Object` interface, which any real Kubernetes object
// satisfies -- including the InferenceService used here, since it embeds
// ObjectMeta -- so that guard defends against non-Kubernetes runtime.Objects
// rather than a wrong domain kind, and legitimately renders the object's
// real (harmless) name / a zero-value age instead of "?".
func TestModelAndRuntimeColumnsWrongType(t *testing.T) {
	wrong := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "not-a-model-or-runtime"}}
	domainTypedColumn := map[string]bool{
		"NAME": true, "SCOPE": true, "ARCH": true, "PARAMS": true,
		"FORMAT": true, "STATE": true, "DISABLED": true, "FORMATS": true,
	}
	for _, resource := range []string{"models", "runtimes"} {
		e, err := resolve(resource)
		require.NoError(t, err, resource)
		for _, c := range e.Columns {
			var got string
			assert.NotPanics(t, func() { got = c.Extract(wrong) }, "%s/%s", resource, c.Name)
			switch {
			case c.Name == "AGE":
				assert.Equal(t, "-", got, "%s/%s", resource, c.Name)
			case resource == "runtimes" && c.Name == "NAME":
				assert.Equal(t, "not-a-model-or-runtime", got, "%s/%s", resource, c.Name)
			case domainTypedColumn[c.Name]:
				assert.Equal(t, "?", got, "%s/%s", resource, c.Name)
			}
		}
	}
}

func TestLongTailColumns(t *testing.T) {
	mt := "LoRA"
	base := "llama-3-3-70b"
	desired := int32(4)
	cases := []struct {
		resource string
		obj      runtime.Object
		want     map[string]string
	}{
		{"acceleratorclasses", &v1beta1.AcceleratorClass{
			ObjectMeta: metav1.ObjectMeta{Name: "h100"},
			Spec:       v1beta1.AcceleratorClassSpec{Vendor: "nvidia", Family: "hopper"},
		}, map[string]string{"NAME": "h100", "VENDOR": "nvidia", "FAMILY": "hopper"}},
		{"benchmarkjobs", &v1beta1.BenchmarkJob{
			ObjectMeta: metav1.ObjectMeta{Name: "bj"},
			Status:     v1beta1.BenchmarkJobStatus{State: "Running"},
		}, map[string]string{"NAME": "bj", "STATE": "Running"}},
		{"finetunedweights", &v1beta1.FineTunedWeight{
			ObjectMeta: metav1.ObjectMeta{Name: "ftw"},
			Spec: v1beta1.FineTunedWeightSpec{
				ModelType:    &mt,
				BaseModelRef: v1beta1.ObjectReference{Name: &base},
			},
		}, map[string]string{"NAME": "ftw", "TYPE": "LoRA", "BASEMODEL": "llama-3-3-70b"}},
		{"acceleratorquotas", &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: "team-a", Generation: 7},
			Spec: v1beta1.AcceleratorQuotaSpec{
				Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
				ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: "root"},
			},
			Status: v1beta1.AcceleratorQuotaStatus{
				ObservedGeneration: 7,
				Budgets: []v1beta1.AcceleratorBudgetStatus{{
					ResourceName: "nvidia.com/gpu", ResourceFlavor: "h100",
					Nominal: resource.MustParse("8"), Admitted: resource.MustParse("3"),
				}},
				Conditions: []metav1.Condition{{Type: v1beta1.AcceleratorQuotaReady, Status: metav1.ConditionTrue}},
			},
		}, map[string]string{
			"NAME": "team-a", "ROLE": "ClusterQueue", "PARENT": "root",
			"RESOURCE": "nvidia.com/gpu", "FLAVOR": "h100", "NOMINAL": "8",
			"ADMITTED": "3", "SOURCE": "Reported", "READY": "True",
		}},
		{"inferencereplicas", &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: "rep"},
			Spec: v1beta1.InferenceReplicaSpec{
				Component: v1beta1.EngineComponent,
				ParentRef: v1beta1.ParentReference{Name: "llama-70b"},
				Replicas:  &desired,
				Paused:    true,
			},
			Status: v1beta1.InferenceReplicaStatus{
				Replicas: 3, ReadyReplicas: 2, AvailableReplicas: 2, ServingReplicas: 1,
				UpdatedReplicas: 2, CurrentRevision: "rep-a", UpdateRevision: "rep-b",
				Migrations: []v1beta1.MigrationStatus{{}, {}},
				Conditions: []metav1.Condition{{
					Type: "RolloutStalled", Status: metav1.ConditionTrue, Reason: "InstancesFailing",
				}},
			},
		}, map[string]string{
			"NAME": "rep", "COMPONENT": "engine", "PARENT": "llama-70b",
			"DESIRED": "4", "CURRENT": "3", "READY": "2", "AVAILABLE": "2",
			"SERVING": "1", "UPDATED": "2", "CURRENT-REVISION": "rep-a",
			"UPDATE-REVISION": "rep-b", "MIGRATIONS": "2",
			"PAUSED": "true", "LIFECYCLE": "RolloutStalled=True", "REASON": "InstancesFailing",
			"LIFECYCLE-FRESHNESS": "Current",
		}},
		{"workloadclusters", &v1beta1.WorkloadCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "wc", Generation: 5},
			Spec: v1beta1.WorkloadClusterSpec{ClusterSource: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{
					SecretRef: corev1.SecretReference{Namespace: "ome", Name: "wc-kubeconfig"},
					Key:       "west.yaml",
				},
			}},
			Status: v1beta1.WorkloadClusterStatus{Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 4, Reason: "Connected",
			}}},
		}, map[string]string{
			"NAME": "wc", "CONNECTION": "KubeConfig", "REFERENCE": "ome/wc-kubeconfig",
			"KEY": "west.yaml", "READY": "True", "GENERATION": "5", "OBSERVED-GENERATION": "4", "REASON": "Connected",
		}},
	}
	for _, tc := range cases {
		e, err := resolve(tc.resource)
		require.NoError(t, err, tc.resource)
		got := map[string]string{}
		for _, c := range e.Columns {
			got[c.Name] = c.Extract(tc.obj)
		}
		for k, v := range tc.want {
			assert.Equal(t, v, got[k], "%s/%s", tc.resource, k)
		}
	}
}

// TestLongTailColumnsWrongType extends the nil-safety guarantee pinned by
// TestModelAndRuntimeColumnsWrongType (see its doc comment) to the five Task
// 2.3 entries. Unlike models/runtimes, none of these entries merge multiple
// concrete kinds behind one column set -- each is a single domain type -- so
// every column here (including NAME/AGE) guards on that entry's own type and
// reports "?" for any other kind; no metav1.Object broad-guard exception
// applies.
func TestLongTailColumnsWrongType(t *testing.T) {
	wrong := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "not-a-long-tail-resource"}}
	for _, resource := range []string{
		"acceleratorclasses", "acceleratorquotas", "benchmarkjobs", "finetunedweights",
		"inferencereplicas", "workloadclusters",
	} {
		e, err := resolve(resource)
		require.NoError(t, err, resource)
		for _, c := range e.Columns {
			var got string
			assert.NotPanics(t, func() { got = c.Extract(wrong) }, "%s/%s", resource, c.Name)
			assert.Equal(t, "?", got, "%s/%s", resource, c.Name)
		}
	}
}

func TestInferenceReplicaInventoryFallbacks(t *testing.T) {
	entry, err := resolve("ir")
	require.NoError(t, err)
	replica := &v1beta1.InferenceReplica{}
	got := map[string]string{}
	for _, column := range entry.Columns {
		got[column.Name] = column.Extract(replica)
	}
	assert.Equal(t, "-", got["DESIRED"])
	assert.Equal(t, "-", got["CURRENT-REVISION"])
	assert.Equal(t, "-", got["UPDATE-REVISION"])
	assert.Equal(t, "-", got["COORDINATION"])
	assert.Equal(t, "Unavailable", got["LIFECYCLE"])
	assert.Equal(t, "-", got["REASON"])
}

func TestInferenceReplicaLifecycleConditionPriorityAndFreshness(t *testing.T) {
	replica := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Status: v1beta1.InferenceReplicaStatus{Conditions: []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionTrue, Reason: "MinimumAvailable", ObservedGeneration: 2},
			{Type: "RolloutStalled", Status: metav1.ConditionTrue, Reason: "InstancesFailing", ObservedGeneration: 1},
		}},
	}

	got := inferenceReplicaLifecycle(replica)
	assert.Equal(t, "RolloutStalled=True", got.state, "an active stall must not be hidden by Ready=True")
	assert.Equal(t, "InstancesFailing", got.reason)
	assert.Equal(t, "Stale", got.freshness)

	replica.Status.Conditions[1].Status = metav1.ConditionFalse
	got = inferenceReplicaLifecycle(replica)
	assert.Equal(t, "Ready=True", got.state)
	assert.Equal(t, "MinimumAvailable", got.reason)
	assert.Equal(t, "Current", got.freshness)
}

func TestWorkloadClusterConnectionModes(t *testing.T) {
	tests := []struct {
		name      string
		source    v1beta1.ClusterConnectionSource
		wantKind  string
		wantValue string
		wantKey   string
	}{
		{
			name: "cluster profile",
			source: v1beta1.ClusterConnectionSource{
				ClusterProfileRef: &v1beta1.ClusterProfileRef{Name: "gpu-west"},
			},
			wantKind: "ClusterProfile", wantValue: "gpu-west", wantKey: "-",
		},
		{
			name: "kubeconfig default key",
			source: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{SecretRef: corev1.SecretReference{Name: "config"}},
			},
			wantKind: "KubeConfig", wantValue: "config", wantKey: "kubeconfig",
		},
		{
			name: "kubeconfig custom key",
			source: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{
					SecretRef: corev1.SecretReference{Name: "config"}, Key: "west.yaml",
				},
			},
			wantKind: "KubeConfig", wantValue: "config", wantKey: "west.yaml",
		},
		{
			name: "kubeconfig missing secret name",
			source: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{SecretRef: corev1.SecretReference{Namespace: "ome"}},
			},
			wantKind: "KubeConfig", wantValue: "-", wantKey: "kubeconfig",
		},
		{
			name:     "invalid empty source",
			wantKind: "Invalid", wantValue: "-", wantKey: "-",
		},
		{
			name: "invalid ambiguous source",
			source: v1beta1.ClusterConnectionSource{
				KubeConfig:        &v1beta1.KubeConfigSource{SecretRef: corev1.SecretReference{Name: "config"}},
				ClusterProfileRef: &v1beta1.ClusterProfileRef{Name: "profile"},
			},
			wantKind: "Invalid", wantValue: "-", wantKey: "-",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := workloadClusterConnection(&v1beta1.WorkloadCluster{
				Spec: v1beta1.WorkloadClusterSpec{ClusterSource: test.source},
			})
			assert.Equal(t, test.wantKind, got.kind)
			assert.Equal(t, test.wantValue, got.reference)
			assert.Equal(t, test.wantKey, got.key)
		})
	}
}

// TestRuntimeFormatsColumnFallback pins a fix for the FORMATS column rendering "-"
// on clusters running older OME operators that populate the deprecated flat
// .name field instead of the nested .modelFormat.name. The column now falls back
// to .modelFormat.name when the flat field is unset, maintaining version-skew
// tolerance.
func TestRuntimeFormatsColumnFallback(t *testing.T) {
	e, err := resolve("runtimes")
	require.NoError(t, err)

	cases := []struct {
		name     string
		runtime  runtime.Object
		expected string
	}{
		{
			name: "nested modelFormat.name only (old operator v1.2.1)",
			runtime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "old-runtime"},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:        "", // deprecated field empty
							ModelFormat: &v1beta1.ModelFormat{Name: "safetensors"},
						},
					},
				},
			},
			expected: "safetensors",
		},
		{
			name: "mixed: flat name and nested modelFormat.name",
			runtime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "mixed-runtime"},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:        "onnx", // new field
							ModelFormat: &v1beta1.ModelFormat{Name: "onnx"},
						},
						{
							Name:        "", // deprecated field empty
							ModelFormat: &v1beta1.ModelFormat{Name: "safetensors"},
						},
					},
				},
			},
			expected: "onnx,safetensors",
		},
		{
			name: "both empty (neither flat nor nested)",
			runtime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "empty-runtime"},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:        "",
							ModelFormat: &v1beta1.ModelFormat{Name: ""},
						},
					},
				},
			},
			expected: "-",
		},
		{
			name: "nil ModelFormat with empty Name",
			runtime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "nil-modelformat"},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:        "",
							ModelFormat: nil,
						},
					},
				},
			},
			expected: "-",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ""
			for _, c := range e.Columns {
				if c.Name == "FORMATS" {
					got = c.Extract(tc.runtime)
					break
				}
			}
			assert.Equal(t, tc.expected, got)
		})
	}
}
