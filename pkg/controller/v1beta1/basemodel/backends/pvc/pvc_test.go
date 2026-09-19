package pvc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

func testMetadataJobConfig() MetadataJobConfig {
	return MetadataJobConfig{
		Image:          "ome-agent:test",
		ServiceAccount: "ome-model-metadata",
	}
}

func TestIsPVCStorage(t *testing.T) {
	tests := []struct {
		name string
		spec *v1beta1.BaseModelSpec
		want bool
	}{
		{name: "nil spec", spec: nil, want: false},
		{name: "nil storage", spec: &v1beta1.BaseModelSpec{}, want: false},
		{name: "nil storage uri", spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{}}, want: false},
		{
			name: "pvc namespaced uri",
			spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("pvc://my-pvc/models/llama2-7b")}},
			want: true,
		},
		{
			name: "pvc cluster-scoped uri",
			spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("pvc://shared:my-pvc/models/llama2-7b")}},
			want: true,
		},
		{
			name: "huggingface uri",
			spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("hf://meta-llama/Llama-2-7b")}},
			want: false,
		},
		{
			name: "oci uri",
			spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("oci://n/ns/b/bucket/o/path")}},
			want: false,
		},
		{
			name: "garbage uri",
			spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("not-a-uri")}},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPVCStorage(tc.spec); got != tc.want {
				t.Fatalf("isPVCStorage = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResolvePVCNamespace(t *testing.T) {
	tests := []struct {
		name            string
		modelNamespace  string
		isClusterScoped bool
		components      *storage.PVCStorageComponents
		want            string
		wantErr         bool
	}{
		{
			name:       "nil components",
			components: nil,
			wantErr:    true,
		},
		{
			name:           "namespaced, no ns prefix in uri",
			modelNamespace: "models",
			components:     &storage.PVCStorageComponents{PVCName: "pvc1", SubPath: "x"},
			want:           "models",
		},
		{
			name:           "namespaced, ns prefix in uri is rejected",
			modelNamespace: "models",
			components:     &storage.PVCStorageComponents{Namespace: "other", PVCName: "pvc1", SubPath: "x"},
			wantErr:        true,
		},
		{
			name:            "cluster-scoped, ns prefix in uri",
			isClusterScoped: true,
			components:      &storage.PVCStorageComponents{Namespace: "shared", PVCName: "pvc1", SubPath: "x"},
			want:            "shared",
		},
		{
			name:            "cluster-scoped, missing ns prefix is rejected",
			isClusterScoped: true,
			components:      &storage.PVCStorageComponents{PVCName: "pvc1", SubPath: "x"},
			wantErr:         true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolvePVCNamespace(tc.modelNamespace, tc.isClusterScoped, tc.components)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got namespace=%q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("namespace = %q, want %q", got, tc.want)
			}
		})
	}
}

func newPVCTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 AddToScheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("batchv1 AddToScheme: %v", err)
	}
	// rbacv1: ensureMetadataJobRBAC creates a RoleBinding in the OME
	// namespace before the Job is submitted (covers PVC-backed BMs in
	// non-OME namespaces). The reconcile path's fake client needs the
	// kind registered or the create errors out.
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("rbacv1 AddToScheme: %v", err)
	}
	return scheme
}

func newBoundPVC(ns, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func newPendingPVC(ns, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
}

// jobWithCondition returns a Job whose name matches buildMetadataJob's
// deterministic naming, carrying the given condition. Used to simulate
// Job-watcher-driven re-reconciles in tests.
func jobWithCondition(t *testing.T, obj client.Object, isClusterScoped bool, components *storage.PVCStorageComponents, pvcNamespace string, condType batchv1.JobConditionType, message string) *batchv1.Job {
	t.Helper()
	scheme := newPVCTestScheme(t)
	job, err := buildMetadataJob(obj, isClusterScoped, components, pvcNamespace, testMetadataJobConfig(), scheme)
	if err != nil {
		t.Fatalf("buildMetadataJob: %v", err)
	}
	job.Status = batchv1.JobStatus{
		Conditions: []batchv1.JobCondition{
			{Type: condType, Status: corev1.ConditionTrue, Message: message},
		},
	}
	return job
}

func TestReconcilePVCStorage_BaseModel(t *testing.T) {
	scheme := newPVCTestScheme(t)
	ctx := context.Background()
	log := logf.Log.WithName("test")

	bm := func() *v1beta1.BaseModel {
		return &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "models", UID: "uid"},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://my-pvc/models/llama"),
			}},
		}
	}
	components := &storage.PVCStorageComponents{PVCName: "my-pvc", SubPath: "models/llama"}

	tests := []struct {
		name         string
		bm           *v1beta1.BaseModel
		seed         []client.Object
		cfg          MetadataJobConfig
		wantState    v1beta1.LifeCycleState
		wantReason   string
		wantCondType string
		wantCondVal  metav1.ConditionStatus
		wantJobCount int
	}{
		{
			name: "valid bound PVC creates Job and asserts InTransit",
			bm:   bm(),
			seed: []client.Object{newBoundPVC("models", "my-pvc")},
			cfg:  testMetadataJobConfig(),
			// First reconcile creates the Job and asserts InTransit while
			// the agent runs; ConfigMap takes over once it completes.
			wantState:    v1beta1.LifeCycleStateInTransit,
			wantReason:   v1beta1.ModelConditionReasonPVCMetadataExtracting,
			wantCondType: v1beta1.ModelConditionSourceReachable,
			wantCondVal:  metav1.ConditionTrue,
			wantJobCount: 1,
		},
		{
			name: "Job exists, no ConfigMap yet → state untouched (still InTransit from prior step)",
			bm:   bm(),
			seed: []client.Object{
				newBoundPVC("models", "my-pvc"),
				jobWithCondition(t, bm(), false, components, "models", batchv1.JobComplete, ""),
			},
			cfg: testMetadataJobConfig(),
			// reconciler doesn't translate Job status; it reads the ConfigMap.
			// No ConfigMap seeded → no state/condition write at all.
			wantState:    "",
			wantCondType: "", // no condition
		},
		{
			name: "PVC missing",
			bm: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "models"},
				Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
					StorageUri: ptr.To("pvc://no-such/models/llama"),
				}},
			},
			cfg:          testMetadataJobConfig(),
			wantState:    v1beta1.LifeCycleStateFailed,
			wantReason:   v1beta1.ModelConditionReasonPVCNotFound,
			wantCondType: v1beta1.ModelConditionSourceReachable,
			wantCondVal:  metav1.ConditionFalse,
		},
		{
			name:         "PVC pending",
			bm:           bm(),
			seed:         []client.Object{newPendingPVC("models", "my-pvc")},
			cfg:          testMetadataJobConfig(),
			wantState:    v1beta1.LifeCycleStateInTransit,
			wantReason:   v1beta1.ModelConditionReasonPVCNotBound,
			wantCondType: v1beta1.ModelConditionSourceReachable,
			wantCondVal:  metav1.ConditionTrue,
		},
		{
			name: "namespaced uri rejected for namespaced BaseModel",
			bm: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "models"},
				Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
					StorageUri: ptr.To("pvc://other:my-pvc/models/llama"),
				}},
			},
			cfg:          testMetadataJobConfig(),
			wantState:    v1beta1.LifeCycleStateFailed,
			wantReason:   v1beta1.ModelConditionReasonPVCInvalid,
			wantCondType: v1beta1.ModelConditionSourceReachable,
			wantCondVal:  metav1.ConditionFalse,
		},
		{
			name:         "missing image config → Failed with PVCConfigMissing",
			bm:           bm(),
			seed:         []client.Object{newBoundPVC("models", "my-pvc")},
			cfg:          MetadataJobConfig{},
			wantState:    v1beta1.LifeCycleStateFailed,
			wantReason:   v1beta1.ModelConditionReasonPVCConfigMissing,
			wantCondType: v1beta1.ModelConditionSourceReachable,
			wantCondVal:  metav1.ConditionFalse,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objs := append([]client.Object{tc.bm}, tc.seed...)
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(&v1beta1.BaseModel{}).
				Build()

			if _, err := Reconcile(ctx, c, scheme, log, tc.bm, &tc.bm.Spec, false, tc.cfg); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := &v1beta1.BaseModel{}
			if err := c.Get(ctx, types.NamespacedName{Name: tc.bm.Name, Namespace: tc.bm.Namespace}, got); err != nil {
				t.Fatalf("get baseModel: %v", err)
			}
			// wantState == "" means the reconciler must not write state.
			if tc.wantState != "" && got.Status.State != tc.wantState {
				t.Errorf("state = %q, want %q", got.Status.State, tc.wantState)
			}
			if tc.wantState == "" && got.Status.State != "" {
				t.Errorf("state was written (%q) but wantState is empty — reconciler must defer to ConfigMap path", got.Status.State)
			}
			if tc.wantCondType != "" {
				cond := meta.FindStatusCondition(got.Status.Conditions, tc.wantCondType)
				if cond == nil {
					t.Fatalf("expected condition %q to be set", tc.wantCondType)
				}
				if cond.Status != tc.wantCondVal {
					t.Errorf("cond status = %q, want %q", cond.Status, tc.wantCondVal)
				}
				if cond.Reason != tc.wantReason {
					t.Errorf("cond reason = %q, want %q", cond.Reason, tc.wantReason)
				}
			}

			if tc.wantJobCount > 0 {
				jobs := &batchv1.JobList{}
				if err := c.List(ctx, jobs, client.InNamespace(tc.bm.Namespace)); err != nil {
					t.Fatalf("list jobs: %v", err)
				}
				if len(jobs.Items) != tc.wantJobCount {
					t.Errorf("got %d jobs, want %d", len(jobs.Items), tc.wantJobCount)
				}
			}
		})
	}
}

// TestApplyPVCStatusFromConfigMap covers the new direct-Get path. PVC
// status flows from a single ConfigMap (not the per-node List), and
// Status.NodesReady stays empty because there is no node concept here.
func TestApplyPVCStatusFromConfigMap(t *testing.T) {
	scheme := newPVCTestScheme(t)
	ctx := context.Background()
	log := logf.Log.WithName("test")

	makeCM := func(modelName, ns string, isCluster bool, status shared.ModelStatus, cfg *shared.ModelConfig, errMsg string) *corev1.ConfigMap {
		t.Helper()
		entry := shared.ModelEntry{Name: modelName, Status: status, Config: cfg}
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		labels := map[string]string{
			constants.PVCStorageConfigMapLabel:  "true",
			constants.PVCMetadataModelNameLabel: modelName,
		}
		var ann map[string]string
		if errMsg != "" {
			ann = map[string]string{constants.PVCMetadataLastErrorAnnotation: errMsg}
		}
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        constants.GetPVCMetadataConfigMapName(modelName, ns, isCluster),
				Namespace:   constants.OMENamespace,
				Labels:      labels,
				Annotations: ann,
			},
			Data: map[string]string{
				constants.GetModelConfigMapKey(ns, modelName, isCluster): string(data),
			},
		}
	}

	t.Run("Ready: state→Ready, spec updated, NodesReady empty", func(t *testing.T) {
		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: "models"},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("pvc://my-pvc/p")}},
		}
		cm := makeCM("llama-7b", "models", false, shared.ModelStatusReady, &shared.ModelConfig{
			ModelType: "llama", ModelArchitecture: "LlamaForCausalLM", MaxTokens: 4096,
		}, "")
		c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bm, cm).
			WithStatusSubresource(&v1beta1.BaseModel{}).
			Build()

		_, err := applyPVCStatusFromConfigMap(ctx, c, log, bm, false, nil)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		got := &v1beta1.BaseModel{}
		if err := c.Get(ctx, types.NamespacedName{Name: bm.Name, Namespace: bm.Namespace}, got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status.State != v1beta1.LifeCycleStateReady {
			t.Errorf("state = %q, want Ready", got.Status.State)
		}
		if len(got.Status.NodesReady) != 0 {
			t.Errorf("PVC NodesReady must be empty; got %v", got.Status.NodesReady)
		}
		if got.Spec.ModelType == nil || *got.Spec.ModelType != "llama" {
			t.Errorf("spec.ModelType not applied: %+v", got.Spec.ModelType)
		}
	})

	t.Run("Failed: state→Failed with extraction reason + agent error", func(t *testing.T) {
		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: "models"},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("pvc://my-pvc/p")}},
		}
		cm := makeCM("llama-7b", "models", false, shared.ModelStatusFailed, nil, "config.json missing")
		c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bm, cm).
			WithStatusSubresource(&v1beta1.BaseModel{}).
			Build()

		_, err := applyPVCStatusFromConfigMap(ctx, c, log, bm, false, nil)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		got := &v1beta1.BaseModel{}
		if err := c.Get(ctx, types.NamespacedName{Name: bm.Name, Namespace: bm.Namespace}, got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status.State != v1beta1.LifeCycleStateFailed {
			t.Errorf("state = %q, want Failed", got.Status.State)
		}
		ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
		if ready == nil || ready.Reason != v1beta1.ModelConditionReasonPVCMetadataExtractionFailed {
			t.Errorf("Ready condition wrong: %+v", ready)
		}
	})

	t.Run("ConfigMap absent: state untouched", func(t *testing.T) {
		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: "models"},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("pvc://my-pvc/p")}},
		}
		c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bm).
			WithStatusSubresource(&v1beta1.BaseModel{}).
			Build()
		// Pass a still-running Job (no JobFailed condition) so the
		// new JobFailed branch in applyPVCStatusFromConfigMap is NOT taken.
		runningJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "models"}}
		res, err := applyPVCStatusFromConfigMap(ctx, c, log, bm, false, runningJob)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		if res.RequeueAfter == 0 {
			t.Errorf("expected requeue when ConfigMap absent")
		}
		got := &v1beta1.BaseModel{}
		_ = c.Get(ctx, types.NamespacedName{Name: bm.Name, Namespace: bm.Namespace}, got)
		if got.Status.State != "" {
			t.Errorf("state was written %q despite missing ConfigMap", got.Status.State)
		}
	})

	t.Run("ConfigMap absent + Job Failed: state→Failed with Job message", func(t *testing.T) {
		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: "models"},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("pvc://my-pvc/p")}},
		}
		c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bm).
			WithStatusSubresource(&v1beta1.BaseModel{}).
			Build()

		// Simulate a Job that exhausted its BackoffLimit. The agent
		// pod was OOM-killed before its SIGTERM handler could run, so
		// no per-PVC ConfigMap was ever written.
		failedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "models"},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "BackoffLimitExceeded",
				Message: "Job has reached the specified backoff limit",
			}}},
		}

		if _, err := applyPVCStatusFromConfigMap(ctx, c, log, bm, false, failedJob); err != nil {
			t.Fatalf("apply: %v", err)
		}
		got := &v1beta1.BaseModel{}
		if err := c.Get(ctx, types.NamespacedName{Name: bm.Name, Namespace: bm.Namespace}, got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status.State != v1beta1.LifeCycleStateFailed {
			t.Errorf("state = %q, want Failed (Job exhausted BackoffLimit)", got.Status.State)
		}
		ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
		if ready == nil || ready.Reason != v1beta1.ModelConditionReasonPVCMetadataExtractionFailed {
			t.Errorf("Ready condition wrong: %+v", ready)
		}
		if ready != nil && !strings.Contains(ready.Message, "BackoffLimitExceeded") {
			t.Errorf("expected Job condition reason in message, got %q", ready.Message)
		}
	})
}

// TestJobFailedConditionMessage exhaustively covers the JobFailed
// detection helper. Catches regressions in either the predicate
// (wrong condition type / wrong status) or the message assembly.
func TestJobFailedConditionMessage(t *testing.T) {
	cases := []struct {
		name    string
		job     *batchv1.Job
		wantOk  bool
		wantSub string // substring expected in the returned message
	}{
		{name: "nil job", job: nil},
		{name: "no conditions", job: &batchv1.Job{}},
		{
			name: "JobFailed=True with Reason+Message",
			job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Reason: "DeadlineExceeded", Message: "Job was active longer than specified deadline",
			}}}},
			wantOk:  true,
			wantSub: "DeadlineExceeded: Job was active longer than specified deadline",
		},
		{
			name: "JobFailed=True with empty Message: synthesizes a default",
			job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
			}}}},
			wantOk:  true,
			wantSub: "exhausted its retry budget",
		},
		{
			name: "JobFailed=False (transient): not yet terminal",
			job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionFalse,
			}}}},
		},
		{
			name: "JobComplete=True: success, never reports Failed",
			job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
			}}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, msg := jobFailedConditionMessage(tc.job)
			if ok != tc.wantOk {
				t.Errorf("ok = %v, want %v (msg=%q)", ok, tc.wantOk, msg)
			}
			if tc.wantSub != "" && !strings.Contains(msg, tc.wantSub) {
				t.Errorf("msg %q missing %q", msg, tc.wantSub)
			}
		})
	}
}

func TestReconcilePVCStorage_ClusterBaseModel(t *testing.T) {
	scheme := newPVCTestScheme(t)
	ctx := context.Background()
	log := logf.Log.WithName("test")

	cbm := func() *v1beta1.ClusterBaseModel {
		return &v1beta1.ClusterBaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-llama", UID: "uid-cbm"},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://shared:my-pvc/models/llama"),
			}},
		}
	}

	t.Run("valid bound PVC → creates Job in PVC namespace", func(t *testing.T) {
		c := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cbm(), newBoundPVC("shared", "my-pvc")).
			WithStatusSubresource(&v1beta1.ClusterBaseModel{}).
			Build()

		_, err := Reconcile(ctx, c, scheme, log, cbm(), &cbm().Spec, true, testMetadataJobConfig())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := &v1beta1.ClusterBaseModel{}
		if err := c.Get(ctx, types.NamespacedName{Name: cbm().Name}, got); err != nil {
			t.Fatalf("get clusterBaseModel: %v", err)
		}
		if got.Status.State != v1beta1.LifeCycleStateInTransit {
			t.Errorf("state = %q, want In_Transit", got.Status.State)
		}

		jobs := &batchv1.JobList{}
		if err := c.List(ctx, jobs, client.InNamespace("shared")); err != nil {
			t.Fatalf("list jobs: %v", err)
		}
		if len(jobs.Items) != 1 {
			t.Fatalf("expected 1 Job in 'shared' ns, got %d", len(jobs.Items))
		}
	})

	t.Run("missing namespace prefix is rejected", func(t *testing.T) {
		bad := cbm()
		bad.Spec.Storage.StorageUri = ptr.To("pvc://my-pvc/models/llama")
		c := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(bad).
			WithStatusSubresource(&v1beta1.ClusterBaseModel{}).
			Build()

		if _, err := Reconcile(ctx, c, scheme, log, bad, &bad.Spec, true, testMetadataJobConfig()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := &v1beta1.ClusterBaseModel{}
		if err := c.Get(ctx, types.NamespacedName{Name: bad.Name}, got); err != nil {
			t.Fatalf("get clusterBaseModel: %v", err)
		}
		if got.Status.State != v1beta1.LifeCycleStateFailed {
			t.Errorf("state = %q, want Failed", got.Status.State)
		}
		cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
		if cond == nil || cond.Reason != v1beta1.ModelConditionReasonPVCInvalid {
			t.Errorf("expected PVCInvalid reason, got %+v", cond)
		}
	})
}
