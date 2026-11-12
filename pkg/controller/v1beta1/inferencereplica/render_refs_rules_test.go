package inferencereplica

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// editOperatorConfig rewrites blocks of the operator ConfigMap the rendering
// reconciler reads and keeps the rest of the map as the fixture ships it.
func editOperatorConfig(t *testing.T, r *Reconciler, blocks map[string]string) {
	t.Helper()
	cms := r.Clientset.CoreV1().ConfigMaps(constants.OMENamespace)
	cm, err := cms.Get(context.Background(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	maps.Copy(cm.Data, blocks)
	if _, err := cms.Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// advisoryCatalog is a model and a named runtime whose declared formats do
// not list the model's.
func advisoryCatalog() (*v1beta1.ClusterBaseModel, *v1beta1.ClusterServingRuntime) {
	model, rt := refsCatalog()
	rt.Spec.SupportedModelFormats = []v1beta1.SupportedModelFormat{{Name: "other", ModelFormat: &v1beta1.ModelFormat{Name: "other"}, AutoSelect: ptr.To(true)}}
	return model, rt
}

func TestRenderFromRefs_RetriesAnOperatorConfigThatDoesNotLoad(t *testing.T) {
	for _, tc := range []struct {
		name   string
		blocks map[string]string
		absent bool
		want   string
	}{
		{name: "a malformed InferenceService block", blocks: map[string]string{controllerconfig.PodMonitorConfigName: "{"}, want: "load rendering config: "},
		{name: "a malformed deploy block", blocks: map[string]string{controllerconfig.DeployConfigName: "{"}, want: "load deploy config: "},
		{name: "the InferenceService block is read before the deploy block", blocks: map[string]string{controllerconfig.PodMonitorConfigName: "{", controllerconfig.DeployConfigName: "{"}, want: "load rendering config: "},
		{name: "a ConfigMap that is not there", absent: true, want: "load rendering config: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model, rt := refsCatalog()
			ir := refsReplica()
			r, _, _ := renderingReconciler(t, model, rt, ir)
			if tc.absent {
				if err := r.Clientset.CoreV1().ConfigMaps(constants.OMENamespace).Delete(context.Background(), constants.InferenceServiceConfigMapName, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				editOperatorConfig(t, r, tc.blocks)
			}

			runners, err := r.renderFromRefs(context.Background(), ir)
			var blocked *renderBlocked
			if err == nil || !strings.HasPrefix(err.Error(), tc.want) || errors.As(err, &blocked) || runners != nil {
				t.Fatalf("renderFromRefs = %v, %v; want no runners and a retried error opening with %q", runners, err, tc.want)
			}
		})
	}
}

func TestRenderFromRefs_ReportsAControllerWithoutAClientset(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	r, _, _ := renderingReconciler(t, model, rt, ir)
	r.Clientset = nil

	_, err := r.renderFromRefs(context.Background(), ir)
	blocked := blockedRender(t, err, ReasonRenderFailed)
	if !strings.Contains(blocked.Message, "not configured") {
		t.Errorf("message = %q, want it to say rendering is not configured", blocked.Message)
	}
}

func TestRenderFromRefs_AdvisoryEventNamesTheRuntimeAndTheModel(t *testing.T) {
	model, rt := advisoryCatalog()
	ir := refsReplica()
	r, _, rec := renderingReconciler(t, model, rt, ir)

	runners, err := r.renderFromRefs(context.Background(), ir)
	if err != nil || len(runners) != 1 {
		t.Fatalf("renderFromRefs = %v, %v; want the named runtime rendered", runners, err)
	}
	events := drainEvents(rec)
	const opening = "Warning " + EventReasonRuntimeCompatibilityAdvisory + " Runtime " + refsRuntimeName + " does not declare support for model " + refsModelName + " ("
	const closing = "); proceeding because the runtime was named explicitly"
	if len(events) != 1 || !strings.HasPrefix(events[0], opening) || !strings.HasSuffix(events[0], closing) {
		t.Fatalf("events = %q, want one warning opening with %q and closing with %q", events, opening, closing)
	}
}

func TestRenderFromRefs_AdvisoryWithoutARecorderStillRenders(t *testing.T) {
	model, rt := advisoryCatalog()
	ir := refsReplica()
	r, _, _ := renderingReconciler(t, model, rt, ir)
	r.Recorder = nil

	runners, err := r.renderFromRefs(context.Background(), ir)
	if err != nil || len(runners) != 1 {
		t.Fatalf("renderFromRefs without a recorder = %v, %v; want the named runtime rendered", runners, err)
	}
}

func TestRenderFromRefs_LogsAnAutoSelectedRuntimeOnly(t *testing.T) {
	for _, tc := range []struct {
		name      string
		named     bool
		wantLines int
	}{
		{name: "an auto-selected runtime is logged", wantLines: 1},
		{name: "a named runtime is not", named: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model, rt := refsCatalog()
			ir := refsReplica()
			if !tc.named {
				ir.Spec.RuntimeRef = nil
			}
			r, _, _ := renderingReconciler(t, model, rt, ir)
			var lines []string
			ctx := logr.NewContext(context.Background(), funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{Verbosity: 1}))

			runners, err := r.renderFromRefs(ctx, ir)
			if err != nil || len(runners) != 1 {
				t.Fatalf("renderFromRefs = %v, %v; want one runner", runners, err)
			}
			var hits []string
			for _, line := range lines {
				if strings.Contains(line, `"msg"="Auto-selected runtime"`) {
					hits = append(hits, line)
				}
			}
			if len(hits) != tc.wantLines {
				t.Fatalf("auto-selection log lines = %q, want %d", hits, tc.wantLines)
			}
			if tc.wantLines == 1 && (!strings.Contains(hits[0], `"runtime"="`+refsRuntimeName+`"`) || !strings.Contains(hits[0], `"model"="`+refsModelName+`"`)) {
				t.Fatalf("auto-selection log line = %q, want the runtime and the model named", hits[0])
			}
		})
	}
}

func TestRenderFromRefs_PieceMissingMessageNamesTheRuntimeKind(t *testing.T) {
	model, rt := refsCatalog()
	namespaced := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-ns", Namespace: refsNamespace}, Spec: *rt.Spec.DeepCopy()}
	for _, tc := range []struct {
		name    string
		runtime string
		want    string
	}{
		{name: "a cluster runtime", runtime: refsRuntimeName, want: `ClusterServingRuntime "runtime-a" has no decoder piece; a decoder replica cannot render from it`},
		{name: "a namespaced runtime", runtime: "runtime-ns", want: `ServingRuntime "runtime-ns" has no decoder piece; a decoder replica cannot render from it`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ir := refsReplica()
			ir.Spec.Component = v1beta1.DecoderComponent
			ir.Spec.RuntimeRef.Name = tc.runtime
			r, _, _ := renderingReconciler(t, model, rt, namespaced, ir)

			_, err := r.renderFromRefs(context.Background(), ir)
			if blocked := blockedRender(t, err, ReasonRuntimePieceMissing); blocked.Message != tc.want {
				t.Fatalf("message = %q, want %q", blocked.Message, tc.want)
			}
		})
	}
}

func TestRenderFromRefs_ReportsAPieceThatCannotRender(t *testing.T) {
	model, rt := refsCatalog()
	rt.Spec.EngineConfig = &v1beta1.EngineSpec{}
	ir := refsReplica()
	r, _, _ := renderingReconciler(t, model, rt, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blocked := blockedRender(t, err, ReasonRenderFailed)
	if !strings.Contains(blocked.Message, "no containers found in pod spec") {
		t.Errorf("message = %q, want the renderer's words", blocked.Message)
	}
}

func TestRenderFromRefs_AppliesTheOperatorBlocksToTheRenderedPod(t *testing.T) {
	for _, tc := range []struct {
		name string
		// blocks are the operator ConfigMap blocks the row sets.
		blocks map[string]string
		want   func(*testing.T, corev1.PodSpec)
	}{
		{name: "the deploy block's termination grace period reaches the pod",
			blocks: map[string]string{controllerconfig.DeployConfigName: `{"defaultDeploymentMode":"RawDeployment","terminationGracePeriodSeconds":45}`},
			want: func(t *testing.T, pod corev1.PodSpec) {
				if got := ptr.Deref(pod.TerminationGracePeriodSeconds, 0); got != 45 {
					t.Fatalf("terminationGracePeriodSeconds = %d, want the deploy block's 45", got)
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model, rt := refsCatalog()
			ir := refsReplica()
			r, _, _ := renderingReconciler(t, model, rt, ir)
			editOperatorConfig(t, r, tc.blocks)

			runners, err := r.renderFromRefs(context.Background(), ir)
			if err != nil || len(runners) != 1 {
				t.Fatalf("renderFromRefs = %v, %v; want one runner", runners, err)
			}
			tc.want(t, runners[0].Template.Spec)
		})
	}
}
