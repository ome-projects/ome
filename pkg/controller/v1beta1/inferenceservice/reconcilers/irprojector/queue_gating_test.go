package irprojector

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// acceleratorNames is the list a wired dispatch site passes, from the manager's
// --accelerator-resources flag.
var acceleratorNames = []string{
	constants.NvidiaGPUResourceType,
	"amd.com/gpu",
	constants.GoogleTPUResourceType,
}

// cpuOnlyPodSpec is a router-shaped pod: real cpu/memory requests, no
// accelerator anywhere.
func cpuOnlyPodSpec() *corev1.PodSpec {
	return &corev1.PodSpec{Containers: []corev1.Container{{
		Name:  "ome-router",
		Image: "router:1.0",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		},
	}}}
}

// acceleratorPodSpec is an engine-shaped pod holding chips.
func acceleratorPodSpec(name string) *corev1.PodSpec {
	return &corev1.PodSpec{Containers: []corev1.Container{{
		Name:  "ome-container",
		Image: "sgl:1.0",
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceName(name): resource.MustParse("4")},
		},
	}}}
}

// queuedParams returns Params for a Component carrying the Kueue queue-name
// label on BOTH metadata sources the projector merges: the rendered
// per-Component ObjectMeta and the merged ComponentExtensionSpec. The
// multi-cluster placer writes spec.<component>.labels, which reaches the
// projector by both routes, so an exemption that clears only one of them still
// lands the label on the pod template.
func queuedParams(t *testing.T, isvc *v1beta1.InferenceService, queue string) Params {
	t.Helper()
	p := minimalParams(t, isvc, fake.NewClientBuilder().WithScheme(testScheme(t)).Build())
	p.ObjectMeta.Labels[constants.KueueQueueLabelKey] = queue
	if isvc.Spec.Engine.Labels == nil {
		isvc.Spec.Engine.Labels = map[string]string{}
	}
	isvc.Spec.Engine.Labels[constants.KueueQueueLabelKey] = queue
	p.QuotaAcceleratorResources = acceleratorNames
	return p
}

// committedIR projects and reads back the IR the projector wrote.
func committedIR(t *testing.T, g *gomega.WithT, p Params) *v1beta1.InferenceReplica {
	t.Helper()
	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(p.Client.Get(context.Background(), types.NamespacedName{
		Name:      InferenceReplicaName(p.ISVC.Name, p.Component),
		Namespace: p.ISVC.Namespace,
	}, got)).To(gomega.Succeed())
	return got
}

// TestEnsureInferenceReplica_QueueLabelGating is the whole rule: a Component
// whose rendered pods request an accelerator stays under Kueue admission
// control; one that requests none is released, so it never waits at a gate for
// a budget it charges nothing against.
//
// The decision is per Component, OR'd across the leader and worker templates —
// a multi-node Component is one gang, and releasing a cpu-only leader alone
// would let it start while its workers waited on quota.
func TestEnsureInferenceReplica_QueueLabelGating(t *testing.T) {
	const queue = "team-a"

	tests := []struct {
		name string
		// podSpec is the leader / single-pod template; workerPodSpec is set
		// only for the multi-pod cases.
		podSpec       *corev1.PodSpec
		workerPodSpec *corev1.PodSpec
		// unconfigured clears the accelerator list, standing in for a manager
		// started without --accelerator-resources.
		unconfigured bool
		wantGated    bool
		wantRunners  int
	}{
		{
			name:        "cpu-only component is released",
			podSpec:     cpuOnlyPodSpec(),
			wantGated:   false,
			wantRunners: 1,
		},
		{
			name:        "nvidia component stays gated",
			podSpec:     acceleratorPodSpec(constants.NvidiaGPUResourceType),
			wantGated:   true,
			wantRunners: 1,
		},
		{
			name:        "amd component stays gated",
			podSpec:     acceleratorPodSpec("amd.com/gpu"),
			wantGated:   true,
			wantRunners: 1,
		},
		{
			name:        "tpu component stays gated",
			podSpec:     acceleratorPodSpec(constants.GoogleTPUResourceType),
			wantGated:   true,
			wantRunners: 1,
		},
		{
			name:        "vendor outside the configured list is released",
			podSpec:     acceleratorPodSpec("example.com/fpga"),
			wantGated:   false,
			wantRunners: 1,
		},
		{
			// The gang holds chips even though its leader does not, so both
			// runners stay gated.
			name:          "gang with a cpu leader and accelerator workers stays gated on both runners",
			podSpec:       cpuOnlyPodSpec(),
			workerPodSpec: acceleratorPodSpec(constants.GoogleTPUResourceType),
			wantGated:     true,
			wantRunners:   2,
		},
		{
			name:          "all-cpu gang releases both runners",
			podSpec:       cpuOnlyPodSpec(),
			workerPodSpec: cpuOnlyPodSpec(),
			wantGated:     false,
			wantRunners:   2,
		},
		{
			// The fail-safe. With the flag unset there is no basis to tell an
			// accelerator request from a cpu one, and the two errors are not
			// symmetric: gating a cpu pod is a delay, releasing an accelerator
			// pod is unbudgeted silicon.
			name:         "no configured resources governs everything",
			podSpec:      cpuOnlyPodSpec(),
			unconfigured: true,
			wantGated:    true,
			wantRunners:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			isvc := baselineISVC("llama", "prod")
			p := queuedParams(t, isvc, queue)
			p.PodSpec = tt.podSpec
			if tt.workerPodSpec != nil {
				p.MultiPod = true
				p.WorkerSize = 3
				p.WorkerPodSpec = tt.workerPodSpec
			}
			if tt.unconfigured {
				p.QuotaAcceleratorResources = nil
			}

			got := committedIR(t, g, p)

			g.Expect(got.Spec.Runners).To(gomega.HaveLen(tt.wantRunners))
			for _, r := range got.Spec.Runners {
				labels := r.Template.ObjectMeta.Labels
				if tt.wantGated {
					g.Expect(labels).To(gomega.HaveKeyWithValue(constants.KueueQueueLabelKey, queue),
						"runner %q must carry the queue-name label Kueue gates on", r.Name)
				} else {
					g.Expect(labels).NotTo(gomega.HaveKey(constants.KueueQueueLabelKey),
						"runner %q must not carry the queue-name label", r.Name)
				}
			}
			// The IR object mirrors the same decision, so kubectl-side
			// filtering never disagrees with what the pods carry.
			if tt.wantGated {
				g.Expect(got.Labels).To(gomega.HaveKeyWithValue(constants.KueueQueueLabelKey, queue))
			} else {
				g.Expect(got.Labels).NotTo(gomega.HaveKey(constants.KueueQueueLabelKey))
			}
		})
	}
}

// TestEnsureInferenceReplica_ReleaseKeepsEveryOtherLabel pins the blast radius:
// releasing a Component removes one key, not the label set. Selectors,
// ownership labels and user-authored per-Component labels are untouched, and so
// is the priority-class label, which is inert without a queue name.
func TestEnsureInferenceReplica_ReleaseKeepsEveryOtherLabel(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	p := queuedParams(t, isvc, "team-a")
	p.PodSpec = cpuOnlyPodSpec()
	isvc.Spec.Engine.Labels["team"] = "ml-platform"
	p.ObjectMeta.Labels[constants.KueueWorkloadPriorityClassLabelKey] = "high"

	tmpl := committedIR(t, g, p).Spec.Runners[0].Template.ObjectMeta.Labels

	g.Expect(tmpl).NotTo(gomega.HaveKey(constants.KueueQueueLabelKey))
	g.Expect(tmpl).To(gomega.HaveKeyWithValue(constants.InferenceServicePodLabelKey, isvc.Name),
		"the OMENative selector labels must survive the release")
	g.Expect(tmpl).To(gomega.HaveKeyWithValue("team", "ml-platform"),
		"user-authored per-Component labels must survive the release")
	g.Expect(tmpl).To(gomega.HaveKeyWithValue(constants.KueueWorkloadPriorityClassLabelKey, "high"),
		"only the queue-name label gates a pod; the priority-class label is inert without it")
}

// TestEnsureInferenceReplica_ReleaseDoesNotMutateCallerState pins that the
// release works on copies. p.ObjectMeta.Labels is the dispatch site's map —
// also handed to the stable Service and PodMonitor — and p.ComponentExt points
// into the merged ComponentExtensionSpec the ISVC reconciler reuses for
// autoscaling and status. Clearing a key in place would reach both.
func TestEnsureInferenceReplica_ReleaseDoesNotMutateCallerState(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	p := queuedParams(t, isvc, "team-a")
	p.PodSpec = cpuOnlyPodSpec()

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Expect(p.ObjectMeta.Labels).To(gomega.HaveKeyWithValue(constants.KueueQueueLabelKey, "team-a"),
		"the caller's rendered ObjectMeta must be left as the dispatch site built it")
	g.Expect(isvc.Spec.Engine.Labels).To(gomega.HaveKeyWithValue(constants.KueueQueueLabelKey, "team-a"),
		"the merged ComponentExtensionSpec must be left as the ISVC reconciler built it")
}

// TestEnsureInferenceReplica_ReleaseIsIdempotent pins that a second pass with
// the same inputs no-ops. The projector skips the write when nothing it owns
// changed; a release that rebuilt maps unstably would churn the IR's
// resourceVersion every reconcile and fight the IR controller's status writes.
func TestEnsureInferenceReplica_ReleaseIsIdempotent(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	p := queuedParams(t, isvc, "team-a")
	p.PodSpec = cpuOnlyPodSpec()

	first := committedIR(t, g, p)
	second := committedIR(t, g, p)

	g.Expect(second.ResourceVersion).To(gomega.Equal(first.ResourceVersion),
		"a re-projection with unchanged inputs must not write")
}
