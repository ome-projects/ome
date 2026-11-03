package servingruntime

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/gomega"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

// Two runtimes conflict when both auto-select the same format, version,
// architecture, framework and size range at the same priority; a disabled
// runtime and a runtime's own earlier state never conflict.
func TestValidateServingRuntimePriority(t *testing.T) {
	samePriority := gomega.Equal(fmt.Errorf(InvalidPriorityError, "vllm"))
	tests := []struct {
		name            string
		newRuntime      *v1beta1.ServingRuntime
		existingRuntime *v1beta1.ServingRuntime
		want            gomega.OmegaMatcher
	}{
		{
			name:            "When existing serving runtime is disabled it should return nil",
			newRuntime:      servingRuntime("new-runtime", "test"),
			existingRuntime: servingRuntime("existing-runtime", "test", withDisabled(true)),
			want:            gomega.BeNil(),
		},
		{
			name:            "When new serving runtime and existing runtime are same it should return nil",
			newRuntime:      servingRuntime("example-runtime", "test"),
			existingRuntime: servingRuntime("example-runtime", "test"),
			want:            gomega.BeNil(),
		},
		{
			name:            "When model format is different it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test"),
			existingRuntime: servingRuntime("example-runtime-2", "test", withFormat("lightgbm")),
			want:            gomega.BeNil(),
		},
		{
			name:            "When autoselect is false in the new serving runtime it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withAutoSelect(false)),
			existingRuntime: servingRuntime("example-runtime-2", "test"),
			want:            gomega.BeNil(),
		},
		{
			name:            "When autoselect is not specified in the new serving runtime it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withoutAutoSelect()),
			existingRuntime: servingRuntime("example-runtime-2", "test"),
			want:            gomega.BeNil(),
		},
		{
			name:            "When autoselect is false in the existing serving runtime it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test"),
			existingRuntime: servingRuntime("example-runtime-2", "test", withAutoSelect(false)),
			want:            gomega.BeNil(),
		},
		{
			name:            "When model version is nil in both serving runtime and priority is same then it should return error",
			newRuntime:      servingRuntime("example-runtime-1", "test", withoutVersion()),
			existingRuntime: servingRuntime("example-runtime-2", "test", withoutVersion()),
			want:            samePriority,
		},
		{
			name:            "When model version is nil in both serving runtime and priority is not same then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withoutVersion(), withPriority(2)),
			existingRuntime: servingRuntime("example-runtime-2", "test", withoutVersion()),
			want:            gomega.BeNil(),
		},
		{
			name:            "When model version is nil in new serving runtime and priority is same then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withoutVersion()),
			existingRuntime: servingRuntime("example-runtime-2", "test"),
			want:            gomega.BeNil(),
		},
		{
			name:            "When model version is nil in existing serving runtime and priority is same then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test"),
			existingRuntime: servingRuntime("example-runtime-2", "test", withoutVersion()),
			want:            gomega.BeNil(),
		},
		{
			name:            "When two serving runtime has the same supported model format then it should return error",
			newRuntime:      servingRuntime("example-runtime-1", "test", withArchitecture("CohereForCausalLM")),
			existingRuntime: servingRuntime("example-runtime-2", "test", withArchitecture("CohereForCausalLM")),
			want:            samePriority,
		},
		{
			name:            "When two serving runtime has the same supported model format but different architecture then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withArchitecture("CohereForCausalLM")),
			existingRuntime: servingRuntime("example-runtime-2", "test", withArchitecture("Cohere2ForCasualLM")),
			want:            gomega.BeNil(),
		},
		{
			name: "When two serving runtime has the same supported model format but different framework then it should return nil",
			newRuntime: servingRuntime("example-runtime-1", "test",
				withArchitecture("LlamaForCasualLM"), withModelFormat("safetensors", "1.0.0"), withFramework("Transformers", "1.0.0")),
			existingRuntime: servingRuntime("example-runtime-2", "test",
				withArchitecture("LlamaForCasualLM"), withModelFormat("safetensors", "1.0.0"), withFramework("Transformers", "1.2.0")),
			want: gomega.BeNil(),
		},
		{
			name: "When two serving runtime has the same supported model format but different size then it should return nil",
			newRuntime: servingRuntime("example-runtime-1", "test",
				withFormat("safetensors"), withArchitecture("LlamaForCasualLM"), withSizeRange("100B", "200B")),
			existingRuntime: servingRuntime("example-runtime-2", "test",
				withFormat("safetensors"), withArchitecture("LlamaForCasualLM"), withSizeRange("300B", "600B")),
			want: gomega.BeNil(),
		},
		{
			name:            "When model version is same in both serving runtime and priority is same then it should return error",
			newRuntime:      servingRuntime("example-runtime-1", "test"),
			existingRuntime: servingRuntime("example-runtime-2", "test"),
			want:            samePriority,
		},
		{
			name:            "When model version is different but priority is same then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withVersion("1.3")),
			existingRuntime: servingRuntime("example-runtime-2", "test", withVersion("1.0")),
			want:            gomega.BeNil(),
		},
		{
			name:            "When priority is nil in both serving runtime then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withoutPriority()),
			existingRuntime: servingRuntime("example-runtime-2", "test", withoutPriority()),
			want:            gomega.BeNil(),
		},
		{
			name:            "When priority is nil in new serving runtime and priority is specified in existing serving runtime then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test", withoutPriority()),
			existingRuntime: servingRuntime("example-runtime-2", "test"),
			want:            gomega.BeNil(),
		},
		{
			name:            "When priority is nil in existing serving runtime and priority is specified in new serving runtime then it should return nil",
			newRuntime:      servingRuntime("example-runtime-1", "test"),
			existingRuntime: servingRuntime("example-runtime-2", "test", withoutPriority()),
			want:            gomega.BeNil(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			err := validateServingRuntimePriority(&tt.newRuntime.Spec, &tt.existingRuntime.Spec,
				tt.newRuntime.Name, tt.existingRuntime.Name)
			g.Expect(err).To(tt.want)
		})
	}
}

func TestValidateModelFormatPrioritySame(t *testing.T) {
	tests := []struct {
		name    string
		runtime *v1beta1.ServingRuntime
		want    gomega.OmegaMatcher
	}{
		{
			name:    "When different priority assigned for the same model format in the runtime then it should return error",
			runtime: servingRuntime("example-runtime-1", "test", withFormats(autoSelectedFormat("vllm", 1), autoSelectedFormat("vllm", 2))),
			want:    gomega.Equal(fmt.Errorf("different priorities assigned for the model format %s", "vllm")),
		},
		{
			name:    "When same priority assigned for the same model format in the runtime then it should return nil",
			runtime: servingRuntime("example-runtime-1", "test", withFormats(autoSelectedFormat("vllm", 2), autoSelectedFormat("vllm", 2))),
			want:    gomega.BeNil(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			err := validation.ValidateModelFormatPrioritySame(&tt.runtime.Spec)
			g.Expect(err).To(tt.want)
		})
	}
}

// safetensorsAt is a cluster runtime that auto-selects safetensors at the
// given priority.
func safetensorsAt(name string, priority int32) *v1beta1.ClusterServingRuntime {
	return clusterServingRuntime(name, withFormat("safetensors"), withPriority(priority))
}

// The cluster validator compares the admitted runtime with every other
// cluster runtime, so a priority taken by another one is refused.
func TestClusterServingRuntimeValidator_PriorityConflictAcrossCSRs(t *testing.T) {
	g := gomega.NewWithT(t)
	validator := newClusterServingRuntimeValidator(t, safetensorsAt("rt-a", 1), safetensorsAt("rt-b", 2))

	resp := validator.Handle(context.Background(), createReq(t, safetensorsAt("rt-c", 1)))

	g.Expect(resp.Allowed).To(gomega.BeFalse(), "a priority another cluster runtime holds must be refused")
	g.Expect(resp.Result).NotTo(gomega.BeNil())
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("safetensors"), "the refusal names the format")
}

func TestClusterServingRuntimeValidator_NoPriorityConflict(t *testing.T) {
	g := gomega.NewWithT(t)
	validator := newClusterServingRuntimeValidator(t, safetensorsAt("rt-a", 1), safetensorsAt("rt-b", 2))

	resp := validator.Handle(context.Background(), createReq(t, safetensorsAt("rt-c", 3)))

	g.Expect(resp.Allowed).To(gomega.BeTrue(), "a free priority is admitted; got refusal: %v", resp.Result)
}

// A runtime's own stored state is skipped, so an update at the same priority
// is not a conflict with itself.
func TestClusterServingRuntimeValidator_SelfUpdateNotFlagged(t *testing.T) {
	g := gomega.NewWithT(t)
	original := safetensorsAt("rt-a", 1)
	validator := newClusterServingRuntimeValidator(t, original)

	updated := clusterServingRuntime("rt-a", withFormat("safetensors"), withPriority(1), withImage("ome/vllm:newer"))
	resp := validator.Handle(context.Background(), updateReq(t, original, updated))

	g.Expect(resp.Allowed).To(gomega.BeTrue(), "a self-update is not a priority conflict; got refusal: %v", resp.Result)
}

// The per-format priority check runs with no other runtime present, so the
// first runtime admitted cannot carry one format at two priorities.
func TestValidator_SpecOnlyChecksRunWithNoExistingRuntimes(t *testing.T) {
	conflicting := withFormats(autoSelectedFormat("safetensors", 1), autoSelectedFormat("safetensors", 2))

	t.Run("clusterservingruntime", func(t *testing.T) {
		g := gomega.NewWithT(t)
		resp := newClusterServingRuntimeValidator(t).Handle(context.Background(),
			createReq(t, clusterServingRuntime("rt-solo", conflicting)))

		g.Expect(resp.Allowed).To(gomega.BeFalse(), "one format at two priorities is refused with no other cluster runtime")
		g.Expect(resp.Result.Message).To(gomega.ContainSubstring("safetensors"))
	})

	t.Run("servingruntime", func(t *testing.T) {
		g := gomega.NewWithT(t)
		resp := newServingRuntimeValidator(t).Handle(context.Background(),
			createReq(t, servingRuntime("rt-solo", "test", conflicting)))

		g.Expect(resp.Allowed).To(gomega.BeFalse(), "one format at two priorities is refused with no other runtime in the namespace")
		g.Expect(resp.Result.Message).To(gomega.ContainSubstring("safetensors"))
	})
}
