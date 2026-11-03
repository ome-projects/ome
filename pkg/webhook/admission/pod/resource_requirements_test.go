package pod

import (
	"strings"
	"testing"
)

// Resource strings come from the inferenceservice-config ConfigMap, so a bad
// or missing value must surface as an admission error rather than a panic.
func TestContainerBuilders_BadOrMissingResourceValue_ErrorNamesTheField(t *testing.T) {
	tests := map[string]struct {
		cpuLimit, memoryLimit  string
		cpuRequest, memRequest string
		wantErrContains        string
	}{
		"unparsable cpu limit": {
			cpuLimit: "2 cores", memoryLimit: "1Gi", cpuRequest: "1", memRequest: "1Gi",
			wantErrContains: "cpuLimit",
		},
		"unparsable memory request": {
			cpuLimit: "1", memoryLimit: "1Gi", cpuRequest: "1", memRequest: "1 gig",
			wantErrContains: "memoryRequest",
		},
		"missing value": {
			cpuLimit: "1", memoryLimit: "", cpuRequest: "1", memRequest: "1Gi",
			wantErrContains: "memoryLimit is required",
		},
	}

	for name, tt := range tests {
		t.Run(name+"/fine-tuned adapter", func(t *testing.T) {
			_, err := agentContainer((&FineTunedAdapterInjector{
				CpuLimit: tt.cpuLimit, MemoryLimit: tt.memoryLimit,
				CpuRequest: tt.cpuRequest, MemoryRequest: tt.memRequest,
			}).agentContainerSpec(), nil, nil, nil)
			if err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("error %q does not mention %q", err, tt.wantErrContains)
			}
		})

		t.Run(name+"/serving sidecar", func(t *testing.T) {
			_, err := agentContainer((&ServingSidecarInjector{
				CpuLimit: tt.cpuLimit, MemoryLimit: tt.memoryLimit,
				CpuRequest: tt.cpuRequest, MemoryRequest: tt.memRequest,
			}).agentContainerSpec(), nil, nil, nil)
			if err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("error %q does not mention %q", err, tt.wantErrContains)
			}
		})
	}
}

func TestContainerBuilders_ValidResourceValues_SetsLimitsAndRequests(t *testing.T) {
	c, err := agentContainer((&FineTunedAdapterInjector{
		CpuLimit: "1", MemoryLimit: "1Gi", CpuRequest: "100m", MemoryRequest: "100Mi",
	}).agentContainerSpec(), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := c.Resources.Limits.Cpu().String(); got != "1" {
		t.Fatalf("cpu limit = %s, want 1", got)
	}
	if got := c.Resources.Requests.Memory().String(); got != "100Mi" {
		t.Fatalf("memory request = %s, want 100Mi", got)
	}
}

func TestInjectorContainerBuilders_UnparsableCPULimit_ReturnError(t *testing.T) {
	tests := map[string]func() error{
		"fine-tuned adapter": func() error {
			_, err := agentContainer((&FineTunedAdapterInjector{
				CpuLimit:      "bad-cpu",
				MemoryLimit:   "1Gi",
				CpuRequest:    "1",
				MemoryRequest: "1Gi",
			}).agentContainerSpec(), nil, nil, nil)
			return err
		},
		"serving sidecar": func() error {
			_, err := agentContainer((&ServingSidecarInjector{
				CpuLimit:      "bad-cpu",
				MemoryLimit:   "1Gi",
				CpuRequest:    "1",
				MemoryRequest: "1Gi",
			}).agentContainerSpec(), nil, nil, nil)
			return err
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := test(); err == nil {
				t.Fatalf("expected invalid resource quantity error")
			}
		})
	}
}
