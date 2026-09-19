package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestMergeArgs(t *testing.T) {
	tests := []struct {
		name          string
		containerArgs []string
		overrideArgs  []string
		expected      []string
	}{
		{
			name: "Merge multiline args with backslash continuation",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--port=8080 \
--enable-metrics \
--log-requests \
--model-path="$MODEL_PATH" \
--mem-frac=0.9`},
			overrideArgs: []string{`--tp-size=4`},
			expected: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--port=8080 \
--enable-metrics \
--log-requests \
--model-path="$MODEL_PATH" \
--mem-frac=0.9 \
--tp-size=4`},
		},
		{
			name: "Merge multiline args without trailing backslash",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--mem-frac=0.9`},
			overrideArgs: []string{`--tp-size=8`},
			expected: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--mem-frac=0.9 \
--tp-size=8`},
		},
		{
			name:          "Empty override args returns container args",
			containerArgs: []string{`python3 -m server`},
			overrideArgs:  []string{},
			expected:      []string{`python3 -m server`},
		},
		{
			name:          "Empty container args returns override args",
			containerArgs: []string{},
			overrideArgs:  []string{`--tp-size=4`},
			expected:      []string{`--tp-size=4`},
		},
		{
			name:          "Single-line args appends new arg",
			containerArgs: []string{`python3 -m server`},
			overrideArgs:  []string{`--debug`},
			expected:      []string{`python3 -m server`, `--debug`},
		},
		{
			name: "Multiple override args on multiline",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0`},
			overrideArgs: []string{`--tp-size=4`, `--pp-size=2`},
			expected: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--tp-size=4 \
--pp-size=2`},
		},
		{
			name:          "Multiline args with newlines",
			containerArgs: []string{"python3 -m sglang.launch_server\n--host=0.0.0.0\n--port=8080"},
			overrideArgs:  []string{`--tp-size=4`},
			expected:      []string{"python3 -m sglang.launch_server \\\n--host=0.0.0.0 \\\n--port=8080 \\\n--tp-size=4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeArgs(tt.containerArgs, tt.overrideArgs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMergeArgsListFormat(t *testing.T) {
	tests := []struct {
		name          string
		containerArgs []string
		overrideArgs  []string
		expected      []string
	}{
		{
			name:          "List format: override replaces existing key",
			containerArgs: []string{"--tp-size=4", "--port=8080"},
			overrideArgs:  []string{"--tp-size=8"},
			expected:      []string{"--tp-size=8", "--port=8080"},
		},
		{
			name:          "List format: new arg appended",
			containerArgs: []string{"--tp-size=4", "--port=8080"},
			overrideArgs:  []string{"--host=0.0.0.0"},
			expected:      []string{"--tp-size=4", "--port=8080", "--host=0.0.0.0"},
		},
		{
			name:          "List format: multiple overrides",
			containerArgs: []string{"--tp-size=4", "--pp-size=2", "--port=8080"},
			overrideArgs:  []string{"--tp-size=8", "--pp-size=4", "--debug"},
			expected:      []string{"--tp-size=8", "--pp-size=4", "--port=8080", "--debug"},
		},
		{
			name:          "List format: duplicate same value ignored",
			containerArgs: []string{"--tp-size=4", "--port=8080"},
			overrideArgs:  []string{"--tp-size=4"},
			expected:      []string{"--tp-size=4", "--port=8080"},
		},
		{
			name:          "List format: space-separated key value override",
			containerArgs: []string{"--tp-size", "4", "--port", "8080"},
			overrideArgs:  []string{"--tp-size", "8"},
			expected:      []string{"--tp-size", "8", "--port", "8080"},
		},
		{
			name:          "List format: mixed formats",
			containerArgs: []string{"--tp-size=4", "--port", "8080"},
			overrideArgs:  []string{"--tp-size=8", "--host=0.0.0.0"},
			expected:      []string{"--tp-size=8", "--port", "8080", "--host=0.0.0.0"},
		},
		{
			name:          "List format: command with args",
			containerArgs: []string{"python3", "-m", "server", "--port=8080"},
			overrideArgs:  []string{"--debug"},
			expected:      []string{"python3", "-m", "server", "--port=8080", "--debug"},
		},
		{
			name:          "List format: empty lists",
			containerArgs: []string{},
			overrideArgs:  []string{},
			expected:      []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeArgs(tt.containerArgs, tt.overrideArgs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOverrideArgParam(t *testing.T) {
	tests := []struct {
		name          string
		containerArgs []string
		key           string
		value         int64
		expectedArgs  []string
		expectedFound bool
	}{
		{
			name: "Override existing parameter with equals sign (multiline)",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--tp-size=4 \
--mem-frac=0.9`},
			key:   "--tp-size",
			value: 8,
			expectedArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--tp-size=8 \
--mem-frac=0.9`},
			expectedFound: true,
		},
		{
			name: "Override existing parameter with space (multiline)",
			containerArgs: []string{`python3 -m server \
--tp-size 4 \
--mem-frac=0.9`},
			key:   "--tp-size",
			value: 8,
			expectedArgs: []string{`python3 -m server \
--tp-size=8 \
--mem-frac=0.9`},
			expectedFound: true,
		},
		{
			name: "Parameter not found (multiline)",
			containerArgs: []string{`python3 -m server \
--host=0.0.0.0`},
			key:   "--tp-size",
			value: 8,
			expectedArgs: []string{`python3 -m server \
--host=0.0.0.0`},
			expectedFound: false,
		},
		{
			name:          "Empty container args",
			containerArgs: []string{},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{},
			expectedFound: false,
		},
		{
			name: "Override pipeline parallel size (multiline)",
			containerArgs: []string{`python3 -m server \
--pp-size=2 \
--tp-size=4`},
			key:   "--pp-size",
			value: 4,
			expectedArgs: []string{`python3 -m server \
--pp-size=4 \
--tp-size=4`},
			expectedFound: true,
		},
		{
			name: "Override tensor-parallel-size long form (multiline)",
			containerArgs: []string{`python3 -m server \
--tensor-parallel-size=4`},
			key:   "--tensor-parallel-size",
			value: 8,
			expectedArgs: []string{`python3 -m server \
--tensor-parallel-size=8`},
			expectedFound: true,
		},
		{
			name:          "List format: override with equals sign",
			containerArgs: []string{"--tp-size=4", "--port=8080"},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{"--tp-size=8", "--port=8080"},
			expectedFound: true,
		},
		{
			name:          "List format: override with space separator",
			containerArgs: []string{"--tp-size", "4", "--port", "8080"},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{"--tp-size", "8", "--port", "8080"},
			expectedFound: true,
		},
		{
			name:          "List format: parameter not found",
			containerArgs: []string{"--port=8080", "--host=0.0.0.0"},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{"--port=8080", "--host=0.0.0.0"},
			expectedFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := OverrideArgParam(tt.containerArgs, tt.key, tt.value)
			assert.Equal(t, tt.expectedFound, found)
			assert.Equal(t, tt.expectedArgs, result)
		})
	}
}

func TestOverrideCommandParam(t *testing.T) {
	tests := []struct {
		name            string
		containerCmd    []string
		key             string
		value           int64
		expectedCmd     []string
		expectedUpdated bool
	}{
		{
			name:            "Override with equals sign",
			containerCmd:    []string{"python3", "-m", "server", "--tp-size=4"},
			key:             "--tp-size",
			value:           8,
			expectedCmd:     []string{"python3", "-m", "server", "--tp-size=8"},
			expectedUpdated: true,
		},
		{
			name:            "Override with space separator",
			containerCmd:    []string{"python3", "-m", "server", "--tp-size", "4"},
			key:             "--tp-size",
			value:           8,
			expectedCmd:     []string{"python3", "-m", "server", "--tp-size", "8"},
			expectedUpdated: true,
		},
		{
			name:            "Parameter not found",
			containerCmd:    []string{"python3", "-m", "server"},
			key:             "--tp-size",
			value:           8,
			expectedCmd:     []string{"python3", "-m", "server"},
			expectedUpdated: false,
		},
		{
			name:            "Empty command",
			containerCmd:    []string{},
			key:             "--tp-size",
			value:           8,
			expectedCmd:     []string{},
			expectedUpdated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, updated := OverrideCommandParam(tt.containerCmd, tt.key, tt.value)
			assert.Equal(t, tt.expectedUpdated, updated)
			assert.Equal(t, tt.expectedCmd, result)
		})
	}
}

func TestMergeArgsEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		containerArgs []string
		overrideArgs  []string
		expected      []string
	}{
		{
			name:          "Nil container args",
			containerArgs: nil,
			overrideArgs:  []string{`--tp-size=4`},
			expected:      []string{`--tp-size=4`},
		},
		{
			name:          "Nil override args",
			containerArgs: []string{`python3 -m server`},
			overrideArgs:  nil,
			expected:      []string{`python3 -m server`},
		},
		{
			name:          "Both nil",
			containerArgs: nil,
			overrideArgs:  nil,
			expected:      nil,
		},
		{
			name:          "Both empty",
			containerArgs: []string{},
			overrideArgs:  []string{},
			expected:      []string{},
		},
		{
			name: "Override with whitespace (multiline)",
			containerArgs: []string{`python3 -m server \
--port=8080`},
			overrideArgs: []string{`  --tp-size=4  `},
			expected: []string{`python3 -m server \
--port=8080 \
--tp-size=4`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeArgs(tt.containerArgs, tt.overrideArgs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMergeArgsUnion(t *testing.T) {
	tests := []struct {
		name          string
		containerArgs []string
		overrideArgs  []string
		expected      []string
	}{
		{
			name: "Union: duplicate arg not added",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--port=8080 \
--tp-size=4`},
			overrideArgs: []string{`--tp-size=4`},
			expected: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--port=8080 \
--tp-size=4`},
		},
		{
			name: "Union: new arg added, duplicate ignored",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--tp-size=4`},
			overrideArgs: []string{`--tp-size=4`, `--pp-size=2`},
			expected: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--tp-size=4 \
--pp-size=2`},
		},
		{
			name: "Union: all new args added",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0`},
			overrideArgs: []string{`--tp-size=4`, `--pp-size=2`, `--dp=1`},
			expected: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--tp-size=4 \
--pp-size=2 \
--dp=1`},
		},
		{
			name: "Union: case sensitive comparison",
			containerArgs: []string{`python3 -m server \
--Enable-Metrics`},
			overrideArgs: []string{`--enable-metrics`},
			expected: []string{`python3 -m server \
--Enable-Metrics \
--enable-metrics`},
		},
		{
			name: "Union: arg with different values - override replaces",
			containerArgs: []string{`python3 -m server \
--tp-size=4 \
--port=8080`},
			overrideArgs: []string{`--tp-size=8`, `--host=0.0.0.0`},
			expected: []string{`python3 -m server \
--tp-size=8 \
--port=8080 \
--host=0.0.0.0`},
		},
		{
			name: "Union: empty override args",
			containerArgs: []string{`python3 -m server \
--tp-size=4`},
			overrideArgs: []string{},
			expected: []string{`python3 -m server \
--tp-size=4`},
		},
		{
			name: "Union: multiple overrides with replacement",
			containerArgs: []string{`python3 -m server \
--tp-size=4 \
--pp-size=2 \
--port=8080`},
			overrideArgs: []string{`--tp-size=8`, `--pp-size=4`, `--new-flag`},
			expected: []string{`python3 -m server \
--tp-size=8 \
--pp-size=4 \
--port=8080 \
--new-flag`},
		},
		{
			name: "Union: args with space separator (--key value format)",
			containerArgs: []string{`python3 -m server \
--tp-size 4 \
--port 8080`},
			overrideArgs: []string{`--tp-size 8`, `--host 0.0.0.0`},
			expected: []string{`python3 -m server \
--tp-size 8 \
--port 8080 \
--host 0.0.0.0`},
		},
		{
			name: "Union: flag without value",
			containerArgs: []string{`python3 -m server \
--enable-metrics \
--port=8080`},
			overrideArgs: []string{`--enable-metrics`, `--debug`},
			expected: []string{`python3 -m server \
--enable-metrics \
--port=8080 \
--debug`},
		},
		{
			name: "Union: multi-line override with flag replacement",
			containerArgs: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--port=8080 \
--enable-metrics \
--log-requests \
--model-path="$MODEL_PATH" \
--tp-size=4 \
--mem-frac=0.9`},
			overrideArgs: []string{`--tp-size=2`},
			expected: []string{`python3 -m sglang.launch_server \
--host=0.0.0.0 \
--port=8080 \
--enable-metrics \
--log-requests \
--model-path="$MODEL_PATH" \
--tp-size=2 \
--mem-frac=0.9`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeArgs(tt.containerArgs, tt.overrideArgs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOverrideKeyValueInSlice(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		key           string
		value         int64
		expectedArgs  []string
		expectedFound bool
	}{
		{
			name:          "Override with equals format",
			args:          []string{"--tp-size=4", "--port=8080"},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{"--tp-size=8", "--port=8080"},
			expectedFound: true,
		},
		{
			name:          "Override with space format",
			args:          []string{"--tp-size", "4", "--port", "8080"},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{"--tp-size", "8", "--port", "8080"},
			expectedFound: true,
		},
		{
			name:          "Key not found",
			args:          []string{"--port=8080"},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{"--port=8080"},
			expectedFound: false,
		},
		{
			name:          "Empty slice",
			args:          []string{},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{},
			expectedFound: false,
		},
		{
			name:          "Key at end without value (space format)",
			args:          []string{"--port=8080", "--tp-size"},
			key:           "--tp-size",
			value:         8,
			expectedArgs:  []string{"--port=8080", "--tp-size"},
			expectedFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := overrideKeyValueInSlice(tt.args, tt.key, tt.value)
			assert.Equal(t, tt.expectedFound, found)
			assert.Equal(t, tt.expectedArgs, result)
		})
	}
}

func TestMergeEngineSpecPreservesRuntimeServicePortAppProtocols(t *testing.T) {
	runtimeEngine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			ServicePortAppProtocols: map[string]string{"http": "kubernetes.io/h2c"},
		},
	}

	merged, err := MergeEngineSpec(runtimeEngine, &v1beta1.EngineSpec{})

	assert.NoError(t, err)
	assert.Equal(t, runtimeEngine.ServicePortAppProtocols, merged.ServicePortAppProtocols)
}

// TestMergeRuntimeContainers_NamePrecedence pins the three-step
// container-name fallback walk MergeRuntimeContainers applies after the
// strategic-merge patch. (An empty merged Name deadlocks
// OMENative rollouts at Pod create time with
// `spec.containers[0].name: Required value`.)
//
//	(a) predictor name set → predictor name wins (post-merge).
//	(b) predictor name empty + runtime name set → runtime name wins.
//	(c) both empty → "ome-container" canonical default.
func TestMergeRuntimeContainers_NamePrecedence(t *testing.T) {
	t.Run("(a) predictor wins", func(t *testing.T) {
		merged, err := MergeRuntimeContainers(
			&v1.Container{Name: "rt-name", Image: "rt:v1"},
			&v1.Container{Name: "user-name", Image: "user:v1"},
		)
		assert.NoError(t, err)
		assert.Equal(t, "user-name", merged.Name)
	})

	t.Run("(b) predictor empty falls back to runtime", func(t *testing.T) {
		merged, err := MergeRuntimeContainers(
			&v1.Container{Name: "rt-name", Image: "rt:v1"},
			&v1.Container{Name: "", Image: "user:v1"},
		)
		assert.NoError(t, err)
		assert.Equal(t, "rt-name", merged.Name)
	})

	t.Run("(c) both empty falls back to ome-container", func(t *testing.T) {
		merged, err := MergeRuntimeContainers(
			&v1.Container{Name: "", Image: "rt:v1"},
			&v1.Container{Name: "", Image: "user:v1"},
		)
		assert.NoError(t, err)
		assert.Equal(t, constants.MainContainerName, merged.Name)
	})
}

// TestRestoreRunnerName_FallbackPrecedence pins the precedence walk
// restoreRunnerName performs on the v1beta1.RunnerSpec layer (sibling
// of the v1.Container layer in MergeRuntimeContainers). The two paths
// must produce identical final names so the rendered Pod is consistent
// regardless of which merger ran last upstream.
func TestRestoreRunnerName_FallbackPrecedence(t *testing.T) {
	t.Run("(a) merged name preserved", func(t *testing.T) {
		merged := &v1beta1.RunnerSpec{Container: v1.Container{Name: "user-name"}}
		runtime := &v1beta1.RunnerSpec{Container: v1.Container{Name: "rt-name"}}
		restoreRunnerName(merged, runtime)
		assert.Equal(t, "user-name", merged.Name)
	})

	t.Run("(b) merged empty falls back to runtime name", func(t *testing.T) {
		merged := &v1beta1.RunnerSpec{Container: v1.Container{Name: ""}}
		runtime := &v1beta1.RunnerSpec{Container: v1.Container{Name: "rt-name"}}
		restoreRunnerName(merged, runtime)
		assert.Equal(t, "rt-name", merged.Name)
	})

	t.Run("(c) both empty falls back to ome-container", func(t *testing.T) {
		merged := &v1beta1.RunnerSpec{Container: v1.Container{Name: ""}}
		runtime := &v1beta1.RunnerSpec{Container: v1.Container{Name: ""}}
		restoreRunnerName(merged, runtime)
		assert.Equal(t, constants.MainContainerName, merged.Name)
	})

	t.Run("nil runtime + empty merged falls back to default", func(t *testing.T) {
		merged := &v1beta1.RunnerSpec{Container: v1.Container{Name: ""}}
		restoreRunnerName(merged, nil)
		assert.Equal(t, constants.MainContainerName, merged.Name)
	})

	t.Run("nil merged is a no-op", func(t *testing.T) {
		// must not panic
		restoreRunnerName(nil, &v1beta1.RunnerSpec{Container: v1.Container{Name: "x"}})
	})
}

// --- Component-level PodSpec → Leader/Worker fold ---------------------
//
// These pin the multi-node fold: component-level pod fields declared on
// engineConfig/decoderConfig (volumes, nodeSelector, tolerations,
// affinity, imagePullSecrets, ...) must flow into the rendered
// leader/worker pods. A renderer sourcing its base PodSpec from
// Leader.PodSpec / Worker.PodSpec only would, for a runtime declaring
// `engineConfig.volumes:[dshm]` + a leader/worker runner that mounts
// `dshm`, produce pods that mount a volume that does not exist → the
// apiserver rejects them.

func emptyDirVolume(name string) v1.Volume {
	return v1.Volume{
		Name:         name,
		VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}},
	}
}

func volumeNames(vols []v1.Volume) []string {
	names := make([]string, 0, len(vols))
	for _, v := range vols {
		names = append(names, v.Name)
	}
	return names
}

func findVolume(vols []v1.Volume, name string) *v1.Volume {
	for i := range vols {
		if vols[i].Name == name {
			return &vols[i]
		}
	}
	return nil
}

// TestMergeEngineSpec_FoldComponentPodSpec covers the multi-node leader +
// worker fold for the engine component.
func TestMergeEngineSpec_FoldComponentPodSpec(t *testing.T) {
	tolComponent := v1.Toleration{Key: "component", Operator: v1.TolerationOpExists}
	tolLeader := v1.Toleration{Key: "leader", Operator: v1.TolerationOpExists}
	affinityComponent := &v1.Affinity{NodeAffinity: &v1.NodeAffinity{}}

	t.Run("leader and worker inherit component-level pod fields", func(t *testing.T) {
		runtimeEngine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				Volumes:          []v1.Volume{emptyDirVolume("dshm")},
				NodeSelector:     map[string]string{"gpu": "a100"},
				Tolerations:      []v1.Toleration{tolComponent},
				Affinity:         affinityComponent,
				ImagePullSecrets: []v1.LocalObjectReference{{Name: "regcred"}},
			},
			// Leader/Worker declare only the runner mount, NOT the volume —
			// the shape that depends on the fold.
			Leader: &v1beta1.LeaderSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:         "ome-container",
					VolumeMounts: []v1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
				}},
			},
			Worker: &v1beta1.WorkerSpec{
				Size: ptrInt(2),
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:         "ome-container",
					VolumeMounts: []v1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
				}},
			},
		}
		isvcEngine := &v1beta1.EngineSpec{}

		merged, err := MergeEngineSpec(runtimeEngine, isvcEngine)
		assert.NoError(t, err)
		assert.NotNil(t, merged.Leader)
		assert.NotNil(t, merged.Worker)

		// Leader inherits all component-level pod fields.
		assert.Contains(t, volumeNames(merged.Leader.Volumes), "dshm",
			"leader must inherit the component-level dshm volume backing its volumeMount")
		assert.Equal(t, "a100", merged.Leader.NodeSelector["gpu"])
		assert.Contains(t, merged.Leader.Tolerations, tolComponent)
		assert.Equal(t, affinityComponent, merged.Leader.Affinity)
		assert.Equal(t, []v1.LocalObjectReference{{Name: "regcred"}}, merged.Leader.ImagePullSecrets)

		// Worker inherits all component-level pod fields too.
		assert.Contains(t, volumeNames(merged.Worker.Volumes), "dshm",
			"worker must inherit the component-level dshm volume backing its volumeMount")
		assert.Equal(t, "a100", merged.Worker.NodeSelector["gpu"])
		assert.Contains(t, merged.Worker.Tolerations, tolComponent)
		assert.Equal(t, affinityComponent, merged.Worker.Affinity)
		assert.Equal(t, []v1.LocalObjectReference{{Name: "regcred"}}, merged.Worker.ImagePullSecrets)
	})

	t.Run("leader/worker-level fields override component on conflict", func(t *testing.T) {
		// Strategic-merge semantics carried by the PodSpec field tags:
		// name-keyed lists (volumes, imagePullSecrets) merge by name; atomic
		// fields (nodeSelector, tolerations) are replaced wholesale by the
		// child.
		runtimeEngine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				Volumes:          []v1.Volume{emptyDirVolume("dshm")},
				NodeSelector:     map[string]string{"gpu": "a100"},
				Tolerations:      []v1.Toleration{tolComponent},
				ImagePullSecrets: []v1.LocalObjectReference{{Name: "component-cred"}},
			},
			Leader: &v1beta1.LeaderSpec{
				PodSpec: v1beta1.PodSpec{
					Volumes: []v1.Volume{{
						Name:         "scratch",
						VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/scratch"}},
					}},
					NodeSelector:     map[string]string{"gpu": "h100"},
					Tolerations:      []v1.Toleration{tolLeader},
					ImagePullSecrets: []v1.LocalObjectReference{{Name: "leader-cred"}},
				},
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container"}},
			},
			Worker: &v1beta1.WorkerSpec{
				Size:   ptrInt(1),
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container"}},
			},
		}

		merged, err := MergeEngineSpec(runtimeEngine, &v1beta1.EngineSpec{})
		assert.NoError(t, err)

		// Volumes merge by name — component's dshm and the leader's scratch
		// are both present, each keeping its own source.
		assert.ElementsMatch(t, []string{"dshm", "scratch"}, volumeNames(merged.Leader.Volumes))
		assert.NotNil(t, findVolume(merged.Leader.Volumes, "dshm").EmptyDir)
		assert.Equal(t, "/scratch", findVolume(merged.Leader.Volumes, "scratch").HostPath.Path)

		// ImagePullSecrets merge by name — both kept.
		assert.ElementsMatch(t,
			[]v1.LocalObjectReference{{Name: "component-cred"}, {Name: "leader-cred"}},
			merged.Leader.ImagePullSecrets)

		// NodeSelector / Tolerations are atomic — the leader's win wholesale.
		assert.Equal(t, "h100", merged.Leader.NodeSelector["gpu"])
		assert.Equal(t, []v1.Toleration{tolLeader}, merged.Leader.Tolerations)
	})

	t.Run("single-pod path unchanged", func(t *testing.T) {
		// No Leader/Worker: component-level pod fields must stay exactly
		// where the single-pod renderer reads them (the top-level PodSpec)
		// and the fold must be a no-op.
		runtimeEngine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				Volumes:      []v1.Volume{emptyDirVolume("dshm")},
				NodeSelector: map[string]string{"gpu": "a100"},
				Tolerations:  []v1.Toleration{tolComponent},
			},
			Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container"}},
		}

		merged, err := MergeEngineSpec(runtimeEngine, &v1beta1.EngineSpec{})
		assert.NoError(t, err)
		assert.Nil(t, merged.Leader)
		assert.Nil(t, merged.Worker)
		// Component-level fields remain on the top-level PodSpec untouched.
		assert.Equal(t, []string{"dshm"}, volumeNames(merged.Volumes))
		assert.Equal(t, "a100", merged.NodeSelector["gpu"])
		assert.Equal(t, []v1.Toleration{tolComponent}, merged.Tolerations)
	})

	t.Run("runtime-nil ISVC-only path also folds", func(t *testing.T) {
		// When the runtime declares no engine, the merge takes the
		// DeepCopy branch — the fold must still run so an all-ISVC
		// multi-node spec is rendered correctly.
		isvcEngine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{Volumes: []v1.Volume{emptyDirVolume("dshm")}},
			Leader: &v1beta1.LeaderSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:         "ome-container",
					VolumeMounts: []v1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
				}},
			},
			Worker: &v1beta1.WorkerSpec{Size: ptrInt(1)},
		}

		merged, err := MergeEngineSpec(nil, isvcEngine)
		assert.NoError(t, err)
		assert.Contains(t, volumeNames(merged.Leader.Volumes), "dshm")
		assert.Contains(t, volumeNames(merged.Worker.Volumes), "dshm")
	})
}

// TestMergeDecoderSpec_FoldComponentPodSpec mirrors the engine fold for
// the decoder component — DecoderSpec has the identical structure and
// shared the same bug.
func TestMergeDecoderSpec_FoldComponentPodSpec(t *testing.T) {
	tolComponent := v1.Toleration{Key: "component", Operator: v1.TolerationOpExists}
	affinityComponent := &v1.Affinity{NodeAffinity: &v1.NodeAffinity{}}

	t.Run("leader and worker inherit component-level pod fields", func(t *testing.T) {
		runtimeDecoder := &v1beta1.DecoderSpec{
			PodSpec: v1beta1.PodSpec{
				Volumes:          []v1.Volume{emptyDirVolume("dshm")},
				NodeSelector:     map[string]string{"gpu": "h100"},
				Tolerations:      []v1.Toleration{tolComponent},
				Affinity:         affinityComponent,
				ImagePullSecrets: []v1.LocalObjectReference{{Name: "regcred"}},
			},
			Leader: &v1beta1.LeaderSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:         "ome-container",
					VolumeMounts: []v1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
				}},
			},
			Worker: &v1beta1.WorkerSpec{
				Size: ptrInt(2),
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:         "ome-container",
					VolumeMounts: []v1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
				}},
			},
		}

		merged, err := MergeDecoderSpec(runtimeDecoder, &v1beta1.DecoderSpec{})
		assert.NoError(t, err)

		assert.Contains(t, volumeNames(merged.Leader.Volumes), "dshm")
		assert.Equal(t, "h100", merged.Leader.NodeSelector["gpu"])
		assert.Contains(t, merged.Leader.Tolerations, tolComponent)
		assert.Equal(t, affinityComponent, merged.Leader.Affinity)
		assert.Equal(t, []v1.LocalObjectReference{{Name: "regcred"}}, merged.Leader.ImagePullSecrets)

		assert.Contains(t, volumeNames(merged.Worker.Volumes), "dshm")
		assert.Equal(t, "h100", merged.Worker.NodeSelector["gpu"])
		assert.Contains(t, merged.Worker.Tolerations, tolComponent)
		assert.Equal(t, affinityComponent, merged.Worker.Affinity)
		assert.Equal(t, []v1.LocalObjectReference{{Name: "regcred"}}, merged.Worker.ImagePullSecrets)
	})

	t.Run("leader-level field wins on conflict", func(t *testing.T) {
		runtimeDecoder := &v1beta1.DecoderSpec{
			PodSpec: v1beta1.PodSpec{NodeSelector: map[string]string{"tier": "component"}},
			Leader: &v1beta1.LeaderSpec{
				PodSpec: v1beta1.PodSpec{NodeSelector: map[string]string{"tier": "leader"}},
				Runner:  &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container"}},
			},
			Worker: &v1beta1.WorkerSpec{Size: ptrInt(1)},
		}

		merged, err := MergeDecoderSpec(runtimeDecoder, &v1beta1.DecoderSpec{})
		assert.NoError(t, err)
		assert.Equal(t, "leader", merged.Leader.NodeSelector["tier"])
	})

	t.Run("single-pod path unchanged", func(t *testing.T) {
		runtimeDecoder := &v1beta1.DecoderSpec{
			PodSpec: v1beta1.PodSpec{Volumes: []v1.Volume{emptyDirVolume("dshm")}},
			Runner:  &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container"}},
		}

		merged, err := MergeDecoderSpec(runtimeDecoder, &v1beta1.DecoderSpec{})
		assert.NoError(t, err)
		assert.Nil(t, merged.Leader)
		assert.Nil(t, merged.Worker)
		assert.Equal(t, []string{"dshm"}, volumeNames(merged.Volumes))
	})
}

// TestFoldComponentPodSpecIntoLeaderWorker_NilSafety pins the guard
// behavior of the fold helper directly.
func TestFoldComponentPodSpecIntoLeaderWorker_NilSafety(t *testing.T) {
	t.Run("nil component is a no-op", func(t *testing.T) {
		leader := &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{Volumes: []v1.Volume{emptyDirVolume("keep")}}}
		assert.NoError(t, foldComponentPodSpecIntoLeaderWorker(nil, leader, nil))
		assert.Equal(t, []string{"keep"}, volumeNames(leader.Volumes))
	})

	t.Run("nil leader and worker do not panic", func(t *testing.T) {
		component := &v1beta1.PodSpec{Volumes: []v1.Volume{emptyDirVolume("dshm")}}
		assert.NoError(t, foldComponentPodSpecIntoLeaderWorker(component, nil, nil))
	})
}

func ptrInt(i int) *int { return &i }

// TestMergeSchedulerName pins the fill-only contract of the top-level
// schedulerName back-fill: empty levels gain the runtime's top-level
// name, already-set levels keep theirs, and absent inputs are no-ops.
func TestMergeSchedulerName(t *testing.T) {
	runtimeWithScheduler := func() *v1beta1.ServingRuntimeSpec {
		return &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "custom-scheduler"},
		}
	}

	t.Run("fills every empty pod-spec level", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: ptrInt(2)},
		}
		decoder := &v1beta1.DecoderSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: ptrInt(1)},
		}
		router := &v1beta1.RouterSpec{}

		MergeSchedulerName(runtimeWithScheduler(), engine, decoder, router)

		assert.Equal(t, "custom-scheduler", engine.SchedulerName)
		assert.Equal(t, "custom-scheduler", engine.Leader.SchedulerName)
		assert.Equal(t, "custom-scheduler", engine.Worker.SchedulerName)
		assert.Equal(t, "custom-scheduler", decoder.SchedulerName)
		assert.Equal(t, "custom-scheduler", decoder.Leader.SchedulerName)
		assert.Equal(t, "custom-scheduler", decoder.Worker.SchedulerName)
		assert.Equal(t, "custom-scheduler", router.SchedulerName)
	})

	t.Run("set levels always win", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{SchedulerName: "component-scheduler"},
			Leader:  &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{SchedulerName: "leader-scheduler"}},
			Worker:  &v1beta1.WorkerSpec{Size: ptrInt(2)},
		}

		MergeSchedulerName(runtimeWithScheduler(), engine, nil, nil)

		assert.Equal(t, "component-scheduler", engine.SchedulerName)
		assert.Equal(t, "leader-scheduler", engine.Leader.SchedulerName)
		assert.Equal(t, "custom-scheduler", engine.Worker.SchedulerName,
			"an unset worker level must still gain the top-level name")
	})

	t.Run("empty top-level name is a no-op", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}}

		MergeSchedulerName(&v1beta1.ServingRuntimeSpec{}, engine, nil, nil)

		assert.Empty(t, engine.SchedulerName)
		assert.Empty(t, engine.Leader.SchedulerName)
	})

	t.Run("nil runtime and nil components do not panic", func(t *testing.T) {
		MergeSchedulerName(nil, &v1beta1.EngineSpec{}, nil, nil)
		MergeSchedulerName(runtimeWithScheduler(), nil, nil, nil)
	})
}
