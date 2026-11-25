package runtimeinheritance

import (
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Each test covers one case of the merge contract:
//   scalar / map / named-list / plain-list / nested-struct.

func TestMerge_NilHandling(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)}
	child := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(false)}

	got, err := Merge(nil, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got).To(gomega.BeNil())

	got, err = Merge(parent, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got).To(gomega.Equal(parent))
	g.Expect(got).NotTo(gomega.BeIdenticalTo(parent), "must deep-copy, not alias")

	got, err = Merge(nil, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got).To(gomega.Equal(child))
}

func TestMerge_ScalarChildWins(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)}
	child := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(false)}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(*got.Disabled).To(gomega.BeFalse())
}

func TestMerge_ScalarChildUnsetTakesParent(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)}
	child := &v1beta1.ServingRuntimeSpec{}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(*got.Disabled).To(gomega.BeTrue())
}

func TestMerge_NamedListPerKeyMergesEnvVars(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{
				Name: "ome-container",
				Env: []corev1.EnvVar{
					{Name: "NCCL_DEBUG", Value: "INFO"},
					{Name: "FROM_PARENT", Value: "yes"},
				},
			}},
		},
	}
	child := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{
				Name: "ome-container",
				Env: []corev1.EnvVar{
					{Name: "NCCL_DEBUG", Value: "TRACE"}, // child wins
					{Name: "FROM_CHILD", Value: "yes"},
				},
			}},
		},
	}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got.Containers).To(gomega.HaveLen(1))
	envByName := map[string]string{}
	for _, e := range got.Containers[0].Env {
		envByName[e.Name] = e.Value
	}
	g.Expect(envByName).To(gomega.HaveKeyWithValue("NCCL_DEBUG", "TRACE"))
	g.Expect(envByName).To(gomega.HaveKeyWithValue("FROM_PARENT", "yes"))
	g.Expect(envByName).To(gomega.HaveKeyWithValue("FROM_CHILD", "yes"))
}

func TestMerge_EnvVarSourceChildReplacesParent(t *testing.T) {
	configMapSource := &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Key: "level"}}
	secretSource := &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}, Key: "token"}}

	tests := []struct {
		name   string
		parent corev1.EnvVar
		child  corev1.EnvVar
	}{
		{"literal replaced by source", corev1.EnvVar{Name: "X", Value: "parent"}, corev1.EnvVar{Name: "X", ValueFrom: configMapSource}},
		{"source replaced by literal", corev1.EnvVar{Name: "X", ValueFrom: configMapSource}, corev1.EnvVar{Name: "X", Value: "child"}},
		{"source replaced by other source", corev1.EnvVar{Name: "X", ValueFrom: configMapSource}, corev1.EnvVar{Name: "X", ValueFrom: secretSource}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			spec := func(env corev1.EnvVar) *v1beta1.ServingRuntimeSpec {
				return &v1beta1.ServingRuntimeSpec{
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{{Name: "ome-container", Env: []corev1.EnvVar{env}}},
					},
					EngineConfig: &v1beta1.EngineSpec{
						Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Env: []corev1.EnvVar{env}}},
					},
				}
			}

			got, err := Merge(spec(tt.parent), spec(tt.child))
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(got.Containers[0].Env).To(gomega.Equal([]corev1.EnvVar{tt.child}))
			g.Expect(got.EngineConfig.Runner.Env).To(gomega.Equal([]corev1.EnvVar{tt.child}))
		})
	}
}

func TestMerge_PlainListChildReplaces(t *testing.T) {
	g := gomega.NewWithT(t)
	// Command is a plain list — child replaces parent entirely.
	parent := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{
				Name:    "ome-container",
				Command: []string{"python", "-m", "old"},
			}},
		},
	}
	child := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{
				Name:    "ome-container",
				Command: []string{"python", "-m", "new"},
			}},
		},
	}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got.Containers[0].Command).To(gomega.Equal([]string{"python", "-m", "new"}))
}

func TestMerge_PlainListChildUnsetTakesParent(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{
				Name:    "ome-container",
				Command: []string{"python", "-m", "parent"},
			}},
		},
	}
	child := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "child-image"}},
		},
	}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got.Containers[0].Command).To(gomega.Equal([]string{"python", "-m", "parent"}))
	g.Expect(got.Containers[0].Image).To(gomega.Equal("child-image"))
}

func TestMerge_MapPerKeyMerge(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			NodeSelector: map[string]string{"arch": "arm64", "from": "parent"},
		},
	}
	child := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			NodeSelector: map[string]string{"arch": "amd64", "from": "child"},
		},
	}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got.NodeSelector).To(gomega.HaveKeyWithValue("arch", "amd64"))
	g.Expect(got.NodeSelector).To(gomega.HaveKeyWithValue("from", "child"))
}

func TestMerge_NestedPodSpecRecurses(t *testing.T) {
	g := gomega.NewWithT(t)
	// Parent supplies infra (volumes), child supplies engine container.
	parent := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Volumes: []corev1.Volume{{
				Name:         "dshm",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
			}},
		},
	}
	child := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sglang:latest"}},
		},
	}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got.Volumes).To(gomega.HaveLen(1))
	g.Expect(got.Volumes[0].Name).To(gomega.Equal("dshm"))
	g.Expect(got.Containers).To(gomega.HaveLen(1))
	g.Expect(got.Containers[0].Image).To(gomega.Equal("sglang:latest"))
}

// Regression: ServingRuntimeSpec.Containers has no `omitempty` tag,
// so an empty child marshals to `"containers": null`. Without the
// null-stripping pre-process, strategic merge would WIPE the parent's
// containers when the child supplies no engine container. This test
// keeps that behaviour locked.
func TestMerge_EmptyChildPreservesParentContainers(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{
				Name: "ome-container",
				Env:  []corev1.EnvVar{{Name: "PRESERVED", Value: "yes"}},
			}},
		},
	}
	child := &v1beta1.ServingRuntimeSpec{}

	got, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got.Containers).To(gomega.HaveLen(1))
	g.Expect(got.Containers[0].Env).To(gomega.HaveLen(1))
	g.Expect(got.Containers[0].Env[0].Name).To(gomega.Equal("PRESERVED"))
}

func TestMerge_DoesNotMutateInputs(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)}
	parentBefore := parent.DeepCopy()
	child := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(false)}
	childBefore := child.DeepCopy()

	_, err := Merge(parent, child)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(parent).To(gomega.Equal(parentBefore))
	g.Expect(child).To(gomega.Equal(childBefore))
}
