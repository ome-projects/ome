package pod

import (
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/onsi/gomega"
	gomegaTypes "github.com/onsi/gomega/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gomodules.xyz/jsonpatch/v2"
	"google.golang.org/protobuf/proto"
	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestMutator_Handle(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	omeNamespace := v1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Namespace",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.OMENamespace,
		},
		Spec:   v1.NamespaceSpec{},
		Status: v1.NamespaceStatus{},
	}

	if err := c.Create(context.TODO(), &omeNamespace); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Errorf("failed to create namespace: %v", err)
	}
	mutator := Mutator{Client: c, Clientset: clientset, Decoder: admission.NewDecoder(c.Scheme())}

	cases := map[string]struct {
		configMap v1.ConfigMap
		request   admission.Request
		pod       v1.Pod
		matcher   gomegaTypes.GomegaMatcher
	}{
		"should not mutate non isvc pods": {
			configMap: v1.ConfigMap{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ConfigMap",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      constants.InferenceServiceConfigMapName,
					Namespace: constants.OMENamespace,
				},
				Immutable: nil,
				Data: map[string]string{
					constants.AgentConfigMapKeyName: `{
        				"image" : "ome/agent:latest",
        				"memoryRequest": "100Mi",
        				"memoryLimit": "1Gi",
        				"cpuRequest": "100m",
        				"cpuLimit": "1"
    				}`,
				},
				BinaryData: nil,
			},
			request: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UID: types.UID(uuid.NewString()),
					Kind: metav1.GroupVersionKind{
						Group:   "",
						Version: "v1",
						Kind:    "Pod",
					},
					Resource: metav1.GroupVersionResource{
						Group:    "",
						Version:  "v1",
						Resource: "pods",
					},
					SubResource: "",
					RequestKind: &metav1.GroupVersionKind{
						Group:   "",
						Version: "v1",
						Kind:    "Pod",
					},
					RequestResource: &metav1.GroupVersionResource{
						Group:    "",
						Version:  "v1",
						Resource: "pods",
					},
					RequestSubResource: "",
					Name:               "",
					Namespace:          "default",
					Operation:          admissionv1.Create,
					Object:             runtime.RawExtension{},
					OldObject:          runtime.RawExtension{},
					DryRun:             nil,
					Options:            runtime.RawExtension{},
				},
			},
			pod: v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
			},
			matcher: gomega.Equal(admission.Response{
				Patches: nil,
				AdmissionResponse: admissionv1.AdmissionResponse{
					UID:     "",
					Allowed: true,
					Result: &metav1.Status{
						TypeMeta: metav1.TypeMeta{},
						ListMeta: metav1.ListMeta{},
						Status:   "",
						Message:  "",
						Reason:   "",
						Details:  nil,
						Code:     200,
					},
					Patch:            nil,
					PatchType:        nil,
					AuditAnnotations: nil,
					Warnings:         nil,
				},
			}),
		},
		"should mutate isvc pods": {
			configMap: v1.ConfigMap{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ConfigMap",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      constants.InferenceServiceConfigMapName,
					Namespace: constants.OMENamespace,
				},
				Immutable: nil,
				Data: map[string]string{
					constants.AgentConfigMapKeyName: `{
        				"image" : "ome/agent:latest",
        				"memoryRequest": "100Mi",
        				"memoryLimit": "1Gi",
        				"cpuRequest": "100m",
        				"cpuLimit": "1"
    				}`,
				},
				BinaryData: nil,
			},
			request: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UID: types.UID(uuid.NewString()),
					Kind: metav1.GroupVersionKind{
						Group:   "",
						Version: "v1",
						Kind:    "Pod",
					},
					Resource: metav1.GroupVersionResource{
						Group:    "",
						Version:  "v1",
						Resource: "pods",
					},
					SubResource: "",
					RequestKind: &metav1.GroupVersionKind{
						Group:   "",
						Version: "v1",
						Kind:    "Pod",
					},
					RequestResource: &metav1.GroupVersionResource{
						Group:    "",
						Version:  "v1",
						Resource: "pods",
					},
					RequestSubResource: "",
					Name:               "",
					Namespace:          "default",
					Operation:          admissionv1.Create,
					Object:             runtime.RawExtension{},
					OldObject:          runtime.RawExtension{},
					DryRun:             nil,
					Options:            runtime.RawExtension{},
				},
			},
			pod: v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.InferenceServicePodLabelKey: "isvc-pod",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: constants.MainContainerName,
						},
					},
				},
			},
			matcher: gomega.BeEquivalentTo(admission.Response{
				Patches: []jsonpatch.JsonPatchOperation{
					{
						Operation: "add",
						Path:      "/metadata/annotations",
						Value: map[string]interface{}{
							"ome.io/enable-metric-aggregation":  "",
							"ome.io/enable-prometheus-scraping": "",
						},
					},
					{
						Operation: "add",
						Path:      "/metadata/namespace",
						Value:     "default",
					},
				},
				AdmissionResponse: admissionv1.AdmissionResponse{
					UID:              "",
					Allowed:          true,
					Result:           nil,
					Patch:            nil,
					PatchType:        (*admissionv1.PatchType)(proto.String(string(admissionv1.PatchTypeJSONPatch))),
					AuditAnnotations: nil,
					Warnings:         nil,
				},
			}),
		},
		"should mutate training pods": {
			configMap: v1.ConfigMap{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ConfigMap",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      constants.InferenceServiceConfigMapName,
					Namespace: constants.OMENamespace,
				},
				Immutable: nil,
				Data: map[string]string{
					constants.AgentConfigMapKeyName: `{
        				"image" : "ome/agent:latest",
        				"memoryRequest": "100Mi",
        				"memoryLimit": "1Gi",
        				"cpuRequest": "100m",
        				"cpuLimit": "1"
    				}`,
				},
				BinaryData: nil,
			},
			request: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UID: types.UID(uuid.NewString()),
					Kind: metav1.GroupVersionKind{
						Group:   "",
						Version: "v1",
						Kind:    "Pod",
					},
					Resource: metav1.GroupVersionResource{
						Group:    "",
						Version:  "v1",
						Resource: "pods",
					},
					SubResource: "",
					RequestKind: &metav1.GroupVersionKind{
						Group:   "",
						Version: "v1",
						Kind:    "Pod",
					},
					RequestResource: &metav1.GroupVersionResource{
						Group:    "",
						Version:  "v1",
						Resource: "pods",
					},
					RequestSubResource: "",
					Name:               "",
					Namespace:          "default",
					Operation:          admissionv1.Create,
					Object:             runtime.RawExtension{},
					OldObject:          runtime.RawExtension{},
					DryRun:             nil,
					Options:            runtime.RawExtension{},
				},
			},
			pod: v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.TrainingJobPodLabelKey: "trainingjob",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "trainer",
						},
					},
				},
			},
			matcher: gomega.BeEquivalentTo(admission.Response{
				Patches: []jsonpatch.JsonPatchOperation{
					{
						Operation: "add",
						Path:      "/metadata/annotations",
						Value: map[string]interface{}{
							"ome.io/enable-metric-aggregation":  "",
							"ome.io/enable-prometheus-scraping": "",
						},
					},
					{
						Operation: "add",
						Path:      "/metadata/namespace",
						Value:     "default",
					},
				},
				AdmissionResponse: admissionv1.AdmissionResponse{
					UID:              "",
					Allowed:          true,
					Result:           nil,
					Patch:            nil,
					PatchType:        (*admissionv1.PatchType)(proto.String(string(admissionv1.PatchTypeJSONPatch))),
					AuditAnnotations: nil,
					Warnings:         nil,
				},
			}),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.Create(context.TODO(), &tc.configMap); err != nil {
				t.Errorf("failed to create config map: %v", err)
			}
			byteData, err := json.Marshal(tc.pod)
			if err != nil {
				t.Errorf("failed to marshal pod data: %v", err)
			}
			tc.request.Object.Raw = byteData
			res := mutator.Handle(context.TODO(), tc.request)
			sortPatches(res.Patches)
			g.Expect(res).Should(tc.matcher)
			if err := c.Delete(context.TODO(), &tc.configMap); err != nil {
				t.Errorf("failed to delete configmap %v", err)
			}
		})
	}
}

// TestMutator_Handle_MalformedInjectorConfig verifies that a bad injector
// block in inferenceservice-config surfaces as an admission error instead
// of panicking the webhook handler.
func TestMutator_Handle_MalformedInjectorConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	omeNamespace := v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	}
	if err := c.Create(context.TODO(), &omeNamespace); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("failed to create namespace: %v", err)
	}
	mutator := Mutator{Client: c, Clientset: clientset, Decoder: admission.NewDecoder(c.Scheme())}

	for name, key := range map[string]string{
		"servingSidecar":   servingSidecarConfigMapKeyName,
		"fineTunedAdapter": fineTunedAdapterConfigMapKeyName,
	} {
		t.Run(name, func(t *testing.T) {
			configMap := v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      constants.InferenceServiceConfigMapName,
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{key: `{"image": not-json`},
			}
			if err := c.Create(context.TODO(), &configMap); err != nil {
				t.Fatalf("failed to create config map: %v", err)
			}
			defer func() {
				if err := c.Delete(context.TODO(), &configMap); err != nil {
					t.Errorf("failed to delete configmap: %v", err)
				}
			}()

			pod := v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{constants.InferenceServicePodLabelKey: "isvc-pod"},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{Name: constants.MainContainerName}},
				},
			}
			raw, err := json.Marshal(pod)
			g.Expect(err).To(gomega.BeNil())

			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UID:       types.UID(uuid.NewString()),
					Namespace: "default",
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: raw},
				},
			}

			var res admission.Response
			g.Expect(func() { res = mutator.Handle(context.TODO(), req) }).NotTo(gomega.Panic())
			g.Expect(res.Allowed).To(gomega.BeFalse())
			g.Expect(res.Result.Code).To(gomega.Equal(int32(500)))
		})
	}
}

// sortPatches sorts the slice of patches by Path so that the comparison works
// when there are > 1 patches. Note: make sure the matcher Patches are sorted.
func sortPatches(patches []jsonpatch.JsonPatchOperation) {
	if len(patches) > 1 {
		sort.Slice(patches, func(i, j int) bool {
			return patches[i].Path < patches[j].Path
		})
	}
}

// TestMutator_Handle_MissingConfigMap verifies that admission of a
// labeled pod is denied with a 500 when inferenceservice-config is
// absent — the failurePolicy=Fail contract means a missing config
// surfaces loudly instead of admitting an uninjected pod.
func TestMutator_Handle_MissingConfigMap(t *testing.T) {
	mutator := Mutator{Client: c, Clientset: clientset, Decoder: admission.NewDecoder(c.Scheme())}

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{constants.InferenceServicePodLabelKey: "isvc-pod"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Name: constants.MainContainerName}},
		},
	}
	raw, err := json.Marshal(pod)
	require.NoError(t, err)

	res := mutator.Handle(context.TODO(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			UID:       types.UID(uuid.NewString()),
			Namespace: "default",
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
	assert.False(t, res.Allowed)
	require.NotNil(t, res.Result)
	assert.Equal(t, int32(500), res.Result.Code)
}

// TestMutatorChain_ReinvocationIdempotent runs the full injector chain
// twice over one pod and requires the second pass to be a no-op. The
// webhook registers with reinvocationPolicy=IfNeeded, so any other
// mutating webhook in the cluster can trigger a re-run against the
// already-mutated pod; a non-idempotent injector would stack duplicate
// env/volumes/containers.
func TestMutatorChain_ReinvocationIdempotent(t *testing.T) {
	mutator := Mutator{Client: fakeclient.New(t)}

	configMap := &v1.ConfigMap{Data: map[string]string{}}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chain-pod",
			Namespace: "default",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "chain-isvc",
			},
			Annotations: map[string]string{
				constants.RDMAAutoInjectAnnotationKey:              "true",
				constants.RuntimeShmProfileAnnotationKey:           "default",
				constants.RuntimeProbeProfileAnnotationKey:         "sglang-http",
				constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Name: DefaultContainerName}},
		},
	}

	require.NoError(t, mutator.mutate(pod, configMap))
	// Sanity: the first pass actually injected work for every
	// annotation-gated injector in the chain.
	require.NotEmpty(t, pod.Spec.Containers[0].Env, "rdma profile env expected")
	require.NotEmpty(t, pod.Spec.Volumes, "rdma/shm volumes expected")
	require.NotNil(t, pod.Spec.Containers[0].ReadinessProbe, "probe profile expected")
	require.Equal(t, "true", pod.Annotations[constants.PrometheusScrapeAnnotationKey])

	first := pod.DeepCopy()
	require.NoError(t, mutator.mutate(pod, configMap))
	assert.Equal(t, first, pod, "second mutate pass must be a byte-for-byte no-op")
}
