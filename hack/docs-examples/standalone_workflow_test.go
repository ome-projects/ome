package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/webhook/admission/inferencereplica"
	"sigs.k8s.io/ome/pkg/webhook/admission/servingruntime"
)

// Validate the actual standalone examples and the boundaries described next
// to them. This invokes admission, not a running workload controller or image.
func TestStandaloneDocumentationFixtures(t *testing.T) {
	dir := filepath.Join("..", "..", "config", "samples", "docs", "standalone-http")
	namespace := &corev1.Namespace{}
	rt := &v1beta1.ServingRuntime{}
	inline := &v1beta1.InferenceReplica{}
	referenced := &v1beta1.InferenceReplica{}
	service := &corev1.Service{}
	readWorkflowYAML(t, filepath.Join(dir, "namespace.yaml"), namespace)
	readWorkflowYAML(t, filepath.Join(dir, "servingruntime.yaml"), rt)
	readWorkflowYAML(t, filepath.Join(dir, "inferencereplica.yaml"), inline)
	readWorkflowYAML(t, filepath.Join(dir, "runtime-ref-replica.yaml"), referenced)
	readWorkflowYAML(t, filepath.Join(dir, "service.yaml"), service)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	config := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data: map[string]string{
			controllerconfig.InferenceReplicaConfigName: `{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"]}}`,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(namespace, rt).Build()
	validator := &inferencereplica.Validator{
		Decoder:   admission.NewDecoder(scheme),
		Reader:    cl,
		Clientset: k8sfake.NewSimpleClientset(config),
	}
	rawRuntime, err := json.Marshal(rt)
	if err != nil {
		t.Fatal(err)
	}
	runtimeValidator := &servingruntime.ServingRuntimeValidator{Client: cl, Decoder: admission.NewDecoder(scheme)}
	response := runtimeValidator.Handle(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Namespace: rt.Namespace,
		Name:      rt.Name,
		Object:    runtime.RawExtension{Raw: rawRuntime},
	}})
	if !response.Allowed {
		t.Fatalf("standalone runtime fixture denied: %v", response.Result)
	}
	check := func(t *testing.T, old, current *v1beta1.InferenceReplica, denial string) {
		t.Helper()
		request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: current.Namespace,
			Name:      current.Name,
			UserInfo:  authenticationv1.UserInfo{Username: "documentation-reader", Groups: []string{"system:authenticated"}},
			Object:    runtime.RawExtension{Raw: marshalWorkflowReplica(t, current)},
		}}
		if old != nil {
			request.Operation = admissionv1.Update
			request.OldObject.Raw = marshalWorkflowReplica(t, old)
		}
		response := validator.Handle(t.Context(), request)
		if denial == "" {
			if !response.Allowed {
				t.Fatalf("documented standalone operation denied: %v", response.Result)
			}
			return
		}
		if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, denial) {
			t.Fatalf("expected denial containing %q, got allowed=%v, result=%v", denial, response.Allowed, response.Result)
		}
	}

	for _, replica := range []*v1beta1.InferenceReplica{inline, referenced} {
		t.Run(replica.Name, func(t *testing.T) {
			if replica.Namespace != namespace.Name || replica.Spec.ParentRef != nil || len(replica.OwnerReferences) != 0 {
				t.Fatal("fixture must be standalone in the namespace the guide creates")
			}
			check(t, nil, replica, "")
			scaled := replica.DeepCopy()
			scaled.Spec.Replicas = ptr.To(int32(2))
			check(t, replica, scaled, "")
			paused := scaled.DeepCopy()
			paused.Spec.Paused = true
			check(t, scaled, paused, "")
		})
	}
	if referenced.Spec.RuntimeRef == nil || referenced.Spec.RuntimeRef.Name != rt.Name || referenced.Spec.RuntimeRef.Kind == nil || *referenced.Spec.RuntimeRef.Kind != "ServingRuntime" || referenced.Spec.ModelRef != nil || len(referenced.Spec.Runners) != 0 {
		t.Fatal("reference variant must select the namespaced HTTP runtime without a model or stored runners")
	}
	if len(inline.Spec.Runners) != 1 || inline.Spec.Runners[0].Size != 1 {
		t.Fatal("the inline lab must have one single-pod runner per Instance")
	}
	if service.Namespace != inline.Namespace || len(service.Spec.Selector) == 0 {
		t.Fatal("user-managed Service must select the inline replica in its namespace")
	}
	for key, value := range service.Spec.Selector {
		if inline.Spec.Runners[0].Template.Labels[key] != value {
			t.Fatalf("Service selector %s=%s does not match the runner template", key, value)
		}
	}

	t.Run("documented template patch", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(dir, "update-response.json"))
		if err != nil {
			t.Fatal(err)
		}
		patch, err := jsonpatch.DecodePatch(data)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := patch.Apply(marshalWorkflowReplica(t, inline))
		if err != nil {
			t.Fatal(err)
		}
		updated := &v1beta1.InferenceReplica{}
		if err := json.Unmarshal(raw, updated); err != nil {
			t.Fatal(err)
		}
		check(t, inline, updated, "")
		if got := updated.Spec.Runners[0].Template.Spec.Containers[0].Args[1]; got != "-text=hello from standalone v2" {
			t.Fatalf("patch produced unexpected response argument: %s", got)
		}
	})

	for _, test := range []struct {
		name   string
		patch  string
		denial string
	}{
		{"immutable role", `{"spec":{"component":"decoder"}}`, "spec.component is immutable"},
		{"cannot attach a parent", `{"spec":{"parentRef":{"name":"parent-http"}}}`, "spec.parentRef is immutable"},
		{"mixed template sources", `{"spec":{"runtimeRef":{"name":"http-echo"}}}`, "runners"},
		{"no template source", `{"spec":{"runners":null}}`, "required unless"},
		{"nested availability delay", `{"spec":{"lifecycle":{"minReadySeconds":5}}}`, "set spec.minReadySeconds"},
		{"no generated HPA", `{"spec":{"autoscaler":{"class":"HPA"}}}`, "creates no scaler"},
		{"no generated KEDA", `{"spec":{"autoscaler":{"class":"KEDA"}}}`, "creates no scaler"},
		{"placement fields are reserved", `{"spec":{"placementReplicaLimit":2}}`, "written only by the InferenceService controller"},
		{"no standalone migration", `{"metadata":{"annotations":{"ome.io/migration-request-v1-docs":"{}"}}}`, "manual migration is not available"},
	} {
		t.Run(test.name, func(t *testing.T) {
			check(t, inline, mergeWorkflowReplica(t, inline, test.patch), test.denial)
		})
	}
	for _, patch := range []string{
		`{"spec":{"runtimeRef":{"autoSync":false}}}`,
		`{"spec":{"runtimeRef":{"revision":"some-revision"}}}`,
	} {
		t.Run("runtime pins are rejected", func(t *testing.T) {
			check(t, referenced, mergeWorkflowReplica(t, referenced, patch), "live runtime")
		})
	}
	t.Run("a projected replica is not a standalone edit target", func(t *testing.T) {
		projected := inline.DeepCopy()
		projected.Spec.ParentRef = &v1beta1.ParentReference{Name: "parent-http"}
		projected.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
			Name: "parent-http", UID: "parent-http-uid", Controller: ptr.To(true),
		}}
		updated := projected.DeepCopy()
		updated.Spec.Replicas = ptr.To(int32(2))
		updated.Annotations = map[string]string{constants.InferenceReplicaControllerWriteAnnotationKey: "true"}
		check(t, projected, updated, "edit the InferenceService instead")
	})
}

func marshalWorkflowReplica(t *testing.T, replica *v1beta1.InferenceReplica) []byte {
	t.Helper()
	raw, err := json.Marshal(replica)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mergeWorkflowReplica(t *testing.T, replica *v1beta1.InferenceReplica, patch string) *v1beta1.InferenceReplica {
	t.Helper()
	raw, err := jsonpatch.MergePatch(marshalWorkflowReplica(t, replica), []byte(patch))
	if err != nil {
		t.Fatal(err)
	}
	updated := &v1beta1.InferenceReplica{}
	if err := json.Unmarshal(raw, updated); err != nil {
		t.Fatal(err)
	}
	return updated
}
