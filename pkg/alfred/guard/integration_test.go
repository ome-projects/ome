package guard

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

// TestAdmissionIntegration proves that the real API server accepts Alfred's
// JSONPatch request while rejecting broader writes, without restricting owners.
func TestAdmissionIntegration(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run real admission integration")
	}
	if _, err := os.Stat(filepath.Join(assets, "kube-apiserver")); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile("../../../config/crd/full/ome.io_inferenceservices.yaml")
	if err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err = yaml.Unmarshal(b, crd); err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{BinaryAssetsDirectory: assets, CRDs: []*apiextensionsv1.CustomResourceDefinition{crd}}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	_ = admissionv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = rbacv1.AddToScheme(scheme)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resolved := &resolver.ClientDiscoveryResolver{Discovery: discoveryClient}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(context.Context) (bool, error) {
		_, err := resolved.ResolveSchema(schema.GroupVersionKind{Group: "ome.io", Version: "v1beta1", Kind: "InferenceService"})
		return err == nil, nil
	}); err != nil {
		t.Fatalf("resolve installed CRD schema: %v", err)
	}
	checker := &validating.TypeChecker{SchemaResolver: resolved, RestMapper: admin.RESTMapper()}
	if warnings := checker.Check(Policy(PolicyName, "system:serviceaccount:ome:ome-alfred")); len(warnings) != 0 {
		t.Fatalf("guard does not type-check against installed CRD: %+v", warnings)
	}
	for _, obj := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workloads"}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "test-alfred"}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{"ome.io"}, Resources: []string{"inferenceservices", "inferenceservices/status"}, Verbs: []string{"get", "patch", "update"}}}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "test-alfred"}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "test-alfred"}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "ome-alfred", Namespace: "ome"}}},
		Policy(PolicyName, "system:serviceaccount:ome:ome-alfred"), Binding(BindingName, PolicyName),
	} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	// Envtest has no kube-controller-manager. Publish only the independently
	// verified type-check result above, then verify the server-defaulted specs.
	var installed admissionv1.ValidatingAdmissionPolicy
	if err := admin.Get(ctx, client.ObjectKey{Name: PolicyName}, &installed); err != nil {
		t.Fatal(err)
	}
	installed.Status.ObservedGeneration = installed.Generation
	installed.Status.TypeChecking = &admissionv1.TypeChecking{}
	if err := admin.Status().Update(ctx, &installed); err != nil {
		t.Fatal(err)
	}
	if err := (&Guard{Reader: admin, Namespace: "ome", ServiceAccount: "ome-alfred"}).Check(ctx); err != nil {
		t.Fatalf("server-defaulted policy rejected: %v", err)
	}
	actorCfg := rest.CopyConfig(cfg)
	actorCfg.Impersonate.UserName = "system:serviceaccount:ome:ome-alfred"
	actorCfg.Impersonate.Groups = []string{"system:serviceaccounts", "system:serviceaccounts:ome", "system:authenticated"}
	actor, err := client.New(actorCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ome.io/v1beta1", "kind": "InferenceService", "metadata": map[string]any{"name": "example", "namespace": "workloads", "annotations": map[string]any{"owner": "keep"}}, "spec": map[string]any{}}}
	if err := admin.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	// Wait for policy/binding informer propagation, not just object existence.
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		err := actor.Patch(ctx, obj.DeepCopy(), client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"forbidden":"true"}}}`)), client.DryRunAll)
		return apierrors.IsInvalid(err) || apierrors.IsForbidden(err), nil
	})
	if err != nil {
		t.Fatalf("guard never enforced: %v", err)
	}
	for _, tc := range []struct {
		name, patch string
		status      bool
	}{
		{"labels", `{"metadata":{"labels":{"x":"y"}}}`, false},
		{"finalizers", `{"metadata":{"finalizers":["test.example/finalizer"]}}`, false},
		{"owners", `{"metadata":{"ownerReferences":[{"apiVersion":"v1","kind":"ConfigMap","name":"owner","uid":"12345678-1234-1234-1234-123456789abc"}]}}`, false},
		{"replace", `{"metadata":{"annotations":{"owner":"replace"}}}`, false},
		{"remove", `{"metadata":{"annotations":{"owner":null}}}`, false},
		{"unrelated", `{"metadata":{"annotations":{"x":"y"}}}`, false},
		{"spec", `{"spec":{"deploymentMode":"OMENative"}}`, false},
		{"status", `{"status":{"url":"http://changed.example"}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.status {
				err = actor.Status().Patch(ctx, obj.DeepCopy(), client.RawPatch(types.MergePatchType, []byte(tc.patch)))
			} else {
				err = actor.Patch(ctx, obj.DeepCopy(), client.RawPatch(types.MergePatchType, []byte(tc.patch)))
			}
			if err == nil || !strings.Contains(err.Error(), PolicyName) {
				t.Fatalf("forbidden patch error = %v", err)
			}
		})
	}
	patch, _ := json.Marshal([]map[string]any{{"op": "test", "path": "/metadata/uid", "value": string(obj.GetUID())}, {"op": "test", "path": "/metadata/resourceVersion", "value": obj.GetResourceVersion()}, {"op": "add", "path": "/metadata/annotations/ome.io~1migration-request-v1-12345678-1234-1234-1234-123456789abc", "value": "{}"}})
	if err := actor.Patch(ctx, obj, client.RawPatch(types.JSONPatchType, patch)); err != nil {
		t.Fatalf("valid annotation addition denied: %v", err)
	}
	if err := actor.Patch(ctx, obj, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"ome.io/migration-request-v1-12345678-1234-1234-1234-123456789abc":"{}"}}}`))); err != nil {
		t.Fatalf("identical retry denied: %v", err)
	}
	if err := admin.Patch(ctx, obj, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"owner":"allowed"}}}`))); err != nil {
		t.Fatalf("owner update denied: %v", err)
	}
	bare := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ome.io/v1beta1", "kind": "InferenceService", "metadata": map[string]any{"name": "bare", "namespace": "workloads"}, "spec": map[string]any{}}}
	if err := admin.Create(ctx, bare); err != nil {
		t.Fatal(err)
	}
	if err := actor.Patch(ctx, bare, client.RawPatch(types.JSONPatchType, []byte(`[{"op":"add","path":"/metadata/annotations","value":{"ome.io/migration-request-v1-12345678-1234-1234-1234-123456789abc":"{}"}}]`))); err != nil {
		t.Fatalf("first annotations map denied: %v", err)
	}
}
