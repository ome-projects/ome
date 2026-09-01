package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	alfredServiceAccount = "ome-alfred"
	alfredNamespace      = "ome"
)

func TestRenderedAlfredLeaseContract(t *testing.T) {
	objects := renderAlfredManifests(t)

	t.Run("capability namespace follows the runtime namespace", func(t *testing.T) {
		deployment := manifestAs[appsv1.Deployment](t, findManifest(t, objects, "Deployment", "ome-alfred"))
		if len(deployment.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf("Alfred containers = %d, want 1", len(deployment.Spec.Template.Spec.Containers))
		}
		container := &deployment.Spec.Template.Spec.Containers[0]
		var podNamespace *corev1.EnvVar
		for i := range container.Env {
			env := &container.Env[i]
			if env.Name == "POD_NAMESPACE" {
				podNamespace = env
				break
			}
		}
		if podNamespace == nil || podNamespace.ValueFrom == nil || podNamespace.ValueFrom.FieldRef == nil ||
			podNamespace.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
			t.Fatalf("POD_NAMESPACE must come from metadata.namespace, got %+v", podNamespace)
		}
		for _, arg := range container.Args {
			if strings.HasPrefix(arg, "--namespace") {
				t.Fatalf("deployment argument %q overrides POD_NAMESPACE", arg)
			}
		}

		configMap := manifestAs[corev1.ConfigMap](t, findManifest(t, objects, "ConfigMap", "alfred-config"))
		store, err := config.NewStoreForNamespace("ome-prod")
		if err != nil {
			t.Fatalf("NewStoreForNamespace: %v", err)
		}
		outcome, err := store.Update([]byte(configMap.Data["config.yaml"]))
		if err != nil || outcome != config.OutcomeSuccess {
			t.Fatalf("load rendered Alfred config: %s, %v", outcome, err)
		}
		if got := store.Get().OMENativeCapabilityLeaseNamespace; got != "ome-prod" {
			t.Fatalf("capability namespace = %q, want runtime namespace %q", got, "ome-prod")
		}
	})

	t.Run("pre-created leader Lease is spec-less", func(t *testing.T) {
		lease := findManifest(t, objects, "Lease", leaderElectionID)
		if lease.GetNamespace() != alfredNamespace {
			t.Fatalf("leader Lease namespace = %q, want %q", lease.GetNamespace(), alfredNamespace)
		}
		if _, hasSpec := lease.Object["spec"]; hasSpec {
			t.Fatalf("leader Lease must be pre-created without producer state: %+v", lease.Object["spec"])
		}
	})

	role := manifestAs[rbacv1.Role](t, findManifest(t, objects, "Role", alfredServiceAccount))
	clusterRole := manifestAs[rbacv1.ClusterRole](t, findManifest(t, objects, "ClusterRole", alfredServiceAccount))
	roleBinding := manifestAs[rbacv1.RoleBinding](t, findManifest(t, objects, "RoleBinding", alfredServiceAccount))
	clusterRoleBinding := manifestAs[rbacv1.ClusterRoleBinding](t, findManifest(t, objects, "ClusterRoleBinding", alfredServiceAccount))

	t.Run("bindings target ome-alfred", func(t *testing.T) {
		if role.Namespace != alfredNamespace || roleBinding.Namespace != alfredNamespace {
			t.Fatalf("Role/RoleBinding namespaces = %q/%q, want %q",
				role.Namespace, roleBinding.Namespace, alfredNamespace)
		}
		assertBinding(t, roleBinding.RoleRef, roleBinding.Subjects, "Role")
		assertBinding(t, clusterRoleBinding.RoleRef, clusterRoleBinding.Subjects, "ClusterRole")
	})

	// The service account receives both bindings, so authorization is the
	// union of these rules. Auditing either object in isolation could miss a
	// broad grant on the other one.
	effectiveRules := append(append([]rbacv1.PolicyRule(nil), role.Rules...), clusterRole.Rules...)
	t.Run("effective Lease verbs are name scoped", func(t *testing.T) {
		verbs := []string{"get", "update", "create", "list", "watch", "patch", "delete", "deletecollection"}
		tests := []struct {
			name    string
			lease   string
			allowed map[string]bool
		}{
			{name: "leader", lease: leaderElectionID, allowed: map[string]bool{"get": true, "update": true}},
			{name: "capability", lease: constants.OMENativeExecutorCapabilityLeaseName, allowed: map[string]bool{"get": true}},
			{name: "unrelated", lease: "unrelated-lease", allowed: map[string]bool{}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				for _, verb := range verbs {
					got := allowsLease(effectiveRules, verb, test.lease)
					if got != test.allowed[verb] {
						t.Errorf("effective permission %s leases/%s = %t, want %t",
							verb, test.lease, got, test.allowed[verb])
					}
				}
			})
		}
	})

	t.Run("InferenceService writes wait for the Dispatcher", func(t *testing.T) {
		for _, resource := range []string{"inferenceservices", "inferenceservices/status"} {
			for _, verb := range []string{"get", "list", "watch"} {
				if !allowsResource(effectiveRules, "ome.io", resource, verb) {
					t.Errorf("effective permission %s %s/%s = false, want read access", verb, "ome.io", resource)
				}
			}
			for _, verb := range []string{"create", "update", "patch", "delete", "deletecollection"} {
				if allowsResource(effectiveRules, "ome.io", resource, verb) {
					t.Errorf("effective permission %s %s/%s = true before Dispatcher and admission guard", verb, "ome.io", resource)
				}
			}
		}
	})
}

func TestAllowsResourceRecognizesWildcardGrants(t *testing.T) {
	tests := []struct {
		name            string
		groups          []string
		resources       []string
		verbs           []string
		checkedResource string
		want            bool
	}{
		{name: "global wildcard", groups: []string{"*"}, resources: []string{"*"}, verbs: []string{"*"}, checkedResource: "inferenceservices", want: true},
		{name: "exact resource", groups: []string{"ome.io"}, resources: []string{"inferenceservices"}, verbs: []string{"patch"}, checkedResource: "inferenceservices", want: true},
		{name: "all subresources", groups: []string{"ome.io"}, resources: []string{"inferenceservices/*"}, verbs: []string{"patch"}, checkedResource: "inferenceservices/status", want: true},
		{name: "named subresource wildcard", groups: []string{"ome.io"}, resources: []string{"*/status"}, verbs: []string{"patch"}, checkedResource: "inferenceservices/status", want: true},
		{name: "two-part wildcard", groups: []string{"ome.io"}, resources: []string{"*/*"}, verbs: []string{"patch"}, checkedResource: "inferenceservices/status", want: true},
		{name: "other resource", groups: []string{"ome.io"}, resources: []string{"pods"}, verbs: []string{"patch"}, checkedResource: "inferenceservices", want: false},
		{name: "other subresource", groups: []string{"ome.io"}, resources: []string{"*/scale"}, verbs: []string{"patch"}, checkedResource: "inferenceservices/status", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rules := []rbacv1.PolicyRule{{
				APIGroups: test.groups,
				Resources: test.resources,
				Verbs:     test.verbs,
			}}
			if got := allowsResource(rules, "ome.io", test.checkedResource, "patch"); got != test.want {
				t.Fatalf("allowsResource(%v, %q) = %t, want %t", test.resources, test.checkedResource, got, test.want)
			}
		})
	}
}

func renderAlfredManifests(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve manifests test path")
	}
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "config", "alfred")
	kustomizationRaw, err := os.ReadFile(filepath.Join(dir, "kustomization.yaml"))
	if err != nil {
		t.Fatalf("read config/alfred kustomization: %v", err)
	}
	var kustomization struct {
		APIVersion string   `json:"apiVersion"`
		Kind       string   `json:"kind"`
		Namespace  string   `json:"namespace"`
		Resources  []string `json:"resources"`
	}
	if err := yaml.UnmarshalStrict(kustomizationRaw, &kustomization); err != nil {
		t.Fatalf("decode config/alfred kustomization: %v", err)
	}
	if len(kustomization.Resources) == 0 {
		t.Fatal("config/alfred kustomization has no resources")
	}

	var objects []*unstructured.Unstructured
	for _, resource := range kustomization.Resources {
		raw, err := os.ReadFile(filepath.Join(dir, resource))
		if err != nil {
			t.Fatalf("read rendered resource %q: %v", resource, err)
		}
		decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
		for {
			var object unstructured.Unstructured
			if err := decoder.Decode(&object); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("decode rendered resource %q: %v", resource, err)
			}
			if len(object.Object) == 0 {
				continue
			}
			if object.GetNamespace() == "" && object.GetKind() != "ClusterRole" && object.GetKind() != "ClusterRoleBinding" {
				object.SetNamespace(kustomization.Namespace)
			}
			objects = append(objects, &object)
		}
	}
	return objects
}

func findManifest(t *testing.T, objects []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	t.Helper()
	var found *unstructured.Unstructured
	for _, object := range objects {
		if object.GetKind() != kind || object.GetName() != name {
			continue
		}
		if found != nil {
			t.Fatalf("rendered duplicate %s/%s", kind, name)
		}
		found = object
	}
	if found == nil {
		t.Fatalf("rendered %s/%s not found", kind, name)
	}
	return found
}

func manifestAs[T any](t *testing.T, object *unstructured.Unstructured) *T {
	t.Helper()
	raw, err := json.Marshal(object.Object)
	if err != nil {
		t.Fatal(err)
	}
	var typed T
	if err := json.Unmarshal(raw, &typed); err != nil {
		t.Fatalf("decode %s/%s: %v", object.GetKind(), object.GetName(), err)
	}
	return &typed
}

func assertBinding(t *testing.T, ref rbacv1.RoleRef, subjects []rbacv1.Subject, wantKind string) {
	t.Helper()
	if ref.APIGroup != rbacv1.GroupName || ref.Kind != wantKind || ref.Name != alfredServiceAccount {
		t.Fatalf("roleRef = %+v, want %s/%s", ref, wantKind, alfredServiceAccount)
	}
	want := rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Name: alfredServiceAccount, Namespace: alfredNamespace}
	if len(subjects) != 1 || subjects[0] != want {
		t.Fatalf("subjects = %+v, want only %+v", subjects, want)
	}
}

func allowsLease(rules []rbacv1.PolicyRule, verb, name string) bool {
	for _, rule := range rules {
		if !containsRBAC(rule.APIGroups, "coordination.k8s.io") ||
			!containsRBAC(rule.Resources, "leases") ||
			!containsRBAC(rule.Verbs, verb) {
			continue
		}
		// Empty resourceNames grants every name. Treat "*" conservatively as
		// broad too, so a manifest cannot evade this safety audit by spelling
		// an apparent wildcard there.
		if len(rule.ResourceNames) == 0 || containsRBAC(rule.ResourceNames, name) {
			return true
		}
	}
	return false
}

func containsRBAC(values []string, want string) bool {
	for _, value := range values {
		if value == want || value == "*" {
			return true
		}
	}
	return false
}

func allowsResource(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	for _, rule := range rules {
		if !containsRBAC(rule.APIGroups, group) || !containsRBAC(rule.Verbs, verb) {
			continue
		}
		for _, candidate := range rule.Resources {
			if matchesRBACResource(candidate, resource) {
				return true
			}
		}
	}
	return false
}

func matchesRBACResource(pattern, resource string) bool {
	patternParts, resourceParts := strings.Split(pattern, "/"), strings.Split(resource, "/")
	if len(patternParts) != len(resourceParts) {
		return false
	}
	for i := range patternParts {
		if patternParts[i] != "*" && patternParts[i] != resourceParts[i] {
			return false
		}
	}
	return true
}
