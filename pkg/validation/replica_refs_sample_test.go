package validation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// repoConfig is the repository's config directory, from this package's own.
var repoConfig = filepath.Join("..", "..", "config")

// The sample under config/samples/replica-refs is one service over standalone
// replicas. Every object passes the generated CRD schema the API server
// applies first; the service keeps the rules this package states; and every
// replica the sample ships is one the service names, with the component of
// its role.
func TestReplicaRefsSample_StaysAValidReferencedService(t *testing.T) {
	svc, replicas := sampleObjects(t, filepath.Join(repoConfig, "samples", "replica-refs"))
	if findings := schemaFindings(t, crdSchemaValidator(t, "ome.io_inferenceservices.yaml"), &svc); len(findings) > 0 {
		t.Fatalf("the sample service fails the InferenceService schema: %s", strings.Join(findings, "; "))
	}
	replicaSchema := crdSchemaValidator(t, "ome.io_inferencereplicas.yaml")
	if err := ValidateInferenceServiceName(svc.Name); err != nil {
		t.Fatalf("service name: %v", err)
	}
	if err := ValidateReplicaRefs(&svc.Spec); err != nil {
		t.Fatalf("the sample service breaks the referenced-form rules: %v", err)
	}
	named := map[string]bool{}
	for _, c := range ComponentRoles {
		name := ReferencedReplica(&svc.Spec, c)
		if name == "" {
			continue
		}
		named[name] = true
		checkReferencedReplica(t, &svc, c, name, replicas)
		ir := replicas[name]
		if findings := schemaFindings(t, replicaSchema, &ir); len(findings) > 0 {
			t.Fatalf("%s fails the InferenceReplica schema: %s", name, strings.Join(findings, "; "))
		}
	}
	for name := range replicas {
		if !named[name] {
			t.Fatalf("the sample ships %s, which the service does not name", name)
		}
	}
}

// checkReferencedReplica asserts the replica a role names is one the sample
// ships: in the service's namespace, standalone, of the role's component,
// with a service-shaped name and one template source.
func checkReferencedReplica(t *testing.T, svc *v1beta1.InferenceService, c v1beta1.ComponentType, name string, replicas map[string]v1beta1.InferenceReplica) {
	t.Helper()
	ir, ok := replicas[name]
	if !ok {
		t.Fatalf("%s names %s, which the sample does not ship", ReplicaRefsField(c), name)
	}
	if ir.Namespace != svc.Namespace {
		t.Fatalf("%s lives in %s; the service names replicas of its own namespace %s", name, ir.Namespace, svc.Namespace)
	}
	if ir.Spec.Component != c {
		t.Fatalf("%s names %s, whose component is %s", ReplicaRefsField(c), name, ir.Spec.Component)
	}
	if parent := ProjectedBy(&ir); parent != "" {
		t.Fatalf("%s is projected by %s; the service names standalone replicas", name, parent)
	}
	if err := ValidateInferenceServiceName(ir.Name); err != nil {
		t.Fatalf("replica name: %v", err)
	}
	if err := ValidateInferenceReplicaTemplateSource(len(ir.Spec.Runners), ir.Spec.ModelRef != nil, ir.Spec.RuntimeRef != nil, false); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// sampleObjects decodes every document under dir into the one service the
// sample holds and its replicas by name.
func sampleObjects(t *testing.T, dir string) (v1beta1.InferenceService, map[string]v1beta1.InferenceReplica) {
	t.Helper()
	var services []v1beta1.InferenceService
	replicas := map[string]v1beta1.InferenceReplica{}
	for _, doc := range sampleDocuments(t, dir) {
		var meta metav1.TypeMeta
		if err := yaml.Unmarshal(doc, &meta); err != nil {
			t.Fatalf("decode: %v", err)
		}
		switch meta.Kind {
		case "InferenceService":
			var isvc v1beta1.InferenceService
			if err := yaml.UnmarshalStrict(doc, &isvc); err != nil {
				t.Fatalf("decode InferenceService: %v", err)
			}
			services = append(services, isvc)
		case "InferenceReplica":
			var ir v1beta1.InferenceReplica
			if err := yaml.UnmarshalStrict(doc, &ir); err != nil {
				t.Fatalf("decode InferenceReplica: %v", err)
			}
			replicas[ir.Name] = ir
		default:
			t.Fatalf("the sample carries a %q; it holds InferenceServices and InferenceReplicas only", meta.Kind)
		}
	}
	if len(services) != 1 {
		t.Fatalf("the sample holds %d InferenceServices, want 1", len(services))
	}
	return services[0], replicas
}

// sampleDocuments returns the YAML documents of every file in dir.
func sampleDocuments(t *testing.T, dir string) [][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var docs [][]byte
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		for _, doc := range strings.Split(string(data), "\n---") {
			if strings.TrimSpace(doc) != "" {
				docs = append(docs, []byte(doc))
			}
		}
	}
	return docs
}

// crdSchemaValidator builds the OpenAPI validator of one served CRD under
// config/crd/full, the check the API server runs before any webhook.
func crdSchemaValidator(t *testing.T, file string) apiservervalidation.SchemaValidator {
	t.Helper()
	path := filepath.Join(repoConfig, "crd", "full", file)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Schema == nil || crd.Spec.Versions[0].Schema.OpenAPIV3Schema == nil {
		t.Fatalf("%s: expected exactly one served version with a schema", path)
	}
	var schema apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &schema, nil); err != nil {
		t.Fatalf("convert %s schema: %v", path, err)
	}
	validator, _, err := apiservervalidation.NewSchemaValidator(&schema)
	if err != nil {
		t.Fatalf("build validator for %s: %v", path, err)
	}
	return validator
}

// schemaFindings validates one object against a CRD schema and returns every
// finding worded as the API server words it.
func schemaFindings(t *testing.T, validator apiservervalidation.SchemaValidator, obj runtime.Object) []string {
	t.Helper()
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, finding := range apiservervalidation.ValidateCustomResource(nil, content, validator) {
		out = append(out, finding.Error())
	}
	return out
}
