package runtime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	crdvalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	kjson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const inferenceServiceCRDName = "inferenceservices.ome.io"

var inferenceServiceCRDDecoder = func() k8sruntime.Decoder {
	scheme := k8sruntime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()
}()

// loadInferenceServiceStructuralSchema loads the exact CRD that governs file
// InferenceServices. File mode cannot ask an API server to apply CRD defaults,
// so it uses the same Kubernetes structural-schema defaulting implementation.
func (o *renderOptions) loadInferenceServiceStructuralSchema() (*structuralschema.Structural, error) {
	path := o.inferenceServiceCRDPath
	data, err := o.readFile(path)
	if err != nil {
		return nil, fmt.Errorf("read --inference-service-crd: %w", err)
	}
	raw, err := singleCRDDocument(path, data)
	if err != nil {
		return nil, err
	}
	var typeMeta metav1.TypeMeta
	if err := kjson.Unmarshal(raw, &typeMeta); err != nil {
		return nil, fmt.Errorf("--inference-service-crd %s: decode type metadata: %w", path, err)
	}
	if typeMeta.APIVersion != apiextensionsv1.SchemeGroupVersion.String() || typeMeta.Kind != "CustomResourceDefinition" {
		return nil, fmt.Errorf("--inference-service-crd %s: want apiextensions.k8s.io/v1 CustomResourceDefinition, got %s %s", path, typeMeta.APIVersion, typeMeta.Kind)
	}

	decoded, gvk, err := inferenceServiceCRDDecoder.Decode(raw, nil, nil)
	if err != nil {
		if k8sruntime.IsNotRegisteredError(err) && gvk != nil {
			return nil, fmt.Errorf("--inference-service-crd %s: want apiextensions.k8s.io/v1 CustomResourceDefinition, got %s %s", path, gvk.GroupVersion(), gvk.Kind)
		}
		return nil, fmt.Errorf("--inference-service-crd %s: decode manifest: %w", path, err)
	}
	crd, ok := decoded.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		return nil, fmt.Errorf("--inference-service-crd %s: want apiextensions.k8s.io/v1 CustomResourceDefinition, got %s %s", path, gvk.GroupVersion(), gvk.Kind)
	}
	if err := validateInferenceServiceCRDIdentity(crd); err != nil {
		return nil, fmt.Errorf("--inference-service-crd %s: %w", path, err)
	}

	var selected *apiextensionsv1.CustomResourceDefinitionVersion
	for i := range crd.Spec.Versions {
		version := &crd.Spec.Versions[i]
		if version.Name != v1beta1.SchemeGroupVersion.Version {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("--inference-service-crd %s: version %s is defined more than once", path, version.Name)
		}
		selected = version
	}
	if selected == nil {
		return nil, fmt.Errorf("--inference-service-crd %s: version %s is not defined", path, v1beta1.SchemeGroupVersion.Version)
	}
	if !selected.Served {
		return nil, fmt.Errorf("--inference-service-crd %s: version %s is not served", path, selected.Name)
	}
	if selected.Schema == nil || selected.Schema.OpenAPIV3Schema == nil {
		return nil, fmt.Errorf("--inference-service-crd %s: version %s has no openAPIV3Schema", path, selected.Name)
	}

	internalValidation := &apiextensions.CustomResourceValidation{}
	if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(
		selected.Schema, internalValidation, nil,
	); err != nil {
		return nil, fmt.Errorf("--inference-service-crd %s: convert version %s schema: %w", path, selected.Name, err)
	}
	structural, err := structuralschema.NewStructural(internalValidation.OpenAPIV3Schema)
	if err != nil {
		return nil, fmt.Errorf("--inference-service-crd %s: version %s schema is not structural: %w", path, selected.Name, err)
	}
	if structuralErrors := structuralschema.ValidateStructural(field.NewPath("openAPIV3Schema"), structural); len(structuralErrors) > 0 {
		return nil, fmt.Errorf("--inference-service-crd %s: version %s schema is not structural: %w", path, selected.Name, structuralErrors.ToAggregate())
	}
	defaultErrors, err := structuraldefaulting.ValidateDefaults(
		context.Background(), field.NewPath("openAPIV3Schema"), structural, true, true,
	)
	if err != nil {
		return nil, fmt.Errorf("--inference-service-crd %s: validate version %s defaults: %w", path, selected.Name, err)
	}
	if len(defaultErrors) > 0 {
		return nil, fmt.Errorf("--inference-service-crd %s: version %s defaults are invalid: %w", path, selected.Name, defaultErrors.ToAggregate())
	}
	// The API server owns a copy and prunes fields in object-valued defaults
	// which the schema would itself prune. Mirror that preparation here.
	structural = structural.DeepCopy()
	if err := structuraldefaulting.PruneDefaults(structural); err != nil {
		return nil, fmt.Errorf("--inference-service-crd %s: prepare version %s defaults: %w", path, selected.Name, err)
	}
	validator, _, err := crdvalidation.NewSchemaValidator(internalValidation.OpenAPIV3Schema)
	if err != nil {
		return nil, fmt.Errorf("--inference-service-crd %s: create validation schema: %w", path, err)
	}
	o.validateFileSpec = func(raw []byte) error {
		object := map[string]interface{}{}
		if err := kjson.Unmarshal(raw, &object); err != nil {
			return err
		}
		if object["apiVersion"] != v1beta1.SchemeGroupVersion.String() || object["kind"] != "InferenceService" {
			return nil
		}
		return crdvalidation.ValidateCustomResource(field.NewPath("InferenceService"), object, validator).ToAggregate()
	}
	return structural, nil
}

func validateInferenceServiceCRDIdentity(crd *apiextensionsv1.CustomResourceDefinition) error {
	wantGVK := v1beta1.SchemeGroupVersion.WithKind("InferenceService")
	if crd.Name != inferenceServiceCRDName ||
		crd.Spec.Group != wantGVK.Group ||
		crd.Spec.Names.Plural != "inferenceservices" ||
		crd.Spec.Names.Kind != wantGVK.Kind ||
		crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
		return fmt.Errorf(
			"want namespaced CRD %s for %s %s, got name=%q group=%q plural=%q kind=%q scope=%q",
			inferenceServiceCRDName, wantGVK.GroupVersion(), wantGVK.Kind,
			crd.Name, crd.Spec.Group, crd.Spec.Names.Plural, crd.Spec.Names.Kind, crd.Spec.Scope,
		)
	}
	if crd.Spec.PreserveUnknownFields {
		return errors.New("spec.preserveUnknownFields must be false")
	}
	return nil
}

func singleCRDDocument(path string, data []byte) ([]byte, error) {
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var raw []byte
	for {
		document, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("--inference-service-crd %s: read manifest: %w", path, err)
		}
		converted, err := yaml.YAMLToJSONStrict(document)
		if err != nil {
			return nil, fmt.Errorf("--inference-service-crd %s: decode manifest: %w", path, err)
		}
		if len(converted) == 0 || string(converted) == "null" {
			continue
		}
		if raw != nil {
			return nil, fmt.Errorf("--inference-service-crd %s: manifest must contain exactly one object", path)
		}
		raw = converted
	}
	if raw == nil {
		return nil, fmt.Errorf("--inference-service-crd %s: manifest contains no object", path)
	}
	return raw, nil
}

// defaultFileInferenceService applies CRD defaults to only the file object
// Kubernetes would store as an InferenceService. The returned JSON is then
// passed to the existing strict typed decoder, retaining unknown-field errors.
func defaultFileInferenceService(raw []byte, structural *structuralschema.Structural) ([]byte, error) {
	var typeMeta metav1.TypeMeta
	if err := kjson.Unmarshal(raw, &typeMeta); err != nil {
		return nil, err
	}
	wantGVK := v1beta1.SchemeGroupVersion.WithKind("InferenceService")
	if typeMeta.APIVersion != wantGVK.GroupVersion().String() || typeMeta.Kind != wantGVK.Kind {
		return raw, nil
	}

	object := map[string]interface{}{}
	if err := kjson.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	structuraldefaulting.Default(object, structural)
	return kjson.Marshal(object)
}
