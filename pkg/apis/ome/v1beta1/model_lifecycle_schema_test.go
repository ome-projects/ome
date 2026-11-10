package v1beta1

import (
	"os"
	"path/filepath"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"
)

func TestModelLifecycleSchema(t *testing.T) {
	for _, name := range []string{"basemodels", "clusterbasemodels", "finetunedweights"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("../../../..", "config/crd/full", "ome.io_"+name+".yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var crd apiextensionsv1.CustomResourceDefinition
			if err := yaml.Unmarshal(data, &crd); err != nil {
				t.Fatal(err)
			}
			for _, version := range crd.Spec.Versions {
				if !version.Served {
					continue
				}
				status := version.Schema.OpenAPIV3Schema.Properties["status"]
				completion := status.Properties["rehydration"].Properties["completedRequestID"]
				if completion.Type != "string" {
					t.Fatalf("%s has no string restoration completion field", version.Name)
				}
				if _, exists := status.Properties["nodesEvicted"]; exists {
					t.Fatalf("%s unexpectedly exposes nodesEvicted", version.Name)
				}
				var schema apiextensions.JSONSchemaProps
				if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&status, &schema, nil); err != nil {
					t.Fatal(err)
				}
				validator, _, err := validation.NewSchemaValidator(&schema)
				if err != nil {
					t.Fatal(err)
				}
				for _, state := range []LifeCycleState{LifeCycleStateCreating, LifeCycleStateImporting, LifeCycleStateInTransit, LifeCycleStateInTraining, LifeCycleStateReady, LifeCycleStateFailed, LifeCycleStateEvicted} {
					if errs := validation.ValidateCustomResource(field.NewPath("status"), map[string]interface{}{"state": string(state)}, validator); len(errs) > 0 {
						t.Errorf("%s rejects %s: %v", version.Name, state, errs)
					}
				}
				for _, state := range []string{"Evicting", "Restoring", "unexpected"} {
					if errs := validation.ValidateCustomResource(field.NewPath("status"), map[string]interface{}{"state": state}, validator); len(errs) == 0 {
						t.Errorf("%s unexpectedly accepts %s", version.Name, state)
					}
				}
			}
		})
	}
}
