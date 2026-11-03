package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestAuthoredOrder(t *testing.T) {
	resource := func(kind, name, parent string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ome.io/v1beta1", "kind": kind, "metadata": map[string]any{"name": name, "annotations": map[string]any{"ome.io/inherit-from": parent}}}}
	}
	objects := []*unstructured.Unstructured{resource("InferenceService", "svc", ""), resource("ServingRuntime", "child", "parent"), resource("ClusterServingRuntime", "parent", ""), resource("BaseModel", "model", "")}
	authoredOrder(objects)
	want := []string{"model", "parent", "child", "svc"}
	for i, obj := range objects {
		if obj.GetName() != want[i] {
			t.Fatalf("order[%d]=%s, want %s", i, obj.GetName(), want[i])
		}
	}
}
