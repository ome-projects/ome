package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type clusterPlan struct {
	OMERevision      string `json:"omeRevision"`
	ManagerNamespace string `json:"managerNamespace"`
	Cluster          string `json:"cluster"`
	ManagerRole      string `json:"managerRole"`
}

func readPlan(path string) (clusterPlan, error) {
	var p clusterPlan
	data, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(data, &p)
	if err == nil && (p.Cluster == "" || p.ManagerRole != "" && p.ManagerRole != "control-plane") {
		err = fmt.Errorf("invalid cluster plan")
	}
	return p, err
}

func resourceKey(obj *unstructured.Unstructured) string {
	return obj.GetKind() + "/" + obj.GetNamespace() + "/" + obj.GetName()
}

func seedBaseline(ctx context.Context, api client.Client, root string) error {
	old, err := documents(filepath.Join(root, "baseline.yaml"))
	if err != nil {
		return err
	}
	proposed, err := documents(filepath.Join(root, "proposed.yaml"))
	if err != nil {
		return err
	}
	catalog, err := documents(filepath.Join(root, "catalog.yaml"))
	if err != nil {
		return err
	}
	future := map[string]bool{}
	namespaces := map[string]bool{}
	for _, obj := range append(proposed, catalog...) {
		future[resourceKey(obj)] = true
		if ns := obj.GetNamespace(); ns != "" {
			namespaces[ns] = true
		}
	}
	for ns := range namespaces {
		err := api.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	seeds := map[string]*unstructured.Unstructured{}
	for _, obj := range old {
		if supported[obj.GetKind()] && future[resourceKey(obj)] {
			seeds[resourceKey(obj)] = obj
		}
	}
	// Chart-owned defaults provide the same lookup context as the selected release.
	for _, obj := range catalog {
		seeds[resourceKey(obj)] = obj
	}
	for _, obj := range seeds {
		if err := api.Create(ctx, obj, client.FieldValidation(metav1.FieldValidationStrict)); err != nil {
			return fmt.Errorf("baseline %s is incompatible with selected CRD schema: %w", resourceKey(obj), err)
		}
	}
	return nil
}

func authoredOrder(objects []*unstructured.Unstructured) {
	lookup := map[string]*unstructured.Unstructured{}
	for _, obj := range objects {
		lookup[resourceKey(obj)] = obj
	}
	var depth func(*unstructured.Unstructured, map[string]bool) int
	depth = func(obj *unstructured.Unstructured, seen map[string]bool) int {
		key := resourceKey(obj)
		if seen[key] {
			return 100
		}
		parent := obj.GetAnnotations()["ome.io/inherit-from"]
		if parent == "" {
			return 0
		}
		seen[key] = true
		defer delete(seen, key)
		kind, ns := obj.GetKind(), obj.GetNamespace()
		p := lookup[kind+"/"+ns+"/"+parent]
		if p == nil && kind == "ServingRuntime" {
			p = lookup["ClusterServingRuntime//"+parent]
		}
		if p == nil {
			return 0
		}
		return 1 + depth(p, seen)
	}
	rank := func(obj *unstructured.Unstructured) int {
		switch obj.GetKind() {
		case "ClusterBaseModel", "BaseModel":
			return 0
		case "ClusterServingRuntime", "ServingRuntime":
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(objects, func(i, j int) bool {
		a, b := rank(objects[i]), rank(objects[j])
		if a != b {
			return a < b
		}
		x, y := depth(objects[i], map[string]bool{}), depth(objects[j], map[string]bool{})
		if x != y {
			return x < y
		}
		return resourceKey(objects[i]) < resourceKey(objects[j])
	})
}

func validateAuthored(ctx context.Context, api client.Client, calls *trace, root, role string) ([]result, error) {
	plan, err := readPlan(filepath.Join(root, "plan.json"))
	if err != nil {
		return nil, err
	}
	objects, err := documents(filepath.Join(root, "proposed.yaml"))
	if err != nil {
		return nil, err
	}
	authoredOrder(objects)
	var results []result
	for _, obj := range objects {
		if !supported[obj.GetKind()] {
			continue
		}
		live := &unstructured.Unstructured{}
		live.SetGroupVersionKind(obj.GroupVersionKind())
		getErr := api.Get(ctx, client.ObjectKeyFromObject(obj), live)
		operation := "create"
		if getErr == nil {
			operation = "update"
		} else if !apierrors.IsNotFound(getErr) {
			return nil, getErr
		}
		one, err := probeResource(ctx, api, calls, obj, !(role == "control-plane" && obj.GetKind() == "InferenceService"))
		if err != nil {
			return nil, err
		}
		one.Case = plan.Cluster
		results = append(results, one)

		// Validated dependency changes are stored only in this private API server
		// so later service admission reads the proposed model/runtime catalog.
		if one.Passed && obj.GetKind() != "InferenceService" {
			if operation == "update" {
				obj.SetResourceVersion(live.GetResourceVersion())
				err = api.Update(ctx, obj)
			} else {
				err = api.Create(ctx, obj)
			}
			if err != nil {
				return nil, fmt.Errorf("store validated dependency %s: %w", resourceKey(obj), err)
			}
		}
	}
	return results, nil
}
