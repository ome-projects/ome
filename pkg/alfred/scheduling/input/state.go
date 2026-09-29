package input

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
)

// SameSchedulingState compares the scheduling objects in two captures, ignoring
// only resourceVersion, managedFields, Namespace annotations and Node condition
// lastHeartbeatTime.
// These fields do not affect placement in the supported private schedulers.
// Every other field, including unknown fields, remains significant. In
// particular, Pod status can affect occupancy, nomination and allocated resources.
//
// This neither changes the lossless snapshots nor replaces their integrity/age
// validation, source fencing, fresh policy/budget checks or submission CAS.
func SameSchedulingState(a, b *Snapshot) bool {
	if a == nil || b == nil || len(a.Objects) != len(b.Objects) {
		return false
	}
	left, ok := schedulingState(a)
	if !ok {
		return false
	}
	right, ok := schedulingState(b)
	return ok && reflect.DeepEqual(left, right)
}

type schedulingObjectKey struct {
	apiVersion, kind, namespace, name string
}

func schedulingState(s *Snapshot) (map[schedulingObjectKey]map[string]any, bool) {
	objects := make(map[schedulingObjectKey]map[string]any, len(s.Objects))
	for _, raw := range s.Objects {
		// Decode private copies without projecting through a typed API struct:
		// a projection could silently discard a newly introduced scheduling field.
		// UseNumber preserves integers above float64's exact range.
		decoder := json.NewDecoder(bytes.NewReader(raw.Raw))
		decoder.UseNumber()
		var object map[string]any
		if decoder.Decode(&object) != nil || decoder.Decode(new(any)) != io.EOF {
			return nil, false
		}
		apiVersion, _ := object["apiVersion"].(string)
		kind, _ := object["kind"].(string)
		switch apiVersion + "/" + kind {
		case "v1/Node", "v1/Pod", "v1/Namespace", "v1/Service", "v1/ReplicationController",
			"apps/v1/ReplicaSet", "apps/v1/StatefulSet", "scheduling.x-k8s.io/v1alpha1/PodGroup":
		default:
			return nil, false
		}
		meta, ok := object["metadata"].(map[string]any)
		if !ok {
			return nil, false
		}
		name, _ := meta["name"].(string)
		namespace, _ := meta["namespace"].(string)
		key := schedulingObjectKey{apiVersion, kind, namespace, name}
		if _, exists := objects[key]; exists || name == "" {
			return nil, false
		}
		delete(meta, "resourceVersion")
		delete(meta, "managedFields")
		if kind == "Namespace" && !omitNamespaceAnnotations(meta) {
			return nil, false
		}
		if kind == "Node" && !omitNodeHeartbeats(object) {
			return nil, false
		}
		objects[key] = object
	}
	return objects, true
}

func omitNamespaceAnnotations(meta map[string]any) bool {
	// The pinned in-tree plugins use Namespace names and labels for affinity;
	// OMEGangPack uses PodGroup (not Namespace) annotations. Keep those inputs,
	// unknown fields and all object membership unchanged. Validate the known
	// annotation shape before omitting it, even for identical invalid captures.
	if value := meta["annotations"]; value != nil {
		annotations, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, value := range annotations {
			if _, ok := value.(string); !ok {
				return false
			}
		}
	}
	delete(meta, "annotations")
	return true
}

func omitNodeHeartbeats(object map[string]any) bool {
	if object["status"] == nil {
		return true
	}
	status, ok := object["status"].(map[string]any)
	if !ok {
		return false
	}
	if status["conditions"] == nil {
		return true
	}
	conditions, ok := status["conditions"].([]any)
	if !ok {
		return false
	}
	for _, value := range conditions {
		condition, ok := value.(map[string]any)
		if !ok {
			return false
		}
		// Keep type, status, reason, message and lastTransitionTime: health
		// evaluation and its stabilization windows can depend on these.
		delete(condition, "lastHeartbeatTime")
	}
	return true
}
