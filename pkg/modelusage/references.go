// Package modelusage queries current serving references and maintains BaseModel
// idle history. It starts no controllers or watches and keeps no reference graph.
package modelusage

import (
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ModelReferences contains the distinct names protected by one source. An
// unresolved OME workload conservatively protects every model in its namespace.
type ModelReferences struct {
	Models     []types.NamespacedName
	Unresolved bool
}

// References extracts names without consulting readiness, phase, replica count,
// or deletion timestamps. Sources protect their models until actually removed.
// Supported types match ReferenceQuery's source set, including optional LWS.
func References(object client.Object) (ModelReferences, error) {
	if object == nil {
		return ModelReferences{}, fmt.Errorf("nil reference source")
	}
	refs := map[string]struct{}{}
	unresolved := false
	add := func(name string) {
		if name != "" {
			refs[name] = struct{}{}
		}
	}
	addModel := func(model *v1beta1.ModelRef) {
		if model == nil {
			return
		}
		// OME resolves namespaced BaseModel first, even for explicit ClusterBaseModel
		// kind, then falls back to ClusterBaseModel. Overlays use the same lookup.
		add(model.Name)
		for _, overlay := range model.Overlays {
			add(overlay.Name)
		}
	}
	addWorkload := func(meta metav1.ObjectMeta, spec corev1.PodSpec, managed bool) {
		if !managed && meta.Labels[constants.InferenceServicePodLabelKey] == "" {
			return
		}
		annotation := meta.Annotations[constants.BaseModelName]
		label := meta.Labels[constants.InferenceServiceBaseModelNameLabelKey]
		add(annotation)
		add(label)
		missing := annotation == "" && label == ""
		conflicting := annotation != "" && label != "" && annotation != label
		unresolved = unresolved || missing || conflicting
		for _, volume := range spec.Volumes {
			// Only renderer-named local overlays identify BaseModels. Arbitrary host
			// mounts and remote/emptyDir overlay volumes do not provide model identity.
			if volume.HostPath != nil && strings.HasPrefix(volume.Name, "model-overlay-") {
				name := strings.TrimPrefix(volume.Name, "model-overlay-")
				add(name)
				unresolved = unresolved || name == ""
			}
		}
	}
	managed := object.GetLabels()[constants.InferenceServicePodLabelKey] != ""
	switch source := object.(type) {
	case *v1beta1.InferenceService:
		addModel(source.Spec.Model)
	case *corev1.Pod:
		addWorkload(source.ObjectMeta, source.Spec, managed)
	case *appsv1.Deployment:
		addWorkload(source.Spec.Template.ObjectMeta, source.Spec.Template.Spec, managed)
	case *appsv1.ReplicaSet:
		addWorkload(source.Spec.Template.ObjectMeta, source.Spec.Template.Spec, managed)
	case *appsv1.StatefulSet:
		addWorkload(source.Spec.Template.ObjectMeta, source.Spec.Template.Spec, managed)
	case *lws.LeaderWorkerSet:
		template := source.Spec.LeaderWorkerTemplate
		if template.LeaderTemplate != nil {
			addWorkload(template.LeaderTemplate.ObjectMeta, template.LeaderTemplate.Spec, managed)
		}
		addWorkload(template.WorkerTemplate.ObjectMeta, template.WorkerTemplate.Spec, managed)
	case *v1beta1.InferenceReplica:
		addModel(source.Spec.ModelRef)
		for _, runner := range source.Spec.Runners {
			// The InferenceReplica renderer adds the OME label to emitted pods, even
			// when it is absent from the stored runner template.
			addWorkload(runner.Template.ObjectMeta, runner.Template.Spec, true)
		}
	default:
		return ModelReferences{}, fmt.Errorf("unsupported reference source %T", object)
	}
	result := ModelReferences{Unresolved: unresolved}
	for name := range refs {
		// Templates normally have no namespace; the owning object supplies it.
		result.Models = append(result.Models, types.NamespacedName{Namespace: object.GetNamespace(), Name: name})
	}
	sort.Slice(result.Models, func(i, j int) bool { return result.Models[i].Name < result.Models[j].Name })
	return result, nil
}

// Request is comparable and can be queued directly. ReferenceRemovedAt is the
// immutable event observation time, not a claim that all demand has ended.
// Leave it zero for ordinary eligibility evaluation or startup/relist work.
type Request struct {
	Model              types.NamespacedName
	ReferenceRemovedAt time.Time
}

// RemovedReferences derives release requests from an update or actual deletion
// (newObject == nil). Add/startup events, status-only updates, duplicate names,
// and reference additions produce no requests. Retry the returned request with
// its original timestamp; never replace it with reconciliation time.
func RemovedReferences(oldObject, newObject client.Object, observedAt time.Time) ([]Request, error) {
	if oldObject == nil {
		return nil, nil
	}
	if observedAt.IsZero() {
		return nil, fmt.Errorf("reference removal requires an observation timestamp")
	}
	oldRefs, err := References(oldObject)
	if err != nil {
		return nil, err
	}
	remaining := map[types.NamespacedName]struct{}{}
	if newObject != nil {
		newRefs, err := References(newObject)
		if err != nil {
			return nil, err
		}
		for _, key := range newRefs.Models {
			remaining[key] = struct{}{}
		}
	}
	var requests []Request
	for _, key := range oldRefs.Models {
		if _, exists := remaining[key]; !exists {
			requests = append(requests, Request{Model: key, ReferenceRemovedAt: observedAt.UTC()})
		}
	}
	return requests, nil
}
