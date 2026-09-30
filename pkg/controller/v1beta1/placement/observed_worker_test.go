package placement

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// observedWorkerObjects pairs admission reports with their owned physical
// resources so placement can independently verify serving and budget evidence.
func observedWorkerObjects(member *v1beta1.InferenceService, components ...*v1beta1.InferenceReplica) []client.Object {
	member.Spec.DeploymentMode = ptr.To(constants.OMENative)
	member.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: backendTestRuntime().Name}
	for _, ext := range []*v1beta1.ComponentExtensionSpec{componentExtension(member, v1beta1.EngineComponent), componentExtension(member, v1beta1.DecoderComponent), componentExtension(member, v1beta1.RouterComponent)} {
		if ext != nil && ext.MinReplicas == nil {
			ext.MinReplicas = ptr.To(1)
		}
	}
	objects := []client.Object{member, backendTestRuntime()}
	for _, ir := range components {
		ir.UID = types.UID(ir.Name + "-uid")
		ir.Labels = map[string]string{constants.InferenceServicePodLabelKey: member.Name}
		ir.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(member, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}
		role := v1beta1.EngineComponent
		if ir.Name == member.Name+"-decoder" {
			role = v1beta1.DecoderComponent
		}
		if ir.Name == member.Name+"-router" {
			role = v1beta1.RouterComponent
		}
		ir.Spec.Component = role
		if ir.Spec.ParentRef == nil {
			ir.Spec.ParentRef = &v1beta1.ParentReference{}
		}
		ir.Spec.ParentRef.Name = member.Name
		ir.Spec.Replicas = ptr.To(int32(len(ir.Status.InstanceStatuses)))
		ir.Spec.Runners = []v1beta1.Runner{{Name: "default", Size: 1}}
		for i := range ir.Status.InstanceStatuses {
			row := &ir.Status.InstanceStatuses[i]
			pod := resourceFixture().pods[0].DeepCopy()
			pod.Namespace = member.Namespace
			pod.Name = fmt.Sprintf("%s-%d", ir.Name, i)
			pod.UID = types.UID(pod.Name + "-uid")
			pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ir, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))}
			pod.Labels[constants.InferenceServicePodLabelKey] = member.Name
			pod.Labels[query.LabelInstanceIdx] = fmt.Sprint(i)
			row.RunningRevision = ir.Name + "-a"
			row.PodCount = 1
			if int32(i) < ir.Status.ReadyReplicas {
				row.Phase = v1beta1.OMENativeInstanceReady
				row.ServingPodCount = 1
			} else {
				pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
			}
			objects = append(objects, pod)
		}
		objects = append(objects, ir)
	}
	return objects
}

func componentExtension(member *v1beta1.InferenceService, role v1beta1.ComponentType) *v1beta1.ComponentExtensionSpec {
	switch role {
	case v1beta1.EngineComponent:
		if member.Spec.Engine != nil {
			return &member.Spec.Engine.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if member.Spec.Decoder != nil {
			return &member.Spec.Decoder.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if member.Spec.Router != nil {
			return &member.Spec.Router.ComponentExtensionSpec
		}
	}
	return nil
}
