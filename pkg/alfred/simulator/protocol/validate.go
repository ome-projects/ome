package protocol

import (
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

var strictCodecs = func() serializer.CodecFactory {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := schedulingv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return serializer.NewCodecFactory(scheme, serializer.EnableStrict)
}()

// Validate checks the immutable request and returns a private, deep-copied
// snapshot containing only supported scheduling objects.
func Validate(request Request) (*Snapshot, error) {
	if err := validateEnvelope(request); err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		Objects:    make([]runtime.Object, 0, len(request.ClusterObjects)),
		Nodes:      make(map[string]*corev1.Node),
		Pods:       make(map[types.NamespacedName]*corev1.Pod),
		Namespaces: make(map[string]*corev1.Namespace),
		PodGroups:  make(map[types.NamespacedName]*unstructured.Unstructured),
	}
	for i := range request.ClusterObjects {
		object, err := strictObject(request.ClusterObjects[i])
		if err != nil {
			return nil, fmt.Errorf("clusterObjects[%d]: %w", i, err)
		}
		if err := addObject(snapshot, object); err != nil {
			return nil, fmt.Errorf("clusterObjects[%d]: %w", i, err)
		}
	}
	if len(snapshot.Nodes) == 0 {
		return nil, fmt.Errorf("simulation snapshot has no nodes")
	}

	if err := validateSnapshotReferences(request, snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func validateEnvelope(request Request) error {
	if request.SchemaVersion != SchemaVersion {
		return fmt.Errorf("simulation request schema version %q is unsupported", request.SchemaVersion)
	}
	if err := validIdentifier("request ID", request.RequestID); err != nil {
		return err
	}
	if err := validIdentifier("snapshot ID", request.SnapshotID); err != nil {
		return err
	}
	if request.SnapshotTime.IsZero() {
		return fmt.Errorf("snapshot time must not be zero")
	}
	if err := validSchedulerName(request.Profile.SchedulerName); err != nil {
		return fmt.Errorf("profile identity: %w", err)
	}
	for name, value := range map[string]string{
		"backend": request.Profile.Backend, "scheduler version": request.Profile.SchedulerVersion,
		"configuration ID": request.Profile.ConfigurationID,
	} {
		if err := validIdentifier(name, value); err != nil {
			return fmt.Errorf("profile identity: %w", err)
		}
	}
	if len(request.ReplacementPods) == 0 {
		return fmt.Errorf("simulation request has no replacement pods")
	}
	if len(request.SourcePods) == 0 {
		return fmt.Errorf("simulation request has no source pods")
	}
	return nil
}

func strictObject(extension runtime.RawExtension) (runtime.Object, error) {
	if extension.Object != nil && len(extension.Raw) != 0 {
		return nil, fmt.Errorf("object has both Object and Raw representations")
	}
	if extension.Object == nil && len(extension.Raw) == 0 {
		return nil, fmt.Errorf("object is empty")
	}
	if len(extension.Raw) != 0 {
		object, _, err := strictCodecs.UniversalDeserializer().Decode(extension.Raw, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("strictly decode snapshot object: %w", err)
		}
		return supportedObject(object)
	}

	if _, ok := extension.Object.(*unstructured.Unstructured); ok {
		raw, err := json.Marshal(extension.Object)
		if err != nil {
			return nil, fmt.Errorf("encode unstructured object: %w", err)
		}
		object, _, err := strictCodecs.UniversalDeserializer().Decode(raw, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("strictly decode unstructured object: %w", err)
		}
		return supportedObject(object)
	}
	return supportedObject(extension.Object.DeepCopyObject())
}

func supportedObject(object runtime.Object) (runtime.Object, error) {
	var expected schema.GroupVersionKind
	switch object.(type) {
	case *corev1.Node:
		expected = schema.GroupVersionKind{Version: "v1", Kind: "Node"}
	case *corev1.Pod:
		expected = schema.GroupVersionKind{Version: "v1", Kind: "Pod"}
	case *corev1.Namespace:
		expected = schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}
	case *corev1.Service:
		expected = schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	case *corev1.ReplicationController:
		expected = schema.GroupVersionKind{Version: "v1", Kind: "ReplicationController"}
	case *appsv1.ReplicaSet:
		expected = appsv1.SchemeGroupVersion.WithKind("ReplicaSet")
	case *appsv1.StatefulSet:
		expected = appsv1.SchemeGroupVersion.WithKind("StatefulSet")
	case *schedulingv1alpha1.PodGroup:
		expected = schedulingv1alpha1.SchemeGroupVersion.WithKind("PodGroup")
	default:
		gvk := object.GetObjectKind().GroupVersionKind()
		return nil, fmt.Errorf("unsupported snapshot object %s", gvk.String())
	}
	gvk := object.GetObjectKind().GroupVersionKind()
	if !gvk.Empty() && (gvk.Group != expected.Group || gvk.Version != expected.Version || gvk.Kind != expected.Kind) {
		return nil, fmt.Errorf("object type %T has conflicting kind %s", object, gvk.String())
	}
	return object, nil
}

func addObject(snapshot *Snapshot, object runtime.Object) error {
	switch typed := object.(type) {
	case *corev1.Node:
		if err := validIdentifier("node name", typed.Name); err != nil {
			return err
		}
		if err := validateResourceList("Node capacity", typed.Status.Capacity); err != nil {
			return err
		}
		if err := validateResourceList("Node allocatable", typed.Status.Allocatable); err != nil {
			return err
		}
		if _, exists := snapshot.Nodes[typed.Name]; exists {
			return fmt.Errorf("duplicate Node %q", typed.Name)
		}
		snapshot.Nodes[typed.Name] = typed
	case *corev1.Namespace:
		if err := validIdentifier("namespace name", typed.Name); err != nil {
			return err
		}
		if _, exists := snapshot.Namespaces[typed.Name]; exists {
			return fmt.Errorf("duplicate Namespace %q", typed.Name)
		}
		snapshot.Namespaces[typed.Name] = typed
	case *corev1.Pod:
		key, err := podKey(typed)
		if err != nil {
			return err
		}
		if _, exists := snapshot.Pods[key]; exists {
			return fmt.Errorf("duplicate Pod %s", key.String())
		}
		if err := validateSupportedPod(typed); err != nil {
			return fmt.Errorf("Pod %s: %w", key.String(), err)
		}
		snapshot.Pods[key] = typed
	case *schedulingv1alpha1.PodGroup:
		if err := validIdentifier("PodGroup namespace", typed.Namespace); err != nil {
			return err
		}
		if err := validIdentifier("PodGroup name", typed.Name); err != nil {
			return err
		}
		if typed.DeletionTimestamp != nil {
			return fmt.Errorf("PodGroup %s/%s is deleting", typed.Namespace, typed.Name)
		}
		if typed.UID == "" {
			return fmt.Errorf("PodGroup %s/%s must have a UID", typed.Namespace, typed.Name)
		}
		if typed.Spec.MinMember < 1 {
			return fmt.Errorf("PodGroup %s/%s minMember must be positive", typed.Namespace, typed.Name)
		}
		if err := validateResourceList("PodGroup minResources", typed.Spec.MinResources); err != nil {
			return err
		}
		if typed.Spec.ScheduleTimeoutSeconds != nil && *typed.Spec.ScheduleTimeoutSeconds <= 0 {
			return fmt.Errorf("PodGroup %s/%s scheduleTimeoutSeconds must be positive", typed.Namespace, typed.Name)
		}
		key := types.NamespacedName{Namespace: typed.Namespace, Name: typed.Name}
		if _, exists := snapshot.PodGroups[key]; exists {
			return fmt.Errorf("duplicate PodGroup %s", key.String())
		}
		unstructuredObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed)
		if err != nil {
			return fmt.Errorf("convert PodGroup %s: %w", key.String(), err)
		}
		snapshot.PodGroups[key] = &unstructured.Unstructured{Object: unstructuredObject}
	case *corev1.Service:
		if err := addSelectorDependency(snapshot, "Service", typed.Namespace, typed.Name); err != nil {
			return err
		}
	case *corev1.ReplicationController:
		if err := addSelectorDependency(snapshot, "ReplicationController", typed.Namespace, typed.Name); err != nil {
			return err
		}
	case *appsv1.ReplicaSet:
		if err := addSelectorDependency(snapshot, "ReplicaSet", typed.Namespace, typed.Name); err != nil {
			return err
		}
	case *appsv1.StatefulSet:
		if err := addSelectorDependency(snapshot, "StatefulSet", typed.Namespace, typed.Name); err != nil {
			return err
		}
	}
	snapshot.Objects = append(snapshot.Objects, object)
	return nil
}

func addSelectorDependency(snapshot *Snapshot, kind, namespace, name string) error {
	if err := validIdentifier(kind+" namespace", namespace); err != nil {
		return err
	}
	if err := validIdentifier(kind+" name", name); err != nil {
		return err
	}
	for _, existing := range snapshot.Objects {
		existingKind, existingNamespace, existingName, ok := selectorDependencyIdentity(existing)
		if ok && existingKind == kind && existingNamespace == namespace && existingName == name {
			return fmt.Errorf("duplicate %s %s/%s", kind, namespace, name)
		}
	}
	return nil
}

func validateSnapshotReferences(request Request, snapshot *Snapshot) error {
	if from := request.MigrationFromNode; from != "" {
		if len(validation.IsDNS1123Subdomain(from)) != 0 {
			return fmt.Errorf("migrationFromNode must be a DNS1123 node name")
		}
		if len(request.ExcludedNodes) != 1 || request.ExcludedNodes[0] != from {
			return fmt.Errorf("migration request must exclude exactly migrationFromNode")
		}
		if node := snapshot.Nodes[from]; node == nil || node.DeletionTimestamp != nil {
			return fmt.Errorf("migrationFromNode must be a known live snapshot node")
		}
		hostsSource := false
		for i := range request.SourcePods {
			if request.SourcePods[i].Spec.NodeName == from {
				hostsSource = true
			}
		}
		if !hostsSource {
			return fmt.Errorf("migrationFromNode does not host a source Pod")
		}
		for i := range request.ReplacementPods {
			if !hasMigrationExclusion(&request.ReplacementPods[i], from) {
				return fmt.Errorf("replacementPods[%d] lacks required migration hostname exclusion in every affinity term", i)
			}
		}
	}
	excluded := make(map[string]struct{}, len(request.ExcludedNodes))
	for i, nodeName := range request.ExcludedNodes {
		if err := validIdentifier("excluded node", nodeName); err != nil {
			return fmt.Errorf("excludedNodes[%d]: %w", i, err)
		}
		if _, exists := snapshot.Nodes[nodeName]; !exists {
			return fmt.Errorf("excludedNodes[%d] names unknown node %q", i, nodeName)
		}
		excluded[nodeName] = struct{}{}
	}

	snapshotUIDs := make(map[types.UID]types.NamespacedName, len(snapshot.Pods))
	for key, pod := range snapshot.Pods {
		if pod.UID == "" {
			return fmt.Errorf("snapshot Pod %s must have a UID", key.String())
		}
		if previous, exists := snapshotUIDs[pod.UID]; exists {
			return fmt.Errorf("snapshot Pods %s and %s have duplicate Pod UID %q", previous.String(), key.String(), pod.UID)
		}
		snapshotUIDs[pod.UID] = key
		if _, exists := snapshot.Namespaces[key.Namespace]; !exists {
			return fmt.Errorf("snapshot Pod %s has no supplied Namespace", key.String())
		}
		if pod.Spec.NodeName != "" {
			if _, exists := snapshot.Nodes[pod.Spec.NodeName]; !exists {
				return fmt.Errorf("snapshot Pod %s is bound to unknown Node %q", key.String(), pod.Spec.NodeName)
			}
		}
	}
	for key := range snapshot.PodGroups {
		if _, exists := snapshot.Namespaces[key.Namespace]; !exists {
			return fmt.Errorf("snapshot PodGroup %s has no supplied Namespace", key.String())
		}
	}
	for _, object := range snapshot.Objects {
		kind, namespace, name, ok := selectorDependencyIdentity(object)
		if ok {
			if _, exists := snapshot.Namespaces[namespace]; !exists {
				return fmt.Errorf("snapshot %s %s/%s has no supplied Namespace", kind, namespace, name)
			}
		}
	}
	controllers := suppliedControllers(snapshot.Objects)

	replacementNames := make(map[types.NamespacedName]struct{}, len(request.ReplacementPods))
	replacementUIDs := make(map[types.UID]struct{}, len(request.ReplacementPods))
	for i := range request.ReplacementPods {
		pod := request.ReplacementPods[i].DeepCopy()
		key, err := podKey(pod)
		if err != nil {
			return fmt.Errorf("replacementPods[%d]: %w", i, err)
		}
		if _, duplicate := replacementNames[key]; duplicate {
			return fmt.Errorf("replacementPods[%d] has duplicate name %s", i, key.String())
		}
		replacementNames[key] = struct{}{}
		if pod.UID != "" {
			if _, duplicate := replacementUIDs[pod.UID]; duplicate {
				return fmt.Errorf("replacementPods[%d] has duplicate UID %q", i, pod.UID)
			}
			replacementUIDs[pod.UID] = struct{}{}
			if occupied, collision := snapshotUIDs[pod.UID]; collision {
				return fmt.Errorf("replacement Pod %s UID %q collides with snapshot Pod %s", key.String(), pod.UID, occupied.String())
			}
		}
		if _, exists := snapshot.Namespaces[pod.Namespace]; !exists {
			return fmt.Errorf("replacement Pod %s has no supplied Namespace", key.String())
		}
		if pod.Spec.NodeName != "" {
			return fmt.Errorf("replacement Pod %s is already bound to Node %q", key.String(), pod.Spec.NodeName)
		}
		if pod.DeletionTimestamp != nil {
			return fmt.Errorf("replacement Pod %s is deleting", key.String())
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			return fmt.Errorf("replacement Pod %s is terminal", key.String())
		}
		if effectiveSchedulerName(pod.Spec.SchedulerName) != request.Profile.SchedulerName {
			return fmt.Errorf("replacement Pod %s scheduler name %q does not match profile %q", key.String(), effectiveSchedulerName(pod.Spec.SchedulerName), request.Profile.SchedulerName)
		}
		if err := validateSupportedPod(pod); err != nil {
			return fmt.Errorf("replacement Pod %s: %w", key.String(), err)
		}
		if _, collision := snapshot.Pods[key]; collision {
			return fmt.Errorf("replacement Pod %s collides with snapshot Pod", key.String())
		}
		if err := validateControllerOwner(pod, controllers); err != nil {
			return fmt.Errorf("replacement Pod %s: %w", key.String(), err)
		}
	}

	sourceNames := make(map[types.NamespacedName]struct{}, len(request.SourcePods))
	sourceUIDs := make(map[types.UID]struct{}, len(request.SourcePods))
	for i := range request.SourcePods {
		pod := request.SourcePods[i].DeepCopy()
		key, err := podKey(pod)
		if err != nil {
			return fmt.Errorf("sourcePods[%d]: %w", i, err)
		}
		if pod.UID == "" {
			return fmt.Errorf("source Pod %s must have a UID", key.String())
		}
		if _, duplicate := sourceNames[key]; duplicate {
			return fmt.Errorf("sourcePods[%d] has duplicate name %s", i, key.String())
		}
		sourceNames[key] = struct{}{}
		if _, duplicate := sourceUIDs[pod.UID]; duplicate {
			return fmt.Errorf("sourcePods[%d] has duplicate UID %q", i, pod.UID)
		}
		sourceUIDs[pod.UID] = struct{}{}
		if _, collision := replacementNames[key]; collision {
			return fmt.Errorf("source Pod %s collides with replacement Pod", key.String())
		}
		if _, exists := snapshot.Namespaces[pod.Namespace]; !exists {
			return fmt.Errorf("source Pod %s has no supplied Namespace", key.String())
		}
		if pod.Spec.NodeName == "" {
			return fmt.Errorf("source Pod %s is not bound", key.String())
		}
		if _, exists := excluded[pod.Spec.NodeName]; !exists && request.MigrationFromNode == "" {
			return fmt.Errorf("source Node %q for Pod %s is not explicitly excluded", pod.Spec.NodeName, key.String())
		}
		if err := validateSupportedPod(pod); err != nil {
			return fmt.Errorf("source Pod %s: %w", key.String(), err)
		}
		occupied, exists := snapshot.Pods[key]
		if !exists {
			return fmt.Errorf("source Pod %s is missing from clusterObjects", key.String())
		}
		if !samePod(pod, occupied) {
			return fmt.Errorf("source Pod %s does not exactly match clusterObjects", key.String())
		}
	}
	return nil
}

func hasMigrationExclusion(pod *corev1.Pod, from string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return false
	}
	required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		return false
	}
	for _, term := range required.NodeSelectorTerms {
		found := false
		for _, requirement := range term.MatchExpressions {
			if requirement.Key == corev1.LabelHostname && requirement.Operator == corev1.NodeSelectorOpNotIn {
				for _, value := range requirement.Values {
					if value == from {
						found = true
					}
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func podKey(pod *corev1.Pod) (types.NamespacedName, error) {
	if err := validIdentifier("Pod namespace", pod.Namespace); err != nil {
		return types.NamespacedName{}, err
	}
	if err := validIdentifier("Pod name", pod.Name); err != nil {
		return types.NamespacedName{}, err
	}
	return types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, nil
}

func validateSupportedPod(pod *corev1.Pod) error {
	if pod.Status.NominatedNodeName != "" {
		return fmt.Errorf("nominated Node %q is unsupported", pod.Status.NominatedNodeName)
	}
	if len(pod.Spec.ResourceClaims) != 0 {
		return fmt.Errorf("resource claims are unsupported")
	}
	if pod.Spec.Resources != nil && len(pod.Spec.Resources.Claims) != 0 {
		return fmt.Errorf("pod-level resource claims are unsupported")
	}
	for i := range pod.Spec.InitContainers {
		if err := validateResourceRequirements("init container "+pod.Spec.InitContainers[i].Name, &pod.Spec.InitContainers[i].Resources); err != nil {
			return err
		}
		if len(pod.Spec.InitContainers[i].Resources.Claims) != 0 {
			return fmt.Errorf("init container %q resource claims are unsupported", pod.Spec.InitContainers[i].Name)
		}
	}
	for i := range pod.Spec.Containers {
		if err := validateResourceRequirements("container "+pod.Spec.Containers[i].Name, &pod.Spec.Containers[i].Resources); err != nil {
			return err
		}
		if len(pod.Spec.Containers[i].Resources.Claims) != 0 {
			return fmt.Errorf("container %q resource claims are unsupported", pod.Spec.Containers[i].Name)
		}
	}
	for i := range pod.Spec.EphemeralContainers {
		if err := validateResourceRequirements("ephemeral container "+pod.Spec.EphemeralContainers[i].Name, &pod.Spec.EphemeralContainers[i].Resources); err != nil {
			return err
		}
		if len(pod.Spec.EphemeralContainers[i].Resources.Claims) != 0 {
			return fmt.Errorf("ephemeral container %q resource claims are unsupported", pod.Spec.EphemeralContainers[i].Name)
		}
	}
	if len(pod.Status.ResourceClaimStatuses) != 0 || pod.Status.ExtendedResourceClaimStatus != nil {
		return fmt.Errorf("resource claim status is unsupported")
	}
	if err := validateResourceRequirements("pod-level resources", pod.Spec.Resources); err != nil {
		return err
	}
	if err := validateResourceList("pod overhead", pod.Spec.Overhead); err != nil {
		return err
	}
	if err := validateResourceRequirements("pod status resources", pod.Status.Resources); err != nil {
		return err
	}
	if err := validateResourceList("pod status allocated resources", pod.Status.AllocatedResources); err != nil {
		return err
	}
	for i := range pod.Status.ContainerStatuses {
		if err := validateContainerStatusResources("container status "+pod.Status.ContainerStatuses[i].Name, &pod.Status.ContainerStatuses[i]); err != nil {
			return err
		}
	}
	for i := range pod.Status.InitContainerStatuses {
		if err := validateContainerStatusResources("init container status "+pod.Status.InitContainerStatuses[i].Name, &pod.Status.InitContainerStatuses[i]); err != nil {
			return err
		}
	}
	for i := range pod.Status.EphemeralContainerStatuses {
		if err := validateContainerStatusResources("ephemeral container status "+pod.Status.EphemeralContainerStatuses[i].Name, &pod.Status.EphemeralContainerStatuses[i]); err != nil {
			return err
		}
	}
	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		switch {
		case volume.PersistentVolumeClaim != nil:
			return fmt.Errorf("volume %q uses an unsupported persistent volume claim", volume.Name)
		case volume.CSI != nil:
			return fmt.Errorf("volume %q uses an unsupported inline CSI source", volume.Name)
		case volume.Ephemeral != nil:
			return fmt.Errorf("volume %q uses an unsupported ephemeral volume", volume.Name)
		case volume.GCEPersistentDisk != nil || volume.AWSElasticBlockStore != nil || volume.NFS != nil ||
			volume.ISCSI != nil || volume.Glusterfs != nil || volume.RBD != nil ||
			volume.FlexVolume != nil || volume.Cinder != nil || volume.CephFS != nil ||
			volume.Flocker != nil || volume.FC != nil || volume.AzureFile != nil ||
			volume.VsphereVolume != nil || volume.Quobyte != nil || volume.AzureDisk != nil ||
			volume.PhotonPersistentDisk != nil || volume.PortworxVolume != nil ||
			volume.ScaleIO != nil || volume.StorageOS != nil:
			return fmt.Errorf("volume %q uses an unsupported storage driver", volume.Name)
		}
	}
	return nil
}

func validateContainerStatusResources(context string, status *corev1.ContainerStatus) error {
	if err := validateResourceRequirements(context+" resources", status.Resources); err != nil {
		return err
	}
	return validateResourceList(context+" allocated resources", status.AllocatedResources)
}

func validateResourceRequirements(context string, requirements *corev1.ResourceRequirements) error {
	if requirements == nil {
		return nil
	}
	if err := validateResourceList(context+" requests", requirements.Requests); err != nil {
		return err
	}
	return validateResourceList(context+" limits", requirements.Limits)
}

func validateResourceList(context string, resources corev1.ResourceList) error {
	for name, quantity := range resources {
		if quantity.Sign() < 0 {
			return fmt.Errorf("%s has negative quantity %s for resource %q", context, quantity.String(), name)
		}
	}
	return nil
}

type controllerIdentity struct {
	apiVersion string
	kind       string
	namespace  string
	name       string
}

func suppliedControllers(objects []runtime.Object) map[controllerIdentity]types.UID {
	controllers := make(map[controllerIdentity]types.UID)
	for _, object := range objects {
		kind, namespace, name, ok := selectorDependencyIdentity(object)
		if !ok || kind == "Service" {
			continue
		}
		var apiVersion string
		switch kind {
		case "ReplicationController":
			apiVersion = "v1"
		case "ReplicaSet", "StatefulSet":
			apiVersion = "apps/v1"
		}
		accessor, _ := object.(metav1.Object)
		controllers[controllerIdentity{apiVersion: apiVersion, kind: kind, namespace: namespace, name: name}] = accessor.GetUID()
	}
	return controllers
}

func validateControllerOwner(pod *corev1.Pod, controllers map[controllerIdentity]types.UID) error {
	owner := metav1.GetControllerOfNoCopy(pod)
	if owner == nil {
		return nil
	}
	identity := controllerIdentity{apiVersion: owner.APIVersion, kind: owner.Kind, namespace: pod.Namespace, name: owner.Name}
	switch {
	case owner.APIVersion == "v1" && owner.Kind == "ReplicationController":
	case owner.APIVersion == "apps/v1" && (owner.Kind == "ReplicaSet" || owner.Kind == "StatefulSet"):
	default:
		return nil
	}
	uid, exists := controllers[identity]
	if !exists {
		return fmt.Errorf("controller owner %s %s/%s was not supplied", owner.Kind, pod.Namespace, owner.Name)
	}
	if owner.UID == "" || uid == "" || uid != owner.UID {
		return fmt.Errorf("controller owner %s %s/%s UID %q does not match supplied UID %q", owner.Kind, pod.Namespace, owner.Name, owner.UID, uid)
	}
	return nil
}

func selectorDependencyIdentity(object runtime.Object) (kind, namespace, name string, ok bool) {
	switch typed := object.(type) {
	case *corev1.Service:
		return "Service", typed.Namespace, typed.Name, true
	case *corev1.ReplicationController:
		return "ReplicationController", typed.Namespace, typed.Name, true
	case *appsv1.ReplicaSet:
		return "ReplicaSet", typed.Namespace, typed.Name, true
	case *appsv1.StatefulSet:
		return "StatefulSet", typed.Namespace, typed.Name, true
	default:
		return "", "", "", false
	}
}

func samePod(expected, actual *corev1.Pod) bool {
	expectedCopy := expected.DeepCopy()
	actualCopy := actual.DeepCopy()
	expectedCopy.TypeMeta = metav1.TypeMeta{}
	actualCopy.TypeMeta = metav1.TypeMeta{}
	return apiequality.Semantic.DeepEqual(expectedCopy, actualCopy)
}

func effectiveSchedulerName(name string) string {
	if name == "" {
		return corev1.DefaultSchedulerName
	}
	return name
}

func validSchedulerName(name string) error {
	if err := validIdentifier("scheduler name", name); err != nil {
		return err
	}
	if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
		return fmt.Errorf("scheduler name must be a DNS subdomain: %s", strings.Join(problems, "; "))
	}
	return nil
}

func validIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be blank", name)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must not have surrounding whitespace", name)
	}
	return nil
}
