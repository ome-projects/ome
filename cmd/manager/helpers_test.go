package main

import (
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	testingv1beta1 "sigs.k8s.io/ome/pkg/utils/testing/v1beta1"
)

// The fleet fixture: one InferenceService of single-pod engine Instances,
// its InferenceReplica, and the pods as the apiserver serves them after the
// kubelet, the scheduler and the manager have written to them.
const (
	fleetNamespace = "team-a"
	fleetService   = "example-model"
	fleetRuntime   = "example-runtime"
	fleetRevision  = fleetService + "-engine-2f32f6fe"
	fleetImage     = "registry.example.com/inference/engine:2026.05.17-7304a529"
	fleetImageID   = "registry.example.com/inference/engine@sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	fleetNodes     = 1000
	// fleetStatusInstanceBound is the row bound a large install configures for
	// the status decoder.
	fleetStatusInstanceBound = 20000
)

var fleetTime = metav1.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

// fleetUID is the UID of the object of a kind at index; the middle segment
// tells the kinds apart.
func fleetUID(kind string, index int) types.UID {
	segments := map[string]string{"pod": "0a0d", "repl": "0e01", "isvc": "0c05"}
	return types.UID(fmt.Sprintf("%08x-%s-4000-8000-%012x", index, segments[kind], index))
}

func fleetReplicaName() string {
	return fleetService + "-engine"
}

// fleetContainer is the engine container as a serving runtime declares
// it: the runtime's own environment, the variables the manager injects, two
// ports, accelerator requests and limits, three probes and the mounts of
// the pod's volumes.
func fleetContainer(index int) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "ENGINE_HOST_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "status.podIP"}}},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
		{Name: "COMPILER_PTXAS_PATH", Value: "/usr/local/cuda/bin/ptxas"},
		{Name: "FABRIC_MNNVL_ENABLE", Value: "1"},
		{Name: "FABRIC_CUMEM_ENABLE", Value: "1"},
		{Name: "FABRIC_IB_ADDR_FAMILY", Value: "AF_INET6"},
		{Name: "FABRIC_SOCKET_IFNAME", Value: "eth0"},
		{Name: "FABRIC_SOCKET_FAMILY", Value: "AF_INET6"},
		{Name: "ACCELERATOR_ENABLE_COREDUMP_ON_EXCEPTION", Value: "1"},
		{Name: "ACCELERATOR_COREDUMP_GENERATION_FLAGS", Value: "skip_global_memory,skip_shared_memory,skip_local_memory,skip_constbank_memory,skip_abort"},
		{Name: "ACCELERATOR_COREDUMP_FILE", Value: "/tmp/engine_crash_dump/core.%h.%p"},
		{Name: "OME_INFERENCESERVICE_NAME", Value: fleetService},
		{Name: "OME_COMPONENT", Value: string(v1beta1.EngineComponent)},
		{Name: "OME_COMPONENT_REPLICAS", Value: "1000"},
		{Name: "OME_INSTANCE_INDEX", Value: fmt.Sprint(index)},
		{Name: "OME_INSTANCE_POD_COUNT", Value: "1"},
		{Name: "OME_INSTANCE_POD_RANK", Value: "0"},
		{Name: "OME_INSTANCE_SUBDOMAIN", Value: query.HeadlessServiceName(fleetService, workloadtypes.ComponentEngine)},
		{Name: "OME_LEADER_ADDRESS", Value: fmt.Sprintf("%s.%s", query.PodName(fleetService, workloadtypes.ComponentEngine, int32(index), workloadtypes.RunnerDefault, 0), query.HeadlessServiceName(fleetService, workloadtypes.ComponentEngine))},
		{Name: "OME_RUNNER", Value: workloadtypes.RunnerDefault},
		{Name: "OME_RUNNER_INDEX", Value: "0"},
		{Name: "OME_RUNNER_SIZE", Value: "1"},
	}
	probe := func(failures, delay, period, timeout int32) *corev1.Probe {
		return &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt32(30000), Scheme: corev1.URISchemeHTTP}},
			FailureThreshold:    failures,
			InitialDelaySeconds: delay,
			PeriodSeconds:       period,
			TimeoutSeconds:      timeout,
			SuccessThreshold:    1,
		}
	}
	return corev1.Container{
		Name:  constants.MainContainerName,
		Image: fleetImage,
		Args:  []string{"python3", "-m", "engine.launch_server", "--model-path", "/mnt/models/example-model", "--tp-size", "4", "--port", "30000", "--host", "0.0.0.0", "--mem-fraction-static", "0.85"},
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: 30000, Protocol: corev1.ProtocolTCP},
			{Name: "bootstrap", ContainerPort: 8998, Protocol: corev1.ProtocolTCP},
		},
		Env:       env,
		Resources: fleetResources(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "dshm", MountPath: "/dev/shm"},
			{Name: "tmp-storage", MountPath: "/tmp"},
			{Name: "kube-api-access-x7k2p", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
		},
		StartupProbe:             probe(60, 0, 30, 30),
		ReadinessProbe:           probe(5, 120, 30, 10),
		LivenessProbe:            probe(5, 0, 60, 30),
		TerminationMessagePath:   "/dev/termination-log",
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		ImagePullPolicy:          corev1.PullIfNotPresent,
		SecurityContext: &corev1.SecurityContext{
			Privileged:   ptr.To(true),
			Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"IPC_LOCK", "SYS_PTRACE"}},
		},
	}
}

func fleetResources() corev1.ResourceRequirements {
	quantities := corev1.ResourceList{
		"nvidia.com/gpu":      resource.MustParse("4"),
		"rdma/ib":             resource.MustParse("4"),
		corev1.ResourceCPU:    resource.MustParse("32"),
		corev1.ResourceMemory: resource.MustParse("512Gi"),
	}
	return corev1.ResourceRequirements{Requests: quantities.DeepCopy(), Limits: quantities.DeepCopy()}
}

// fleetPodSpec is the pod spec the manager renders from the runtime, with
// the fields the apiserver's defaulting and the admission plugins add.
func fleetPodSpec(index int) corev1.PodSpec {
	return corev1.PodSpec{
		Volumes: []corev1.Volume{
			{Name: "dshm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr.To(resource.MustParse("940Gi"))}}},
			{Name: "tmp-storage", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "kube-api-access-x7k2p", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: ptr.To(int32(420)),
				Sources: []corev1.VolumeProjection{
					{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{ExpirationSeconds: ptr.To(int64(3607)), Path: "token"}},
					{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
					{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
				},
			}}},
		},
		Containers:                    []corev1.Container{fleetContainer(index)},
		RestartPolicy:                 corev1.RestartPolicyAlways,
		TerminationGracePeriodSeconds: ptr.To(int64(30)),
		DNSPolicy:                     corev1.DNSClusterFirst,
		NodeSelector:                  map[string]string{"kubernetes.io/arch": "arm64"},
		ServiceAccountName:            "default",
		DeprecatedServiceAccount:      "default",
		NodeName:                      fmt.Sprintf("node-%03d", index%fleetNodes),
		SecurityContext:               &corev1.PodSecurityContext{},
		Hostname:                      query.PodName(fleetService, workloadtypes.ComponentEngine, int32(index), workloadtypes.RunnerDefault, 0),
		Subdomain:                     query.HeadlessServiceName(fleetService, workloadtypes.ComponentEngine),
		SchedulerName:                 corev1.DefaultSchedulerName,
		Tolerations: []corev1.Toleration{
			{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
			{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(300))},
			{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(300))},
		},
		Priority:           ptr.To(int32(0)),
		EnableServiceLinks: ptr.To(true),
		PreemptionPolicy:   ptr.To(corev1.PreemptLowerPriority),
		ReadinessGates:     []corev1.PodReadinessGate{{ConditionType: query.ServingConditionType}},
	}
}

// fleetPodStatus is the status the kubelet reports for a serving pod, with
// the serving condition the manager writes.
func fleetPodStatus(index int) corev1.PodStatus {
	started := metav1.NewTime(fleetTime.Add(time.Duration(index) * time.Second))
	ready := metav1.NewTime(started.Add(3 * time.Minute))
	condition := func(conditionType corev1.PodConditionType, at metav1.Time) corev1.PodCondition {
		return corev1.PodCondition{Type: conditionType, Status: corev1.ConditionTrue, LastTransitionTime: at}
	}
	hostIP := fmt.Sprintf("10.%d.%d.%d", 128+index/65536%64, index/256%256, index%256)
	podIP := fmt.Sprintf("10.%d.%d.%d", 64+index/65536%64, index/256%256, index%256)
	return corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			condition(corev1.PodReadyToStartContainers, started),
			condition(corev1.PodInitialized, started),
			condition(corev1.PodReady, ready),
			condition(corev1.ContainersReady, ready),
			condition(corev1.PodScheduled, started),
			condition(query.ServingConditionType, ready),
		},
		HostIP:    hostIP,
		HostIPs:   []corev1.HostIP{{IP: hostIP}},
		PodIP:     podIP,
		PodIPs:    []corev1.PodIP{{IP: podIP}},
		StartTime: &started,
		QOSClass:  corev1.PodQOSBurstable,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:                 constants.MainContainerName,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: started}},
			LastTerminationState: corev1.ContainerState{},
			Ready:                true,
			RestartCount:         0,
			Image:                fleetImage,
			ImageID:              fleetImageID,
			ContainerID:          fmt.Sprintf("containerd://%064x", index),
			Started:              ptr.To(true),
			AllocatedResources:   fleetResources().Requests,
			Resources:            ptr.To(fleetResources()),
			VolumeMounts: []corev1.VolumeMountStatus{
				{Name: "dshm", MountPath: "/dev/shm", RecursiveReadOnly: ptr.To(corev1.RecursiveReadOnlyDisabled)},
				{Name: "tmp-storage", MountPath: "/tmp", RecursiveReadOnly: ptr.To(corev1.RecursiveReadOnlyDisabled)},
				{Name: "kube-api-access-x7k2p", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true, RecursiveReadOnly: ptr.To(corev1.RecursiveReadOnlyDisabled)},
			},
			User: &corev1.ContainerUser{Linux: &corev1.LinuxContainerUser{UID: 0, GID: 0, SupplementalGroups: []int64{0}}},
		}},
	}
}

// fleetPodAnnotations are the pod template annotations serving pods
// carry: metrics scraping, a network attachment and the default container.
func fleetPodAnnotations() map[string]string {
	return map[string]string{
		"prometheus.io/path":                      "/metrics",
		"prometheus.io/port":                      "30000",
		"prometheus.io/scrape":                    "true",
		"networking.example.com/attachments":      `[{"name":"rdma-0","namespace":"network-system"},{"name":"rdma-1","namespace":"network-system"},{"name":"rdma-2","namespace":"network-system"},{"name":"rdma-3","namespace":"network-system"}]`,
		"kubectl.kubernetes.io/default-container": constants.MainContainerName,
	}
}

// fleetPod is the pod of the engine Instance at index as the apiserver
// serves it: the labels and owner the manager writes, the spec after
// defaulting, the kubelet's status, and the field ownership records of the
// three writers.
func fleetPod(index int) *corev1.Pod {
	labels := testfixture.PodLabels(fleetService, workloadtypes.ComponentEngine, int32(index), workloadtypes.RunnerDefault, 1, 0)
	labels[query.LabelRevisionHash] = fleetRevision
	created := metav1.NewTime(fleetTime.Add(time.Duration(index) * time.Second))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              query.PodName(fleetService, workloadtypes.ComponentEngine, int32(index), workloadtypes.RunnerDefault, 0),
			Namespace:         fleetNamespace,
			UID:               fleetUID("pod", index),
			Generation:        1,
			CreationTimestamp: created,
			Labels:            labels,
			Annotations:       fleetPodAnnotations(),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         v1beta1.SchemeGroupVersion.String(),
				Kind:               "InferenceReplica",
				Name:               fleetReplicaName(),
				UID:                fleetUID("repl", 0),
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec:   fleetPodSpec(index),
		Status: fleetPodStatus(index),
	}
	pod.ManagedFields = managedFieldsFor(pod)
	return pod
}

// managedFieldsFor is the field ownership the apiserver records on a pod
// the manager created, the scheduler bound and the kubelet runs: each
// writer's tree, derived from the pod's own fields so it grows with them.
func managedFieldsFor(pod *corev1.Pod) []metav1.ManagedFieldsEntry {
	raw := map[string]any{}
	data, err := json.Marshal(pod)
	if err == nil {
		err = json.Unmarshal(data, &raw)
	}
	if err != nil {
		panic(err)
	}
	metadata, _ := raw["metadata"].(map[string]any)
	status, _ := raw["status"].(map[string]any)
	at := pod.CreationTimestamp
	entry := func(manager, subresource string, tree map[string]any) metav1.ManagedFieldsEntry {
		fields, err := json.Marshal(fieldsV1(tree))
		if err != nil {
			panic(err)
		}
		return metav1.ManagedFieldsEntry{
			Manager:     manager,
			Operation:   metav1.ManagedFieldsOperationUpdate,
			APIVersion:  "v1",
			Time:        &at,
			FieldsType:  "FieldsV1",
			FieldsV1:    &metav1.FieldsV1{Raw: fields},
			Subresource: subresource,
		}
	}
	var servingCondition []any
	if conditions, ok := status["conditions"].([]any); ok {
		for _, condition := range conditions {
			if m, ok := condition.(map[string]any); ok && m["type"] == string(query.ServingConditionType) {
				servingCondition = append(servingCondition, m)
			}
		}
	}
	return []metav1.ManagedFieldsEntry{
		entry("ome-manager", "", map[string]any{
			"metadata": pick(metadata, "labels", "annotations", "ownerReferences"),
			"spec":     raw["spec"],
		}),
		entry("kube-scheduler", "binding", map[string]any{"spec": map[string]any{"nodeName": ""}}),
		entry("kubelet", "status", map[string]any{
			"status": pick(status, "conditions", "containerStatuses", "hostIP", "hostIPs", "phase", "podIP", "podIPs", "startTime"),
		}),
		entry("ome-manager", "status", map[string]any{"status": map[string]any{"conditions": servingCondition}}),
	}
}

func pick(m map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, key := range keys {
		if v, ok := m[key]; ok {
			out[key] = v
		}
	}
	return out
}

// fieldsV1 renders a value as the apiserver's field tree: every object key
// becomes "f:<key>", a list of keyed members becomes one "k:{...}" entry per
// member with the "." entry the apiserver writes for a member, and every
// other value a leaf.
func fieldsV1(v any) map[string]any {
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, value := range v {
			out["f:"+key] = fieldsV1(value)
		}
		return out
	case []any:
		out := make(map[string]any, len(v))
		for _, item := range v {
			member, ok := item.(map[string]any)
			if !ok {
				return map[string]any{}
			}
			key := listMemberKey(member)
			if key == "" {
				return map[string]any{}
			}
			tree := fieldsV1(member)
			tree["."] = map[string]any{}
			out[key] = tree
		}
		return out
	default:
		return map[string]any{}
	}
}

// listMemberKey is the "k:{...}" key of a list member, from the first of
// the fields the API types key their lists by.
func listMemberKey(member map[string]any) string {
	for _, field := range []string{"name", "type", "mountPath", "containerPort", "topologyKey", "uid", "ip", "key", "path"} {
		if value, ok := member[field]; ok {
			data, err := json.Marshal(map[string]any{field: value})
			if err != nil {
				return ""
			}
			return "k:" + string(data)
		}
	}
	return ""
}

// fleetReplica is the InferenceReplica of the engine component with the
// given rows stored in the given form, as the apiserver serves it: the
// spec the service controller projects (one runner carrying the pod
// template) and the status the replica controller writes.
func fleetReplica(rows []v1beta1.OMENativeInstanceStatus, form irstatus.Encoding) *v1beta1.InferenceReplica {
	logical := irstatustest.LogicalStatus(rows)
	selection, err := irstatus.SelectCandidate(logical, fleetStatusInstanceBound)
	if err != nil {
		panic(err)
	}
	status := selection.DenseV1.Status
	if form == irstatus.EncodingColumnarV2 {
		if selection.ColumnarV2 == nil {
			panic("the rows are not eligible for the column form: " + string(selection.IneligibleReason))
		}
		status = selection.ColumnarV2.Status
	}
	count := int32(len(rows))
	status.ObservedGeneration = 1
	status.ReadyReplicas, status.ServingReplicas, status.AvailableReplicas = count, count, count
	status.UpdatedReplicas, status.UpdatedReadyReplicas = count, count
	status.UpdateRevision = fleetRevision
	status.Conditions = []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllInstancesReady", Message: fmt.Sprintf("%d of %d Instances ready", count, count), LastTransitionTime: fleetTime, ObservedGeneration: 1},
		{Type: "Progressing", Status: metav1.ConditionFalse, Reason: "Settled", Message: "the replica is at its spec", LastTransitionTime: fleetTime, ObservedGeneration: 1},
	}
	template := fleetPodSpec(0)
	template.NodeName, template.Hostname, template.Subdomain = "", "", ""
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:              fleetReplicaName(),
			Namespace:         fleetNamespace,
			UID:               fleetUID("repl", 0),
			Generation:        1,
			CreationTimestamp: fleetTime,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: fleetService,
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         v1beta1.SchemeGroupVersion.String(),
				Kind:               "InferenceService",
				Name:               fleetService,
				UID:                fleetUID("isvc", 0),
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef:       &v1beta1.ParentReference{Name: fleetService},
			Component:       v1beta1.EngineComponent,
			ModelRef:        &v1beta1.ModelRef{Name: fleetService},
			RuntimeRef:      &v1beta1.ServingRuntimeRef{Name: fleetRuntime},
			Replicas:        ptr.To(count),
			MinReadySeconds: 30,
			Runners: []v1beta1.Runner{{
				Name: v1beta1.RunnerNameDefault,
				Size: 1,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Annotations: fleetPodAnnotations()},
					Spec:       template,
				},
			}},
		},
		Status: *status,
	}
}

// fleetReplicaAt is the fleet replica under its own name and UID, so that a
// cache can hold several of the one shape at once.
func fleetReplicaAt(index int, rows []v1beta1.OMENativeInstanceStatus, form irstatus.Encoding) *v1beta1.InferenceReplica {
	replica := fleetReplica(rows, form)
	replica.Name = fmt.Sprintf("%s-%d", replica.Name, index)
	replica.UID = fleetUID("repl", index)
	return replica
}

// fleetInferenceService is the parent service of the fleet: the engine
// component at the given count, its model and runtime, and the status the
// service controller publishes at rest.
func fleetInferenceService(count int) *v1beta1.InferenceService {
	engine := &v1beta1.EngineSpec{
		PodSpec:                v1beta1.PodSpec{Volumes: fleetPodSpec(0).Volumes[:2], NodeSelector: map[string]string{"kubernetes.io/arch": "arm64"}, Tolerations: fleetPodSpec(0).Tolerations[:1]},
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(count), MaxReplicas: count},
	}
	isvc := testingv1beta1.MakeInferenceService(fleetService, fleetNamespace).
		Model(fleetService).
		Runtime(fleetRuntime).
		Engine(engine).
		DeploymentMode(constants.OMENative).
		Annotation("prometheus.io/scrape", "true").
		Obj()
	isvc.UID = fleetUID("isvc", 0)
	isvc.Generation = 1
	isvc.CreationTimestamp = fleetTime
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {},
	}
	isvc.Status.ModelStatus = v1beta1.ModelStatus{TransitionStatus: v1beta1.UpToDate}
	return isvc
}
