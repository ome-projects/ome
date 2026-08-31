package main

import (
	"flag"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func resetFlags(t *testing.T, args []string) {
	t.Helper()
	oldArgs := os.Args
	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	})
	flag.CommandLine = flag.NewFlagSet(args[0], flag.ExitOnError)
	os.Args = args
}

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts.metricsAddr != ":8080" || opts.probeAddr != ":8081" {
		t.Fatalf("default addresses: %+v", opts)
	}
	if !opts.enableLeaderElection {
		t.Fatal("leader election must default on")
	}
	if opts.configMapName != "alfred-config" || opts.configMapKey != "config.yaml" {
		t.Fatalf("default config source: %+v", opts)
	}
	if opts.namespace == "" {
		t.Fatal("namespace must have a default")
	}
}

func TestGetOptionsDefaults(t *testing.T) {
	resetFlags(t, []string{"alfred"})
	opts := GetOptions()
	if opts.metricsAddr != ":8080" || opts.probeAddr != ":8081" {
		t.Fatalf("defaults not applied: %+v", opts)
	}
	if !opts.enableLeaderElection || opts.configMapName != "alfred-config" || opts.configMapKey != "config.yaml" {
		t.Fatalf("defaults not applied: %+v", opts)
	}
}

func TestGetOptionsCustom(t *testing.T) {
	resetFlags(t, []string{
		"alfred",
		"--metrics-bind-address=:9090",
		"--health-probe-bind-address=:9091",
		"--leader-elect=false",
		"--namespace=caretaker",
		"--config-name=my-config",
		"--config-key=alfred.yaml",
	})
	opts := GetOptions()
	if opts.metricsAddr != ":9090" || opts.probeAddr != ":9091" {
		t.Fatalf("addresses not parsed: %+v", opts)
	}
	if opts.enableLeaderElection {
		t.Fatal("--leader-elect=false not parsed")
	}
	if opts.namespace != "caretaker" || opts.configMapName != "my-config" || opts.configMapKey != "alfred.yaml" {
		t.Fatalf("config source not parsed: %+v", opts)
	}
}

func TestPodCacheTransform(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "p",
			Labels:      map[string]string{"keep": "me"},
			Annotations: map[string]string{"drop": "me"},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kubelet"},
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "noncontroller", UID: "ignored"},
				{
					APIVersion: "ome.io/v1beta1",
					Kind:       "InferenceReplica",
					Name:       "ir-0",
					UID:        "controller-uid",
					Controller: &controller,
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName:     "node1",
			NodeSelector: map[string]string{"pool": "h100"},
			Containers: []corev1.Container{{
				Name:    "main",
				Command: []string{"serve"},
				Env:     []corev1.EnvVar{{Name: "X", Value: "1"}},
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("2"),
				}},
				VolumeMounts: []corev1.VolumeMount{{Name: "v"}},
			}},
			InitContainers: []corev1.Container{{
				Name:          "sidecar",
				RestartPolicy: &always,
				Env:           []corev1.EnvVar{{Name: "Y", Value: "2"}},
			}},
			Volumes:     []corev1.Volume{{Name: "v"}},
			Tolerations: []corev1.Toleration{{Key: "gpu"}},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main"}},
		},
	}

	out, err := podCacheTransform(pod)
	if err != nil {
		t.Fatal(err)
	}
	got := out.(*corev1.Pod)

	// Stripped: everything the snapshot never reads.
	if got.Annotations != nil || got.ManagedFields != nil {
		t.Fatalf("metadata not stripped: %+v", got.ObjectMeta)
	}
	if len(got.OwnerReferences) != 1 {
		t.Fatalf("owner references = %+v, want only controller", got.OwnerReferences)
	}
	owner := got.OwnerReferences[0]
	if owner.APIVersion != "ome.io/v1beta1" || owner.Kind != "InferenceReplica" ||
		owner.Name != "ir-0" || owner.UID != "controller-uid" ||
		owner.Controller == nil || !*owner.Controller {
		t.Fatalf("controller owner not preserved exactly: %+v", owner)
	}
	if got.Spec.Volumes != nil || got.Spec.Tolerations != nil {
		t.Fatalf("spec not stripped: %+v", got.Spec)
	}
	if c := got.Spec.Containers[0]; c.Command != nil || c.Env != nil || c.VolumeMounts != nil {
		t.Fatalf("container not stripped: %+v", c)
	}
	if got.Spec.InitContainers[0].Env != nil {
		t.Fatalf("init container not stripped: %+v", got.Spec.InitContainers[0])
	}
	if got.Status.ContainerStatuses != nil {
		t.Fatalf("status not stripped: %+v", got.Status)
	}

	// Kept: everything the snapshot reads.
	if got.Labels["keep"] != "me" || got.Spec.NodeName != "node1" || got.Spec.NodeSelector["pool"] != "h100" {
		t.Fatalf("needed fields lost: %+v", got)
	}
	if got.Spec.Containers[0].Resources.Limits.Name("nvidia.com/gpu", resource.DecimalSI).Value() != 2 {
		t.Fatalf("resources lost: %+v", got.Spec.Containers[0].Resources)
	}
	if got.Spec.InitContainers[0].RestartPolicy == nil {
		t.Fatal("sidecar restart policy lost (GPU accounting needs it)")
	}
	if got.Status.Phase != corev1.PodRunning {
		t.Fatalf("phase lost: %+v", got.Status)
	}

	// Non-pod objects pass through untouched.
	cm := &corev1.ConfigMap{}
	if out, err := podCacheTransform(cm); err != nil || out != cm {
		t.Fatalf("non-pod transform: %v %v", out, err)
	}
}

func TestPodCacheTransformStripsNonControllerOwners(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "p",
		OwnerReferences: []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "observer", UID: "observer-uid"},
		},
	}}

	out, err := podCacheTransform(pod)
	if err != nil {
		t.Fatal(err)
	}
	if owners := out.(*corev1.Pod).OwnerReferences; len(owners) != 0 {
		t.Fatalf("non-controller owners retained: %+v", owners)
	}
}

func TestPodIdentity(t *testing.T) {
	t.Setenv("POD_NAME", "alfred-0")
	if got := podIdentity(); got != "alfred-0" {
		t.Fatalf("podIdentity = %q, want alfred-0", got)
	}
	t.Setenv("POD_NAME", "")
	if got := podIdentity(); got == "" {
		t.Fatal("podIdentity must fall back to hostname")
	}
}

func TestPtr(t *testing.T) {
	if v := ptr(42); *v != 42 {
		t.Fatalf("ptr: %v", *v)
	}
}
