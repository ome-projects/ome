package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func newDecoder(t *testing.T) admission.Decoder {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("v1beta1.AddToScheme: %v", err)
	}
	return admission.NewDecoder(s)
}

func encode(t *testing.T, obj *v1beta1.InferenceReplica) []byte {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// baselineIR returns a syntactically-valid projected InferenceReplica
// (InferenceService controller owner reference plus parentRef) suitable
// for cloning into Create / Update fixtures.
func baselineIR(annotations map[string]string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "llama-engine",
			Namespace:   "prod-models",
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: "llama", UID: "isvc-uid", Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{
				Name: "llama",
			},
			Component: v1beta1.EngineComponent,
			Runners: []v1beta1.Runner{
				{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "ome-container",
								Image: "sgl:1.0",
							}},
						},
					},
				},
			},
		},
	}
}

// withControllerWrite stamps the controller-write annotation, matching
// what the ISVC controller's client does on every Create/Update.
func withControllerWrite() map[string]string {
	return map[string]string{
		constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
	}
}

func createReq(t *testing.T, obj *v1beta1.InferenceReplica) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Namespace: obj.Namespace,
		Name:      obj.Name,
		UserInfo:  authenticationv1.UserInfo{Username: "alice"},
		Object:    runtime.RawExtension{Raw: encode(t, obj)},
	}}
}

func updateReq(t *testing.T, oldObj, newObj *v1beta1.InferenceReplica) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update,
		Namespace: newObj.Namespace,
		Name:      newObj.Name,
		UserInfo:  authenticationv1.UserInfo{Username: "alice"},
		OldObject: runtime.RawExtension{Raw: encode(t, oldObj)},
		Object:    runtime.RawExtension{Raw: encode(t, newObj)},
	}}
}

// withReplicas returns a clone of `ir` with Replicas set; convenience
// for the Update-path table cases that need a spec diff.
func withReplicas(ir *v1beta1.InferenceReplica, n int32) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Replicas = &n
	return out
}

// withComponent returns a clone with the Component slot changed.
func withComponent(ir *v1beta1.InferenceReplica, c v1beta1.ComponentType) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Component = c
	return out
}

// withParent returns a clone whose parentRef points at a different ISVC.
func withParent(ir *v1beta1.InferenceReplica, name string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.ParentRef.Name = name
	return out
}

// withFinalizers returns a clone with metadata.finalizers replaced.
func withFinalizers(ir *v1beta1.InferenceReplica, finalizers ...string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Finalizers = finalizers
	return out
}

// withLabel returns a clone with one label added.
func withLabel(ir *v1beta1.InferenceReplica, k, v string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	if out.Labels == nil {
		out.Labels = map[string]string{}
	}
	out.Labels[k] = v
	return out
}

// withOwnerRef returns a clone with one ownerReference added.
func withOwnerRef(ir *v1beta1.InferenceReplica, name string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.OwnerReferences = append(out.OwnerReferences, metav1.OwnerReference{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceService",
		Name:       name,
		UID:        "1234",
	})
	return out
}

// withPacingPartition returns a clone with spec.pacing.partition set
// (creating the Pacing block if nil).
func withPacingPartition(ir *v1beta1.InferenceReplica, partition int32) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	if out.Spec.Pacing == nil {
		out.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	out.Spec.Pacing.Partition = &partition
	return out
}

// withReplicasAndPacingPartition returns a clone with both spec.replicas
// and spec.pacing.partition set. Used by the over-Partition table cases.
func withReplicasAndPacingPartition(ir *v1beta1.InferenceReplica, replicas, partition int32) *v1beta1.InferenceReplica {
	out := withReplicas(ir, replicas)
	if out.Spec.Pacing == nil {
		out.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	out.Spec.Pacing.Partition = &partition
	return out
}

// withImage returns a clone whose first runner uses a different image.
func withImage(ir *v1beta1.InferenceReplica, image string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Runners[0].Template.Spec.Containers[0].Image = image
	return out
}

// withContainerMount returns a clone whose first runner's first
// container mounts `volName` at /mnt/<volName>. No matching volume is
// added, so callers exercising the resolve-success path must pair this
// with withVolume.
func withContainerMount(ir *v1beta1.InferenceReplica, volName string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	c := &out.Spec.Runners[0].Template.Spec.Containers[0]
	c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
		Name:      volName,
		MountPath: "/mnt/" + volName,
	})
	return out
}

// withInitContainerMount returns a clone whose first runner gains an
// initContainer that mounts `volName`. Used to prove initContainers are
// validated against the same declared-volume set as containers.
func withInitContainerMount(ir *v1beta1.InferenceReplica, volName string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	s := &out.Spec.Runners[0].Template.Spec
	s.InitContainers = append(s.InitContainers, corev1.Container{
		Name:  "model-init",
		Image: "init:1.0",
		VolumeMounts: []corev1.VolumeMount{{
			Name:      volName,
			MountPath: "/mnt/" + volName,
		}},
	})
	return out
}

// withVolume returns a clone declaring an emptyDir volume named
// `volName` on the first runner's pod spec.
func withVolume(ir *v1beta1.InferenceReplica, volName string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	s := &out.Spec.Runners[0].Template.Spec
	s.Volumes = append(s.Volumes, corev1.Volume{
		Name:         volName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	return out
}

// withModelPVCVolume returns a clone declaring a model-PVC-style volume
// (the shape UpdatePodSpecVolumes injects for a PVC-backed BaseModel)
// named `volName`. Used to prove a mount that resolves to an
// operator-injected volume is admitted.
func withModelPVCVolume(ir *v1beta1.InferenceReplica, volName string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	s := &out.Spec.Runners[0].Template.Spec
	s.Volumes = append(s.Volumes, corev1.Volume{
		Name: volName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "model-pvc",
				ReadOnly:  true,
			},
		},
	})
	return out
}

// withAutoscaler returns a clone with the Autoscaler block set on
// spec.autoscaler. Used by the IR-side webhook autoscaler shape tests.
func withAutoscaler(ir *v1beta1.InferenceReplica, as *v1beta1.ComponentAutoscaler) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Autoscaler = as
	return out
}

// withReplicasAndAutoscaler returns a clone with both spec.replicas
// and spec.autoscaler set. Used to test the KEDA
// IdleReplicaCount-vs-Replicas (IR-side floor) check.
func withReplicasAndAutoscaler(ir *v1beta1.InferenceReplica, n int32, as *v1beta1.ComponentAutoscaler) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Replicas = &n
	out.Spec.Autoscaler = as
	return out
}

// TestHandle covers every gate with no controller identity configured,
// where the controller-write annotation marks a controller write;
// baselineIR is a projected replica.
//
// Each row builds an admission request from `req(t)`, runs Handle, and
// asserts Allowed plus optional substring(s) the message must contain.
// Cases not built via createReq/updateReq use the inline `req` builder;
// that's how Delete and the malformed-body cases stay in the same table.
func TestHandle(t *testing.T) {
	wcw := withControllerWrite() // baseline annotation; "controller write"

	tests := []struct {
		name         string
		req          func(t *testing.T) admission.Request
		wantAllowed  bool
		wantContains []string // optional substring matches against resp.Result.Message
	}{
		{
			name:         "create without annotation → denied",
			req:          func(t *testing.T) admission.Request { return createReq(t, baselineIR(nil)) },
			wantAllowed:  false,
			wantContains: []string{"is projected by InferenceService prod-models/llama", "rejects a direct CREATE", constants.InferenceReplicaControllerWriteAnnotationKey, "edit the InferenceService instead"},
		},
		{
			name:        "create with empty annotation map → denied (same as nil)",
			req:         func(t *testing.T) admission.Request { return createReq(t, baselineIR(map[string]string{})) },
			wantAllowed: false,
		},
		{
			name: `create with annotation value other than literal "true" → denied`,
			req: func(t *testing.T) admission.Request {
				return createReq(t, baselineIR(map[string]string{
					constants.InferenceReplicaControllerWriteAnnotationKey: "yes",
				}))
			},
			wantAllowed: false,
		},
		{
			name:        "create with controller-write annotation → allowed",
			req:         func(t *testing.T) admission.Request { return createReq(t, baselineIR(wcw)) },
			wantAllowed: true,
		},
		{
			name: "update dropping annotation (metadata only) → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), baselineIR(nil))
			},
			wantAllowed: true,
		},
		{
			name: "update with spec change AND dropped annotation → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withReplicas(baselineIR(nil), 7))
			},
			wantAllowed: false,
		},
		{
			name: "update with spec change (controller write) → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withReplicas(baselineIR(wcw), 3))
			},
			wantAllowed: true,
		},
		{
			name: "update changing spec.parentRef → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withParent(baselineIR(wcw), "different-isvc"))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		{
			name: "update changing spec.component → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withComponent(baselineIR(wcw), v1beta1.DecoderComponent))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.component is immutable"},
		},
		{
			name: "update rerendering runners (controller write) → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withImage(baselineIR(wcw), "sgl:2.0"))
			},
			wantAllowed: true,
		},
		{
			name: "delete (not registered for this webhook) → allowed",
			req: func(t *testing.T) admission.Request {
				return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
					Operation: admissionv1.Delete,
					Namespace: "prod-models",
					Name:      "llama-engine",
				}}
			},
			wantAllowed: true,
		},
		{
			name: "malformed body → errored (Allowed=false)",
			req: func(t *testing.T) admission.Request {
				return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
					Operation: admissionv1.Create,
					Namespace: "prod-models",
					Name:      "llama-engine",
					Object:    runtime.RawExtension{Raw: []byte(`{not json`)},
				}}
			},
			wantAllowed: false,
		},
		// The spec.autoscaler shape check holds for every writer: any
		// admitted write must carry a shape-valid Autoscaler block.
		{
			name: "create with valid hpa autoscaler → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}))
			},
			wantAllowed: true,
		},
		{
			name: "create with valid keda autoscaler (1 trigger) → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerKEDA,
						Keda: &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{
							Type:     "prometheus",
							Metadata: map[string]string{"query": "x", "threshold": "1", "serverAddress": "http://p:9090"},
						}}},
					}))
			},
			wantAllowed: true,
		},
		{
			name: "create with keda + empty Triggers → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerKEDA,
						Keda:  &v1beta1.KedaAutoscaler{Triggers: nil},
					}))
			},
			wantAllowed:  false,
			wantContains: []string{"class=keda requires at least 1 trigger"},
		},
		{
			name: "create with hpa Type=Resource, Resource=nil → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerHPA,
						HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
							Type: autoscalingv2.ResourceMetricSourceType,
						}}},
					}))
			},
			wantAllowed:  false,
			wantContains: []string{"type=Resource but"},
		},
		{
			name: "create with keda IdleReplicaCount >= IR replicas floor → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndAutoscaler(baselineIR(wcw), 2,
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerKEDA,
						Keda: &v1beta1.KedaAutoscaler{
							Triggers: []kedav1.ScaleTriggers{{
								Type:     "prometheus",
								Metadata: map[string]string{"query": "x", "threshold": "1", "serverAddress": "http://p:9090"},
							}},
							IdleReplicaCount: ptr.To[int32](2),
						},
					}))
			},
			wantAllowed:  false,
			wantContains: []string{"keda.idleReplicaCount must be < minReplicas"},
		},
		{
			name: "create with bogus class value → denied (defense-in-depth)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerClass("knative")}))
			},
			wantAllowed:  false,
			wantContains: []string{"is not one of HPA|KEDA|External|None"},
		},
		{
			name: "create with valid external class → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}))
			},
			wantAllowed: true,
		},
		{
			name: "create with valid none class → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}))
			},
			wantAllowed: true,
		},
		// Immutability is decided before ownership: even a controller
		// write may not repoint spec.parentRef or move the replica
		// between Component slots, so for an annotated rewrite the
		// immutability error wins.
		{
			name: "update with annotation, same parentRef → allowed (baseline)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), baselineIR(wcw))
			},
			wantAllowed: true,
		},
		{
			name: "update with annotation, different parentRef.Name → denied (immutability)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withParent(baselineIR(wcw), "different-isvc"))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		{
			name: "update with annotation, different component → denied (immutability)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw),
					withComponent(baselineIR(wcw), v1beta1.DecoderComponent))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.component is immutable"},
		},
		{
			name: "update without annotation, same parentRef (metadata only) → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), baselineIR(nil))
			},
			wantAllowed: true,
		},
		{
			name: "update without annotation, different parentRef.Name → denied (immutability fires first)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw),
					withParent(baselineIR(nil), "different-isvc"))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		// Pacing.Partition must be <=
		// effective Replicas. Over-Partition silently freezes the
		// whole replica set forever (rollout engine treats Partition
		// as a per-index threshold).
		{
			name: "create with pacing nil → allowed (no partition to validate)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, baselineIR(wcw))
			},
			wantAllowed: true,
		},
		{
			name: "create with pacing.partition nil → allowed",
			req: func(t *testing.T) admission.Request {
				ir := baselineIR(wcw)
				ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
				return createReq(t, ir)
			},
			wantAllowed: true,
		},
		{
			name: "create with partition == replicas → allowed (boundary)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 3, 3))
			},
			wantAllowed: true,
		},
		{
			name: "create with partition < replicas → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 5, 2))
			},
			wantAllowed: true,
		},
		{
			name: "create with partition > replicas → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 3, 5))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.pacing.partition", "must be <= spec.replicas"},
		},
		{
			name: "create with partition=2 and replicas nil → denied (defaults to 1)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withPacingPartition(baselineIR(wcw), 2))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.pacing.partition (2)", "spec.replicas (1)"},
		},
		{
			name: "create with partition=0 (default canary) and replicas=5 → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 5, 0))
			},
			wantAllowed: true,
		},
		// volumeMount-resolves-to-a-volume gate. A rendered Runner pod
		// template whose container mounts an undeclared volume is accepted
		// by the apiserver here but REJECTED at pod-create time during
		// reconcile, surfacing only as a buried log error. The webhook
		// turns that silent failure into a clear admission rejection.
		{
			name: "create with container mount of undeclared volume → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withContainerMount(baselineIR(wcw), "dshm"))
			},
			wantAllowed: false,
			wantContains: []string{
				`runner "default"`,
				`container "ome-container"`,
				`volumeMount "dshm" has no matching volume`,
				"engineConfig.leader.volumes",
			},
		},
		{
			name: "create with container mount + matching volume → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withVolume(withContainerMount(baselineIR(wcw), "dshm"), "dshm"))
			},
			wantAllowed: true,
		},
		{
			name: "create with container mount of model-PVC-style injected volume → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withModelPVCVolume(withContainerMount(baselineIR(wcw), "llama-model"), "llama-model"))
			},
			wantAllowed: true,
		},
		{
			name: "create with initContainer mount of undeclared volume → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withInitContainerMount(baselineIR(wcw), "scratch"))
			},
			wantAllowed: false,
			wantContains: []string{
				`runner "default"`,
				`container "model-init"`,
				`volumeMount "scratch" has no matching volume`,
			},
		},
		{
			name: "create with initContainer mount + matching volume → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withVolume(withInitContainerMount(baselineIR(wcw), "scratch"), "scratch"))
			},
			wantAllowed: true,
		},
		{
			name: "create with no volumeMounts at all → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, baselineIR(wcw))
			},
			wantAllowed: true,
		},
		{
			name: "update rerendering runner with dangling mount → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withContainerMount(baselineIR(wcw), "dshm"))
			},
			wantAllowed: false,
			wantContains: []string{
				`volumeMount "dshm" has no matching volume`,
			},
		},
		{
			name: "create with leader/worker runners, worker mounts undeclared dshm → denied naming worker",
			req: func(t *testing.T) admission.Request {
				ir := baselineIR(wcw)
				// Multi-node shape: leader (declares dshm) + worker (mounts
				// dshm but forgets to declare it). The leader is fine; the
				// gate must flag the worker by name.
				ir.Spec.Runners = []v1beta1.Runner{
					{
						Name: v1beta1.RunnerNameLeader,
						Size: 1,
						Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:         "ome-container",
								Image:        "sgl:1.0",
								VolumeMounts: []corev1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
							}},
							Volumes: []corev1.Volume{{
								Name:         "dshm",
								VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
							}},
						}},
					},
					{
						Name: v1beta1.RunnerNameWorker,
						Size: 2,
						Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:         "ome-container",
								Image:        "sgl:1.0",
								VolumeMounts: []corev1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
							}},
							// Worker forgets to declare dshm.
						}},
					},
				}
				return createReq(t, ir)
			},
			wantAllowed: false,
			wantContains: []string{
				`runner "worker"`,
				`volumeMount "dshm" has no matching volume`,
			},
		},
		// Metadata-only updates: an update that keeps the spec and installs
		// no controller owner reference is admitted without the ownership
		// gates, so a finalizer change never wedges teardown behind this
		// fail-closed webhook. A finalizer change combined with a spec edit,
		// a parentRef change or a new controller owner reference still meets
		// the gates.
		{
			name: "finalizer add without annotation → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				old := baselineIR(nil)
				return updateReq(t, old, withFinalizers(old, "ome.io/ir-teardown"))
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer remove without annotation → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				old := withFinalizers(baselineIR(nil), "ome.io/ir-teardown")
				return updateReq(t, old, withFinalizers(old))
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer remove on terminating IR without annotation → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				old := withFinalizers(baselineIR(nil), "ome.io/ir-teardown")
				old.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
				old.DeletionGracePeriodSeconds = ptr.To[int64](0)
				return updateReq(t, old, withFinalizers(old))
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer change + apiserver-mutated fields (rv/generation/managedFields) → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				old := withFinalizers(baselineIR(nil), "ome.io/ir-teardown")
				old.ResourceVersion = "100"
				old.Generation = 3
				newObj := withFinalizers(old)
				newObj.ResourceVersion = "101"
				newObj.Generation = 4
				newObj.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "ome-manager"}}
				return updateReq(t, old, newObj)
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer change + spec change → denied",
			req: func(t *testing.T) admission.Request {
				old := baselineIR(nil)
				return updateReq(t, old,
					withReplicas(withFinalizers(old, "ome.io/ir-teardown"), 7))
			},
			wantAllowed:  false,
			wantContains: []string{"is projected by InferenceService prod-models/llama", "rejects a direct UPDATE"},
		},
		{
			name: "finalizer change + parentRef change → denied (immutability fires)",
			req: func(t *testing.T) admission.Request {
				old := baselineIR(nil)
				return updateReq(t, old,
					withParent(withFinalizers(old, "ome.io/ir-teardown"), "different-isvc"))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		{
			name: "finalizer change + label change → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				old := baselineIR(nil)
				return updateReq(t, old,
					withLabel(withFinalizers(old, "ome.io/ir-teardown"), "app", "rogue"))
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer change + annotation change → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				// Stripping the controller-write annotation alongside a
				// finalizer edit is a metadata-only update, which needs no
				// annotation.
				old := withFinalizers(baselineIR(wcw), "ome.io/ir-teardown")
				newObj := withFinalizers(baselineIR(nil), "ome.io/ir-teardown", "extra")
				return updateReq(t, old, newObj)
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer change + non-controller ownerRef added → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				old := baselineIR(nil)
				return updateReq(t, old,
					withOwnerRef(withFinalizers(old, "ome.io/ir-teardown"), "other-parent"))
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer change + controller ownerRef removed → allowed (removal is admitted)",
			req: func(t *testing.T) admission.Request {
				old := baselineIR(nil)
				newObj := withFinalizers(old, "ome.io/ir-teardown")
				newObj.OwnerReferences = nil
				return updateReq(t, old, newObj)
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "controller ownerRef blockOwnerDeletion cleared without annotation → allowed (metadata only)",
			req: func(t *testing.T) admission.Request {
				old := withBlockOwnerDeletion(baselineIR(nil), true)
				return updateReq(t, old, withBlockOwnerDeletion(old, false))
			},
			wantAllowed:  true,
			wantContains: []string{"metadata-only update"},
		},
		{
			name: "finalizer change + controller ownerRef repointed → denied",
			req: func(t *testing.T) admission.Request {
				old := baselineIR(nil)
				newObj := withFinalizers(old, "ome.io/ir-teardown")
				newObj.OwnerReferences[0].UID = "other-isvc-uid"
				return updateReq(t, old, newObj)
			},
			wantAllowed:  false,
			wantContains: []string{ownerRefDenial},
		},
		{
			name: "create with leader/worker runners, both declare dshm → allowed",
			req: func(t *testing.T) admission.Request {
				ir := baselineIR(wcw)
				mk := func(name v1beta1.RunnerName, size int32) v1beta1.Runner {
					return v1beta1.Runner{
						Name: name,
						Size: size,
						Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:         "ome-container",
								Image:        "sgl:1.0",
								VolumeMounts: []corev1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
							}},
							Volumes: []corev1.Volume{{
								Name:         "dshm",
								VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
							}},
						}},
					}
				}
				ir.Spec.Runners = []v1beta1.Runner{mk(v1beta1.RunnerNameLeader, 1), mk(v1beta1.RunnerNameWorker, 2)}
				return createReq(t, ir)
			},
			wantAllowed: true,
		},
		// A standalone replica renders from exactly one template source:
		// spec.runners, or spec.modelRef and/or spec.runtimeRef, with no
		// runtime pin. Without a Reader no runtime is resolved.
		{
			name:         "standalone create with runners and modelRef → denied",
			req:          func(t *testing.T) admission.Request { return createReq(t, withModelRef(standaloneIR(), "model-a")) },
			wantContains: []string{"spec.runners and spec.modelRef/spec.runtimeRef are exclusive"},
		},
		{
			name: "standalone create with runners and runtimeRef → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withRuntimeRef(standaloneIR(), refsIR(v1beta1.EngineComponent).Spec.RuntimeRef))
			},
			wantContains: []string{"spec.runners and spec.modelRef/spec.runtimeRef are exclusive"},
		},
		{
			name: "standalone create with modelRef alone → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withModelRef(withoutRunners(standaloneIR()), "model-a"))
			},
			wantAllowed: true,
		},
		{
			name:        "standalone create with runtimeRef alone → allowed",
			req:         func(t *testing.T) admission.Request { return createReq(t, refsIR(v1beta1.EngineComponent)) },
			wantAllowed: true,
		},
		{
			name: "standalone create with modelRef and runtimeRef → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withModelRef(refsIR(v1beta1.EngineComponent), "model-a"))
			},
			wantAllowed: true,
		},
		{
			name:         "standalone create with no template source → denied",
			req:          func(t *testing.T) admission.Request { return createReq(t, withoutRunners(standaloneIR())) },
			wantContains: []string{"spec.runners is required unless spec.modelRef or spec.runtimeRef is set"},
		},
		{
			name: "standalone create with runtimeRef.autoSync=false → denied",
			req: func(t *testing.T) admission.Request {
				ir := refsIR(v1beta1.EngineComponent)
				ir.Spec.RuntimeRef.AutoSync = ptr.To(false)
				return createReq(t, ir)
			},
			wantContains: []string{"spec.runtimeRef.autoSync=false and spec.runtimeRef.revision are not honored on an InferenceReplica"},
		},
		{
			name: "standalone create with runtimeRef.revision → denied",
			req: func(t *testing.T) admission.Request {
				ir := refsIR(v1beta1.EngineComponent)
				ir.Spec.RuntimeRef.Revision = ptr.To("runtime-a-1")
				return createReq(t, ir)
			},
			wantContains: []string{"not honored on an InferenceReplica"},
		},
		{
			name: "standalone update adding runners to a refs replica → denied",
			req: func(t *testing.T) admission.Request {
				refs := refsIR(v1beta1.EngineComponent)
				return updateReq(t, refs, withRuntimeRef(standaloneIR(), refs.Spec.RuntimeRef))
			},
			wantContains: []string{"exclusive"},
		},
		{
			name: "standalone update removing the last template source → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, standaloneIR(), withoutRunners(standaloneIR()))
			},
			wantContains: []string{"spec.runners is required unless spec.modelRef or spec.runtimeRef is set"},
		},
		{
			name: "standalone update replacing runners with refs → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, standaloneIR(), refsIR(v1beta1.EngineComponent))
			},
			wantAllowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			v := &Validator{Decoder: newDecoder(t)}
			resp := v.Handle(context.Background(), tc.req(t))
			g.Expect(resp.Allowed).To(gomega.Equal(tc.wantAllowed))
			for _, sub := range tc.wantContains {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(sub))
			}
		})
	}
}

const (
	sliceAccelerator   = "example.com/accelerator"
	sliceTopology      = "example.com/topology"
	sliceProvisioning  = "example.com/provisioning"
	sliceProvisionOnly = "on-demand"
	sliceChip          = "example.com/chip"
)

// sliceConfig provisions 2x2x1 and 2x2x2 slices of accelerator tpu-a, four
// chips to a host, on nodes labeled provisioning=on-demand.
func sliceConfig(t *testing.T) *controllerconfig.TPUSliceProvisioningConfig {
	t.Helper()
	cfg := &controllerconfig.TPUSliceProvisioningConfig{
		ChipResource: sliceChip,
		NodeLabels: controllerconfig.TPUSliceNodeLabels{
			Accelerator: sliceAccelerator,
			Topology:    sliceTopology,
			Slice:       "example.com/slice",
		},
		ProvisionOnly: controllerconfig.TPUSliceLabel{Key: sliceProvisioning, Value: sliceProvisionOnly},
		Accelerators: map[string]controllerconfig.TPUSliceAccelerator{
			"tpu-a": {SliceType: "type-a", ChipsPerHost: 4, Topologies: []string{"2x2x1", "2x2x2"}},
		},
		Slice: controllerconfig.TPUSliceObject{
			OwnerKindLabel: "example.com/owner-kind",
			OwnerNameLabel: "example.com/owner-name",
			Annotations:    map[string]string{"example.com/managed-by": "scheduler"},
			ReadyStates:    []string{"ACTIVE", "ACTIVE_DEGRADED"},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("slice config is invalid: %v", err)
	}
	return cfg
}

// sliceIR returns a controller-written IR whose pods select topology on
// tpu-a and request chips each: one default runner when workers is zero,
// otherwise a leader and that many workers. optedIn marks every template.
func sliceIR(optedIn bool, topology string, chips int64, workers int32) *v1beta1.InferenceReplica {
	var annotations map[string]string
	if optedIn {
		annotations = map[string]string{constants.TPUSliceProvisioningAnnotationKey: "true"}
	}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Annotations: annotations},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{sliceAccelerator: "tpu-a", sliceTopology: topology},
			Containers: []corev1.Container{{
				Name:  "ome-container",
				Image: "sgl:1.0",
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
					sliceChip: *resource.NewQuantity(chips, resource.DecimalSI),
				}},
			}},
		},
	}
	ir := baselineIR(withControllerWrite())
	if workers == 0 {
		ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: template}}
		return ir
	}
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: *template.DeepCopy()},
		{Name: v1beta1.RunnerNameWorker, Size: workers, Template: *template.DeepCopy()},
	}
	return ir
}

// withoutOptIn returns a clone whose runner at index i does not opt in.
func withoutOptIn(ir *v1beta1.InferenceReplica, i int) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	delete(out.Spec.Runners[i].Template.Annotations, constants.TPUSliceProvisioningAnnotationKey)
	return out
}

func sliceNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func TestHandleTPUSliceDemand(t *testing.T) {
	provisionOnly := sliceNode("provision-only", map[string]string{sliceAccelerator: "tpu-a", sliceProvisioning: sliceProvisionOnly})
	static := sliceNode("static", map[string]string{sliceAccelerator: "tpu-a", sliceTopology: "4x4x4"})
	failingList := interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
		return errors.New("boom")
	}}
	const invalid = "pods cannot be placed on a provisioned slice"

	tests := []struct {
		name         string
		unconfigured bool
		// nodes defaults to one provision-only node carrying tpu-a.
		nodes        []client.Object
		funcs        interceptor.Funcs
		req          func(t *testing.T) admission.Request
		wantAllowed  bool
		wantContains []string
	}{
		{
			name:        "create whose pod fills a single-host slice → allowed",
			req:         func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "2x2x1", 4, 0)) },
			wantAllowed: true,
		},
		{
			name:         "create whose pod leaves its slice's chips idle → denied",
			req:          func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "2x2x1", 2, 0)) },
			wantContains: []string{"prod-models/llama-engine", invalid, "topology 2x2x1"},
		},
		{
			name:        "create whose leader and worker fill a two-host slice → allowed",
			req:         func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "2x2x2", 4, 1)) },
			wantAllowed: true,
		},
		{
			name:         "create whose leader and two workers overfill a two-host slice → denied",
			req:          func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "2x2x2", 4, 2)) },
			wantContains: []string{invalid, "topology 2x2x2"},
		},
		{
			name:         "create selecting an unprovisionable topology → denied",
			req:          func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "4x4x4", 4, 15)) },
			wantContains: []string{invalid, "not provisionable"},
		},
		{
			name: "create whose default runner declares two pods → allowed",
			req: func(t *testing.T) admission.Request {
				// An Instance runs one pod from its default runner whatever
				// the runner's Size, and that pod's four chips fill 2x2x1.
				ir := sliceIR(true, "2x2x1", 4, 0)
				ir.Spec.Runners[0].Size = 2
				return createReq(t, ir)
			},
			wantAllowed: true,
		},
		{
			name:        "create that does not opt in → allowed",
			req:         func(t *testing.T) admission.Request { return createReq(t, sliceIR(false, "2x2x1", 2, 0)) },
			wantAllowed: true,
		},
		{
			name: "create whose worker alone opts in → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withoutOptIn(sliceIR(true, "2x2x2", 4, 2), 0))
			},
			wantAllowed: true,
		},
		{
			name:        "create on a static pool → allowed",
			nodes:       []client.Object{static},
			req:         func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "4x4x4", 4, 0)) },
			wantAllowed: true,
		},
		{
			name:         "create without slice provisioning → allowed",
			unconfigured: true,
			req:          func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "2x2x1", 2, 0)) },
			wantAllowed:  true,
		},
		{
			name:        "create whose nodes cannot be read → allowed",
			funcs:       failingList,
			req:         func(t *testing.T) admission.Request { return createReq(t, sliceIR(true, "2x2x1", 2, 0)) },
			wantAllowed: true,
		},
		{
			name: "update introducing the failure → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, sliceIR(true, "2x2x1", 4, 0), sliceIR(true, "2x2x1", 2, 0))
			},
			wantContains: []string{invalid},
		},
		{
			name: "update opting failing runners in → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, sliceIR(false, "2x2x1", 2, 0), sliceIR(true, "2x2x1", 2, 0))
			},
			wantContains: []string{invalid},
		},
		{
			name: "update keeping failing runners → allowed",
			req: func(t *testing.T) admission.Request {
				old := sliceIR(true, "2x2x1", 2, 0)
				return updateReq(t, old, withReplicas(old, 3))
			},
			wantAllowed: true,
		},
		{
			name: "update whose previous runners fail too → allowed",
			req: func(t *testing.T) admission.Request {
				old := sliceIR(true, "2x2x1", 2, 0)
				return updateReq(t, old, withImage(old, "sgl:2.0"))
			},
			wantAllowed: true,
		},
		{
			name: "update opting failing runners out → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, sliceIR(true, "2x2x1", 4, 0), sliceIR(false, "2x2x1", 2, 0))
			},
			wantAllowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			nodes := tc.nodes
			if nodes == nil {
				nodes = []client.Object{provisionOnly}
			}
			v := &Validator{
				Decoder:              newDecoder(t),
				TPUSliceProvisioning: sliceConfig(t),
				Nodes:                fake.NewClientBuilder().WithObjects(nodes...).WithInterceptorFuncs(tc.funcs).Build(),
			}
			if tc.unconfigured {
				v.TPUSliceProvisioning, v.Nodes = nil, nil
			}
			resp := v.Handle(context.Background(), tc.req(t))
			g.Expect(resp.Allowed).To(gomega.Equal(tc.wantAllowed), resp.Result.Message)
			for _, sub := range tc.wantContains {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(sub))
			}
		})
	}
}

// TestHandleTPUSliceDemandScaleReadsNoNodes pins that an update leaving
// the runners alone, such as a scale, reads no nodes.
func TestHandleTPUSliceDemandScaleReadsNoNodes(t *testing.T) {
	nodes := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			t.Error("the nodes were read for an update that keeps the runners")
			return nil
		},
	}).Build()
	v := &Validator{Decoder: newDecoder(t), TPUSliceProvisioning: sliceConfig(t), Nodes: nodes}
	old := sliceIR(true, "2x2x1", 4, 0)
	if resp := v.Handle(context.Background(), updateReq(t, old, withReplicas(old, 3))); !resp.Allowed {
		t.Fatalf("scale update denied: %s", resp.Result.Message)
	}
}
