package lws

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/kmp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

var log = ctrl.Log.WithName("LWSReconciler")

type LWSReconciler struct {
	client       client.Client
	scheme       *runtime.Scheme
	LWS          *lws.LeaderWorkerSet
	ComponentExt *v1beta1.ComponentExtensionSpec
}

func NewLWSReconciler(client client.Client,
	scheme *runtime.Scheme,
	headPod *corev1.PodSpec,
	workerPod *corev1.PodSpec,
	workerSize int32,
	componentExt *v1beta1.ComponentExtensionSpec,
	componentMeta metav1.ObjectMeta) *LWSReconciler {
	return &LWSReconciler{
		client:       client,
		scheme:       scheme,
		LWS:          createLWS(headPod, workerPod, workerSize, componentExt, componentMeta),
		ComponentExt: componentExt,
	}
}

func createLWS(headPod *corev1.PodSpec,
	workerPod *corev1.PodSpec,
	workerSize int32,
	componentExt *v1beta1.ComponentExtensionSpec,
	componentMeta metav1.ObjectMeta) *lws.LeaderWorkerSet {

	headPodMeta := componentMeta.DeepCopy()
	workerPodMeta := componentMeta.DeepCopy()
	lwsObjectMeta := componentMeta.DeepCopy()
	lwsObjectMeta.Name = constants.LWSName(componentMeta.Name)
	headPodMeta.Labels["app"] = constants.GetRawServiceLabel(componentMeta.Name)
	headPodMeta.Labels["ray.io/node-type"] = "head"
	utils.SetPodLabelsFromAnnotations(headPodMeta)
	utils.SetPodLabelsFromAnnotations(workerPodMeta)

	// Need to remove Prometheus annotations for workerPods as workerPods don't expose endpoints
	abandonedWorkerPodAnnotations := []string{
		constants.PrometheusPathAnnotationKey,
		constants.PrometheusPortAnnotationKey,
		constants.PrometheusScrapeAnnotationKey,
	}
	utils.RemovePodAnnotations(workerPodMeta, abandonedWorkerPodAnnotations)

	setDefaultPodSpec(headPod)
	setDefaultPodSpec(workerPod)
	replicas := int32(1)
	// LWS size is the number of workers plus one for the head, and the head is always present, so
	// increment the worker size by one to account for the head
	workerSize = workerSize + 1
	if componentExt.MinReplicas != nil {
		replicas = int32(*componentExt.MinReplicas)
	}
	maxSurge := int32(1)
	maxUnavailable := int32(1)
	SubdomainShared := lws.SubdomainShared
	leaderWorkerSet := &lws.LeaderWorkerSet{
		ObjectMeta: *lwsObjectMeta,
		Spec: lws.LeaderWorkerSetSpec{
			Replicas:      &replicas,
			StartupPolicy: lws.LeaderCreatedStartupPolicy,
			NetworkConfig: &lws.NetworkConfig{
				SubdomainPolicy: &SubdomainShared,
			},
			RolloutStrategy: lws.RolloutStrategy{
				Type: lws.RollingUpdateStrategyType,
				RollingUpdateConfiguration: &lws.RollingUpdateConfiguration{
					MaxUnavailable: intstr.IntOrString{Type: intstr.Int, IntVal: maxUnavailable},
					MaxSurge:       intstr.IntOrString{Type: intstr.Int, IntVal: maxSurge},
				},
			},
			LeaderWorkerTemplate: lws.LeaderWorkerTemplate{
				Size:          &workerSize,
				RestartPolicy: lws.RecreateGroupOnPodRestart,
				LeaderTemplate: &corev1.PodTemplateSpec{
					Spec:       *headPod,
					ObjectMeta: *headPodMeta,
				},
				WorkerTemplate: corev1.PodTemplateSpec{
					Spec:       *workerPod,
					ObjectMeta: *workerPodMeta,
				},
			},
		},
	}

	return leaderWorkerSet

}

func setDefaultPodSpec(podSpec *corev1.PodSpec) {
	if podSpec.DNSPolicy == "" {
		podSpec.DNSPolicy = corev1.DNSClusterFirst
	}
	if podSpec.RestartPolicy == "" {
		podSpec.RestartPolicy = corev1.RestartPolicyAlways
	}
	if podSpec.TerminationGracePeriodSeconds == nil {
		terminationGracePeriodSeconds := int64(corev1.DefaultTerminationGracePeriodSeconds)
		podSpec.TerminationGracePeriodSeconds = &terminationGracePeriodSeconds
	}
	if podSpec.SecurityContext == nil {
		podSpec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if podSpec.SchedulerName == "" {
		podSpec.SchedulerName = corev1.DefaultSchedulerName
	}
	setDefaultContainerSettings(podSpec)
}

func setDefaultContainerSettings(podSpec *corev1.PodSpec) {
	for i := range podSpec.Containers {
		container := &podSpec.Containers[i]
		if container.TerminationMessagePath == "" {
			container.TerminationMessagePath = "/dev/termination-log"
		}
		if len(container.Args) == 0 {
			container.Args = nil
		}
		if container.TerminationMessagePolicy == "" {
			container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
		if container.ImagePullPolicy == "" {
			container.ImagePullPolicy = corev1.PullIfNotPresent
		}
	}
}

func (r *LWSReconciler) Reconcile() (*lws.LeaderWorkerSet, error) {
	checkResult, existingLWS, err := r.checkLeaderWorkerSetExist()
	if err != nil {
		return nil, err
	}
	log.Info("Reconciling LWS", "namespace", r.LWS.Namespace, "name", r.LWS.Name, "checkResult", checkResult.String())
	var opErr error
	switch checkResult {
	case constants.CheckResultCreate:
		opErr = r.client.Create(context.TODO(), r.LWS)
	case constants.CheckResultUpdate:
		// Copy resourceVersion from existing to desired state
		r.LWS.ResourceVersion = existingLWS.ResourceVersion
		opErr = r.client.Update(context.TODO(), r.LWS)
	default:
		return existingLWS, nil
	}

	if opErr != nil {
		log.Error(opErr, "Failed to reconcile LWS", "namespace", r.LWS.Namespace, "name", r.LWS.Name)
		return nil, opErr
	}

	return r.LWS, nil
}

func (r *LWSReconciler) checkLeaderWorkerSetExist() (constants.CheckResultType, *lws.LeaderWorkerSet, error) {
	leaderWorkerSet := &lws.LeaderWorkerSet{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: r.LWS.Name, Namespace: r.LWS.Namespace}, leaderWorkerSet)
	if err != nil {
		if errors.IsNotFound(err) {
			return constants.CheckResultCreate, nil, nil
		}
		return constants.CheckResultUnknown, nil, err
	}

	diff, err := kmp.SafeDiff(r.LWS.Spec, leaderWorkerSet.Spec)
	if err != nil {
		return constants.CheckResultUnknown, nil, err
	}
	if diff != "" {
		log.Info("LWS diff", "namespace", r.LWS.Namespace, "name", r.LWS.Name, "diff", diff)
		return constants.CheckResultUpdate, leaderWorkerSet, nil
	}
	return constants.CheckResultExisted, leaderWorkerSet, nil
}
