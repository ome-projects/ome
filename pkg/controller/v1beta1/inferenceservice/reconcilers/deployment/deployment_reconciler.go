package deployment

import (
	"context"

	"github.com/google/go-cmp/cmp/cmpopts"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/kmp"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

var log = logf.Log.WithName("DeploymentReconciler")

// DeploymentReconciler reconciles raw Kubernetes Deployment resources
type DeploymentReconciler struct {
	client       kclient.Client
	scheme       *runtime.Scheme
	Deployment   *appsv1.Deployment
	componentExt *v1beta1.ComponentExtensionSpec
}

func NewDeploymentReconciler(client kclient.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
	podSpec *corev1.PodSpec) *DeploymentReconciler {
	return &DeploymentReconciler{
		client:       client,
		scheme:       scheme,
		Deployment:   createRawDeployment(componentMeta, componentExt, podSpec),
		componentExt: componentExt,
	}
}

func createRawDeployment(componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
	podSpec *corev1.PodSpec) *appsv1.Deployment {

	podMetadata := componentMeta
	podMetadata.Labels["app"] = constants.TruncateNameWithMaxLength(componentMeta.Name, 63)
	utils.SetPodLabelsFromAnnotations(&podMetadata)
	setDefaultPodSpec(podSpec)

	deployment := &appsv1.Deployment{
		ObjectMeta: componentMeta,
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": constants.TruncateNameWithMaxLength(componentMeta.Name, 63),
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: podMetadata,
				Spec:       *podSpec,
			},
		},
	}

	if componentExt.DeploymentStrategy != nil {
		deployment.Spec.Strategy = *componentExt.DeploymentStrategy
	}

	setDefaultDeploymentSpec(&deployment.Spec)

	return deployment
}

func (r *DeploymentReconciler) checkDeploymentExist() (constants.CheckResultType, *appsv1.Deployment, error) {
	existingDeployment := &appsv1.Deployment{}
	err := r.client.Get(context.TODO(), types.NamespacedName{
		Namespace: r.Deployment.ObjectMeta.Namespace,
		Name:      r.Deployment.ObjectMeta.Name,
	}, existingDeployment)
	if err != nil {
		if apierr.IsNotFound(err) {
			return constants.CheckResultCreate, nil, nil
		}
		return constants.CheckResultUnknown, nil, err
	}

	// Ignore fields related to HPA scaling
	ignoreFields := cmpopts.IgnoreFields(appsv1.DeploymentSpec{}, "Replicas")

	// Perform a dry-run update to populate default values
	if err := r.client.Update(context.TODO(), r.Deployment, kclient.DryRunAll); err != nil {
		log.Error(err, "Failed to perform dry-run update of deployment", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name)
		return constants.CheckResultUnknown, nil, err
	}

	if existingDeployment.Spec.Replicas != nil {
		r.Deployment.Spec.Replicas = existingDeployment.Spec.Replicas
		log.V(1).Info("Preserving existing replicas in target state", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name, "replicas", *r.Deployment.Spec.Replicas)
	}

	diff, err := kmp.SafeDiff(r.Deployment.Spec, existingDeployment.Spec, ignoreFields)
	if err != nil {
		return constants.CheckResultUnknown, nil, err
	}
	if diff != "" {
		log.Info("Deployments differ", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name, "diff", diff)
		return constants.CheckResultUpdate, existingDeployment, nil
	}
	return constants.CheckResultExisted, existingDeployment, nil
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
		if container.TerminationMessagePolicy == "" {
			container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
		if container.ImagePullPolicy == "" {
			container.ImagePullPolicy = corev1.PullIfNotPresent
		}
		setDefaultReadinessProbe(container)
	}
}

func setDefaultReadinessProbe(container *corev1.Container) {
	if container.Name == constants.MainContainerName {
		if container.ReadinessProbe == nil {
			port := int32(8080)
			if len(container.Ports) > 0 {
				port = container.Ports[0].ContainerPort
			}
			container.ReadinessProbe = &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.IntOrString{
							IntVal: port,
						},
					},
				},
				TimeoutSeconds:   1,
				PeriodSeconds:    10,
				SuccessThreshold: 1,
				FailureThreshold: 3,
			}
		}
	}
}

func setDefaultDeploymentSpec(spec *appsv1.DeploymentSpec) {
	if spec.Strategy.Type == "" {
		spec.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
	}
	if spec.Strategy.Type == appsv1.RollingUpdateDeploymentStrategyType && spec.Strategy.RollingUpdate == nil {
		spec.Strategy.RollingUpdate = &appsv1.RollingUpdateDeployment{
			MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 0},
			MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
		}
	}
	if spec.RevisionHistoryLimit == nil {
		revisionHistoryLimit := int32(10)
		spec.RevisionHistoryLimit = &revisionHistoryLimit
	}
	if spec.ProgressDeadlineSeconds == nil {
		progressDeadlineSeconds := int32(600)
		spec.ProgressDeadlineSeconds = &progressDeadlineSeconds
	}
}

func (r *DeploymentReconciler) Reconcile() (*appsv1.Deployment, error) {
	checkResult, deployment, err := r.checkDeploymentExist()
	if err != nil {
		return nil, err
	}
	log.Info("Reconciling deployment", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name, "checkResult", checkResult.String())

	var opErr error
	switch checkResult {
	case constants.CheckResultCreate:
		opErr = r.client.Create(context.TODO(), r.Deployment)
	case constants.CheckResultUpdate:
		opErr = r.client.Update(context.TODO(), r.Deployment)
	default:
		return deployment, nil
	}

	if opErr != nil {
		log.Error(opErr, "Failed to reconcile deployment", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name)
		return nil, opErr
	}

	return r.Deployment, nil
}
