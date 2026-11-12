package deployment

import (
	"context"

	"github.com/google/go-cmp/cmp"
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

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/render"
)

var log = logf.Log.WithName("DeploymentReconciler")

// DeploymentReconciler reconciles raw Kubernetes Deployment resources
type DeploymentReconciler struct {
	client       kclient.Client
	scheme       *runtime.Scheme
	Deployment   *appsv1.Deployment
	componentExt *v1beta1.ComponentExtensionSpec
	// ownedReplicas is the controller-authoritative replica count, non-nil
	// only when no autoscaler owns spec.replicas and the component declares
	// an explicit floor. nil means the live value is preserved.
	ownedReplicas *int32
}

func NewDeploymentReconciler(client kclient.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
	podSpec *corev1.PodSpec,
	resolvedAutoscaler *v1beta1.ComponentAutoscaler) *DeploymentReconciler {
	ownedReplicas := controllerOwnedReplicas(resolvedAutoscaler, componentExt)
	deployment := createRawDeployment(componentMeta, componentExt, podSpec)
	// A new Deployment starts at the declared minReplicas whatever the class: no
	// scaler has written a count yet. The update path decides ownership.
	deployment.Spec.Replicas = initialReplicas(componentExt)
	return &DeploymentReconciler{
		client:        client,
		scheme:        scheme,
		Deployment:    deployment,
		componentExt:  componentExt,
		ownedReplicas: ownedReplicas,
	}
}

// controllerOwnedReplicas returns the replica count the ISVC controller owns
// for this Deployment, or nil when the count must be preserved as-is.
//
// Ownership mirrors the OMENative IR projector: every autoscaler class except
// None has an external writer of spec.replicas (HPA and KEDA target the scale
// subresource; External is an operator-managed scaler), so the controller must
// not fight it. Only class None (nil resolves to None) leaves the controller
// as the sole writer, and even then only an explicit positive minReplicas is
// stamped — without a declared floor the live count stays untouched.
func controllerOwnedReplicas(resolvedAutoscaler *v1beta1.ComponentAutoscaler, componentExt *v1beta1.ComponentExtensionSpec) *int32 {
	if resolvedAutoscaler != nil && resolvedAutoscaler.Class != v1beta1.AutoscalerNone {
		return nil
	}
	return initialReplicas(componentExt)
}

// initialReplicas returns the replica count a new Deployment is created
// with: an explicit positive minReplicas, or nil to take the apiserver default.
func initialReplicas(componentExt *v1beta1.ComponentExtensionSpec) *int32 {
	if componentExt == nil || componentExt.MinReplicas == nil || *componentExt.MinReplicas <= 0 {
		return nil
	}
	replicas := int32(*componentExt.MinReplicas)
	return &replicas
}

func createRawDeployment(componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
	podSpec *corev1.PodSpec) *appsv1.Deployment {

	// Deep-copy the metadata so pod-template label stamping cannot leak into
	// the caller's maps or the Deployment's own object-level metadata.
	deploymentMeta := *componentMeta.DeepCopy()
	podMetadata := *componentMeta.DeepCopy()
	if podMetadata.Labels == nil {
		podMetadata.Labels = map[string]string{}
	}
	podMetadata.Labels[constants.RawDeploymentAppLabel] = constants.GetRawServiceLabel(componentMeta.Name)
	utils.SetPodLabelsFromAnnotations(&podMetadata)
	// The Deployment carries the component's object metadata whole; its pod
	// template takes only the annotations that belong on a pod, so a rewrite
	// of a service annotation leaves the template, and the ReplicaSet behind
	// it, alone.
	podMetadata.Annotations = render.PodTemplateAnnotations(podMetadata.Annotations, componentExt.Annotations)
	setDefaultPodSpec(podSpec)

	deployment := &appsv1.Deployment{
		ObjectMeta: deploymentMeta,
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					constants.RawDeploymentAppLabel: constants.GetRawServiceLabel(componentMeta.Name),
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

	// The policy layer's last-known-good record lives on the live
	// Deployment; the rebuilt desired object must carry it forward or any
	// spec-drift update would erase the freeze/recovery state exactly when
	// a broken policy needs it.
	if lkg, ok := existingDeployment.Annotations[constants.AutoscalerPolicyLastRendered]; ok {
		if r.Deployment.Annotations == nil {
			r.Deployment.Annotations = map[string]string{}
		}
		r.Deployment.Annotations[constants.AutoscalerPolicyLastRendered] = lkg
	}

	// Perform a dry-run update to populate default values
	if err := r.client.Update(context.TODO(), r.Deployment, kclient.DryRunAll); err != nil {
		log.Error(err, "Failed to perform dry-run update of deployment", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name)
		return constants.CheckResultUnknown, nil, err
	}

	var diffOpts []cmp.Option
	if r.ownedReplicas != nil {
		// Controller-owned count: re-assert the declared floor after dry-run
		// defaulting and keep Replicas in the diff so drift from the floor
		// triggers an update.
		r.Deployment.Spec.Replicas = r.ownedReplicas
	} else {
		// Scaler-owned count (or no declared floor): the live value is
		// authoritative — carry it into the target state and exclude the
		// field from the diff.
		diffOpts = append(diffOpts, cmpopts.IgnoreFields(appsv1.DeploymentSpec{}, "Replicas"))
		r.Deployment.Spec.Replicas = existingDeployment.Spec.Replicas
		if existingDeployment.Spec.Replicas != nil {
			log.V(1).Info("Preserving existing replicas in target state", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name, "replicas", *r.Deployment.Spec.Replicas)
		}
	}

	diff, err := kmp.SafeDiff(r.Deployment.Spec, existingDeployment.Spec, diffOpts...)
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
	log.V(1).Info("Reconciling deployment", "namespace", r.Deployment.Namespace, "name", r.Deployment.Name, "checkResult", checkResult.String())

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
