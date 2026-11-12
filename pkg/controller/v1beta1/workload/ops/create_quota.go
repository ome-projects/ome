package ops

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	resourcehelper "k8s.io/component-helpers/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// errSetShortOfQuota is the create path's own answer when the namespace's
// quotas leave room for part of a pod set and none of it is created.
var errSetShortOfQuota = errors.New("the quota leaves room for part of the pod set")

// quotaShortfall is one ResourceQuota that cannot admit a whole pod set:
// the quota, the resource it runs short on, the room it leaves and what
// the set's members need together.
type quotaShortfall struct {
	quota    string
	resource corev1.ResourceName
	room     resource.Quantity
	need     resource.Quantity
	members  int
}

func (s *quotaShortfall) String() string {
	return fmt.Sprintf("quota %s leaves room for %s %s, the %d members need %s",
		s.quota, s.room.String(), s.resource, s.members, s.need.String())
}

// quotaRoomForSet reads the namespace's ResourceQuotas and reports the
// first that cannot admit every pod of the set, or nil when each quota the
// reading can evaluate has room for all of them. An Instance's pods start
// together, so a member admitted ahead of a refused peer would hold quota
// for an Instance that cannot run; asking for the whole set first is what
// keeps a short gang from holding anything. The reading follows the
// apiserver's own accounting (the enforced hard limits against the usage
// it has charged) and leaves to admission whatever it cannot read: a
// manager without leave to list quotas, or a quota whose scopes it does
// not evaluate.
func quotaRoomForSet(ctx context.Context, reader client.Reader, namespace string, pods []*corev1.Pod) (*quotaShortfall, error) {
	quotas := &corev1.ResourceQuotaList{}
	if err := reader.List(ctx, quotas, client.InNamespace(namespace)); err != nil {
		if apierrors.IsForbidden(err) || meta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, err
	}
	for i := range quotas.Items {
		if short := quotaShortOfSet(&quotas.Items[i], pods); short != nil {
			return short, nil
		}
	}
	return nil, nil
}

// quotaShortOfSet reports the resource on which one quota cannot admit the
// pods it applies to, or nil. A quota whose enforced limits are not yet
// written, or whose scopes the reading cannot evaluate for a pod, admits
// here and answers at the create.
func quotaShortOfSet(quota *corev1.ResourceQuota, pods []*corev1.Pod) *quotaShortfall {
	if len(quota.Status.Hard) == 0 {
		return nil
	}
	need := corev1.ResourceList{}
	members := 0
	for _, pod := range pods {
		applies, known := quotaAppliesToPod(quota, pod)
		if !known {
			return nil
		}
		if !applies {
			continue
		}
		members++
		for name, quantity := range podQuotaUsage(pod) {
			sum := need[name]
			sum.Add(quantity)
			need[name] = sum
		}
	}
	for name, hard := range quota.Status.Hard {
		wanted, asked := need[name]
		if !asked || wanted.IsZero() {
			continue
		}
		room := hard.DeepCopy()
		if used, charged := quota.Status.Used[name]; charged {
			room.Sub(used)
		}
		if room.Sign() < 0 {
			room = resource.Quantity{}
		}
		if wanted.Cmp(room) > 0 {
			return &quotaShortfall{quota: quota.Name, resource: name, room: room, need: wanted, members: members}
		}
	}
	return nil
}

// podQuotaUsage is what admission charges a quota for one pod, in the
// names a ResourceQuota's hard limits use: one pod under both pod counts,
// every request under its requests.<name> key, the native compute
// resources also under their bare name, and their limits under
// limits.<name>. Requests and limits are summed the way the apiserver sums
// them, init containers and pod overhead included.
func podQuotaUsage(pod *corev1.Pod) corev1.ResourceList {
	one := resource.MustParse("1")
	usage := corev1.ResourceList{
		corev1.ResourcePods: one,
		"count/pods":        one,
	}
	options := resourcehelper.PodResourcesOptions{}
	for name, quantity := range resourcehelper.PodRequests(pod, options) {
		usage[corev1.ResourceName("requests."+string(name))] = quantity
		if nativeComputeResource(name) {
			usage[name] = quantity
		}
	}
	for name, quantity := range resourcehelper.PodLimits(pod, options) {
		if nativeComputeResource(name) {
			usage[corev1.ResourceName("limits."+string(name))] = quantity
		}
	}
	return usage
}

// nativeComputeResource reports the resources a quota may name bare and
// under a limits. prefix, as the apiserver's pod evaluator does.
func nativeComputeResource(name corev1.ResourceName) bool {
	switch name {
	case corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
		return true
	}
	return strings.HasPrefix(string(name), corev1.ResourceHugePagesPrefix)
}

// quotaAppliesToPod reports whether every scope of the quota matches the
// pod, and whether the reading could tell. A quota with no scope applies to
// every pod; a scope this reading does not evaluate leaves the quota to
// admission.
func quotaAppliesToPod(quota *corev1.ResourceQuota, pod *corev1.Pod) (applies, known bool) {
	for _, scope := range quota.Spec.Scopes {
		matches, evaluated := scopeMatchesPod(corev1.ScopedResourceSelectorRequirement{
			ScopeName: scope,
			Operator:  corev1.ScopeSelectorOpExists,
		}, pod)
		if !evaluated {
			return false, false
		}
		if !matches {
			return false, true
		}
	}
	if quota.Spec.ScopeSelector != nil {
		for _, requirement := range quota.Spec.ScopeSelector.MatchExpressions {
			matches, evaluated := scopeMatchesPod(requirement, pod)
			if !evaluated {
				return false, false
			}
			if !matches {
				return false, true
			}
		}
	}
	return true, true
}

// scopeMatchesPod evaluates one scope requirement against a pod the way
// admission does, for the scopes a pod's own spec answers: whether it has
// an active deadline, whether it is best-effort, which priority class it
// names and whether its affinity reaches across namespaces. Any other
// scope is not evaluated.
func scopeMatchesPod(requirement corev1.ScopedResourceSelectorRequirement, pod *corev1.Pod) (matches, evaluated bool) {
	switch requirement.ScopeName {
	case corev1.ResourceQuotaScopeTerminating:
		return pod.Spec.ActiveDeadlineSeconds != nil, true
	case corev1.ResourceQuotaScopeNotTerminating:
		return pod.Spec.ActiveDeadlineSeconds == nil, true
	case corev1.ResourceQuotaScopeBestEffort:
		return podIsBestEffort(pod), true
	case corev1.ResourceQuotaScopeNotBestEffort:
		return !podIsBestEffort(pod), true
	case corev1.ResourceQuotaScopeCrossNamespacePodAffinity:
		return podAffinityCrossesNamespaces(pod), true
	case corev1.ResourceQuotaScopePriorityClass:
		return priorityClassMatches(requirement, pod.Spec.PriorityClassName)
	}
	return false, false
}

// priorityClassMatches evaluates a PriorityClass scope requirement against
// the class a pod names.
func priorityClassMatches(requirement corev1.ScopedResourceSelectorRequirement, class string) (matches, evaluated bool) {
	switch requirement.Operator {
	case corev1.ScopeSelectorOpExists:
		return class != "", true
	case corev1.ScopeSelectorOpDoesNotExist:
		return class == "", true
	case corev1.ScopeSelectorOpIn:
		return class != "" && contains(requirement.Values, class), true
	case corev1.ScopeSelectorOpNotIn:
		return class != "" && !contains(requirement.Values, class), true
	}
	return false, false
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// podIsBestEffort is the apiserver's reading of a best-effort pod: no
// container, init container or pod-level resource names a positive cpu or
// memory request or limit.
func podIsBestEffort(pod *corev1.Pod) bool {
	lists := []corev1.ResourceList{}
	for _, container := range pod.Spec.InitContainers {
		lists = append(lists, container.Resources.Requests, container.Resources.Limits)
	}
	for _, container := range pod.Spec.Containers {
		lists = append(lists, container.Resources.Requests, container.Resources.Limits)
	}
	if pod.Spec.Resources != nil {
		lists = append(lists, pod.Spec.Resources.Requests, pod.Spec.Resources.Limits)
	}
	for _, list := range lists {
		for name, quantity := range list {
			if (name == corev1.ResourceCPU || name == corev1.ResourceMemory) && quantity.Sign() > 0 {
				return false
			}
		}
	}
	return true
}

// podAffinityCrossesNamespaces reports whether any pod affinity term of
// the pod selects pods beyond its own namespace.
func podAffinityCrossesNamespaces(pod *corev1.Pod) bool {
	if pod.Spec.Affinity == nil {
		return false
	}
	var terms []corev1.PodAffinityTerm
	if affinity := pod.Spec.Affinity.PodAffinity; affinity != nil {
		terms = append(terms, affinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		for _, weighted := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
			terms = append(terms, weighted.PodAffinityTerm)
		}
	}
	if antiAffinity := pod.Spec.Affinity.PodAntiAffinity; antiAffinity != nil {
		terms = append(terms, antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		for _, weighted := range antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			terms = append(terms, weighted.PodAffinityTerm)
		}
	}
	for _, term := range terms {
		if len(term.Namespaces) > 0 || term.NamespaceSelector != nil {
			return true
		}
	}
	return false
}

// requireRoomForSet answers for a set of more than one pod before any
// member is created: nil when every quota the reading can evaluate leaves
// room for all of it. A single pod is admission's to answer. Short, nothing
// is created, the refusal is recorded as a quota refusal is, and the wait
// names the whole set.
func (s *podSetCreate) requireRoomForSet(ctx context.Context, targets []podTarget, pods []*corev1.Pod) error {
	if len(pods) < 2 {
		return nil
	}
	short, err := quotaRoomForSet(ctx, s.deps.Reader(), s.input.Key.Namespace, pods)
	if err != nil {
		return fmt.Errorf("read quota room (instance=%d): %w", s.idx, err)
	}
	if short == nil {
		return nil
	}
	rejection := workload.APIRejection{
		Class:   workload.APIRejectionCapacityBlocked,
		Reason:  workload.RejectionReasonQuotaExceeded,
		Message: short.String(),
	}
	if err := s.recordSetRefusal(ctx, rejection.Message); err != nil {
		return fmt.Errorf("record quota refusal (instance=%d): %w", s.idx, err)
	}
	return &podRejectionError{podName: targets[0].Name, rejection: rejection, err: errSetShortOfQuota}
}

// answerRefusalOfPartialSet answers a quota refusal given after part of the
// set was admitted: the members this pass created are retired, so a gang
// that cannot start whole holds nothing, and the wait names the whole set
// and the members retired.
func (s *podSetCreate) answerRefusalOfPartialSet(ctx context.Context, t podTarget, rejection workload.APIRejection, err error) error {
	retired := len(s.members)
	if derr := s.retireMembers(ctx); derr != nil {
		return fmt.Errorf("retire the members of a set the quota admits in part (instance=%d): %w", s.idx, derr)
	}
	message := fmt.Sprintf("%s; the %d members admitted are retired", rejection.Message, retired)
	if rerr := s.recordSetRefusal(ctx, message); rerr != nil {
		return fmt.Errorf("record quota refusal (instance=%d, pod=%s): %w", s.idx, t.Name, rerr)
	}
	return &podRejectionError{podName: t.Name, rejection: rejection, err: err}
}

// recordSetRefusal records a quota refusal of the whole set on the row and,
// when the refusal becomes the row's reported wait (the hold pass decides
// that by precedence, so a row already queued behind the scheduler or a
// node that stopped reporting hears nothing new), names it with the set's
// size.
func (s *podSetCreate) recordSetRefusal(ctx context.Context, message string) error {
	recorded, incumbent, err := status.RecordCapacityRefusal(ctx, s.input, s.idx)
	if err != nil {
		return err
	}
	if recorded && holds.Enters(workload.RejectionReasonQuotaExceeded, incumbent) {
		workload.RecordNormal(s.deps.Recorder, workload.EventTarget(s.input), workload.EventReasonInstanceQuotaBlocked,
			"OMENative %s waiting on capacity for all %d members: %s", workload.InstanceKey(s.input.Key.Component, s.idx), s.size, message)
	}
	return nil
}

// retireMembers deletes the members this pass created before the quota
// refused the rest of their set. Each delete is expected before it is
// issued and given back when the apiserver refuses it; a member already
// gone is nothing to retire.
func (s *podSetCreate) retireMembers(ctx context.Context) error {
	key := s.input.Key
	for _, member := range s.members {
		s.deps.ExpectationsCache().ExpectDeletes(key.Namespace, key.OwnerName, key.Component, s.idx, 1)
		if err := s.deps.Client.Delete(ctx, member); err != nil {
			s.deps.ExpectationsCache().ObservedDelete(key.Namespace, key.OwnerName, key.Component, s.idx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("delete member %s: %w", member.Name, err)
		}
	}
	s.members = nil
	return nil
}
