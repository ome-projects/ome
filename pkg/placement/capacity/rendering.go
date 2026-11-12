package capacity

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ComponentFingerprint identifies rendered pods before workload projection,
// with RuntimeClass defaults included. It excludes the component's minimum
// replica count. The reader must be direct; callers retain their wider
// dependency fences.
func ComponentFingerprint(ctx context.Context, reader client.Reader, component v1beta1.ComponentType, mode constants.DeploymentModeType, pods []PodSet) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(pods) == 0 || (component != v1beta1.EngineComponent && component != v1beta1.DecoderComponent) {
		return "", fmt.Errorf("rendering requires an engine or decoder pod set")
	}
	if mode != constants.OMENative && mode != constants.RawDeployment && mode != constants.MultiNode {
		return "", fmt.Errorf("deployment mode %q has no supported replica shape", mode)
	}
	pods, classes, err := renderedPodSets(ctx, reader, pods)
	if err != nil {
		return "", err
	}
	if err := runtimeClassesUnchanged(ctx, reader, classes); err != nil {
		return "", err
	}
	encoded, err := fingerprint(struct {
		Component v1beta1.ComponentType
		Mode      constants.DeploymentModeType
		Pods      []PodSet
		Classes   map[string]*nodev1.RuntimeClass
	}{component, mode, pods, classes})
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return encoded, nil
}

// renderedPodSets sorts the pod sets by name, requires each to be complete
// and uniquely named, and folds each pod's RuntimeClass into a copy of its
// spec. The classes read are returned by name; a class that changed between
// two pods is refused.
func renderedPodSets(ctx context.Context, reader client.Reader, pods []PodSet) ([]PodSet, map[string]*nodev1.RuntimeClass, error) {
	pods = slices.Clone(pods)
	slices.SortFunc(pods, func(a, b PodSet) int { return cmp.Compare(a.Name, b.Name) })
	classes := map[string]*nodev1.RuntimeClass{}
	for i := range pods {
		pod := &pods[i]
		if !completePodSet(*pod) || (i > 0 && pods[i-1].Name == pod.Name) {
			return nil, nil, fmt.Errorf("rendering requires unique named pod sets with a positive count and a complete spec")
		}
		pod.Spec = pod.Spec.DeepCopy()
		class, err := ApplyRuntimeClass(ctx, reader, pod.Spec)
		if err != nil {
			return nil, nil, err
		}
		if class == nil {
			continue
		}
		if previous, exists := classes[class.Name]; exists && !sameClassVersion(previous, class) {
			return nil, nil, fmt.Errorf("RuntimeClass %q changed while rendering the component", class.Name)
		}
		classes[class.Name] = class
	}
	return pods, classes, nil
}

// completePodSet reports whether a pod set has a name, a positive count and
// a spec with at least one container.
func completePodSet(pod PodSet) bool {
	return pod.Name != "" && pod.Count > 0 && pod.Spec != nil && len(pod.Spec.Containers) > 0
}

// sameClassVersion reports whether two reads of a RuntimeClass saw the same
// object at the same version.
func sameClassVersion(a, b *nodev1.RuntimeClass) bool {
	return a.UID == b.UID && a.ResourceVersion == b.ResourceVersion
}

// runtimeClassesUnchanged reads every class the pods used once more and
// refuses one that changed, was replaced or is being deleted since. The kept
// copy's metadata is cut down to its name and UID, so versions and
// timestamps stay out of the fingerprint.
func runtimeClassesUnchanged(ctx context.Context, reader client.Reader, classes map[string]*nodev1.RuntimeClass) error {
	for name, class := range classes {
		fresh := &nodev1.RuntimeClass{}
		if err := reader.Get(ctx, client.ObjectKey{Name: name}, fresh); err != nil {
			return err
		}
		if !sameClassVersion(fresh, class) || !fresh.DeletionTimestamp.IsZero() {
			return fmt.Errorf("RuntimeClass %q changed while rendering the component", name)
		}
		class.ObjectMeta = metav1.ObjectMeta{Name: class.Name, UID: class.UID}
		class.TypeMeta = metav1.TypeMeta{}
	}
	return nil
}
