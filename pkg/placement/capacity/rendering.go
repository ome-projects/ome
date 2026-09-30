package capacity

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ComponentFingerprint identifies rendered pods before workload projection,
// with RuntimeClass defaults included. It excludes the component replica floor.
// The reader must be direct; callers retain their wider dependency fences.
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
	pods = slices.Clone(pods)
	slices.SortFunc(pods, func(a, b PodSet) int { return cmp.Compare(a.Name, b.Name) })
	classes := map[string]*nodev1.RuntimeClass{}
	for i := range pods {
		pod := &pods[i]
		if pod.Name == "" || pod.Count <= 0 || pod.Spec == nil || len(pod.Spec.Containers) == 0 || (i > 0 && pods[i-1].Name == pod.Name) {
			return "", fmt.Errorf("rendering requires unique named pod sets with a positive count and a complete spec")
		}
		pod.Spec = pod.Spec.DeepCopy()
		class, err := ApplyRuntimeClass(ctx, reader, pod.Spec)
		if err != nil {
			return "", err
		}
		if class != nil {
			if previous, exists := classes[class.Name]; exists && (previous.UID != class.UID || previous.ResourceVersion != class.ResourceVersion) {
				return "", fmt.Errorf("RuntimeClass %q changed while rendering the component", class.Name)
			}
			classes[class.Name] = class
		}
	}
	for name, class := range classes {
		fresh := &nodev1.RuntimeClass{}
		if err := reader.Get(ctx, client.ObjectKey{Name: name}, fresh); err != nil {
			return "", err
		}
		if fresh.UID != class.UID || fresh.ResourceVersion != class.ResourceVersion || !fresh.DeletionTimestamp.IsZero() {
			return "", fmt.Errorf("RuntimeClass %q changed while rendering the component", name)
		}
		class.ObjectMeta = metav1.ObjectMeta{Name: class.Name, UID: class.UID}
		class.TypeMeta = metav1.TypeMeta{}
	}
	encoded, err := json.Marshal(struct {
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
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
