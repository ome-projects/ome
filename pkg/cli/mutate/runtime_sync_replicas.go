package mutate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

type RuntimeSyncReplicaEvidence struct {
	valid           bool
	uid, rv, digest string
	components      []string
}

func (RuntimeSyncReplicaEvidence) MarshalJSON() ([]byte, error) { return nil, ErrRuntime }
func (RuntimeSyncReplicaEvidence) MarshalYAML() (any, error)    { return nil, ErrRuntime }
func (RuntimeSyncReplicaEvidence) String() string {
	return "<mutate.RuntimeSyncReplicaEvidence redacted>"
}
func (RuntimeSyncReplicaEvidence) GoString() string {
	return "<mutate.RuntimeSyncReplicaEvidence redacted>"
}
func (e RuntimeSyncReplicaEvidence) SameSnapshot(other RuntimeSyncReplicaEvidence) bool {
	return e.valid && other.valid && e.uid == other.uid && e.rv == other.rv && e.digest == other.digest && slices.Equal(e.components, other.components)
}

func CollectRuntimeSyncReplicaEvidence(ctx context.Context, client omeclient.OmeV1beta1Interface, v *v1beta1.InferenceService, components []string, clock reportv1alpha1.Clock) (RuntimeSyncReplicaEvidence, error) {
	if err := ValidateTarget(v); err != nil {
		return RuntimeSyncReplicaEvidence{}, err
	}
	if ctx == nil || client == nil || len(components) > 3 {
		return RuntimeSyncReplicaEvidence{}, ErrRuntime
	}
	if err := ctx.Err(); err != nil {
		return RuntimeSyncReplicaEvidence{}, err
	}
	for i, component := range components {
		if component != "engine" && component != "decoder" && component != "router" || slices.Contains(components[:i], component) {
			return RuntimeSyncReplicaEvidence{}, ErrRuntime
		}
	}
	result := RuntimeSyncReplicaEvidence{valid: true, uid: string(v.UID), rv: v.ResourceVersion, components: append([]string{}, components...)}
	if len(components) == 0 {
		return result, nil
	}
	// Use the action collector's one bounded raw fetch and normalized private
	// copy for every related component. Keep raw copies only for CAS digests.
	var replicas []v1beta1.InferenceReplica
	work, err := collectReplicaEvidence(ctx, client, v, []string{"engine", "decoder", "router"}, clock, nil, &replicas)
	if err != nil {
		return RuntimeSyncReplicaEvidence{}, err
	}
	if work.active || work.transient || work.operations > 0 || work.migrations > 0 {
		return RuntimeSyncReplicaEvidence{}, ErrStale
	}
	reads := map[string]string{}
	seen := map[v1beta1.ComponentType]bool{}
	for i := range replicas {
		ir := &replicas[i]
		seen[ir.Spec.Component] = true
		if ir.Spec.Pacing != nil && ir.Spec.Pacing.RollbackToRevision != nil && *ir.Spec.Pacing.RollbackToRevision != "" || len(ir.Status.RetryBlocks) != 0 {
			return RuntimeSyncReplicaEvidence{}, ErrStale
		}
		if _, present := ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey]; present {
			return RuntimeSyncReplicaEvidence{}, ErrPending
		}
		raw, e := json.Marshal(ir)
		if e != nil {
			return RuntimeSyncReplicaEvidence{}, ErrBounds
		}
		reads[ir.Namespace+"/"+ir.Name] = fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	for _, component := range components {
		if !seen[v1beta1.ComponentType(component)] {
			return RuntimeSyncReplicaEvidence{}, ErrStale
		}
	}
	raw, err := json.Marshal(reads)
	if err != nil {
		return RuntimeSyncReplicaEvidence{}, ErrBounds
	}
	result.digest = fmt.Sprintf("%x", sha256.Sum256(raw))
	if err := ctx.Err(); err != nil {
		return RuntimeSyncReplicaEvidence{}, err
	}
	return result, nil
}
