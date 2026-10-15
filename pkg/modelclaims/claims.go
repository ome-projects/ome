// Package modelclaims coordinates endpoint use and eviction through conditional
// BaseModel annotation writes. It does not create workloads or delete files.
package modelclaims

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const residencyAnnotation = "ome.io/artifact-residency"

// ErrInUse means an endpoint holds a claim, so eviction must not proceed.
var ErrInUse = errors.New("model artifact has use claims")

// Coordinator updates one BaseModel at a time. Reader must bypass the informer
// cache (e.g. manager.GetAPIReader). Neither client is used for serving queries.
type Coordinator struct {
	reader client.Reader
	writer client.Writer
}

// NewCoordinator uses the supplied clients without starting watches or workers.
func NewCoordinator(reader client.Reader, writer client.Writer) *Coordinator {
	return &Coordinator{reader: reader, writer: writer}
}

// HasClaims reports endpoint protection. A missing annotation means no claims;
// a present but invalid value is an error, never evidence that eviction is safe.
func HasClaims(annotations map[string]string) (bool, error) {
	claims, err := parseClaims(annotations)
	return len(claims) != 0, err
}

func parseClaims(annotations map[string]string) (map[string]bool, error) {
	raw, exists := annotations[constants.ModelArtifactUseClaimsAnnotation]
	if !exists {
		return make(map[string]bool), nil
	}
	var claims map[string]bool
	if err := json.Unmarshal([]byte(raw), &claims); err != nil || claims == nil {
		return nil, fmt.Errorf("invalid %s: expected an object of claim IDs mapped to true", constants.ModelArtifactUseClaimsAnnotation)
	}
	for id, held := range claims {
		if strings.TrimSpace(id) == "" || !held {
			return nil, fmt.Errorf("invalid %s: each nonempty claim ID must map to true", constants.ModelArtifactUseClaimsAnnotation)
		}
	}
	return claims, nil
}

// Acquire merges the endpoint claim into a snapshot already validated by the
// preparation caller. The caller must confirm readiness and current residency
// on this exact CR version before calling Acquire. This method only patches the
// claim: it does not read, check readiness or retry. A conflict returns to the
// caller, which must read and validate again before deciding whether to retry.
func (c *Coordinator) Acquire(ctx context.Context, model *v1beta1.BaseModel, claimID string) error {
	if model == nil || model.Namespace == "" || model.Name == "" || model.UID == "" || model.ResourceVersion == "" {
		return errors.New("BaseModel snapshot with namespace, name, UID and resourceVersion is required")
	}
	if strings.TrimSpace(claimID) == "" {
		return errors.New("claim ID is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	claims, err := parseClaims(model.Annotations)
	if err != nil {
		return err
	}
	claims[claimID] = true
	annotations, err := claimAnnotation(claims)
	if err != nil {
		return err
	}
	return c.patchAnnotations(ctx, model, annotations)
}

// Release removes only this endpoint's claim. The caller must first confirm
// that its creation operations and all related workloads have ended. There is
// no timeout-based expiration. A restarted caller can repeat the same release.
func (c *Coordinator) Release(ctx context.Context, key types.NamespacedName, uid types.UID, claimID string) error {
	if strings.TrimSpace(claimID) == "" {
		return errors.New("claim ID is required")
	}
	return c.update(ctx, key, uid, func(model *v1beta1.BaseModel) (map[string]string, error) {
		claims, err := parseClaims(model.Annotations)
		if err != nil || !claims[claimID] {
			return nil, err
		}
		delete(claims, claimID)
		return claimAnnotation(claims)
	})
}

// Evict submits intent only when the current CR has no claims. Callers must
// already have evaluated serving references and every model sharing the artifact.
// This guards one CR; it neither makes a group atomic nor confirms file deletion.
// Both automated and manual requesters must use this conditional protocol.
func (c *Coordinator) Evict(ctx context.Context, key types.NamespacedName, uid types.UID) error {
	return c.update(ctx, key, uid, func(model *v1beta1.BaseModel) (map[string]string, error) {
		if !model.DeletionTimestamp.IsZero() {
			return nil, errors.New("BaseModel is deleting")
		}
		// The existing retention label asks the agent to preserve downloaded files
		// during model deletion or placement cleanup. Honor it here too, including
		// a reservation added after eviction planning.
		if strings.EqualFold(model.Labels[constants.ReserveModelArtifact], "true") {
			return nil, errors.New("model artifact is reserved")
		}
		if model.Spec.Storage != nil && model.Spec.Storage.StorageUri != nil && strings.HasPrefix(*model.Spec.Storage.StorageUri, "vendor://") {
			return nil, errors.New("vendor model cannot be evicted")
		}
		held, err := HasClaims(model.Annotations)
		if err != nil {
			return nil, err
		}
		if held {
			return nil, ErrInUse
		}
		if err := checkResidencyValue(model); err != nil {
			return nil, err
		}
		return map[string]string{residencyAnnotation: "Evicted"}, nil
	})
}

func checkResidencyValue(model *v1beta1.BaseModel) error {
	switch model.Annotations[residencyAnnotation] {
	case "", "Evicted":
		return nil
	default:
		return fmt.Errorf("unrecognized artifact residency %q", model.Annotations[residencyAnnotation])
	}
}

func claimAnnotation(claims map[string]bool) (map[string]string, error) {
	encoded, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}
	return map[string]string{constants.ModelArtifactUseClaimsAnnotation: string(encoded)}, nil
}

// Release and Evict repeat their reads and checks on conflict. Acquire instead
// returns conflicts so its caller can repeat preparation before another write.
func (c *Coordinator) update(ctx context.Context, key types.NamespacedName, uid types.UID, change func(*v1beta1.BaseModel) (map[string]string, error)) error {
	if key.Namespace == "" || key.Name == "" || uid == "" {
		return errors.New("BaseModel namespace, name and UID are required")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		model := &v1beta1.BaseModel{}
		if err := c.reader.Get(ctx, key, model); err != nil {
			return err
		}
		if model.UID != uid || model.ResourceVersion == "" {
			return fmt.Errorf("BaseModel %s UID changed or resourceVersion is missing", key)
		}
		annotations, err := change(model)
		if err != nil || len(annotations) == 0 {
			return err
		}
		return c.patchAnnotations(ctx, model, annotations)
	})
}

// Preserve unrelated fields and condition the write on the checked CR version.
// Patch a copy because clients decode the response into the supplied object.
func (c *Coordinator) patchAnnotations(ctx context.Context, model *v1beta1.BaseModel, annotations map[string]string) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"uid": model.UID, "resourceVersion": model.ResourceVersion,
			"annotations": annotations,
		},
	})
	if err != nil {
		return err
	}
	return c.writer.Patch(ctx, model.DeepCopy(), client.RawPatch(types.MergePatchType, patch))
}
