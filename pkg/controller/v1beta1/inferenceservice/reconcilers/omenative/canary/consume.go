package canary

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ConsumeAnnotations removes applied operator annotations with one metadata
// merge patch against a copy of the object, so the server response cannot
// clobber the caller's in-memory status. The keys are removed whether or not
// the caller's copy still shows them: the pass deleted them in-memory when it
// applied them, and a null for an absent key is a no-op on the server. A
// vanished object is not an error.
func ConsumeAnnotations(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, keys []string) error {
	if c == nil || len(keys) == 0 {
		return nil
	}
	seen := map[string]bool{}
	fields := make([]string, 0, len(keys))
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		fields = append(fields, fmt.Sprintf("%q:null", k))
	}
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%s}}}`, strings.Join(fields, ",")))
	if err := c.Patch(ctx, isvc.DeepCopy(), client.RawPatch(types.MergePatchType, patch)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("consume annotations %v: %w", keys, err)
	}
	return nil
}
