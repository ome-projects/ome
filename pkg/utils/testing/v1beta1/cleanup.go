package v1beta1testing

import (
	"context"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CleanupFunc unblocks deletion of one type's instances in a namespace,
// typically by removing finalizers from objects already being deleted.
// Implementations are idempotent and treat a missing namespace as clean.
type CleanupFunc func(ctx context.Context, c client.Client, namespace string) error

var (
	cleanupMu sync.Mutex
	cleanups  []CleanupFunc
)

// RegisterCleanup adds a namespace cleanup hook. Each builder file registers
// its type's hook from init(), so a new type needs no central edit. Hooks run
// in no guaranteed order; an order-sensitive sequence belongs in one hook.
func RegisterCleanup(fn CleanupFunc) {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	cleanups = append(cleanups, fn)
}

// RunCleanups invokes every registered hook against the namespace and stops
// at the first error.
func RunCleanups(ctx context.Context, c client.Client, namespace string) error {
	cleanupMu.Lock()
	snapshot := append([]CleanupFunc(nil), cleanups...)
	cleanupMu.Unlock()
	for _, fn := range snapshot {
		if err := fn(ctx, c, namespace); err != nil {
			return err
		}
	}
	return nil
}
