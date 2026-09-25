// Package paging drains paginated Kubernetes list APIs in bounded chunks
// instead of issuing a single unbounded LIST, so the CLI never pulls an
// entire large collection into memory (or the API server's) in one request.
package paging

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ChunkSize is the page size used for every paginated list the CLI issues,
// matching kubectl's own default list chunk size.
const ChunkSize = 500

// PageFunc fetches one page of a list. opts carries that page's Limit and
// Continue token, set by ListAllPaged; the closure is responsible for
// merging in any base query fields (LabelSelector, FieldSelector, ...) from
// the caller's own request before issuing the underlying List call. It
// returns the page's items and the response's continue token -- an empty
// token means the list is exhausted.
type PageFunc func(opts metav1.ListOptions) (items []runtime.Object, continueToken string, err error)

// ListAllPaged drains a paginated list API, kubectl-style: it fetches
// ChunkSize items per request, feeding each response's continue token into
// the next request, until the server returns no more continue tokens.
//
// Callers close over their own context and base ListOptions (e.g.
// LabelSelector) inside page; ListAllPaged itself only ever sets Limit and
// Continue, so it stays agnostic to whatever else a specific list call
// needs.
func ListAllPaged(ctx context.Context, page PageFunc) ([]runtime.Object, error) {
	var out []runtime.Object
	opts := metav1.ListOptions{Limit: ChunkSize}
	seenContinueTokens := make(map[string]struct{})
	completedPages := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items, cont, err := page(opts)
		if err != nil {
			return nil, err
		}
		completedPages++
		if cont != "" {
			if _, seen := seenContinueTokens[cont]; seen {
				return nil, fmt.Errorf("continue token cycle detected after page %d", completedPages)
			}
			seenContinueTokens[cont] = struct{}{}
		}
		out = append(out, items...)
		if cont == "" {
			return out, nil
		}
		opts = metav1.ListOptions{Limit: ChunkSize, Continue: cont}
	}
}
