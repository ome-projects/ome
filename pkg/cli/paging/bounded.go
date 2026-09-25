package paging

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Limits bounds both the size of each API request and the total work spent
// draining a Kubernetes list.
type Limits struct {
	PageSize       int64
	MaxItems       int
	MaxPages       int
	RequestTimeout time.Duration
}

// Page is one typed Kubernetes list response.
type Page[T any] struct {
	Items    []T
	Continue string
}

// Result contains the bounded prefix returned by ListBounded.
type Result[T any] struct {
	Items     []T
	Pages     int
	Truncated bool
}

// ListBounded follows Kubernetes continue tokens without exceeding limits.
// Base selectors are copied to every request.
func ListBounded[T any](ctx context.Context, base metav1.ListOptions, limits Limits, fetch func(context.Context, metav1.ListOptions) (Page[T], error)) (Result[T], error) {
	if err := limits.validate(); err != nil {
		return Result[T]{}, err
	}
	result := Result[T]{Items: make([]T, 0, limits.MaxItems)}
	continueToken := base.Continue
	seenContinueTokens := make(map[string]struct{})
	if continueToken != "" {
		seenContinueTokens[continueToken] = struct{}{}
	}

	for result.Pages < limits.MaxPages && len(result.Items) < limits.MaxItems {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		request := base
		request.Continue = continueToken
		request.Limit = limits.PageSize
		remaining := limits.MaxItems - len(result.Items)
		if request.Limit > int64(remaining) {
			request.Limit = int64(remaining)
		}

		requestCtx, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
		page, err := fetch(requestCtx, request)
		cancel()
		result.Pages++
		if err != nil {
			return result, err
		}
		if page.Continue != "" && page.Continue == request.Continue {
			return result, fmt.Errorf("continue token did not advance after page %d", result.Pages)
		}
		if page.Continue != "" {
			if _, seen := seenContinueTokens[page.Continue]; seen {
				return result, fmt.Errorf("continue token cycle detected after page %d", result.Pages)
			}
			seenContinueTokens[page.Continue] = struct{}{}
		}
		if len(page.Items) > remaining {
			result.Items = append(result.Items, page.Items[:remaining]...)
			result.Truncated = true
			return result, nil
		}
		result.Items = append(result.Items, page.Items...)
		if page.Continue == "" {
			return result, nil
		}
		continueToken = page.Continue
	}

	result.Truncated = continueToken != ""
	return result, nil
}

func (limits Limits) validate() error {
	if limits.PageSize <= 0 {
		return fmt.Errorf("page size must be positive")
	}
	if limits.MaxItems <= 0 {
		return fmt.Errorf("item limit must be positive")
	}
	if limits.MaxPages <= 0 {
		return fmt.Errorf("page limit must be positive")
	}
	if limits.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be positive")
	}
	return nil
}
