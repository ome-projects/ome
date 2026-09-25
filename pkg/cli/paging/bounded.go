package paging

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Limits bounds both one consistent Kubernetes list attempt and the total
// work spent recovering one expired continuation. MaxRequests and
// MaxConsumedItems may tighten the recovery allowance. Zero uses the safe
// default of twice the corresponding per-attempt limit.
type Limits struct {
	PageSize         int64
	MaxItems         int
	MaxPages         int
	MaxRequests      int
	MaxConsumedItems int
	RequestTimeout   time.Duration
}

// Page is one typed Kubernetes list response.
type Page[T any] struct {
	Items    []T
	Continue string
}

// Result contains the bounded prefix returned by ListBounded. Pages counts
// every API request, including failures and discarded expired attempts.
// ObservedPages counts successful responses in the current consistent attempt,
// including a response rejected by pagination validation. ReturnedItems counts
// raw items from successful responses in that attempt. ConsumedItems counts raw
// items across every attempt so callers can enforce one shared work budget
// across multiple lists. A nonconforming server page may make either item
// counter exceed that cooperative budget; ListBounded clips it and stops.
type Result[T any] struct {
	Items         []T
	Pages         int
	ObservedPages int
	ReturnedItems int
	ConsumedItems int
	Truncated     bool
}

// ListBounded follows Kubernetes continue tokens without exceeding limits.
// Base selectors are copied to every request. If a continuation from a
// full-list read expires, the incomplete snapshot is discarded and restarted
// once. MaxPages and MaxItems still bound the returned consistent attempt;
// discarded pages and items consume the separate, at-most-double work budget.
func ListBounded[T any](ctx context.Context, base metav1.ListOptions, limits Limits, fetch func(context.Context, metav1.ListOptions) (Page[T], error)) (Result[T], error) {
	if err := limits.Validate(); err != nil {
		return Result[T]{}, err
	}
	maxRequests := limits.MaxRequestAttempts()
	maxConsumedItems := limits.MaxItemWork()
	result := Result[T]{Items: make([]T, 0, limits.MaxItems)}
	continueToken := base.Continue
	seenContinueTokens := make(map[string]struct{})
	restartedAfterExpiration := false
	if continueToken != "" {
		seenContinueTokens[continueToken] = struct{}{}
	}

	for result.Pages < maxRequests && result.ConsumedItems < maxConsumedItems &&
		result.ObservedPages < limits.MaxPages && result.ReturnedItems < limits.MaxItems {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		request := base
		request.Continue = continueToken
		if continueToken != "" {
			// Kubernetes rejects resourceVersion constraints on continuation
			// requests. The continue token already binds the original snapshot;
			// a restart restores the caller's constraints from base.
			request.ResourceVersion = ""
			request.ResourceVersionMatch = ""
		}
		request.Limit = limits.PageSize
		remaining := limits.MaxItems - result.ReturnedItems
		remainingWork := maxConsumedItems - result.ConsumedItems
		if remaining > remainingWork {
			remaining = remainingWork
		}
		if request.Limit > int64(remaining) {
			request.Limit = int64(remaining)
		}

		requestCtx, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
		page, err := fetch(requestCtx, request)
		requestErr := requestCtx.Err()
		cancel()
		result.Pages++
		recoverableExpiration := err != nil &&
			(apierrors.IsResourceExpired(err) || apierrors.IsGone(err)) &&
			base.Continue == "" && request.Continue != "" &&
			result.ObservedPages > 0 && !restartedAfterExpiration
		if recoverableExpiration {
			// A 410 proves that the old continuation snapshot is unusable.
			// Clear it before any cancellation or budget return so callers can
			// never mistake stale prefix data for a valid partial result.
			result.Items = make([]T, 0, limits.MaxItems)
			result.ObservedPages = 0
			result.ReturnedItems = 0
			result.Truncated = false
			continueToken = ""
			seenContinueTokens = make(map[string]struct{})
			restartedAfterExpiration = true
		}
		if requestErr != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return result, contextErr
			}
			return result, requestErr
		}
		if err != nil {
			if recoverableExpiration {
				if contextErr := ctx.Err(); contextErr != nil {
					return result, contextErr
				}
				if result.Pages >= maxRequests || result.ConsumedItems >= maxConsumedItems {
					return result, err
				}
				continue
			}
			return result, err
		}
		result.ObservedPages++
		result.ReturnedItems = saturatingAdd(result.ReturnedItems, len(page.Items))
		result.ConsumedItems = saturatingAdd(result.ConsumedItems, len(page.Items))
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

// MaxRequestAttempts returns the hard request-attempt budget including the
// single expired-continuation recovery allowance.
func (limits Limits) MaxRequestAttempts() int {
	if limits.MaxRequests > 0 {
		return limits.MaxRequests
	}
	return doubledLimit(limits.MaxPages)
}

// MaxItemWork returns the requested item-work budget across both attempts.
// Kubernetes API servers honor list limits; a nonconforming overfilled page is
// still counted truthfully, clipped, and returned as truncated.
func (limits Limits) MaxItemWork() int {
	if limits.MaxConsumedItems > 0 {
		return limits.MaxConsumedItems
	}
	return doubledLimit(limits.MaxItems)
}

func doubledLimit(value int) int {
	if value <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if value > maxInt/2 {
		return maxInt
	}
	return value * 2
}

func saturatingAdd(left, right int) int {
	maxInt := int(^uint(0) >> 1)
	if right > maxInt-left {
		return maxInt
	}
	return left + right
}

// Validate checks both the consistent-attempt limits and the optional total
// recovery-work limits without issuing an API request.
func (limits Limits) Validate() error {
	if limits.PageSize <= 0 {
		return fmt.Errorf("page size must be positive")
	}
	if limits.MaxItems <= 0 {
		return fmt.Errorf("item limit must be positive")
	}
	if limits.MaxPages <= 0 {
		return fmt.Errorf("page limit must be positive")
	}
	if limits.MaxPages > int(^uint(0)>>1)/2 {
		return fmt.Errorf("page limit is too large")
	}
	if limits.MaxRequests < 0 || limits.MaxRequests > doubledLimit(limits.MaxPages) ||
		limits.MaxRequests > 0 && limits.MaxRequests < limits.MaxPages {
		return fmt.Errorf("request limit must be between page limit and twice page limit")
	}
	if limits.MaxItems > int(^uint(0)>>1)/2 {
		return fmt.Errorf("item limit is too large")
	}
	if limits.MaxConsumedItems < 0 || limits.MaxConsumedItems > doubledLimit(limits.MaxItems) ||
		limits.MaxConsumedItems > 0 && limits.MaxConsumedItems < limits.MaxItems {
		return fmt.Errorf("consumed item limit must be between item limit and twice item limit")
	}
	if limits.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be positive")
	}
	return nil
}
