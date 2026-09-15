// Package quotacollection reads the complete bounded AcceleratorQuota snapshot
// required to assemble a trustworthy quota tree.
package quotacollection

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

var (
	// ErrClientRequired guards against manufacturing an empty tree when command
	// wiring did not provide the required cluster-scoped typed client.
	ErrClientRequired = errors.New("AcceleratorQuota client is required")
	// ErrInvalidLimits identifies unusable pagination or timeout bounds.
	ErrInvalidLimits = errors.New("AcceleratorQuota collection limits are invalid")
	// ErrEmptyResponse identifies a broken API/client contract, not an empty list.
	ErrEmptyResponse = errors.New("AcceleratorQuota list response is empty")
)

// Completeness describes the exact bounded list work used by a tree report.
type Completeness struct {
	ObservedPages int
	ObservedItems int
	Truncated     bool
}

// Result is an immutable caller-owned quota snapshot and its collection facts.
type Result struct {
	Quotas       []omev1beta1.AcceleratorQuota
	Completeness Completeness
}

// Collect drains the cluster-scoped AcceleratorQuota list without exceeding
// the supplied item, page, or per-request timeout bounds.
func Collect(
	ctx context.Context,
	client omeclient.AcceleratorQuotaInterface,
	limits paging.Limits,
) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if client == nil {
		return Result{}, ErrClientRequired
	}
	if err := validateLimits(limits); err != nil {
		return Result{}, err
	}

	listed, err := paging.ListBounded(
		ctx,
		metav1.ListOptions{},
		limits,
		func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[omev1beta1.AcceleratorQuota], error) {
			list, listErr := client.List(requestCtx, options)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[omev1beta1.AcceleratorQuota]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[omev1beta1.AcceleratorQuota]{}, listErr
			}
			if list == nil {
				return paging.Page[omev1beta1.AcceleratorQuota]{}, ErrEmptyResponse
			}
			return paging.Page[omev1beta1.AcceleratorQuota]{
				Items: list.Items, Continue: list.Continue,
			}, nil
		},
	)
	if err != nil {
		return Result{}, fmt.Errorf("list AcceleratorQuotas: %w", err)
	}
	return Result{
		Quotas: copyQuotas(listed.Items),
		Completeness: Completeness{
			ObservedPages: listed.Pages,
			ObservedItems: len(listed.Items),
			Truncated:     listed.Truncated,
		},
	}, nil
}

func validateLimits(limits paging.Limits) error {
	if limits.PageSize <= 0 || limits.MaxItems <= 0 || limits.MaxPages <= 0 || limits.RequestTimeout <= 0 {
		return ErrInvalidLimits
	}
	return nil
}

func copyQuotas(values []omev1beta1.AcceleratorQuota) []omev1beta1.AcceleratorQuota {
	result := make([]omev1beta1.AcceleratorQuota, len(values))
	for i := range values {
		result[i] = *values[i].DeepCopy()
	}
	return result
}
