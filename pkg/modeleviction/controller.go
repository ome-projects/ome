// Package modeleviction connects capacity observation, artifact inventory, model
// usage and batch planning. This controller only produces dry-run plans.
package modeleviction

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/artifactinventory"
	"sigs.k8s.io/ome/pkg/evictionplanner"
	"sigs.k8s.io/ome/pkg/modelusage"
)

// Options apply to one controller deployment. Selected pools must store model
// artifacts on the filesystem reported by kubelet as imageFs.
type Options struct {
	NodeSelector   string
	ModelNamespace string
	Interval       time.Duration
	HighWatermark  float64
	LowWatermark   float64
	Workers        int
	RequestTimeout time.Duration
	RoundTimeout   time.Duration
	MaxSampleAge   time.Duration
}

func DefaultOptions() Options {
	return Options{
		ModelNamespace: "ome", Interval: 6 * time.Hour,
		HighWatermark: 0.75, LowWatermark: 0.70, Workers: 4,
		RequestTimeout: 10 * time.Second, RoundTimeout: 10 * time.Minute,
		MaxSampleAge: 5 * time.Minute,
	}
}

func (o Options) validate() (labels.Selector, error) {
	if o.NodeSelector == "" {
		return nil, fmt.Errorf("node selector must explicitly select model storage pools")
	}
	selector, err := labels.Parse(o.NodeSelector)
	if err != nil || selector.Empty() {
		return nil, fmt.Errorf("invalid node selector %q", o.NodeSelector)
	}
	if math.IsNaN(o.LowWatermark) || math.IsNaN(o.HighWatermark) || o.LowWatermark <= 0 || o.LowWatermark >= o.HighWatermark || o.HighWatermark >= 1 {
		return nil, fmt.Errorf("watermarks must satisfy 0 < low < high < 1")
	}
	if o.Interval <= 0 || o.Workers <= 0 || o.RequestTimeout <= 0 || o.RoundTimeout <= 0 || o.MaxSampleAge <= 0 {
		return nil, fmt.Errorf("interval, concurrency, timeouts and sample age must be positive")
	}
	return selector, nil
}

type CapacityReader interface {
	Read(context.Context, *corev1.Node) (CapacitySample, error)
}

type InventoryBuilder interface {
	Build(context.Context, string) (*artifactinventory.Inventory, error)
}

type UsageEvaluator interface {
	Reconcile(context.Context, modelusage.Request, types.UID, bool) (evictionplanner.Eligibility, error)
}

// NodeObservation keeps measurements separate from the planner's projections.
// An error is unknown capacity, never evidence of zero available space.
type NodeObservation struct {
	NodeName       string          `json:"nodeName"`
	Sample         *CapacitySample `json:"sample,omitempty"`
	Error          string          `json:"error,omitempty"`
	InventoryError string          `json:"inventoryError,omitempty"`
}

type ModelError struct {
	Model evictionplanner.ModelIdentity `json:"model"`
	Error string                        `json:"error"`
}

// Report is advisory: projected reclaimed bytes are not confirmed disk recovery.
type Report struct {
	StartedAt    time.Time             `json:"startedAt"`
	FinishedAt   time.Time             `json:"finishedAt"`
	DryRun       bool                  `json:"dryRun"`
	Observations []NodeObservation     `json:"observations"`
	ModelErrors  []ModelError          `json:"modelErrors,omitempty"`
	Plan         *evictionplanner.Plan `json:"plan"`
}

type Controller struct {
	reader    client.Reader
	inventory InventoryBuilder
	usage     UsageEvaluator
	capacity  CapacityReader
	options   Options
	selector  labels.Selector
	clock     clock.Clock
}

// NewController expects fresh API reads for nodes and inventory. UsageEvaluator
// combines current CR reads with the synchronized reference informer indexes.
func NewController(reader client.Reader, inventory InventoryBuilder, usage UsageEvaluator, capacity CapacityReader, options Options) (*Controller, error) {
	selector, err := options.validate()
	if err != nil {
		return nil, err
	}
	if reader == nil || inventory == nil || usage == nil || capacity == nil {
		return nil, fmt.Errorf("all controller dependencies are required")
	}
	return &Controller{reader: reader, inventory: inventory, usage: usage, capacity: capacity, options: options, selector: selector, clock: clock.RealClock{}}, nil
}

// RunOnce collects one bounded snapshot, evaluates each distinct model, then
// delegates all grouping, scoring and cross-node projections to the planner.
// It never submits eviction; history initialization is its only possible write.
func (c *Controller) RunOnce(ctx context.Context) (*Report, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.RoundTimeout)
	defer cancel()
	report := &Report{StartedAt: c.clock.Now(), DryRun: true}
	defer func() { report.FinishedAt = c.clock.Now() }()

	nodes := &corev1.NodeList{}
	if err := c.reader.List(ctx, nodes, client.MatchingLabelsSelector{Selector: c.selector}); err != nil {
		return report, fmt.Errorf("list model nodes: %w", err)
	}
	sort.Slice(nodes.Items, func(i, j int) bool { return nodes.Items[i].Name < nodes.Items[j].Name })
	report.Observations = make([]NodeObservation, len(nodes.Items))
	inventories := make([]*artifactinventory.Inventory, len(nodes.Items))

	// Each worker owns one result slot. Capacity and inventory failures are local
	// to a node; they do not prevent healthy nodes from being evaluated.
	var workers errgroup.Group
	workers.SetLimit(c.options.Workers)
	for i := range nodes.Items {
		workers.Go(func() error {
			n := &nodes.Items[i]
			observation := &report.Observations[i]
			observation.NodeName = n.Name
			sample, err := c.capacity.Read(ctx, n)
			if err == nil {
				err = sample.Validate(n.Name, c.clock.Now(), c.options.MaxSampleAge)
			}
			if err != nil {
				observation.Error = err.Error()
				return nil
			}
			observation.Sample = &sample
			if float64(sample.CapacityBytes-sample.AvailableBytes)/float64(sample.CapacityBytes) < c.options.HighWatermark {
				return nil
			}
			inventories[i], err = c.inventory.Build(ctx, n.Name)
			if err != nil {
				observation.InventoryError = err.Error()
				inventories[i] = nil
			}
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	// A shared CR may appear on several pressure nodes. Query its usage once;
	// reuse that eligibility result for every appearance in this planning round.
	models := map[types.UID]evictionplanner.ModelIdentity{}
	for _, inventory := range inventories {
		if inventory == nil {
			continue
		}
		for _, artifact := range inventory.Artifacts {
			if !artifact.EvictionEligible {
				continue
			}
			for _, model := range artifact.Models {
				models[model.UID] = evictionplanner.ModelIdentity{Namespace: model.Namespace, Name: model.Name, UID: model.UID}
			}
		}
	}
	uids := make([]types.UID, 0, len(models))
	for uid := range models {
		uids = append(uids, uid)
	}
	sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
	eligibility := make(map[types.UID]evictionplanner.Eligibility, len(models))
	for _, uid := range uids {
		model := models[uid]
		requestCtx, stop := context.WithTimeout(ctx, c.options.RequestTimeout)
		result, err := c.usage.Reconcile(requestCtx, modelusage.Request{Model: types.NamespacedName{Namespace: model.Namespace, Name: model.Name}}, uid, false)
		stop()
		if err != nil {
			result = evictionplanner.Eligibility{Reason: "usage_check_failed"}
			report.ModelErrors = append(report.ModelErrors, ModelError{Model: model, Error: err.Error()})
		}
		eligibility[uid] = result
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	// Mapping and history initialization take time. Recheck freshness before
	// planning, and take Now after newly initialized history timestamps.
	input := evictionplanner.Input{Now: c.clock.Now(), EligibilityByUID: eligibility}
	for i := range report.Observations {
		observation := &report.Observations[i]
		if observation.Sample == nil {
			continue
		}
		sample := observation.Sample
		if err := sample.Validate(observation.NodeName, input.Now, c.options.MaxSampleAge); err != nil {
			observation.Error = err.Error()
			continue
		}
		if float64(sample.CapacityBytes-sample.AvailableBytes)/float64(sample.CapacityBytes) < c.options.HighWatermark {
			continue
		}
		input.Nodes = append(input.Nodes, evictionplanner.Node{
			Name: observation.NodeName, CapacityBytes: sample.CapacityBytes, AvailableBytes: sample.AvailableBytes,
			HighWatermark: c.options.HighWatermark, LowWatermark: c.options.LowWatermark,
			Inventory: inventories[i], InventoryError: observation.InventoryError,
		})
	}
	var err error
	report.Plan, err = evictionplanner.Build(input)
	return report, err
}
