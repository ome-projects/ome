package workload

import (
	"fmt"
	"sync"

	"github.com/go-logr/logr"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
)

// WorkloadStrategyManager manages and selects workload strategies.
type WorkloadStrategyManager struct {
	registers  map[string]struct{}
	strategies []WorkloadStrategy // Slice to maintain registration order
	mu         sync.RWMutex
	log        logr.Logger
}

func NewWorkloadStrategyManager(log logr.Logger) *WorkloadStrategyManager {
	return &WorkloadStrategyManager{
		registers:  make(map[string]struct{}),
		strategies: make([]WorkloadStrategy, 0),
		log:        log,
	}
}

// RegisterStrategy registers a workload strategy.
func (m *WorkloadStrategyManager) RegisterStrategy(strategy WorkloadStrategy) error {
	if strategy == nil {
		return fmt.Errorf("cannot register nil strategy")
	}

	name := strategy.GetStrategyName()
	if name == "" {
		return fmt.Errorf("strategy name cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.registers[name]; exists {
		return fmt.Errorf("strategy %s is already registered", name)
	}

	// Add to both map and ordered slice
	m.registers[name] = struct{}{}
	m.strategies = append(m.strategies, strategy)
	m.log.Info("Registered workload strategy", "strategy", name)
	return nil
}

// SelectStrategy selects an appropriate strategy based on conditions.
// Strategy selection process:
// 1. Check deploymentMode
// 2. Iterate through all registered strategies to find the first one where IsApplicable returns true
// 3. Return an error if no applicable strategy is found
func (m *WorkloadStrategyManager) SelectStrategy(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) (WorkloadStrategy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.strategies) == 0 {
		return nil, fmt.Errorf("no workload strategies registered")
	}

	// Iterate through all strategies to find the first applicable one.
	// Note: Strategy registration order matters - special strategies (like RBG) should be registered first,
	// and default strategy (SingleComponent) should be registered last.
	for _, strategy := range m.strategies {
		if strategy.IsApplicable(isvc, deploymentMode) {
			m.log.Info("Selected workload strategy",
				"strategy", strategy.GetStrategyName(),
				"namespace", isvc.Namespace,
				"inferenceService", isvc.Name)
			return strategy, nil
		}
	}

	// If no strategy is applicable, return an error.
	return nil, fmt.Errorf("no applicable workload strategy found for InferenceService %s/%s", isvc.Namespace, isvc.Name)
}

// GetStrategy retrieves a strategy instance by name.
func (m *WorkloadStrategyManager) GetStrategy(name string) (WorkloadStrategy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.registers[name]
	if !exists {
		return nil, fmt.Errorf("strategy %s not found", name)
	}

	for _, v := range m.strategies {
		if v.GetStrategyName() == name {
			return v, nil
		}
	}

	return nil, fmt.Errorf("internal inconsistency: strategy %s is in registers but not in slice", name)
}

// ListStrategies lists all registered strategies in registration order.
func (m *WorkloadStrategyManager) ListStrategies() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.strategies))
	for _, strategy := range m.strategies {
		names = append(names, strategy.GetStrategyName())
	}

	return names
}
