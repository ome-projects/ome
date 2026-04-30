package workload

import (
	"context"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
)

// MockStrategy is a mock implementation of WorkloadStrategy for testing.
type MockStrategy struct {
	name            string
	isApplicable    bool
	validateErr     error
	reconcileResult ctrl.Result
	reconcileErr    error
	reconcileCalled int
	mu              sync.Mutex
}

func NewMockStrategy(name string, isApplicable bool) *MockStrategy {
	return &MockStrategy{
		name:         name,
		isApplicable: isApplicable,
	}
}

func (m *MockStrategy) GetStrategyName() string {
	return m.name
}

func (m *MockStrategy) IsApplicable(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) bool {
	return m.isApplicable
}

func (m *MockStrategy) ValidateDeploymentModes(modes *ComponentDeploymentModes) error {
	return m.validateErr
}

func (m *MockStrategy) ReconcileWorkload(ctx context.Context, request *WorkloadReconcileRequest) (ctrl.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileCalled++
	return m.reconcileResult, m.reconcileErr
}

func (m *MockStrategy) SetValidateError(err error) {
	m.validateErr = err
}

func (m *MockStrategy) SetReconcileResult(result ctrl.Result, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileResult = result
	m.reconcileErr = err
}

func (m *MockStrategy) GetReconcileCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reconcileCalled
}

// TestRegisterStrategy tests various scenarios of registering strategies.
func TestRegisterStrategy(t *testing.T) {
	testCases := []struct {
		name           string
		setup          func() (*WorkloadStrategyManager, WorkloadStrategy, WorkloadStrategy)
		wantErr        bool
		errContains    string
		expectedCount  int
		verifyOriginal bool
	}{
		{
			name: "nil strategy",
			setup: func() (*WorkloadStrategyManager, WorkloadStrategy, WorkloadStrategy) {
				log := logr.Discard()
				manager := NewWorkloadStrategyManager(log)
				return manager, nil, nil
			},
			wantErr:       true,
			errContains:   "cannot register nil strategy",
			expectedCount: 0,
		},
		{
			name: "empty name",
			setup: func() (*WorkloadStrategyManager, WorkloadStrategy, WorkloadStrategy) {
				log := logr.Discard()
				manager := NewWorkloadStrategyManager(log)
				strategy := NewMockStrategy("", true)
				return manager, strategy, nil
			},
			wantErr:       true,
			errContains:   "strategy name cannot be empty",
			expectedCount: 0,
		},
		{
			name: "duplicate strategy",
			setup: func() (*WorkloadStrategyManager, WorkloadStrategy, WorkloadStrategy) {
				log := logr.Discard()
				manager := NewWorkloadStrategyManager(log)
				strategy1 := NewMockStrategy("TestStrategy", true)
				strategy2 := NewMockStrategy("TestStrategy", false)
				err := manager.RegisterStrategy(strategy1)
				require.NoError(t, err)
				return manager, strategy2, strategy1
			},
			wantErr:        true,
			errContains:    "already registered",
			expectedCount:  1,
			verifyOriginal: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager, strategy, originalStrategy := tc.setup()

			err := manager.RegisterStrategy(strategy)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tc.expectedCount, len(manager.registers))

			// Verify original strategy is still in place for duplicate test
			if tc.verifyOriginal {
				retrieved, err := manager.GetStrategy("TestStrategy")
				require.NoError(t, err)
				assert.Equal(t, originalStrategy, retrieved)
			}
		})
	}
}

// TestSelectStrategy tests various scenarios of selecting strategies.
func TestSelectStrategy(t *testing.T) {
	testCases := []struct {
		name        string
		setup       func() (*WorkloadStrategyManager, *v1beta1.InferenceService)
		wantErr     bool
		errContains string
	}{
		{
			name: "no strategy matches",
			setup: func() (*WorkloadStrategyManager, *v1beta1.InferenceService) {
				log := logr.Discard()
				manager := NewWorkloadStrategyManager(log)
				strategy := NewMockStrategy("TestStrategy", false)
				err := manager.RegisterStrategy(strategy)
				require.NoError(t, err)
				isvc := &v1beta1.InferenceService{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-isvc",
						Namespace: "default",
					},
				}
				return manager, isvc
			},
			wantErr:     true,
			errContains: "no applicable workload strategy found",
		},
		{
			name: "no strategies registered",
			setup: func() (*WorkloadStrategyManager, *v1beta1.InferenceService) {
				log := logr.Discard()
				manager := NewWorkloadStrategyManager(log)
				isvc := &v1beta1.InferenceService{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-isvc",
						Namespace: "default",
					},
				}
				return manager, isvc
			},
			wantErr:     true,
			errContains: "no workload strategies registered",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager, isvc := tc.setup()
			deploymentMode := constants.RawDeployment

			selected, err := manager.SelectStrategy(isvc, deploymentMode)

			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, selected)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, selected)
			}
		})
	}
}

// TestGetStrategy tests various scenarios of getting strategies.
func TestGetStrategy(t *testing.T) {
	testCases := []struct {
		name           string
		setup          func() *WorkloadStrategyManager
		strategyName   string
		wantErr        bool
		errContains    string
		expectStrategy bool
	}{
		{
			name: "strategy found",
			setup: func() *WorkloadStrategyManager {
				log := logr.Discard()
				manager := NewWorkloadStrategyManager(log)
				strategy := NewMockStrategy("TestStrategy", true)
				err := manager.RegisterStrategy(strategy)
				require.NoError(t, err)
				return manager
			},
			strategyName:   "TestStrategy",
			wantErr:        false,
			expectStrategy: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := tc.setup()

			retrieved, err := manager.GetStrategy(tc.strategyName)

			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, retrieved)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				if tc.expectStrategy {
					assert.NotNil(t, retrieved)
				}
			}
		})
	}
}

// TestGetStrategy_NotFound tests retrieving a non-existent strategy.
func TestGetStrategy_NotFound(t *testing.T) {
	log := logr.Discard()
	manager := NewWorkloadStrategyManager(log)

	retrieved, err := manager.GetStrategy("NonExistent")
	require.Error(t, err)
	assert.Nil(t, retrieved)
	assert.Contains(t, err.Error(), "strategy NonExistent not found")
}

// TestListStrategies tests listing all registered strategies.
func TestListStrategies(t *testing.T) {
	log := logr.Discard()
	manager := NewWorkloadStrategyManager(log)

	strategy1 := NewMockStrategy("Strategy1", true)
	strategy2 := NewMockStrategy("Strategy2", true)

	err := manager.RegisterStrategy(strategy1)
	require.NoError(t, err)
	err = manager.RegisterStrategy(strategy2)
	require.NoError(t, err)

	names := manager.ListStrategies()
	assert.Equal(t, 2, len(names))
	assert.Contains(t, names, "Strategy1")
	assert.Contains(t, names, "Strategy2")
}

// TestListStrategies_Empty tests listing when no strategies are registered.
func TestListStrategies_Empty(t *testing.T) {
	log := logr.Discard()
	manager := NewWorkloadStrategyManager(log)

	names := manager.ListStrategies()
	assert.Equal(t, 0, len(names))
}
