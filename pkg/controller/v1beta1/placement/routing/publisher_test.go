package routing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementendpoint "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
)

type testTrafficMapPublishPlan struct{}

func (testTrafficMapPublishPlan) Claims() []string { return nil }

type testTrafficMapPublisher struct{ name string }

func (p *testTrafficMapPublisher) Name() string { return p.name }
func (*testTrafficMapPublisher) Stateful() bool { return false }
func (*testTrafficMapPublisher) ResolveOptions(global, _ map[string]string) (map[string]string, error) {
	return global, nil
}
func (*testTrafficMapPublisher) Plan(
	*v1beta1.InferenceService,
	*v1beta1.TrafficMap,
	map[string]string,
) (placementendpoint.TrafficMapPublishPlan, error) {
	return testTrafficMapPublishPlan{}, nil
}
func (*testTrafficMapPublisher) Drain(context.Context, types.NamespacedName, []string) error {
	return nil
}
func (*testTrafficMapPublisher) Apply(
	context.Context,
	placementendpoint.TrafficMapPublishPlan,
) (placementendpoint.TrafficMapPublishResult, error) {
	return placementendpoint.TrafficMapPublishResult{}, nil
}
func (*testTrafficMapPublisher) Unpublish(
	context.Context,
	types.NamespacedName,
	v1beta1.TrafficMapPublisherStatus,
) error {
	return nil
}

func TestTrafficMapPublisherRegistry(t *testing.T) {
	const name = "publisher-registry-test"
	t.Cleanup(func() {
		publishers.mu.Lock()
		defer publishers.mu.Unlock()
		delete(publishers.factories, name)
	})
	var gotOptions map[string]string
	factoryGlobalOptions := map[string]string{"safeOption": "value"}
	require.NoError(t, RegisterTrafficMapPublisher(name, func(
		cfg PublisherConfig,
		_ client.Client,
		_ client.Reader,
	) (*TrafficMapPublisher, error) {
		gotOptions = cfg.Options
		cfg.Options["mutated"] = "inside-factory"
		return &TrafficMapPublisher{
			Publisher:      &testTrafficMapPublisher{name: name},
			GlobalOptions:  factoryGlobalOptions,
			ResyncInterval: 2 * time.Minute,
		}, nil
	}))

	input := map[string]string{"key": "value"}
	got, err := NewTrafficMapPublisher(PublisherConfig{
		Name: name, ResyncInterval: time.Minute, Options: input,
	}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, name, got.Publisher.Name())
	assert.Equal(t, time.Minute, got.ResyncInterval)
	assert.Equal(t, "value", gotOptions["key"])
	assert.NotContains(t, input, "mutated", "factory must not mutate loaded config")
	assert.Equal(t, factoryGlobalOptions, got.GlobalOptions)
	got.GlobalOptions["caller"] = "mutation"
	assert.NotContains(t, factoryGlobalOptions, "caller", "factory-owned options must not alias the result")
	factoryGlobalOptions["factory"] = "mutation"
	assert.NotContains(t, got.GlobalOptions, "factory", "the result must not alias factory-owned options")

	_, err = NewTrafficMapPublisher(PublisherConfig{
		Name: name, ResyncInterval: -time.Second,
	}, nil, nil)
	require.ErrorContains(t, err, "resync interval must not be negative")

	assert.Error(t, RegisterTrafficMapPublisher(name, func(
		PublisherConfig, client.Client, client.Reader,
	) (*TrafficMapPublisher, error) {
		return &TrafficMapPublisher{Publisher: &testTrafficMapPublisher{name: name}}, nil
	}))
	assert.Error(t, RegisterTrafficMapPublisher("", func(
		PublisherConfig, client.Client, client.Reader,
	) (*TrafficMapPublisher, error) {
		return &TrafficMapPublisher{Publisher: &testTrafficMapPublisher{}}, nil
	}))
	assert.Error(t, RegisterTrafficMapPublisher("nil-factory", nil))
}

func TestNewTrafficMapPublisher(t *testing.T) {
	got, err := NewTrafficMapPublisher(PublisherConfig{}, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, got)

	_, err = NewTrafficMapPublisher(
		PublisherConfig{Options: map[string]string{"unused": "value"}}, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "require a publisher name")

	_, err = NewTrafficMapPublisher(PublisherConfig{Name: "not-compiled-in"}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not compiled into this manager")
}

func TestNewTrafficMapPublisherValidatesFactoryResult(t *testing.T) {
	tests := []struct {
		name       string
		result     *TrafficMapPublisher
		wantErr    string
		factoryErr error
	}{
		{name: "factory-error", wantErr: "configure TrafficMap publisher", factoryErr: errors.New("invalid config")},
		{name: "nil-result", wantErr: "factory returned nil"},
		{name: "nil-publisher", result: &TrafficMapPublisher{}, wantErr: "factory returned nil"},
		{
			name: "publisher-name-mismatch",
			result: &TrafficMapPublisher{
				Publisher: &testTrafficMapPublisher{name: "different"},
			},
			wantErr: "returned publisher named",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, RegisterTrafficMapPublisher(tt.name, func(
				PublisherConfig, client.Client, client.Reader,
			) (*TrafficMapPublisher, error) {
				return tt.result, tt.factoryErr
			}))
			t.Cleanup(func() {
				publishers.mu.Lock()
				defer publishers.mu.Unlock()
				delete(publishers.factories, tt.name)
			})

			_, err := NewTrafficMapPublisher(PublisherConfig{Name: tt.name}, nil, nil)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
