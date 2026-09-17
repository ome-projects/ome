package routing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementendpoint "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
)

type testEndpointPublisher struct{ name string }

func (p *testEndpointPublisher) Name() string { return p.name }
func (*testEndpointPublisher) Publish(context.Context, *v1beta1.InferenceService, placementendpoint.Target) error {
	return nil
}
func (*testEndpointPublisher) Unpublish(context.Context, *v1beta1.InferenceService) error {
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
	require.NoError(t, RegisterTrafficMapPublisher(name, func(
		cfg PublisherConfig,
		_ client.Client,
		_ client.Reader,
	) (*TrafficMapPublisher, error) {
		gotOptions = cfg.Options
		cfg.Options["mutated"] = "inside-factory"
		return &TrafficMapPublisher{
			Publisher:      &testEndpointPublisher{name: name},
			ResyncInterval: time.Minute,
		}, nil
	}))

	input := map[string]string{"key": "value"}
	got, err := NewTrafficMapPublisher(PublisherConfig{Name: name, Options: input}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, name, got.Publisher.Name())
	assert.Equal(t, time.Minute, got.ResyncInterval)
	assert.Equal(t, "value", gotOptions["key"])
	assert.NotContains(t, input, "mutated", "factory must not mutate loaded config")

	assert.Error(t, RegisterTrafficMapPublisher(name, func(
		PublisherConfig, client.Client, client.Reader,
	) (*TrafficMapPublisher, error) {
		return &TrafficMapPublisher{Publisher: &testEndpointPublisher{name: name}}, nil
	}))
	assert.Error(t, RegisterTrafficMapPublisher("", func(
		PublisherConfig, client.Client, client.Reader,
	) (*TrafficMapPublisher, error) {
		return &TrafficMapPublisher{Publisher: &testEndpointPublisher{}}, nil
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
