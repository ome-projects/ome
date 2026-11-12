package pod

import (
	"strconv"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/kmp"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestInjectMetricsAggregator_AnnotationOnOffOrUnset_InjectsOnlyWhenOn(t *testing.T) {
	shipped := shippedMetrics(t)
	userMetricPort := v1.ContainerPort{Name: "http-usermetric", ContainerPort: 9091, Protocol: "TCP"}
	// aggregated is the queue-proxy once the aggregator is injected: the three
	// metrics env vars and the aggregate port beside the user-metric port.
	aggregated := v1.Container{
		Name: "queue-proxy",
		Env: []v1.EnvVar{
			{Name: constants.ContainerPrometheusMetricsPortEnvVarKey, Value: strconv.Itoa(int(shipped.ContainerPort))},
			{Name: constants.ContainerPrometheusMetricsPathEnvVarKey, Value: shipped.Path},
			{Name: constants.QueueProxyAggregatePrometheusMetricsPortEnvVarKey, Value: strconv.Itoa(int(shipped.AggregatePort))},
		},
		Ports: []v1.ContainerPort{
			userMetricPort,
			{Name: constants.AggregateMetricsPortName, ContainerPort: shipped.AggregatePort, Protocol: "TCP"},
		},
	}
	untouched := v1.Container{Name: "queue-proxy", Ports: []v1.ContainerPort{userMetricPort}}

	scenarios := []struct {
		name           string
		annotations    map[string]string
		wantQueueProxy v1.Container
	}{
		{
			name:           "aggregation on",
			annotations:    map[string]string{constants.EnableMetricAggregation: "true"},
			wantQueueProxy: aggregated,
		},
		{
			name:           "aggregation unset",
			wantQueueProxy: untouched,
		},
		{
			name:           "aggregation off",
			annotations:    map[string]string{constants.EnableMetricAggregation: "false"},
			wantQueueProxy: untouched,
		},
		{
			name: "prometheus annotations with aggregation on",
			annotations: map[string]string{
				constants.EnableMetricAggregation: "true",
				constants.SetPrometheusAnnotation: "true",
			},
			wantQueueProxy: aggregated,
		},
		{
			name: "prometheus annotations with aggregation off",
			annotations: map[string]string{
				constants.EnableMetricAggregation: "false",
				constants.SetPrometheusAnnotation: "true",
			},
			wantQueueProxy: untouched,
		},
		{
			name: "prometheus annotations off with aggregation on",
			annotations: map[string]string{
				constants.EnableMetricAggregation: "true",
				constants.SetPrometheusAnnotation: "false",
			},
			wantQueueProxy: aggregated,
		},
	}

	ma := shipped

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "deployment", Namespace: "default", Annotations: scenario.annotations},
				Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "sklearn"}, *untouched.DeepCopy()}},
			}
			if err := ma.InjectMetricsAggregator(pod); err != nil {
				t.Fatalf("InjectMetricsAggregator() error = %v", err)
			}
			want := v1.PodSpec{Containers: []v1.Container{{Name: "sklearn"}, scenario.wantQueueProxy}}
			if diff, _ := kmp.SafeDiff(want, pod.Spec); diff != "" {
				t.Errorf("unexpected result (-want +got): %v", diff)
			}
		})
	}
}

// The webhook is registered with reinvocationPolicy=IfNeeded, so the
// injector can run multiple times on the same pod and must converge
// instead of stacking duplicate env vars or ports.
func TestInjectMetricsAggregator_RunTwice_ConvergesWithoutDuplicates(t *testing.T) {
	newPod := func() *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "deployment",
				Namespace: "default",
				Annotations: map[string]string{
					constants.EnableMetricAggregation: "true",
				},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{Name: "sklearn"},
					{
						Name:  "queue-proxy",
						Ports: []v1.ContainerPort{{Name: "http-usermetric", ContainerPort: 9091, Protocol: "TCP"}},
					},
				},
			},
		}
	}

	queueProxy := func(t *testing.T, pod *v1.Pod) *v1.Container {
		t.Helper()
		for i := range pod.Spec.Containers {
			if pod.Spec.Containers[i].Name == "queue-proxy" {
				return &pod.Spec.Containers[i]
			}
		}
		t.Fatal("queue-proxy container not found")
		return nil
	}

	envValuesByName := func(container *v1.Container, name string) []string {
		var values []string
		for _, env := range container.Env {
			if env.Name == name {
				values = append(values, env.Value)
			}
		}
		return values
	}

	ma := shippedMetrics(t)

	t.Run("second invocation is a no-op", func(t *testing.T) {
		pod := newPod()
		if err := ma.InjectMetricsAggregator(pod); err != nil {
			t.Fatalf("first invocation failed: %v", err)
		}
		afterFirst := pod.DeepCopy()

		if err := ma.InjectMetricsAggregator(pod); err != nil {
			t.Fatalf("second invocation failed: %v", err)
		}
		if diff, _ := kmp.SafeDiff(afterFirst.Spec, pod.Spec); diff != "" {
			t.Errorf("second invocation mutated the pod spec (-want +got): %v", diff)
		}

		qp := queueProxy(t, pod)
		for _, name := range []string{
			constants.ContainerPrometheusMetricsPortEnvVarKey,
			constants.ContainerPrometheusMetricsPathEnvVarKey,
			constants.QueueProxyAggregatePrometheusMetricsPortEnvVarKey,
		} {
			if values := envValuesByName(qp, name); len(values) != 1 {
				t.Errorf("expected exactly one %q env var, got %d (%v)", name, len(values), values)
			}
		}
		if len(qp.Ports) != 2 {
			t.Errorf("expected 2 ports after reinvocation, got %d (%v)", len(qp.Ports), qp.Ports)
		}
	})

	t.Run("annotation change between invocations updates in place", func(t *testing.T) {
		pod := newPod()
		if err := ma.InjectMetricsAggregator(pod); err != nil {
			t.Fatalf("first invocation failed: %v", err)
		}

		pod.ObjectMeta.Annotations[constants.ContainerPrometheusPortKey] = "9095"
		if err := ma.InjectMetricsAggregator(pod); err != nil {
			t.Fatalf("second invocation failed: %v", err)
		}

		qp := queueProxy(t, pod)
		values := envValuesByName(qp, constants.ContainerPrometheusMetricsPortEnvVarKey)
		if len(values) != 1 || values[0] != "9095" {
			t.Errorf("expected a single %q env var with value 9095, got %v",
				constants.ContainerPrometheusMetricsPortEnvVarKey, values)
		}
	})
}
