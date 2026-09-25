package validation

import (
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ValidateRouting validates the per-InferenceService routing policy and its
// compatibility with the deprecated placement capacity-factor field.
func ValidateRouting(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}

	var routingFactorsSet bool
	if spec.Routing != nil {
		routingFactorsSet = spec.Routing.CapacityFactors != nil
	}
	var placementFactorsSet bool
	if spec.Placement != nil {
		// The deprecated field remains readable during the compatibility window.
		placementFactorsSet = spec.Placement.CapacityFactors != nil //nolint:staticcheck
	}
	if routingFactorsSet && placementFactorsSet {
		return fmt.Errorf("spec.routing.capacityFactors and deprecated spec.placement.capacityFactors must not both be set")
	}

	if spec.Routing == nil {
		return nil
	}
	for cluster, factor := range spec.Routing.CapacityFactors {
		if factor.Sign() <= 0 {
			return fmt.Errorf("spec.routing.capacityFactors[%q] must be positive, got %q", cluster, factor.String())
		}
	}
	if err := validateRoutingProbe(spec.Routing.Probe); err != nil {
		return err
	}
	if err := validateRoutingCapacity(spec.Routing.Capacity); err != nil {
		return err
	}
	return validateRoutingPublisher(spec.Routing.Publisher)
}

func validateRoutingProbe(probe *v1beta1.RoutingProbeSpec) error {
	if probe == nil {
		return nil
	}
	if probe.Disabled {
		if routingProbeHasOperationalFields(probe) {
			return fmt.Errorf("spec.routing.probe.disabled must not be combined with probe configuration fields")
		}
		return nil
	}
	if probe.Path == "" {
		return fmt.Errorf("spec.routing.probe.path is required")
	}
	if !strings.HasPrefix(probe.Path, "/") {
		return fmt.Errorf("spec.routing.probe.path %q must begin with %q", probe.Path, "/")
	}
	if probe.Method == "" {
		return fmt.Errorf("spec.routing.probe.method is required")
	}
	if !isSafeRoutingHTTPMethod(probe.Method) {
		return fmt.Errorf("spec.routing.probe.method %q must be GET, HEAD, or POST", probe.Method)
	}
	if len(probe.AcceptStatuses) == 0 {
		return fmt.Errorf("spec.routing.probe.acceptStatuses is required")
	}
	if len(probe.GateStatuses) == 0 {
		return fmt.Errorf("spec.routing.probe.gateStatuses is required")
	}
	accepted := make(map[int32]struct{}, len(probe.AcceptStatuses))
	for _, status := range probe.AcceptStatuses {
		if err := validateHTTPStatus("spec.routing.probe.acceptStatuses", status); err != nil {
			return err
		}
		accepted[status] = struct{}{}
	}
	for _, status := range probe.GateStatuses {
		if err := validateHTTPStatus("spec.routing.probe.gateStatuses", status); err != nil {
			return err
		}
		if status == 401 || status == 403 || status == 429 {
			return fmt.Errorf("spec.routing.probe.gateStatuses must not contain %d", status)
		}
		if _, found := accepted[status]; found {
			return fmt.Errorf("spec.routing.probe status %d is in both acceptStatuses and gateStatuses", status)
		}
	}
	if probe.Period.Duration <= 0 {
		return fmt.Errorf("spec.routing.probe.period must be positive")
	}
	if probe.Timeout.Duration <= 0 {
		return fmt.Errorf("spec.routing.probe.timeout must be positive")
	}
	if probe.Timeout.Duration >= probe.Period.Duration {
		return fmt.Errorf("spec.routing.probe.timeout (%s) must be shorter than period (%s)",
			probe.Timeout.Duration, probe.Period.Duration)
	}
	if probe.FailureThreshold <= 0 {
		return fmt.Errorf("spec.routing.probe.failureThreshold must be positive")
	}
	if probe.SuccessThreshold <= 0 {
		return fmt.Errorf("spec.routing.probe.successThreshold must be positive")
	}
	switch probe.AllFailedPolicy {
	case v1beta1.RoutingAllFailedPolicyPreserveTraffic, v1beta1.RoutingAllFailedPolicyDrain:
	default:
		return fmt.Errorf("spec.routing.probe.allFailedPolicy %q must be %q or %q",
			probe.AllFailedPolicy,
			v1beta1.RoutingAllFailedPolicyPreserveTraffic,
			v1beta1.RoutingAllFailedPolicyDrain)
	}
	return nil
}

func routingProbeHasOperationalFields(probe *v1beta1.RoutingProbeSpec) bool {
	return probe.Path != "" ||
		probe.Method != "" ||
		len(probe.AcceptStatuses) != 0 ||
		len(probe.GateStatuses) != 0 ||
		probe.Period.Duration != 0 ||
		probe.Timeout.Duration != 0 ||
		probe.FailureThreshold != 0 ||
		probe.SuccessThreshold != 0 ||
		probe.AllFailedPolicy != ""
}

func validateRoutingCapacity(capacity *v1beta1.RoutingCapacitySpec) error {
	if capacity == nil {
		return nil
	}
	if capacity.Disabled {
		if routingCapacityHasOperationalFields(capacity) {
			return fmt.Errorf("spec.routing.capacity.disabled must not be combined with capacity configuration fields")
		}
		return nil
	}
	if capacity.Path == "" {
		return fmt.Errorf("spec.routing.capacity.path is required")
	}
	if !strings.HasPrefix(capacity.Path, "/") {
		return fmt.Errorf("spec.routing.capacity.path %q must begin with %q", capacity.Path, "/")
	}
	if capacity.Method == "" {
		return fmt.Errorf("spec.routing.capacity.method is required")
	}
	if !isSafeRoutingHTTPMethod(capacity.Method) {
		return fmt.Errorf("spec.routing.capacity.method %q must be GET, HEAD, or POST", capacity.Method)
	}
	if capacity.Format == "" {
		return fmt.Errorf("spec.routing.capacity.format is required")
	}
	if capacity.Period.Duration <= 0 {
		return fmt.Errorf("spec.routing.capacity.period must be positive")
	}
	if capacity.Timeout.Duration <= 0 {
		return fmt.Errorf("spec.routing.capacity.timeout must be positive")
	}
	if capacity.Timeout.Duration >= capacity.Period.Duration {
		return fmt.Errorf("spec.routing.capacity.timeout (%s) must be shorter than period (%s)",
			capacity.Timeout.Duration, capacity.Period.Duration)
	}
	if capacity.Samples <= 0 {
		return fmt.Errorf("spec.routing.capacity.samples must be positive")
	}
	if capacity.Quorum <= 0 {
		return fmt.Errorf("spec.routing.capacity.quorum must be positive")
	}
	if capacity.Quorum > capacity.Samples {
		return fmt.Errorf("spec.routing.capacity.quorum (%d) must not exceed samples (%d)",
			capacity.Quorum, capacity.Samples)
	}
	if capacity.MaxAge.Duration <= 0 {
		return fmt.Errorf("spec.routing.capacity.maxAge must be positive")
	}
	if capacity.MaxAge.Duration < capacity.Period.Duration {
		return fmt.Errorf("spec.routing.capacity.maxAge (%s) must be at least period (%s)",
			capacity.MaxAge.Duration, capacity.Period.Duration)
	}
	if capacity.Quorum > 1 {
		intervals := time.Duration(capacity.Quorum)
		wholePeriods := capacity.MaxAge.Duration / capacity.Period.Duration
		if wholePeriods < intervals {
			return fmt.Errorf("spec.routing.capacity.maxAge (%s) must span at least quorum (%d) period intervals",
				capacity.MaxAge.Duration, capacity.Quorum)
		}
	}
	return nil
}

func routingCapacityHasOperationalFields(capacity *v1beta1.RoutingCapacitySpec) bool {
	return capacity.Path != "" ||
		capacity.Method != "" ||
		capacity.Format != "" ||
		len(capacity.Options) != 0 ||
		capacity.Period.Duration != 0 ||
		capacity.Timeout.Duration != 0 ||
		capacity.Samples != 0 ||
		capacity.Quorum != 0 ||
		capacity.MaxAge.Duration != 0
}

func validateHTTPStatus(field string, status int32) error {
	if status < 100 || status > 599 {
		return fmt.Errorf("%s contains invalid HTTP status %d; statuses must be between 100 and 599", field, status)
	}
	return nil
}

func isSafeRoutingHTTPMethod(method string) bool {
	switch method {
	case "GET", "HEAD", "POST":
		return true
	default:
		return false
	}
}

func validateRoutingPublisher(publisher *v1beta1.RoutingPublisherSpec) error {
	if publisher == nil {
		return nil
	}
	if len(publisher.Options) > v1beta1.MaxRoutingPublisherOptions {
		return fmt.Errorf("spec.routing.publisher.options must contain at most %d entries, got %d",
			v1beta1.MaxRoutingPublisherOptions, len(publisher.Options))
	}
	for key, value := range publisher.Options {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("spec.routing.publisher.options contains an empty or whitespace-only key")
		}
		if len(key) > v1beta1.MaxRoutingPublisherOptionKeyBytes {
			return fmt.Errorf("spec.routing.publisher.options key %q exceeds the %d-byte limit",
				key, v1beta1.MaxRoutingPublisherOptionKeyBytes)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("spec.routing.publisher.options[%q] must not be empty or whitespace-only", key)
		}
		if len(value) > v1beta1.MaxRoutingPublisherOptionValueBytes {
			return fmt.Errorf("spec.routing.publisher.options[%q] exceeds the %d-byte value limit",
				key, v1beta1.MaxRoutingPublisherOptionValueBytes)
		}
	}
	return nil
}
