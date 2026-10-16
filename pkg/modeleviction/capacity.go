package modeleviction

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

const (
	maxSummaryBytes = 4 << 20
	maxClockSkew    = 30 * time.Second
)

// CapacitySample describes the kubelet's image filesystem, not model sizes.
// ObservedAt comes from the kubelet; ReadAt records when this process read it.
type CapacitySample struct {
	NodeName       string    `json:"nodeName"`
	ObservedAt     time.Time `json:"observedAt"`
	ReadAt         time.Time `json:"readAt"`
	CapacityBytes  int64     `json:"capacityBytes"`
	AvailableBytes int64     `json:"availableBytes"`
}

// Validate rejects missing, inconsistent, or stale capacity measurements.
func (s CapacitySample) Validate(nodeName string, now time.Time, maxAge time.Duration) error {
	if nodeName == "" || s.NodeName != nodeName {
		return fmt.Errorf("capacity sample node %q does not match %q", s.NodeName, nodeName)
	}
	if maxAge <= 0 {
		return fmt.Errorf("capacity maximum age must be positive")
	}
	if s.CapacityBytes <= 0 || s.AvailableBytes < 0 || s.AvailableBytes > s.CapacityBytes {
		return fmt.Errorf("invalid filesystem capacity %d and available bytes %d", s.CapacityBytes, s.AvailableBytes)
	}
	for _, timestamp := range []struct {
		name  string
		value time.Time
	}{
		{name: "observation", value: s.ObservedAt},
		{name: "read", value: s.ReadAt},
	} {
		if timestamp.value.IsZero() {
			return fmt.Errorf("capacity sample is missing its %s time", timestamp.name)
		}
		if timestamp.value.After(now.Add(maxClockSkew)) {
			return fmt.Errorf("capacity sample %s time is in the future", timestamp.name)
		}
		if now.Sub(timestamp.value) > maxAge {
			return fmt.Errorf("capacity sample %s time is stale", timestamp.name)
		}
	}
	return nil
}

// KubeletReader reads capacity directly from a node's authenticated HTTPS endpoint.
type KubeletReader struct {
	client *http.Client
	maxAge time.Duration
}

// NewKubeletReader uses only the supplied CA and bearer credentials. API-server
// client certificates, auth plugins, impersonation, proxies, and server-name
// overrides are intentionally not copied to the node connection.
func NewKubeletReader(config *rest.Config, requestTimeout, maxAge time.Duration) (*KubeletReader, error) {
	if config == nil {
		return nil, fmt.Errorf("kubelet configuration is required")
	}
	if config.Insecure {
		return nil, fmt.Errorf("kubelet TLS certificate verification is required")
	}
	if requestTimeout <= 0 || maxAge <= 0 {
		return nil, fmt.Errorf("kubelet request timeout and maximum sample age must be positive")
	}
	tlsConfig, err := transport.TLSConfigFor(&transport.Config{
		TLS: transport.TLSConfig{CAFile: config.CAFile, CAData: config.CAData},
	})
	if err != nil {
		return nil, fmt.Errorf("configure kubelet TLS: %w", err)
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.Proxy = nil
	httpTransport.TLSClientConfig = tlsConfig

	// Prefer the rotating token file over an initial in-memory token. The
	// client-go transport periodically reloads projected ServiceAccount tokens.
	token := config.BearerToken
	if config.BearerTokenFile != "" {
		token = ""
	}
	roundTripper, err := transport.NewBearerAuthWithRefreshRoundTripper(token, config.BearerTokenFile, httpTransport)
	if err != nil {
		return nil, fmt.Errorf("configure kubelet bearer token: %w", err)
	}
	return &KubeletReader{
		client: &http.Client{
			Transport:     roundTripper,
			Timeout:       requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		maxAge: maxAge,
	}, nil
}

// Read fetches only imageFs fields from /stats/summary. Missing data fails closed;
// usedBytes is deliberately ignored because it is not filesystem free space.
func (r *KubeletReader) Read(ctx context.Context, node *corev1.Node) (CapacitySample, error) {
	endpoint, err := kubeletEndpoint(node)
	if err != nil {
		return CapacitySample{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return CapacitySample{}, fmt.Errorf("create kubelet capacity request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := r.client.Do(request)
	if err != nil {
		return CapacitySample{}, fmt.Errorf("read kubelet capacity for node %q: %w", node.Name, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return CapacitySample{}, fmt.Errorf("kubelet capacity for node %q returned HTTP %d", node.Name, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxSummaryBytes+1))
	if err != nil {
		return CapacitySample{}, fmt.Errorf("read kubelet capacity response: %w", err)
	}
	if len(body) > maxSummaryBytes {
		return CapacitySample{}, fmt.Errorf("kubelet capacity response exceeds %d bytes", maxSummaryBytes)
	}
	// Pointers distinguish an absent measurement from a valid zero free bytes.
	var summary struct {
		Node struct {
			NodeName string `json:"nodeName"`
			Runtime  struct {
				ImageFS *struct {
					CapacityBytes  *int64     `json:"capacityBytes"`
					AvailableBytes *int64     `json:"availableBytes"`
					Time           *time.Time `json:"time"`
				} `json:"imageFs"`
			} `json:"runtime"`
		} `json:"node"`
	}
	if err := json.Unmarshal(body, &summary); err != nil {
		return CapacitySample{}, fmt.Errorf("decode kubelet capacity response: %w", err)
	}
	filesystem := summary.Node.Runtime.ImageFS
	if filesystem == nil || filesystem.CapacityBytes == nil || filesystem.AvailableBytes == nil || filesystem.Time == nil {
		return CapacitySample{}, fmt.Errorf("kubelet response is missing image filesystem capacity, available bytes, or time")
	}
	now := time.Now()
	sample := CapacitySample{
		NodeName:       summary.Node.NodeName,
		ObservedAt:     *filesystem.Time,
		ReadAt:         now,
		CapacityBytes:  *filesystem.CapacityBytes,
		AvailableBytes: *filesystem.AvailableBytes,
	}
	if err := sample.Validate(node.Name, now, r.maxAge); err != nil {
		return CapacitySample{}, err
	}
	return sample, nil
}

func kubeletEndpoint(node *corev1.Node) (string, error) {
	if node == nil || node.Name == "" {
		return "", fmt.Errorf("a named node is required to read kubelet capacity")
	}
	var host string
	for _, addressType := range []corev1.NodeAddressType{corev1.NodeInternalIP, corev1.NodeInternalDNS, corev1.NodeHostName} {
		for _, address := range node.Status.Addresses {
			if address.Type == addressType && address.Address != "" {
				host = address.Address
				break
			}
		}
		if host != "" {
			break
		}
	}
	if host == "" {
		return "", fmt.Errorf("node %q has no internal address or hostname", node.Name)
	}
	port := node.Status.DaemonEndpoints.KubeletEndpoint.Port
	if port == 0 {
		port = 10250
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("node %q has invalid kubelet port %d", node.Name, port)
	}
	endpoint := url.URL{Scheme: "https", Host: net.JoinHostPort(host, strconv.Itoa(int(port))), Path: "/stats/summary"}
	return endpoint.String(), nil
}
