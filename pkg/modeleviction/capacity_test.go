package modeleviction

import (
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestCapacitySampleValidate(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	valid := CapacitySample{NodeName: "node-a", ObservedAt: now, ReadAt: now, CapacityBytes: 100, AvailableBytes: 10}
	tests := []struct {
		name    string
		change  func(*CapacitySample)
		wantErr bool
	}{
		{name: "valid"},
		{name: "full disk", change: func(s *CapacitySample) { s.AvailableBytes = 0 }},
		{name: "empty disk", change: func(s *CapacitySample) { s.AvailableBytes = s.CapacityBytes }},
		{name: "maximum clock skew", change: func(s *CapacitySample) { s.ObservedAt = now.Add(30 * time.Second) }},
		{name: "age boundary", change: func(s *CapacitySample) { s.ObservedAt = now.Add(-time.Minute) }},
		{name: "wrong node", change: func(s *CapacitySample) { s.NodeName = "node-b" }, wantErr: true},
		{name: "missing node", change: func(s *CapacitySample) { s.NodeName = "" }, wantErr: true},
		{name: "zero capacity", change: func(s *CapacitySample) { s.CapacityBytes = 0 }, wantErr: true},
		{name: "negative capacity", change: func(s *CapacitySample) { s.CapacityBytes = -1 }, wantErr: true},
		{name: "negative available", change: func(s *CapacitySample) { s.AvailableBytes = -1 }, wantErr: true},
		{name: "available exceeds capacity", change: func(s *CapacitySample) { s.AvailableBytes = 101 }, wantErr: true},
		{name: "missing observation", change: func(s *CapacitySample) { s.ObservedAt = time.Time{} }, wantErr: true},
		{name: "future observation", change: func(s *CapacitySample) { s.ObservedAt = now.Add(31 * time.Second) }, wantErr: true},
		{name: "stale observation", change: func(s *CapacitySample) { s.ObservedAt = now.Add(-time.Minute - time.Nanosecond) }, wantErr: true},
		{name: "missing read time", change: func(s *CapacitySample) { s.ReadAt = time.Time{} }, wantErr: true},
		{name: "future read time", change: func(s *CapacitySample) { s.ReadAt = now.Add(31 * time.Second) }, wantErr: true},
		{name: "stale read time", change: func(s *CapacitySample) { s.ReadAt = now.Add(-time.Minute - time.Nanosecond) }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sample := valid
			if tt.change != nil {
				tt.change(&sample)
			}
			if err := sample.Validate("node-a", now, time.Minute); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	if err := valid.Validate("", now, time.Minute); err == nil {
		t.Fatal("accepted an empty expected node name")
	}
	if err := valid.Validate("node-a", now, 0); err == nil {
		t.Fatal("accepted a nonpositive maximum age")
	}
}

func TestKubeletReaderReadsImageFS(t *testing.T) {
	observed := time.Now().UTC().Truncate(time.Second)
	_, config, node := capacityTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stats/summary" || r.Method != http.MethodGet {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer sample-token" {
			t.Errorf("unexpected authorization header")
		}
		_, _ = io.WriteString(w, capacitySummary(observed, 0))
	}))
	config.BearerToken = "sample-token"
	// These API-server-specific settings must never be forwarded to kubelets.
	config.Username, config.Password = "api-user", "api-password"
	config.CertData, config.KeyData = []byte("unused certificate"), []byte("unused key")
	config.ServerName = "api.example.invalid"
	config.ExecProvider = &clientcmdapi.ExecConfig{Command: "must-not-run"}
	config.Proxy = func(*http.Request) (*url.URL, error) { return nil, fmt.Errorf("must not proxy kubelet traffic") }
	node.Status.Addresses = append([]corev1.NodeAddress{{Type: corev1.NodeHostName, Address: "wrong.invalid"}, {Type: corev1.NodeInternalDNS, Address: "wrong.invalid"}}, node.Status.Addresses...)
	reader, err := NewKubeletReader(config, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	sample, err := reader.Read(context.Background(), node)
	if err != nil {
		t.Fatal(err)
	}
	if sample.NodeName != node.Name || !sample.ObservedAt.Equal(observed) || sample.CapacityBytes != 100 || sample.AvailableBytes != 0 {
		t.Fatalf("unexpected sample: %+v", sample)
	}
	if sample.ReadAt.Before(before) || sample.ReadAt.After(time.Now()) {
		t.Fatalf("invalid read time: %v", sample.ReadAt)
	}
}

func TestKubeletReaderRejectsInvalidResponses(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := capacitySummary(now, 10)
	tests := []struct {
		name, body string
		status     int
	}{
		{name: "stale", body: capacitySummary(now.Add(-2*time.Minute), 10)},
		{name: "future", body: capacitySummary(now.Add(time.Minute), 10)},
		{name: "missing node", body: `{}`},
		{name: "missing runtime", body: `{"node":{"nodeName":"node-a"}}`},
		{name: "missing image filesystem", body: `{"node":{"nodeName":"node-a","runtime":{}}}`},
		{name: "missing capacity", body: strings.Replace(valid, `"capacityBytes":100,`, "", 1)},
		{name: "null capacity", body: strings.Replace(valid, `"capacityBytes":100`, `"capacityBytes":null`, 1)},
		{name: "zero capacity", body: strings.Replace(valid, `"capacityBytes":100`, `"capacityBytes":0`, 1)},
		{name: "missing available", body: strings.Replace(valid, `"availableBytes":10,`, "", 1)},
		{name: "null available", body: strings.Replace(valid, `"availableBytes":10`, `"availableBytes":null`, 1)},
		{name: "negative available", body: capacitySummary(now, -1)},
		{name: "excess available", body: capacitySummary(now, 101)},
		{name: "missing time", body: strings.Replace(valid, `"time":"`+now.Format(time.RFC3339Nano)+`",`, "", 1)},
		{name: "missing node name", body: strings.Replace(valid, `"nodeName":"node-a",`, "", 1)},
		{name: "mismatched node", body: strings.Replace(valid, "node-a", "node-b", 1)},
		{name: "malformed", body: "{"},
		{name: "trailing document", body: valid + ` {}`},
		{name: "oversized", body: valid + strings.Repeat(" ", 4<<20)},
		{name: "unauthorized", status: http.StatusUnauthorized, body: valid},
		{name: "server error", status: http.StatusInternalServerError, body: valid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, config, node := capacityTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.status != 0 {
					w.WriteHeader(tt.status)
				}
				_, _ = io.WriteString(w, tt.body)
			}))
			reader, err := NewKubeletReader(config, time.Second, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reader.Read(context.Background(), node); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
}

func TestKubeletReaderTimeoutAndCancellation(t *testing.T) {
	_, config, node := capacityTestServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	reader, err := NewKubeletReader(config, 50*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(context.Background(), node); err == nil {
		t.Fatal("expected request timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.Read(ctx, node); err == nil {
		t.Fatal("expected context cancellation")
	}
}

func TestKubeletReaderRejectsUntrustedCertificate(t *testing.T) {
	_, _, node := capacityTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { t.Error("reached untrusted server") }))
	reader, err := NewKubeletReader(&rest.Config{}, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(context.Background(), node); err == nil {
		t.Fatal("accepted untrusted certificate")
	}
}

func TestKubeletReaderDoesNotFollowRedirects(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("followed redirect") }))
	t.Cleanup(destination.Close)
	_, config, node := capacityTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	reader, err := NewKubeletReader(config, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(context.Background(), node); err == nil {
		t.Fatal("accepted redirect")
	}
}

func TestKubeletReaderTokenFile(t *testing.T) {
	_, config, node := capacityTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer file-token" {
			t.Error("did not use token file")
		}
		_, _ = io.WriteString(w, capacitySummary(time.Now(), 10))
	}))
	config.BearerTokenFile = filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(config.BearerTokenFile, []byte("file-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config.BearerToken = "old-inline-token"
	reader, err := NewKubeletReader(config, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	config.BearerTokenFile = filepath.Join(t.TempDir(), "missing")
	if _, err := NewKubeletReader(config, time.Second, time.Minute); err == nil {
		t.Fatal("accepted unreadable token file")
	}
}

func TestKubeletReaderConfiguration(t *testing.T) {
	tests := []struct {
		name            string
		config          *rest.Config
		timeout, maxAge time.Duration
	}{
		{name: "nil config", timeout: time.Second, maxAge: time.Minute},
		{name: "insecure TLS", config: &rest.Config{TLSClientConfig: rest.TLSClientConfig{Insecure: true}}, timeout: time.Second, maxAge: time.Minute},
		{name: "invalid CA", config: &rest.Config{TLSClientConfig: rest.TLSClientConfig{CAData: []byte("invalid")}}, timeout: time.Second, maxAge: time.Minute},
		{name: "zero timeout", config: &rest.Config{}, maxAge: time.Minute},
		{name: "zero max age", config: &rest.Config{}, timeout: time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewKubeletReader(tt.config, tt.timeout, tt.maxAge); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}

func TestKubeletReaderNodeAddresses(t *testing.T) {
	for _, addressType := range []corev1.NodeAddressType{corev1.NodeInternalIP, corev1.NodeInternalDNS, corev1.NodeHostName} {
		t.Run(string(addressType), func(t *testing.T) {
			_, config, node := capacityTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, capacitySummary(time.Now(), 10))
			}))
			node.Status.Addresses[0].Type = addressType
			reader, err := NewKubeletReader(config, time.Second, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reader.Read(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			if _, err := reader.Read(context.Background(), nil); err == nil {
				t.Fatal("accepted nil node")
			}
			node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeExternalIP, Address: "127.0.0.1"}}
			if _, err := reader.Read(context.Background(), node); err == nil {
				t.Fatal("accepted external address")
			}
		})
	}
}

func TestKubeletEndpoint(t *testing.T) {
	tests := []struct {
		name      string
		addresses []corev1.NodeAddress
		port      int32
		want      string
	}{
		{name: "default port", addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}}, want: "https://10.0.0.1:10250/stats/summary"},
		{name: "IPv6", addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "2001:db8::1"}}, port: 10251, want: "https://[2001:db8::1]:10251/stats/summary"},
		{name: "DNS preferred over hostname", addresses: []corev1.NodeAddress{{Type: corev1.NodeHostName, Address: "node-host"}, {Type: corev1.NodeInternalDNS, Address: "node.internal"}}, want: "https://node.internal:10250/stats/summary"},
		{name: "empty internal IP falls back", addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP}, {Type: corev1.NodeHostName, Address: "node-host"}}, want: "https://node-host:10250/stats/summary"},
		{name: "missing address"},
		{name: "negative port", addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}}, port: -1},
		{name: "port overflow", addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}}, port: 65536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Addresses: tt.addresses, DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: tt.port}}}}
			endpoint, err := kubeletEndpoint(node)
			if tt.want == "" {
				if err == nil {
					t.Fatal("accepted invalid endpoint")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if endpoint != tt.want {
				t.Fatalf("endpoint = %q, want %q", endpoint, tt.want)
			}
		})
	}
}

func capacitySummary(observed time.Time, available int64) string {
	// usedBytes is deliberately invalid: filesystem pressure uses availableBytes.
	return fmt.Sprintf(`{"node":{"nodeName":"node-a","runtime":{"imageFs":{"capacityBytes":100,"availableBytes":%d,"time":"%s","usedBytes":"ignored"}}}}`, available, observed.Format(time.RFC3339Nano))
}

func capacityTestServer(t *testing.T, handler http.Handler) (*httptest.Server, *rest.Config, *corev1.Node) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	address, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, portText, err := net.SplitHostPort(address.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	config := &rest.Config{TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{
		Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: host}},
		DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: int32(port)}},
	}}
	return server, config, node
}
