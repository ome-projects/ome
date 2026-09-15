package waitsource

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("PRIVATE response-body credential")
}
func TestUnreadableBodyNeverReturnsRawReaderError(t *testing.T) {
	body := &trackedBody{Reader: errorReader{}}
	bounded := &boundedBody{body: body, remaining: responseByteLimit + 1}
	_, err := bounded.Read(make([]byte, 16))
	require.Error(t, err)
	require.Equal(t, "WaitResponseUnreadable", err.Error())
	require.Equal(t, int32(1), body.closed.Load())
	require.NoError(t, bounded.Close())
	require.Equal(t, int32(1), body.closed.Load())
}
func TestBoundedTransportClosesOversizeAndFailureBodies(t *testing.T) {
	for _, tc := range []struct {
		name         string
		length       int64
		transportErr error
		response     bool
		body         bool
	}{
		{"length", responseByteLimit + 1, nil, true, true},
		{"transport", -1, errors.New("PRIVATE"), true, true},
		{"nilresponse", 0, nil, false, false},
		{"nilbody", 0, nil, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader("PRIVATE")}
			transport := boundedTransport{base: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				if !tc.response {
					return nil, tc.transportErr
				}
				response := &http.Response{Request: r, ContentLength: tc.length}
				if tc.body {
					response.Body = body
				}
				return response, tc.transportErr
			})}
			request, err := http.NewRequestWithContext(context.Background(), "GET", "http://localhost", nil)
			require.NoError(t, err)
			response, err := transport.RoundTrip(request)
			require.Error(t, err)
			require.Nil(t, response)
			if tc.body {
				require.Equal(t, int32(1), body.closed.Load())
				require.Zero(t, body.read.Load())
			}
		})
	}
}
func TestExactLimitValidBodyIsAcceptedAndDetached(t *testing.T) {
	jsonBody := `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"uid","resourceVersion":"rv"}}`
	body := &trackedBody{Reader: strings.NewReader(jsonBody + strings.Repeat(" ", int(responseByteLimit)-len(jsonBody)))}
	cfg := &rest.Config{Host: "http://localhost", Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Request: r, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body, ContentLength: -1}, nil
	})}
	source, err := NewInferenceService(cfg, "work", "service")
	require.NoError(t, err)
	snapshot, err := source.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, "service", snapshot.Value.Name)
	require.Equal(t, responseByteLimit, body.read.Load())
	require.Equal(t, int32(1), body.closed.Load())
	v := wireService()
	snapshot, err = source.Decode(v)
	require.NoError(t, err)
	snapshot.Value.Name = "changed"
	require.Equal(t, "service", v.Name)
}
func TestBodyLimitRemainsClosedOnRepeatedReads(t *testing.T) {
	body := &trackedBody{Reader: strings.NewReader("xxxx")}
	bounded := &boundedBody{body: body, remaining: 3}
	n, err := bounded.Read(make([]byte, 5))
	require.Zero(t, n)
	require.Equal(t, errBodyLimit, err)
	n, err = bounded.Read(make([]byte, 5))
	require.Zero(t, n)
	require.Equal(t, errBodyLimit, err)
	require.Equal(t, int64(3), body.read.Load())
	require.Equal(t, int32(1), body.closed.Load())
	_, err = io.ReadAll(bounded)
	require.Error(t, err)
}
func TestSourceRejectsMissingWatchVersionAndMalformedHost(t *testing.T) {
	source, err := NewInferenceService(&rest.Config{Host: "http://localhost"}, "work", "service")
	require.NoError(t, err)
	_, err = source.Watch(context.Background(), "")
	require.Error(t, err)
	_, err = NewInferenceService(&rest.Config{Host: "http://[invalid"}, "work", "service")
	require.Error(t, err)
}
