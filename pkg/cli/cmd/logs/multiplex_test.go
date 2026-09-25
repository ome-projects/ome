package logs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rc(s string) io.ReadCloser { return io.NopCloser(strings.NewReader(s)) }

func TestMultiplexPrefixesEveryLine(t *testing.T) {
	var out bytes.Buffer
	engine := newObservedReadCloser(strings.NewReader("a\nb\n"))
	decoder := newObservedReadCloser(strings.NewReader("c\n"))
	err := multiplexWithBackground([]namedStream{
		{Prefix: "[engine/p1] ", Reader: engine},
		{Prefix: "[decoder/p2] ", Reader: decoder},
	}, &out)
	require.NoError(t, err)
	assertReaderClosed(t, engine.closed)
	assertReaderClosed(t, decoder.closed)
	got := out.String()
	assert.Contains(t, got, "[engine/p1] a\n")
	assert.Contains(t, got, "[engine/p1] b\n")
	assert.Contains(t, got, "[decoder/p2] c\n")
}

func TestMultiplexSingleStreamNoPrefix(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, multiplexWithBackground([]namedStream{{Prefix: "", Reader: rc("raw\n")}}, &out))
	assert.Equal(t, "raw\n", out.String())
}

func TestMultiplexDoesNotInterleaveWithinALine(t *testing.T) {
	long := strings.Repeat("x", 64*1024) // exceeds default bufio.Scanner token size guard
	var out bytes.Buffer
	require.NoError(t, multiplexWithBackground([]namedStream{{Prefix: "[e/p] ", Reader: rc(long + "\n")}}, &out))
	assert.Equal(t, "[e/p] "+long+"\n", out.String())
}

func TestMultiplexReaderErrorClosesSibling(t *testing.T) {
	want := errors.New("reader failed")
	assertMultiplexFailureClosesSibling(t, errorReader{err: want}, io.Discard, want)
}

func TestMultiplexWriterErrorClosesSibling(t *testing.T) {
	want := errors.New("writer failed")
	assertMultiplexFailureClosesSibling(t, strings.NewReader("line\n"), errorWriter{err: want}, want)
}

func TestMultiplexOversizedLineClosesSibling(t *testing.T) {
	assertMultiplexFailureClosesSibling(t, strings.NewReader(strings.Repeat("x", 1024*1024+1)), io.Discard, bufio.ErrTooLong)
}

func TestMultiplexParentCancellationClosesEveryReader(t *testing.T) {
	parent, stopParent := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancelCause(parent)
	first := newCloseBlockedReader()
	second := newCloseBlockedReader()
	result := make(chan error, 1)
	go func() {
		result <- multiplex(ctx, cancel, []namedStream{{Reader: first}, {Reader: second}}, io.Discard)
	}()
	waitForReadStart(t, first.started)
	waitForReadStart(t, second.started)
	stopParent()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("multiplex did not return after parent cancellation")
	}
	assert.Equal(t, int32(1), first.closeCount.Load())
	assert.Equal(t, int32(1), second.closeCount.Load())
}

func assertMultiplexFailureClosesSibling(t *testing.T, reader io.Reader, out io.Writer, want error) {
	t.Helper()
	quiet := &closeBlockedReader{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
		err:     errors.New("sibling closed"),
	}
	defer quiet.Close()
	trigger := newObservedReadCloser(gatedReader{Reader: reader, ready: quiet.started})
	result := make(chan error, 1)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		// Put the quiet stream first so its close-induced error cannot mask
		// the error that triggered cancellation.
		result <- multiplex(ctx, cancel, []namedStream{{Reader: quiet}, {Reader: trigger}}, out)
	}()
	select {
	case err := <-result:
		assertReaderClosed(t, quiet.closed)
		assertReaderClosed(t, trigger.closed)
		require.ErrorIs(t, err, want)
		require.ErrorIs(t, context.Cause(ctx), want)
	case <-time.After(time.Second):
		t.Fatal("multiplex did not close its quiet sibling and return within one second")
	}
}

type closeBlockedReader struct {
	started    chan struct{}
	closed     chan struct{}
	once       sync.Once
	err        error
	closeCount atomic.Int32
}

func newCloseBlockedReader() *closeBlockedReader {
	return &closeBlockedReader{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
		err:     errors.New("reader closed"),
	}
}

func (r *closeBlockedReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.closed
	return 0, r.err
}

func (r *closeBlockedReader) Close() error {
	r.once.Do(func() {
		r.closeCount.Add(1)
		close(r.closed)
	})
	return nil
}

type observedReadCloser struct {
	io.Reader
	closed chan struct{}
	once   sync.Once
}

func newObservedReadCloser(reader io.Reader) *observedReadCloser {
	return &observedReadCloser{Reader: reader, closed: make(chan struct{})}
}

func (r *observedReadCloser) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

func assertReaderClosed(t *testing.T, closed <-chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	default:
		t.Error("multiplex returned without closing a reader")
	}
}

type gatedReader struct {
	io.Reader
	ready <-chan struct{}
}

func (r gatedReader) Read(p []byte) (int, error) {
	<-r.ready
	return r.Reader.Read(p)
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

func multiplexWithBackground(streams []namedStream, out io.Writer) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	return multiplex(ctx, cancel, streams, out)
}

func waitForReadStart(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}
}
