//go:build linux || darwin

package mutate

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type promptSignal struct {
	once  sync.Once
	ready chan struct{}
}

func (s *promptSignal) Write(p []byte) (int, error) {
	s.once.Do(func() { close(s.ready) })
	return len(p), nil
}

func TestNativeConfirmationRestoresTerminalOnAnswerAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "yes", true: "cancel"}[cancel], func(t *testing.T) {
			master, slave := openTestPTY(t)
			defer master.Close()
			defer slave.Close()
			before, err := getTerminalState(int(slave.Fd()))
			require.NoError(t, err)
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			prompt := &promptSignal{ready: make(chan struct{})}
			done := make(chan error, 1)
			go func() { done <- Confirm(ctx, slave, prompt, false) }()
			select {
			case <-prompt.ready:
			case err := <-done:
				t.Fatalf("confirmation failed before native prompt: %v", err)
			case <-time.After(time.Second):
				t.Fatal("native prompt did not appear")
			}
			if cancel {
				stop()
			} else {
				_, err = io.WriteString(master, "yes\n")
				require.NoError(t, err)
			}
			select {
			case err = <-done:
				if cancel {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(time.Second):
				t.Fatal("confirmation reader did not exit")
			}
			after, err := getTerminalState(int(slave.Fd()))
			require.NoError(t, err)
			require.Equal(t, before, after, "terminal state must restore before any patch")
		})
	}
}
