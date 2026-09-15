package mutate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfirmationNeverAcceptsNonTTYDeclineEOFOrCancellation(t *testing.T) {
	var out bytes.Buffer
	require.Error(t, Confirm(context.Background(), bytes.NewBufferString("yes\n"), &out, false))
	require.NoError(t, Confirm(context.Background(), nil, &out, true))
	for _, answer := range []string{"no\n", "\n", "yesterday\n", "yes"} {
		reader := bytes.NewBufferString(answer)
		err := readConfirmation(context.Background(), &out, func(context.Context) (byte, error) { return reader.ReadByte() })
		require.Error(t, err)
	}
	for _, answer := range []string{"yes\n", "Y\r", "yeX\b s\n"} {
		reader := bytes.NewBufferString(answer)
		err := readConfirmation(context.Background(), &out, func(context.Context) (byte, error) { return reader.ReadByte() })
		if answer == "yeX\b s\n" {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, Confirm(ctx, nil, &out, true), context.Canceled)
	require.ErrorIs(t, readConfirmation(ctx, &out, func(context.Context) (byte, error) { return 0, io.EOF }), context.Canceled)
	require.Error(t, readConfirmation(context.Background(), errorWriter{}, func(context.Context) (byte, error) { t.Fatal("must not read after prompt failure"); return 0, nil }))
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("SECRET_WRITER_DETAIL") }

func TestConfirmationControlInputAndEveryOutputFailureAreClosed(t *testing.T) {
	for _, answer := range []string{string([]byte{3}), string([]byte{4}), string([]byte{27}), "\xff", strings.Repeat("y", 17)} {
		input := bytes.NewBufferString(answer)
		err := readConfirmation(context.Background(), io.Discard, func(context.Context) (byte, error) { return input.ReadByte() })
		require.Error(t, err)
	}
	for _, answer := range []string{"\byes\n", "yeX\bs\n", "YES\n"} {
		input := bytes.NewBufferString(answer)
		require.NoError(t, readConfirmation(context.Background(), io.Discard, func(context.Context) (byte, error) { return input.ReadByte() }))
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		require.ErrorIs(t, readConfirmation(context.Background(), io.Discard, func(context.Context) (byte, error) { return 0, cause }), cause)
	}
	for limit := 0; limit < 5; limit++ {
		input := bytes.NewBufferString("yes\n")
		err := readConfirmation(context.Background(), &failAfterWriter{remaining: limit}, func(context.Context) (byte, error) { return input.ReadByte() })
		require.Error(t, err)
		require.NotContains(t, err.Error(), "SECRET_WRITER_DETAIL")
	}
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer reader.Close()
	defer writer.Close()
	require.ErrorIs(t, Confirm(context.Background(), reader, io.Discard, false), ErrConfirmation)
}

type failAfterWriter struct{ remaining int }

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.remaining == 0 {
		return 0, errors.New("SECRET_WRITER_DETAIL")
	}
	w.remaining--
	return len(p), nil
}
