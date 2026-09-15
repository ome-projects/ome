package mutate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var ErrConfirmation = errors.New("action not confirmed; noninteractive input requires --yes")

// Confirm never starts an uncancellable reader goroutine. Native terminal
// input is polled in raw mode and restored before the command can patch.
func Confirm(ctx context.Context, in io.Reader, out io.Writer, yes bool) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	if yes {
		return nil
	}
	file, ok := in.(*os.File)
	if !ok {
		return ErrConfirmation
	}
	restore, err := makeTerminalRaw(file)
	if err != nil {
		return ErrConfirmation
	}
	defer func() {
		if e := restore(); e != nil && err == nil {
			err = errors.New("restore confirmation terminal failed; no request submitted")
		}
	}()
	return readConfirmation(ctx, out, func(ctx context.Context) (byte, error) { return terminalReadByte(ctx, file) })
}

func readConfirmation(ctx context.Context, out io.Writer, read func(context.Context) (byte, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := fmt.Fprint(out, "Confirm this exact action? [y/N] "); err != nil {
		return errors.New("write confirmation prompt failed")
	}
	answer := []byte{}
	for len(answer) <= 16 {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, err := read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return ErrConfirmation
		}
		switch value {
		case '\r', '\n':
			if _, err = fmt.Fprintln(out); err != nil {
				return errors.New("write confirmation response failed")
			}
			text := strings.ToLower(string(answer))
			if text == "yes" || text == "y" {
				return nil
			}
			return ErrConfirmation
		case 3:
			return context.Canceled
		case 4:
			return ErrConfirmation
		case 8, 127:
			if len(answer) > 0 {
				answer = answer[:len(answer)-1]
			}
		default:
			if value < 32 || value > 126 {
				return ErrConfirmation
			}
			answer = append(answer, value)
			if _, err = out.Write([]byte{value}); err != nil {
				return errors.New("write confirmation response failed")
			}
		}
	}
	return ErrConfirmation
}
