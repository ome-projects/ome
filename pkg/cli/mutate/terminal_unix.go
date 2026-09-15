//go:build linux || darwin

package mutate

import (
	"context"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func makeTerminalRaw(file *os.File) (func() error, error) {
	fd := int(file.Fd())
	original, err := getTerminalState(fd)
	if err != nil {
		return nil, err
	}
	raw := *original
	raw.Lflag &^= unix.ECHO | unix.ICANON | unix.ISIG
	raw.Iflag &^= unix.ICRNL
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err = setTerminalState(fd, &raw); err != nil {
		return nil, err
	}
	return func() error { return setTerminalState(fd, original) }, nil
}

func terminalInputReady(file *os.File) (bool, error) {
	values := []unix.PollFd{{Fd: int32(file.Fd()), Events: unix.POLLIN}}
	n, err := unix.Poll(values, 50)
	if err == unix.EINTR {
		return false, nil
	}
	return n > 0, err
}

func terminalReadByte(ctx context.Context, file *os.File) (byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ready, err := terminalInputReady(file)
		if err != nil {
			return 0, err
		}
		if !ready {
			continue
		}
		var value [1]byte
		n, err := file.Read(value[:])
		if err != nil {
			return 0, err
		}
		if n != 1 {
			return 0, io.EOF
		}
		return value[0], nil
	}
}
