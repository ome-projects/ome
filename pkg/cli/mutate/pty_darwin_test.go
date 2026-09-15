package mutate

import (
	"bytes"
	"os"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func openTestPTY(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	require.NoError(t, err)
	for _, command := range []uintptr{unix.TIOCPTYGRANT, unix.TIOCPTYUNLK} {
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), command, 0)
		require.Zero(t, errno)
	}
	var name [128]byte
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), unix.TIOCPTYGNAME, uintptr(unsafe.Pointer(&name[0])))
	require.Zero(t, errno)
	end := bytes.IndexByte(name[:], 0)
	require.Positive(t, end)
	slave, err := os.OpenFile(string(name[:end]), os.O_RDWR|syscall.O_NOCTTY, 0)
	require.NoError(t, err)
	return master, slave
}
