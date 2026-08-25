//go:build linux

package ociobjectstore

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// preallocateFile reserves backing storage for size bytes when the filesystem
// supports fallocate. Unsupported filesystems fall back to a sparse truncate;
// allocation failures such as ENOSPC are returned to the caller.
func preallocateFile(file *os.File, size int64) (bool, error) {
	if size == 0 {
		return false, file.Truncate(0)
	}

	for {
		err := unix.Fallocate(int(file.Fd()), 0, 0, size)
		switch {
		case err == nil:
			return true, file.Truncate(size)
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.ENOSYS), errors.Is(err, unix.EINVAL):
			return false, file.Truncate(size)
		default:
			return false, err
		}
	}
}
