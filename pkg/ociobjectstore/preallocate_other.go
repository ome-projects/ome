//go:build !linux

package ociobjectstore

import "os"

func preallocateFile(file *os.File, size int64) (bool, error) {
	return false, file.Truncate(size)
}
