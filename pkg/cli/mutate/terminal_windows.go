package mutate

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

var readConsoleInput = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReadConsoleInputW")

func makeTerminalRaw(file *os.File) (func() error, error) {
	handle := windows.Handle(file.Fd())
	var original uint32
	if err := windows.GetConsoleMode(handle, &original); err != nil {
		return nil, err
	}
	raw := original &^ (windows.ENABLE_LINE_INPUT | windows.ENABLE_ECHO_INPUT | windows.ENABLE_PROCESSED_INPUT)
	if err := windows.SetConsoleMode(handle, raw); err != nil {
		return nil, err
	}
	return func() error { return windows.SetConsoleMode(handle, original) }, nil
}

// Consume one signaled console event, not ReadFile: mouse/resize events signal
// the handle too, but cannot satisfy a character read. This command is the sole
// stdin consumer and never starts a competing reader goroutine.
func terminalReadByte(ctx context.Context, file *os.File) (byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		handle := windows.Handle(file.Fd())
		value, err := windows.WaitForSingleObject(handle, 50)
		if err != nil {
			return 0, err
		}
		if value != windows.WAIT_OBJECT_0 {
			continue
		}
		// INPUT_RECORD is a WORD, alignment padding, and a 16-byte event union.
		var record [20]byte
		var count uint32
		ok, _, _ := readConsoleInput.Call(uintptr(handle), uintptr(unsafe.Pointer(&record[0])), 1, uintptr(unsafe.Pointer(&count)))
		if ok == 0 || count != 1 {
			return 0, errors.New("read confirmation console event failed")
		}
		if binary.LittleEndian.Uint16(record[:2]) != 1 || binary.LittleEndian.Uint32(record[4:8]) == 0 {
			continue
		}
		character := binary.LittleEndian.Uint16(record[14:16])
		if character == 0 {
			continue
		}
		if character > 126 {
			return 0, ErrConfirmation
		}
		return byte(character), nil
	}
}
