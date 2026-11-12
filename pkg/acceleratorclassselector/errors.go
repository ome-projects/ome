package acceleratorclassselector

import (
	"fmt"
)

// AcceleratorNotFoundError indicates that a specified accelerator class doesn't exist.
type AcceleratorNotFoundError struct {
	AcceleratorClassName string
}

// Error implements the error interface.
func (e *AcceleratorNotFoundError) Error() string {
	return fmt.Sprintf("accelerator class %s not found at cluster scope",
		e.AcceleratorClassName)
}
