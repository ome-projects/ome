package modelagent

import "fmt"

// Download scheduling policy affects queued remote transfers, never cleanup,
// reuse, model eligibility, or cancellation. It is fixed before workers start.
const (
	DownloadSchedulingPolicyPriority = "priority"
	DownloadSchedulingPolicyFIFO     = "fifo"
)

// GopherOption configures a Gopher before any task can be dispatched.
type GopherOption func(*Gopher) error

// WithDownloadSchedulingPolicy selects download ordering for this agent.
// Changing the policy requires constructing a new agent (a Pod rollout).
func WithDownloadSchedulingPolicy(policy string) GopherOption {
	return func(gopher *Gopher) error {
		if err := ValidateDownloadSchedulingPolicy(policy); err != nil {
			return err
		}
		gopher.taskQueue.downloadSchedulingPolicy = policy
		return nil
	}
}

// ValidateDownloadSchedulingPolicy rejects misspellings instead of silently
// enabling priority when FIFO was requested.
func ValidateDownloadSchedulingPolicy(policy string) error {
	switch policy {
	case DownloadSchedulingPolicyPriority, DownloadSchedulingPolicyFIFO:
		return nil
	default:
		return fmt.Errorf("invalid download scheduling policy %q: expected priority or fifo", policy)
	}
}
