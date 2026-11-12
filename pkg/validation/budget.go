// Shared semantic validation for rolling-update budget knobs
// (maxSurge / maxUnavailable *intstr.IntOrString).
//
// The CRD schema's XIntOrString marker only gates the structural shape
// (must parse as integer or string). These helpers add the semantic
// checks the apiserver can't express — integers must be >= 0, strings
// must be "<n>%" with 0 <= n <= 100 — parameterized by the full field
// path so every caller (per-Component lifecycle, rollout groups) reports
// the exact location of the invalid value. Anything outside those forms
// resolves to zero in the runtime parser (ScaledCountFromIntOrString),
// silently deleting the budget, so admission rejects it instead.
package validation

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"
)

// The two ways a budget string fails to read as "<n>%".
var (
	errNoPercentSign     = errors.New("budget string does not end in %")
	errPercentNotANumber = errors.New("budget percent is not a number")
)

// intOrPercent reads a budget value as a number: an integer as itself, a
// "<n>%" string as n with percent set. Each caller adds its own range rule.
func intOrPercent(v *intstr.IntOrString) (n int, percent bool, err error) {
	if v.Type == intstr.Int {
		return v.IntValue(), false, nil
	}
	if !strings.HasSuffix(v.StrVal, "%") {
		return 0, true, errNoPercentSign
	}
	n, err = strconv.Atoi(strings.TrimSuffix(v.StrVal, "%"))
	if err != nil {
		return 0, true, errPercentNotANumber
	}
	return n, true, nil
}

// validateBudgetIntOrString enforces the semantic rules for a
// rolling-update budget value:
//   - integer values >= 0
//   - string values match "<n>%" with 0 <= n <= 100
//
// path is the full field path (e.g.
// "spec.rollout.groups[0].rollingUpdate.maxSurge") reported verbatim in
// every error. Returns nil for nil inputs — nil means "use default",
// and every defaulting layer fills a non-zero budget.
func validateBudgetIntOrString(path string, v *intstr.IntOrString) error {
	if v == nil {
		return nil
	}
	n, percent, err := intOrPercent(v)
	switch {
	case errors.Is(err, errNoPercentSign):
		return fmt.Errorf("%s=%q must be an integer count or a percent string like \"25%%\" (%s)",
			path, v.StrVal, ReasonInvalidRollingUpdatePercent)
	case err != nil:
		return fmt.Errorf("%s=%q has a malformed percent value (%s)",
			path, v.StrVal, ReasonInvalidRollingUpdatePercent)
	case percent && (n < 0 || n > 100):
		return fmt.Errorf("%s=%q percent must be between 0%% and 100%% (%s)",
			path, v.StrVal, ReasonInvalidRollingUpdatePercent)
	case !percent && n < 0:
		return fmt.Errorf("%s=%d must be >= 0 (%s)",
			path, n, ReasonInvalidRollingUpdateInteger)
	}
	return nil
}

// budgetResolvesToZero reports whether the runtime budget resolver
// (ScaledCountFromIntOrString) turns v into a zero budget: an explicit
// zero ("0%" or integer 0), a negative integer and a string it cannot
// read all do. A positive percent is never zero here: its scaled value
// depends on the replica count, which admission cannot know. nil is not
// zero: nil means "use default", and every defaulting layer fills a
// non-zero budget.
func budgetResolvesToZero(v *intstr.IntOrString) bool {
	if v == nil {
		return false
	}
	n, _, err := intOrPercent(v)
	return err != nil || n <= 0
}
