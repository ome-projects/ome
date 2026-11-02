package validation

import "regexp"

const (
	IsvcNameFmt = "[a-z]([-a-z0-9]*[a-z0-9])?"
)

var IsvcNameRegex = regexp.MustCompile("^" + IsvcNameFmt + "$")
