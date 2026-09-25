// Package instanceannouncement validates the durable once-only message
// markers recorded on an OMENative instance status row.
package instanceannouncement

import (
	"regexp"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/safetext"
)

const (
	maxTokenBytes  = 128
	maxMarkerBytes = 2*maxTokenBytes + 1
)

var (
	reasonToken    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,127}$`)
	operationToken = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:+-]{0,127}$`)
	slackToken     = regexp.MustCompile(`(?:xox[a-z]|xapp)-`)
)

// Value is the safe, structured representation of one status marker.
type Value struct {
	Reason  string
	Episode string
}

// Parse accepts bounded Kubernetes-reason and episode tokens. An operation
// marker can legitimately remain after that operation ends until a later
// announcement prunes it, so validation checks its canonical shape rather
// than requiring it to equal the row's current operation.
func Parse(marker string) (Value, bool) {
	if len(marker) > maxMarkerBytes {
		return Value{}, false
	}
	reason, episode, found := strings.Cut(marker, "@")
	if !found || strings.ContainsRune(episode, '@') || len(reason) > maxTokenBytes || len(episode) > maxTokenBytes ||
		!reasonToken.MatchString(reason) || safetext.Sanitize(reason, maxTokenBytes) != reason {
		return Value{}, false
	}
	if strings.HasPrefix(episode, "#") {
		incarnation, err := strconv.ParseInt(strings.TrimPrefix(episode, "#"), 10, 64)
		if err != nil || incarnation < 0 || episode != "#"+strconv.FormatInt(incarnation, 10) {
			return Value{}, false
		}
	} else if !operationToken.MatchString(episode) || slackToken.MatchString(episode) ||
		safetext.Sanitize(episode, maxTokenBytes) != episode {
		return Value{}, false
	}
	return Value{Reason: reason, Episode: episode}, true
}
