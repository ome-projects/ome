//go:build race

package allocbudget

// raceDetector reports whether the race detector is compiled in. Under it
// sync.Pool drops entries at random, so an allocation count is not stable.
const raceDetector = true
