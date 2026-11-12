package holds

import "context"

// unlessServing returns the wait a hold would report, or a release when the
// row's pod set is already fully serving: the bookkeeping the hold read may be
// stale, and a stale reading cannot stop a workload that is in rotation. wait
// is called only for a row that is not serving.
func unlessServing(ctx context.Context, in PassInput, row Row, wait func() reading) (reading, error) {
	serving, err := in.serving(ctx, row)
	if err != nil {
		return reading{}, err
	}
	if serving {
		return reading{release: true}, nil
	}
	return wait(), nil
}
