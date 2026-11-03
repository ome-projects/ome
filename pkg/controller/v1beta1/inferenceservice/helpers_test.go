package inferenceservice

import "sigs.k8s.io/ome/pkg/utils/testing/fakeclient"

// refusedObjectWrites returns the options that refuse every Create, Update,
// Patch and Delete with err, and a count of the writes refused so far.
// Status writes still land, so a pass may record its conditions while the
// test proves it wrote nothing else.
func refusedObjectWrites(err error) (opts []fakeclient.Option, count func() int) {
	var counters []*fakeclient.Calls
	for _, verb := range []fakeclient.Verb{fakeclient.Create, fakeclient.Update, fakeclient.Patch, fakeclient.Delete} {
		calls := fakeclient.CountingVerb(verb)
		counters = append(counters, calls)
		opts = append(opts, calls, fakeclient.FailingVerb(verb, fakeclient.EveryCall, err))
	}
	return opts, func() int {
		n := 0
		for _, c := range counters {
			n += c.Count()
		}
		return n
	}
}
