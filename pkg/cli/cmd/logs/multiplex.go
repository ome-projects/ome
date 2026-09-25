// Package logs implements `kubectl ome logs`: component-aware log streaming
// for the pods behind an InferenceService.
package logs

import (
	"bufio"
	"io"
	"sync"
)

type namedStream struct {
	Prefix string
	Reader io.ReadCloser
}

// multiplex copies every stream to out line-by-line, prefixing each line,
// serialized by a mutex so lines never interleave mid-line. Blocks until all
// workers exit; the first failure closes every reader. Reader close is also
// guaranteed on successful EOF.
func multiplex(streams []namedStream, out io.Writer) error {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		stopOnce sync.Once
		firstErr error
	)
	closed := make([]sync.Once, len(streams))
	closeStream := func(i int) {
		closed[i].Do(func() { _ = streams[i].Reader.Close() })
	}
	closeAll := func(err error) {
		if err == nil {
			return
		}
		stopOnce.Do(func() {
			// Preserve the triggering error before closing siblings unblocks
			// their scanners, which may report errors caused by the close.
			firstErr = err
			for i := range streams {
				closeStream(i)
			}
		})
	}
	for i, s := range streams {
		wg.Add(1)
		go func(i int, s namedStream) {
			defer wg.Done()
			defer closeStream(i)
			scanner := bufio.NewScanner(s.Reader)
			scanner.Buffer(make([]byte, 64*1024), 1024*1024)
			for scanner.Scan() {
				mu.Lock()
				_, werr := io.WriteString(out, s.Prefix+scanner.Text()+"\n")
				mu.Unlock()
				if werr != nil {
					closeAll(werr)
					return
				}
			}
			closeAll(scanner.Err())
		}(i, s)
	}
	wg.Wait()
	return firstErr
}
