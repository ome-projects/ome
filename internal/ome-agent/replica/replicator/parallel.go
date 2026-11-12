package replicator

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

// ReplicationResult is the outcome of moving one object.
type ReplicationResult struct {
	source ociobjectstore.ObjectURI
	target ociobjectstore.ObjectURI
	error  error
}

// objectCopier moves one object at a time and writes the log line for each outcome.
type objectCopier interface {
	copyObject(object common.ReplicationObject) ReplicationResult
	logOutcome(result ReplicationResult)
}

// replicateInParallel moves every object through the copier on `connections` workers, logs the
// progress after each outcome and fails when any object failed. An object whose name ends in
// "/" stands for a directory and is not copied.
func replicateInParallel(objects []common.ReplicationObject, connections int, copier objectCopier, logger logging.Interface) error {
	startTime := time.Now()
	results := make(chan ReplicationResult, len(objects))
	runWorkers(PrepareObjectChannel(objects), connections, func(object common.ReplicationObject) {
		if strings.HasSuffix(object.GetName(), "/") {
			return
		}
		results <- copier.copyObject(object)
	}, func() { close(results) })

	successCount, errorCount := 0, 0
	for result := range results {
		if result.error != nil {
			errorCount++
		} else {
			successCount++
		}
		copier.logOutcome(result)
		LogProgress(successCount, errorCount, len(objects), startTime, logger)
	}

	logger.Infof("Replication completed with %d successes and %d errors in %v", successCount, errorCount, time.Since(startTime))
	if errorCount > 0 {
		return fmt.Errorf("%d/%d replications failed", errorCount, len(objects))
	}
	return nil
}

// runWorkers starts `count` goroutines that each take items from the channel until it closes
// and run work on them, then calls done once after the last worker has returned.
func runWorkers[T any](items <-chan T, count int, work func(T), done func()) {
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range items {
				work(item)
			}
		}()
	}

	go func() {
		wg.Wait()
		done()
	}()
}

// PrepareObjectChannel feeds the objects to a channel sized for all of them and closes it
// after the last one.
func PrepareObjectChannel(objects []common.ReplicationObject) chan common.ReplicationObject {
	objChan := make(chan common.ReplicationObject, len(objects))
	go func() {
		defer close(objChan)
		for _, object := range objects {
			objChan <- object
		}
	}()
	return objChan
}

// LogProgress writes one progress line: the share of objects finished, the counts and the
// time spent so far.
func LogProgress(successCount, errorCount, totalObjects int, startTime time.Time, logger logging.Interface) {
	progress := float64(successCount+errorCount) / float64(totalObjects) * 100
	elapsedTime := time.Since(startTime)
	logger.Infof("Progress: %.2f%%, Success: %d, Errors: %d, Total: %d, Elapsed Time: %v", progress, successCount, errorCount, totalObjects, elapsedTime)
}
