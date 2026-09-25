package logs

import (
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

type logTarget struct {
	podName string
	prefix  string
	options corev1.PodLogOptions
}

type openLogStreamFunc func(
	context.Context,
	coreclient.PodInterface,
	logTarget,
) (io.ReadCloser, error)

func defaultOpenLogStream(
	ctx context.Context,
	pods coreclient.PodInterface,
	target logTarget,
) (io.ReadCloser, error) {
	options := target.options
	return pods.GetLogs(target.podName, &options).Stream(ctx)
}

func consumeLogs(
	ctx context.Context,
	pods coreclient.PodInterface,
	targets []logTarget,
	follow bool,
	maxLogRequests int,
	open openLogStreamFunc,
	out io.Writer,
) error {
	if follow && len(targets) > maxLogRequests {
		return fmt.Errorf(
			"you are attempting to follow %d log streams, but maximum allowed concurrency is %d, use --max-log-requests to increase the limit",
			len(targets), maxLogRequests,
		)
	}
	if follow {
		return consumeFollow(ctx, pods, targets, open, out)
	}
	return consumeSequential(ctx, pods, targets, open, out)
}

func consumeSequential(
	ctx context.Context,
	pods coreclient.PodInterface,
	targets []logTarget,
	open openLogStreamFunc,
	out io.Writer,
) error {
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}
		streamCtx, cancel := context.WithCancelCause(ctx)
		reader, err := open(streamCtx, pods, target)
		if err != nil {
			wrapped := fmt.Errorf("streaming logs for pod %s: %w", target.podName, err)
			cancel(wrapped)
			return wrapped
		}
		err = multiplex(streamCtx, cancel, []namedStream{{Prefix: target.prefix, Reader: reader}}, out)
		cancel(err)
		if err != nil {
			return err
		}
	}
	return nil
}

func consumeFollow(
	ctx context.Context,
	pods coreclient.PodInterface,
	targets []logTarget,
	open openLogStreamFunc,
	out io.Writer,
) error {
	streamCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	streams := make([]namedStream, 0, len(targets))
	for _, target := range targets {
		if err := streamCtx.Err(); err != nil {
			cancel(err)
			closeNamedStreams(streams)
			return err
		}
		reader, err := open(streamCtx, pods, target)
		if err != nil {
			wrapped := fmt.Errorf("streaming logs for pod %s: %w", target.podName, err)
			cancel(wrapped)
			closeNamedStreams(streams)
			return wrapped
		}
		streams = append(streams, namedStream{Prefix: target.prefix, Reader: reader})
	}
	return multiplex(streamCtx, cancel, streams, out)
}

func closeNamedStreams(streams []namedStream) {
	for i := range streams {
		_ = streams[i].Reader.Close()
	}
}
