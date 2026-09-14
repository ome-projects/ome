package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"sigs.k8s.io/ome/alfred-simulator/protocol"
	"sigs.k8s.io/ome/alfred-simulator/worker"
)

const (
	defaultTimeout = 10 * time.Second
	maxTimeout     = 60 * time.Second
	maxInputBytes  = 16 << 20
)

type options struct {
	backend         string
	schedulerConfig string
	printProfile    bool
	timeout         time.Duration
}

type profileOutput struct {
	Identity       protocol.ProfileIdentity `json:"identity"`
	GangScheduling bool                     `json:"gangScheduling"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	exitCode := runWithOwnedInput(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(exitCode)
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return runContext(context.Background(), args, stdin, stdout, stderr)
}

// runWithOwnedInput lets a process signal interrupt a blocked request read.
// runContext remains synchronous and does not assume ownership of injected IO.
func runWithOwnedInput(ctx context.Context, args []string, stdin io.ReadCloser, stdout, stderr io.Writer) int {
	readFinished := make(chan struct{})
	watcherFinished := make(chan struct{})
	go func() {
		defer close(watcherFinished)
		select {
		case <-ctx.Done():
			_ = stdin.Close()
		case <-readFinished:
		}
	}()

	exitCode := runContext(ctx, args, stdin, stdout, stderr)
	close(readFinished)
	<-watcherFinished
	return exitCode
}

func runContext(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	options, err := parseOptions(args, stderr)
	if err != nil {
		writeError(stderr, err)
		return 2
	}

	configYAML, err := os.ReadFile(options.schedulerConfig)
	if err != nil {
		writeError(stderr, fmt.Errorf("read scheduler configuration %q: %w", options.schedulerConfig, err))
		return 1
	}
	profile, err := worker.LoadProfile(options.backend, configYAML)
	if err != nil {
		writeError(stderr, fmt.Errorf("load scheduler profile: %w", err))
		return 1
	}

	if options.printProfile {
		if err := writeJSON(stdout, profileOutput{Identity: profile.Identity, GangScheduling: profile.GangScheduling}); err != nil {
			writeError(stderr, fmt.Errorf("write profile identity: %w", err))
			return 1
		}
		return 0
	}

	request, err := protocol.Decode(stdin, maxInputBytes)
	if err != nil {
		writeError(stderr, fmt.Errorf("decode request: %w", err))
		return 1
	}
	evaluationContext, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()
	result, err := profile.Evaluate(evaluationContext, request)
	if err != nil {
		writeError(stderr, fmt.Errorf("evaluate request: %w", err))
		return 1
	}
	if err := writeJSON(stdout, result); err != nil {
		writeError(stderr, fmt.Errorf("write result: %w", err))
		return 1
	}
	return 0
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	options := options{timeout: defaultTimeout}
	flags := flag.NewFlagSet("alfred-simulator", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.backend, "backend", "", "opaque backend identity")
	flags.StringVar(&options.schedulerConfig, "scheduler-config", "", "path to one KubeSchedulerConfiguration")
	flags.BoolVar(&options.printProfile, "print-profile", false, "print the calculated profile identity and exit")
	flags.DurationVar(&options.timeout, "timeout", defaultTimeout, "simulation timeout (positive, at most 60s)")
	if err := flags.Parse(args); err != nil {
		return options, fmt.Errorf("parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return options, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if strings.TrimSpace(options.backend) == "" {
		return options, errors.New("--backend is required")
	}
	if strings.TrimSpace(options.schedulerConfig) == "" {
		return options, errors.New("--scheduler-config is required")
	}
	if options.timeout <= 0 || options.timeout > maxTimeout {
		return options, fmt.Errorf("--timeout must be positive and no greater than %s", maxTimeout)
	}
	return options, nil
}

func writeJSON(writer io.Writer, value any) error {
	return json.NewEncoder(writer).Encode(value)
}

func writeError(writer io.Writer, err error) {
	_, _ = fmt.Fprintf(writer, "alfred-simulator: %v\n", err)
}
