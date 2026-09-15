// Command worker-result replays public API scheduling input through the deployed
// simulator. It writes evidence only: it never creates or patches API objects.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func main() {
	if err := run(os.Args[1:]); err != nil && err != flag.ErrHelp {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("worker-result", flag.ContinueOnError)
	kubeconfig := flags.String("kubeconfig", "", "explicit private kubeconfig")
	kubecontext := flags.String("context", "", "explicit kind context")
	namespace := flags.String("namespace", "", "source namespace")
	name := flags.String("name", "", "source InferenceService")
	node := flags.String("node", "", "actual source node")
	alfredNamespace := flags.String("alfred-namespace", "ome", "deployed Alfred namespace")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *kubeconfig == "" || *kubecontext != "kind-alfred-e2e" || *namespace == "" || *name == "" || *node == "" {
		return fmt.Errorf("explicit kubeconfig, kind-alfred-e2e context, namespace, name and node are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := clientcmd.LoadFromFile(*kubeconfig)
	if err != nil {
		return fmt.Errorf("load private kubeconfig: %w", err)
	}
	rc, err := clientcmd.NewNonInteractiveClientConfig(*raw, *kubecontext, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return fmt.Errorf("select private context: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := ome.AddToScheme(scheme); err != nil {
		return err
	}
	c, err := client.New(rc, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: *alfredNamespace, Name: "alfred-config"}, cm); err != nil {
		return err
	}
	cfg, err := config.Load([]byte(cm.Data["config.yaml"]))
	if err != nil {
		return err
	}
	snap, err := input.Capture(ctx, c, time.Now)
	if err != nil {
		return err
	}
	request, err := input.BuildExecutionRequest(snap, input.Source{
		Namespace: *namespace, InferenceService: *name, Component: ome.EngineComponent,
		Instance: 0, FromNode: *node,
	}, cfg.Scheduling, fmt.Sprintf("acceptance-%d", time.Now().UnixNano()), nil, time.Now(), 30*time.Second)
	if err != nil {
		return err
	}
	if request.Profile.Backend != "kind-default-v135" {
		return fmt.Errorf("unexpected worker profile: %+v", request.Profile)
	}
	var wire bytes.Buffer
	if err := json.NewEncoder(&wire).Encode(request); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig", *kubeconfig, "--context", *kubecontext,
		"-n", *alfredNamespace, "exec", "-i", "deployment/ome-alfred", "--", "/alfred-simulator",
		"--backend", "kind-default-v135", "--scheduler-config", "/etc/alfred-simulation/default-scheduler.yaml")
	cmd.Stdin = &wire
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("deployed worker: %w", err)
	}
	var result scheduling.Result
	if err := json.Unmarshal(output, &result); err != nil {
		return fmt.Errorf("decode deployed worker: %w", err)
	}
	if err := scheduling.ValidateResponse(request, result); err != nil {
		return fmt.Errorf("validate deployed worker: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Provenance string             `json:"provenance"`
		Validated  bool               `json:"validated"`
		Request    scheduling.Request `json:"request"`
		Result     scheduling.Result  `json:"result"`
	}{"deployed-worker", true, request, result})
}
