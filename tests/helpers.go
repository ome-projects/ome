package integration_tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	. "github.com/onsi/gomega"
)

// TestLogger provides a simple logging function for tests
var TestLogger = struct {
	Enabled bool
	Debug   func(format string, args ...interface{})
	Info    func(format string, args ...interface{})
	Error   func(format string, args ...interface{})
}{
	Enabled: true,
	Debug: func(format string, args ...interface{}) {
		if os.Getenv("TEST_DEBUG") == "true" {
			fmt.Printf("[DEBUG] "+format+"\n", args...)
		}
	},
	Info: func(format string, args ...interface{}) {
		fmt.Printf("[INFO] "+format+"\n", args...)
	},
	Error: func(format string, args ...interface{}) {
		fmt.Printf("[ERROR] "+format+"\n", args...)
	},
}

// RunAgent runs the agent with the given arguments and returns stdout, stderr, and error
func RunAgent(binaryPath string, args ...string) (string, string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	TestLogger.Info("Running command: %s %v", binaryPath, args)
	err := cmd.Run()
	if err != nil {
		TestLogger.Error("Command failed with error: %v", err)
	} else {
		TestLogger.Info("Command completed successfully")
	}

	return stdout.String(), stderr.String(), err
}

// RunAgentWithTimeout runs the agent with a timeout and returns stdout, stderr, and error
func RunAgentWithTimeout(timeout time.Duration, binaryPath string, args ...string) (string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// CreateTempConfig creates a temporary config file with the given content
func CreateTempConfig(content string) string {
	tempFile, err := os.CreateTemp("", "ome-agent-config-*.yaml")
	Expect(err).NotTo(HaveOccurred(), "Failed to create temp config file")

	_, err = tempFile.WriteString(content)
	Expect(err).NotTo(HaveOccurred(), "Failed to write to temp config file")

	err = tempFile.Close()
	Expect(err).NotTo(HaveOccurred(), "Failed to close temp config file")

	return tempFile.Name()
}

// CreateMockConfig creates a mock config file for the specified agent
func CreateMockConfig(agentType string) string {
	var content string

	switch agentType {
	case "enigma":
		content = `
debug: true
# Mock enigma agent config
encryption:
  key_id: mock-key-id
  region: us-west-2
`
	case "hf-download":
		content = `
debug: true
# Mock HF download agent config
endpoint: https://huggingface.co
model_name: gpt2
local_path: /tmp/models
`
	case "replica":
		content = `
debug: true
# Mock replica agent config
source:
  bucket: source-bucket
  region: us-west-2
destination:
  bucket: dest-bucket
  region: us-east-1
`

	case "serving-agent":
		content = `
debug: true
# Mock serving sidecar config
model_id: gpt2
port: 8080
`
	case "fine-tuned-adapter":
		content = `
debug: true
# Mock fine-tuned adapter config
base_model: /path/to/base
adapter: /path/to/adapter
output: /path/to/output
`
	default:
		content = `
debug: true
# Generic mock config
`
	}

	return CreateTempConfig(content)
}
