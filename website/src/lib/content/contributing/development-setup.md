---
title: Set up a development environment
description: Install the tools OME needs, build and test it locally, and deploy your own build to a Kubernetes cluster for development.
---

This guide sets up a checkout of OME for development. You clone the repository, build OME's binaries, run the tests, and run your build of the OME controller on a local [kind](https://kind.sigs.k8s.io) cluster. Then it covers what to regenerate when you change the API, and how to debug the controller from your IDE. To change only the docs, you need Git, Make, Go, Node and pnpm: see [Check a docs change](#check-a-docs-change).

<div class="prerequisites" markdown>

- Go 1.26 or newer.
- Git, Make and a C compiler. On macOS, install the Xcode Command Line Tools. On Linux, install GCC and G++ (the `build-essential` package on Debian and Ubuntu), `pkg-config`, and the OpenSSL development package, such as `libssl-dev`.
- A Rust toolchain with `cargo`, such as the stable toolchain from [rustup](https://rustup.rs). It builds `pkg/xet`, the Hugging Face Xet library that the model agent and the OME agent link.
- Docker. For the kind cluster in Step 4, give it at least 4 CPUs and 8 GiB of memory.
- `kubectl`, and Helm 3 or Helm 4.
- `jq`.
- Python 3 and `pip`, for the pre-commit hooks.
- To change the docs: Node 22 or newer and pnpm 10.

</div>

The Makefile downloads the other tools it needs into `bin/` the first time a target uses them, such as controller-gen, kustomize, kind, setup-envtest, yq and golangci-lint. The first build of the Xet library downloads its Rust dependencies.

## Step 1: Clone the repository

Clone the repository. The clone can live anywhere: it doesn't need to be under your `GOPATH`.

```bash
git clone https://github.com/ome-projects/ome.git
cd ome
```

Git clones the repository into the directory `ome`. To send a change back, you work in a fork: see [Pull requests and OEPs](pull-requests-and-oeps.md).

Install [pre-commit](https://pre-commit.com), then the repository's hooks, which run when you commit:

```bash
pip install pre-commit
pre-commit install
```

`pre-commit install` prints the path of the Git hook it installed. If your Python refuses `pip install` outside a virtual environment, install pre-commit with `pipx install pre-commit` instead.

The hooks, which [`.pre-commit-config.yaml`](https://github.com/ome-projects/ome/blob/main/.pre-commit-config.yaml) lists, trim trailing whitespace, fix final newlines and run codespell on the files you commit. When you commit Go files, they also run `gofmt -s`, `go vet` and `go mod tidy`. `helm-lint` and `helm-template` lint and render every chart on every commit, even when no chart changed, with the `helm` on your `PATH`.

The `no-commit-to-branch` hook refuses commits on `main`, so create a branch for your work. Name it `<type>/<description>` or `<username>/<description>`, in lowercase: the `branch-name-check` hook checks the name when you push, if you install it with `pre-commit install --hook-type pre-push`.

```bash
git switch -c yourname/first-change
```

Git switches to the new branch. To check the hooks, run them on a file:

```bash
pre-commit run --files README.md
```

The first run downloads and sets up the hooks. Then pre-commit prints a line for each hook, ending in Passed, or in Skipped for a hook that has no files to check.

## Step 2: Build

Each of OME's binaries has a make target that builds it into `bin/`:

| Target | Builds | Needs Rust |
| --- | --- | --- |
| `make ome-manager` | `bin/manager`, the OME controller | Yes |
| `make model-agent` | `bin/model-agent`, the [model agent](../guides/operate-ome/model-agent.md) | Yes |
| `make ome-agent` | `bin/ome-agent`, the OME agent, which downloads, replicates and encrypts models | Yes |
| `make kubectl-ome` | `bin/kubectl-ome`, the [kubectl-ome](../reference/kubectl-ome/overview.md) plugin | No |
| `make alfred` | `bin/alfred`, [Alfred](../concepts/scheduling/alfred.md), which is alpha | No |

The targets that need Rust first run `make xet-build`, which builds the Xet library into `pkg/xet/target/release`. The controller doesn't use the library, though, so without Rust you can build it with `go build -o bin/manager ./cmd/manager`. The [OME scheduler](../concepts/scheduling/ome-scheduler.md), which is alpha, is a separate Go module in `scheduler/`, and `make ome-scheduler-image` builds its image.

Build the controller:

```bash
make ome-manager
```

Make prints each step, and its last line says that the build is complete. The controller's binary is `bin/manager`.

## Step 3: Run the tests

Run the unit tests and the coverage check, as CI does:

```bash
make test
make coverage
```

`make test` runs `make fmt`, `make vet` and `make manifests`, installs setup-envtest and builds the Xet library. Then it runs the tests in `cmd/`, `pkg/` and `internal/`. A test that needs a Kubernetes API server starts its own with [envtest](https://book.kubebuilder.io/reference/envtest.html), which runs a local etcd and kube-apiserver for Kubernetes 1.30. When every test passes, the last line says so. `make coverage` then prints the coverage of `cmd/`, `pkg/` and `internal/`, and fails if their average is below the floor that `COVER_MIN` sets in the Makefile, currently 50%.

The tests don't need a cluster. To be sure that none of them reaches the cluster your kubeconfig points at, run them as `KUBECONFIG=/dev/null make test`.

`make fmt` and `make manifests` rewrite files that are out of date, so `git status` can show changes after `make test`. Commit them with your change.

`make test-no-xet` skips the Xet build and the `cmd/ome-agent` tests, but some of the packages it tests, such as `cmd/model-agent` and `pkg/modelagent`, still link the Xet library. So it passes only when `pkg/xet/target/release` already holds a build of the library, such as one from an earlier `make xet-build`. Without Rust, run `go test` on the packages you changed that don't link the library, and let CI run the rest.

### Run one test

Tests in packages that use envtest need `KUBEBUILDER_ASSETS`, the directory that holds envtest's etcd and kube-apiserver. `make envtest` installs setup-envtest into `bin/`, and `bin/setup-envtest use 1.30 -p path` downloads the binaries the first time, then prints their directory:

```bash
make envtest
KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.30 -p path)" go test ./pkg/webhook/admission/pod/... -run 'TestMutator_Handle$'
```

`go test` prints `ok` and the package, `sigs.k8s.io/ome/pkg/webhook/admission/pod`.

Packages that don't use envtest need only `go test`, such as `go test ./pkg/runtimeselector/...`. On macOS, `make test` also sets `DYLD_LIBRARY_PATH` to `pkg/xet/target/release`, so that test binaries that link the Xet library can load it. Set it the same way when you run those tests with `go test`.

### Lint your change

```bash
make fmt
make vet
make ci-lint
```

`make fmt` formats the code with `go fmt` and goimports, and `make vet` runs `go vet`. `make ci-lint` runs golangci-lint as CI's Lint check does, and reports each problem with its file and line. `make lint-fix` fixes the problems it can. If you changed `go.mod`, run `make tidy` too.

### Check a docs change

The docs site is `website/`, which needs Node 22 or newer and pnpm 10. This site replaces the old Hugo site in `site/`. A docs change must pass the site's checks, which run from `website/`, and the YAML check, a Go program that runs from the repository root:

```bash
cd website
pnpm install
pnpm lint
pnpm check
pnpm test
pnpm build
cd ..
make docs-examples
```

Each `pnpm` check exits with an error, and says what's wrong, when your change breaks it. `make docs-examples` prints each problem, then a count of the objects it checked and the problems it found, then the kinds it skipped because their CRDs aren't installed. [Checks](writing-docs.md#checks) explains what each check covers, and [Writing docs](writing-docs.md) how to write and preview a page.

## Step 4: Deploy your build to a cluster

This step runs your build of the OME controller on a kind cluster, a Kubernetes cluster that runs in a Docker container on your machine.

!!! warning "Use a local cluster"
    Deploy development builds only to a cluster of your own, such as the kind cluster below, and never to a shared cluster. The charts install cluster-wide objects: CRDs, ClusterRoles, and admission webhooks with `failurePolicy: Fail`. While a broken build's webhooks don't answer, the API server rejects changes to OME's resources for every user of the cluster. The commands below use your current kubectl context, so check it before you run them.

### Create a kind cluster

```bash
make kind
bin/kind create cluster --name ome-dev
```

`make kind` installs kind into `bin/`. kind then creates a one-node cluster named `ome-dev`, and switches your kubectl context to it. Check the context:

```bash
kubectl config current-context
```

It prints `kind-ome-dev`.

### Build and load the controller image

Build the controller image with the tag `dev`, for your machine's platform: `linux/arm64` on Apple silicon, or the default, `linux/amd64`, on most other machines. The commands below are for Apple silicon. For `linux/amd64`, leave out `ARCH=linux/arm64`. Docker builds the controller inside the image, so this doesn't need Rust on your machine.

```bash
make ome-image TAG=dev ARCH=linux/arm64
```

Make runs `make fmt` and `make vet`, builds the image `ghcr.io/moirai-internal/ome-manager:dev`, and prints that the image is built. It pushes nothing. The Makefile names images after `REGISTRY`, whose default is the registry of OME's release images, so the name matches the one the chart uses when you set the controller's tag to `dev`.

If `nerdctl` is on your `PATH`, the Makefile builds with it rather than with Docker, and kind can't load the image. Add `DOCKER_BUILD_CMD=docker` to the command.

Load the image onto the cluster's node:

```bash
bin/kind load docker-image ghcr.io/moirai-internal/ome-manager:dev --name ome-dev
```

kind copies the image from Docker onto the node. Check that the node has it:

```bash
docker exec ome-dev-control-plane crictl images
```

The list includes `ghcr.io/moirai-internal/ome-manager` with the tag `dev`.

### Install OME

Install cert-manager as in [Step 1 of Install OME](../getting-started/install.md#step-1-install-cert-manager). Then install the CRDs from your checkout:

```bash
helm upgrade --install ome-crd ./charts/ome-crd --namespace ome --create-namespace
```

Helm creates the namespace `ome`, installs the release, and reports its status as `deployed`.

Then install OME from `charts/ome-resources`, with your image. Helm 4 needs `--server-side=false`, as [Step 3 of Install OME](../getting-started/install.md#step-3-install-ome) explains.

=== "Helm 4"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      --set ome.controller.tag=dev \
      --set ome.controller.replicaCount=1 \
      --set ome.controller.resources.requests.cpu=500m \
      --set ome.controller.resources.requests.memory=1Gi \
      --server-side=false
    ```

=== "Helm 3"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      --set ome.controller.tag=dev \
      --set ome.controller.replicaCount=1 \
      --set ome.controller.resources.requests.cpu=500m \
      --set ome.controller.resources.requests.memory=1Gi
    ```

The chart runs the controller as `ghcr.io/moirai-internal/ome-manager:dev`, with the pull policy `IfNotPresent`, so the node runs the image you loaded. `replicaCount=1` runs one replica in place of three, and the lower requests, 500 millicores and 1 GiB of memory in place of 2 CPUs and 4 GiB, fit a kind node. They also leave room for the second controller pod that a restart starts before it stops the first. The chart's other images, such as the OME agent's, stay at the release version in the chart's values.

The first install fails with a webhook error, because the webhook certificate isn't ready yet: see [The first install fails with a webhook error](../getting-started/install.md#the-first-install-fails-with-a-webhook-error). Wait for the certificate and the controller:

```bash
kubectl wait --for=condition=Ready certificate/serving-cert -n ome --timeout=5m
kubectl rollout status deployment/ome-controller-manager -n ome --timeout=5m
```

kubectl reports that the certificate's condition is met, and that the deployment rolled out. Then run the same `helm upgrade --install` command again. Helm upgrades the release and reports its status as `deployed`. If it fails with the same error again, run it again: each upgrade writes the chart's empty CA bundle back into the webhook configurations until cert-manager adds the CA again. The same goes for the later `helm upgrade --install` commands on this page.

Check that the controller runs your image:

```bash
kubectl get deployment ome-controller-manager -n ome -o jsonpath='{.spec.template.spec.containers[0].image}'
```

It prints `ghcr.io/moirai-internal/ome-manager:dev`. Then read the controller's log:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1
```

The log includes the line `Starting manager`, which the controller logs when it has set up its controllers and webhooks, just before it starts them.

The controller runs without OME's optional components, but some InferenceServices need them: RawDeployment components, for example, need the Prometheus Operator's PodMonitor CRD. See [Optional components](../getting-started/install.md#optional-components). The controller looks for most of their CRDs only when it starts, so after you install one, restart the controller with `kubectl rollout restart deployment/ome-controller-manager -n ome`.

### Deploy a change

After you change the controller, build and load the image again, then restart the controller so that it runs the new image:

```bash
make ome-image TAG=dev ARCH=linux/arm64
bin/kind load docker-image ghcr.io/moirai-internal/ome-manager:dev --name ome-dev
kubectl rollout restart deployment/ome-controller-manager -n ome
kubectl rollout status deployment/ome-controller-manager -n ome --timeout=5m
```

kubectl reports that it restarted the deployment, and then that the deployment rolled out. Check the log as above. If you change a chart, or the API types (see [Change the API](#change-the-api)), run the two `helm upgrade --install` commands again too.

### Run your model agent too

The chart doesn't run the model agent by default. To run your build of it, build and load its image, which also builds inside Docker:

```bash
make model-agent-image TAG=dev ARCH=linux/arm64
bin/kind load docker-image ghcr.io/moirai-internal/model-agent:dev --name ome-dev
```

Make builds the image `ghcr.io/moirai-internal/model-agent:dev`, and kind copies it onto the node. Then turn the model agent on, with your image:

=== "Helm 4"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      --set ome.controller.tag=dev \
      --set ome.controller.replicaCount=1 \
      --set ome.controller.resources.requests.cpu=500m \
      --set ome.controller.resources.requests.memory=1Gi \
      --set modelAgent.enabled=true \
      --set modelAgent.image.tag=dev \
      --set modelAgent.image.pullPolicy=IfNotPresent \
      --set modelAgent.resources.requests.cpu=500m \
      --set modelAgent.resources.requests.memory=1Gi \
      --server-side=false
    ```

=== "Helm 3"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      --set ome.controller.tag=dev \
      --set ome.controller.replicaCount=1 \
      --set ome.controller.resources.requests.cpu=500m \
      --set ome.controller.resources.requests.memory=1Gi \
      --set modelAgent.enabled=true \
      --set modelAgent.image.tag=dev \
      --set modelAgent.image.pullPolicy=IfNotPresent \
      --set modelAgent.resources.requests.cpu=500m \
      --set modelAgent.resources.requests.memory=1Gi
    ```

The chart's default pull policy for the model agent is `Always`, which would have the node pull `ghcr.io/moirai-internal/model-agent:dev` from the registry rather than run the image you loaded. The lower requests fit a kind node: by default, the model agent requests 10 CPUs and 100 GiB of memory. Check that it runs:

```bash
kubectl rollout status daemonset/ome-model-agent-daemonset -n ome --timeout=5m
```

kubectl reports that the daemon set rolled out. [Run the model agent](../guides/operate-ome/model-agent.md) covers what the model agent does and how to configure it.

### Run the controller on your machine

To debug the controller, or to try a change without building an image, run it on your machine against the kind cluster. First stop the controller in the cluster, and delete OME's webhook configurations. The controller on your machine doesn't serve webhooks, and OME's webhooks have `failurePolicy: Fail`, so while they point at a stopped controller, the API server rejects changes to OME's resources.

```bash
kubectl scale deployment/ome-controller-manager -n ome --replicas=0
kubectl get validatingwebhookconfigurations,mutatingwebhookconfigurations -o name | grep '\.ome\.io$' | xargs kubectl delete
```

kubectl reports that it scaled the deployment, and names each webhook configuration it deletes. `make delete-webhooks` deletes only the webhook configurations in `config/webhook/manifests.yaml`, which misses some that the chart installs, such as `servingruntime-preset.ome.io`.

Then run the controller:

```bash
go run ./cmd/manager --zap-encoder=console --metrics-bind-address=127.0.0.1:8080 --health-probe-addr=127.0.0.1:8081
```

The controller connects to the cluster of your current kubectl context, logs `Starting manager` when it has set up, and runs until you stop it with Ctrl+C. The flags keep its metrics and health endpoints on `127.0.0.1`, where by default they listen on every interface.

From another terminal, check that the controller is ready:

```bash
curl -s http://127.0.0.1:8081/readyz
```

It prints `ok`.

Objects that you create or change while the controller runs on your machine skip OME's webhooks, so OME doesn't default or validate them. To go back to the controller in the cluster, stop the one on your machine and scale the deployment back up:

```bash
kubectl scale deployment/ome-controller-manager -n ome --replicas=1
```

kubectl reports that it scaled the deployment. Then run your `helm upgrade --install ome` command again, which recreates the webhook configurations.

## Change the API

The API types are in `pkg/apis/ome/v1beta1/`. After you change them, regenerate the code and manifests, in the order the Generated Code Drift check uses:

```bash
make manifests
make generate
```

Each prints the steps it runs:

- `make manifests` regenerates the DeepCopy methods, the CRDs in `config/crd/full` and `config/crd/minimal`, the controller's ClusterRole in `config/rbac` from the `+kubebuilder:rbac` markers in `pkg/controller/`, and the copies of the CRDs and the ClusterRole in the Helm charts.
- `make generate` regenerates the client libraries in `pkg/client`, the defaulting functions, and the OpenAPI definitions and `swagger.json` in `pkg/openapi`. It also runs `go mod tidy`.

Commit every file they change or create. On each pull request, the Generated Code Drift check runs both, and fails if they change a committed file. It doesn't notice a file that they create, such as the CRD of a new kind, so check `git status` for new files.

Then update the docs that come from the types:

- Regenerate the [OME API](../reference/api/ome.v1beta1.md) reference with `make generate-apiref`, as [The API reference](writing-docs.md#the-api-reference) explains.
- Run `make docs-examples` to check the YAML examples in the docs against the new CRDs.

Major features and API changes need an OME Enhancement Proposal (OEP): see [Pull requests and OEPs](pull-requests-and-oeps.md).

## IDE setup

Any editor with Go support works, such as VS Code or Cursor with the Go extension, or GoLand. The repository ignores `.vscode/` and `.idea/`, so your editor's settings stay out of your commits.

To debug the controller, first prepare the cluster as in [Run the controller on your machine](#run-the-controller-on-your-machine): stop the controller in the cluster, and delete the webhook configurations. The debugger then runs the controller against your current kubectl context.

### VS Code and Cursor

Add a launch configuration to `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "OME controller",
      "type": "go",
      "request": "launch",
      "mode": "debug",
      "program": "${workspaceFolder}/cmd/manager",
      "args": [
        "--zap-encoder=console",
        "--metrics-bind-address=127.0.0.1:8080",
        "--health-probe-addr=127.0.0.1:8081"
      ]
    }
  ]
}
```

Start OME controller from Run and Debug. The Debug Console shows `Starting manager` when the controller has set up.

To run and debug envtest tests from the editor, set `KUBEBUILDER_ASSETS` for the Go extension's tests in `.vscode/settings.json`, to the directory that `bin/setup-envtest use 1.30 -p path` prints:

```json
{
  "go.testEnvVars": {
    "KUBEBUILDER_ASSETS": "/path/printed/by/setup-envtest"
  }
}
```

### GoLand

Add a Go Build run configuration. Set Run kind to Package, Package path to `sigs.k8s.io/ome/cmd/manager`, Working directory to the repository root, and Program arguments to `--zap-encoder=console --metrics-bind-address=127.0.0.1:8080 --health-probe-addr=127.0.0.1:8081`. When you run or debug it, the Run window shows `Starting manager` when the controller has set up.

To run envtest tests, add `KUBEBUILDER_ASSETS` to the environment variables of the Go Test configuration template, set to the directory that `bin/setup-envtest use 1.30 -p path` prints.

## Troubleshooting

### Make can't find `cargo`

`make xet-build` needs a Rust toolchain, and so do the targets that run it, such as `make test`, `make ome-manager`, `make model-agent`, `make ome-agent` and `make ome-agent-image`. Install one, such as the stable toolchain from [rustup](https://rustup.rs), then open a new shell so that `cargo` is on your `PATH`.

Without Rust, you can still build the controller with `go build -o bin/manager ./cmd/manager`, build its image with `make ome-image`, and run `go test` on packages that don't link the Xet library.

### The linker can't find the Xet library

When the Xet library isn't built, building or testing a package that links it fails with `ld: library 'xet' not found` on macOS, or `cannot find -lxet` on Linux. `make test-no-xet` fails the same way, since it doesn't build the library. Run `make xet-build`, which builds it into `pkg/xet/target/release`.

### An envtest test can't find etcd or kube-apiserver

Without `KUBEBUILDER_ASSETS`, envtest looks for its binaries in `/usr/local/kubebuilder/bin`, so a test that starts an API server fails with an error that names a file there, such as `/usr/local/kubebuilder/bin/etcd`. Set `KUBEBUILDER_ASSETS` as in [Run one test](#run-one-test).

### Make can't find `jq`

`make manifests`, which `make test` runs too, pipes a CRD through `jq`. Install jq.

### A pre-commit hook fails

- `helm-lint` and `helm-template` fail when `helm` isn't on your `PATH`. Install Helm, or skip them with `SKIP=helm-lint,helm-template git commit`.
- `no-commit-to-branch` fails on `main`. Create a branch, and commit there.
- `dev-images-contract` runs when you change the dev images or PR validation workflow, or the scripts behind them, and needs yq 4: put it on your `PATH`, or point `YQ_BIN` at the one the Makefile installs, as in `YQ_BIN=bin/yq git commit`.
- A hook that fixes files, such as `trailing-whitespace` or `go-fmt`, fails when it changes them. Add its changes, and commit again.

### The Generated Code Drift check fails

The check runs `make manifests` and `make generate` on your pull request, and fails if they change a committed file. Run them yourself as in [Change the API](#change-the-api), commit the files they change, and push again.

### `make generate` fails the API rule check

`make generate` checks the API types against Kubernetes' API rules, and writes the violations it finds to `hack/current_violation_exceptions.list`. When they differ from the known ones in `hack/violation_exceptions.list`, it fails with:

```text
ERROR: API rule check failed. Reported violations in file hack/current_violation_exceptions.list differ from known violations in file hack/violation_exceptions.list.
```

Compare the two files to find the new line. A list field without a list type, or a field whose JSON name doesn't match its Go name, adds one. Fix the field, for example with a `// +listType=atomic` marker on a list, and run `make generate` again. If the violation is intended, copy the current file over the known one, and commit both.

### The controller pod can't pull its image

The pod shows `ErrImagePull` or `ImagePullBackOff` when the node doesn't have your image, so it tries to pull it from `ghcr.io/moirai-internal`. Check that the name and tag you loaded match the deployment's image, that you loaded it into the cluster `ome-dev`, and that you built it with Docker rather than nerdctl. Then load it again.

### The controller fails with `exec format error`

The controller's log says `exec format error` when the image was built for a platform other than your node's. Build it again with your machine's platform, such as `ARCH=linux/arm64` on Apple silicon, then load it and restart the controller as in [Deploy a change](#deploy-a-change).

### The controller pod stays Pending

When no node has enough CPU or memory for the pod, its events, which `kubectl describe pod -n ome -l control-plane=ome-controller-manager` shows, say so. Give Docker more CPUs and memory, or add `--set prometheus.enabled=false` to your `helm upgrade --install ome` command to skip the chart's Prometheus.

## Clean up

When you're done, delete the kind cluster:

```bash
bin/kind delete cluster --name ome-dev
```

kind deletes the cluster's container and removes its context from your kubeconfig.

## Next steps

- [Pull requests and OEPs](pull-requests-and-oeps.md): send your change, and propose a larger one.
- [Writing docs](writing-docs.md): write, preview and check a docs page.
- [How OME works](../concepts/architecture/how-ome-works.md): the components your build runs, and how they work together.
