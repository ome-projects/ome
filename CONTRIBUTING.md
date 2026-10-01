# Contribution Guidelines

Thank you for your interest in contributing to OME! This repository is open to everyone and welcomes all kinds of contributions, no matter how small or large. There are several ways you can contribute to the project:

* Identify and report any issues or bugs.
* Suggest or implement new features.

This document explains how to set up a development environment, so you can get started contributing.

## Contributing Guidelines

### Coding Style Guide

In general, we adhere to the [Golang Style Guide](https://google.github.io/styleguide/go/). We include a formatting command `make fmt` to format the code.

### Pull Requests

When submitting a pull request:

1. Make sure your code has been rebased on top of the latest commit on the main branch.
2. Ensure code is properly formatted by running `make fmt`, `make vet`, and `make tidy`.
3. Add new test cases. In the case of a bug fix, the tests should fail without your code changes. For new features, try to cover as many variants as reasonably possible.
4. Modify the documentation as necessary.
5. Fill out the pull request template completely. The template will guide you through providing all necessary information.
6. Link any related issues using the "Fixes #123" syntax to automatically close them when the PR is merged.

#### Documentation
The live documentation is the Hugo site in `site/`, so documentation changes go there. The redesigned site that replaces it at launch lives in [ome-projects/ome-docs](https://github.com/ome-projects/ome-docs).

#### Pre-commit check
**Pre-commit hooks** run these checks automatically on every commit.
- **General Checks**: Trailing whitespace, end-of-file fixing, YAML/TOML validation, merge conflict detection, large file prevention
- **Go Formatting**: `go-fmt`, `go-vet` with workspace-wide formatting
- **Helm Linting**: `helm-lint` with all warnings treated as errors
- **Spell Checking**: `codespell` for catching typos across all text files
- **Docs Site**: `npm-lockfile-sync` keeps `site/package-lock.json` in sync with `package.json`, so CI's `npm ci` stays reproducible
To set up:

```bash
pip install pre-commit
pre-commit install
```
To run it:
```bash
pre-commit run --all-files #run all hooks on all files.
pre-commit run go-fmt --all-files # run go-fmt hooks on all files.
```

> **Note:** The `npm-lockfile-sync` hook regenerates the lockfile via `npm install --package-lock-only`, whose output can vary slightly across npm versions. To avoid spurious lockfile diffs, use the same Node.js major version as CI — **Node 20** (e.g. `nvm use 20`).

### PR Template

It is required to classify your PR and make the commit message concise and useful. Prefix the PR title appropriately to indicate the type of change. Please use one of the following:

* `[Bugfix]` for bug fixes.
* `[Core]` for core controller changes. This includes build, version upgrade, changes across all controllers.
* `[API]` for all OME API changes.
* `[Helm]` for changes related to helm charts.
* `[Docs]` for changes related to documentation.
* `[CI/Tests]` for unittests and integration tests.
* `[Misc]` for PRs that do not fit the above categories. Please use this sparingly.
* `[OEP]` for OME enhancements proposals.

Open source community also recommends keeping the commit message title within 52 characters and each line in the message content within 72 characters.

### OME Enhancement Proposals (OEPs)

An OEP (OME Enhancement Proposal) is required for substantial changes to OME. You should create an OEP when proposing:

1. Any significant architectural changes
2. Major feature additions such as new CRD
3. Breaking API changes
4. Changes that affect multiple components
5. Modifications to core behaviors or interfaces

#### How to Use the OEP Template

1. Create a new branch for your OEP
2. Copy the template from `oeps/NNNN-template/README.md` to a new directory:
   ```bash
   cp -r oeps/NNNN-template oeps/XXXX-descriptive-name
   ```
   where:
   * `XXXX` is the next available number in sequence
   * `descriptive-name` is a brief, hyphen-separated description

3. Fill out each section of the template:
   * **Title**: Clear, concise description of the enhancement
   * **Summary**: High-level overview of the proposal
   * **Motivation**: Why this change is needed
   * **Goals/Non-Goals**: Specific objectives and scope boundaries
   * **Proposal**: Detailed description of the enhancement
   * **Design Details**: Technical implementation specifics
   * **Alternatives**: Other approaches considered

4. Submit the OEP as a PR with the `[OEP]` prefix
5. Work with reviewers to refine the proposal
6. Once approved, implementation PRs can reference the OEP number

#### OEP Review Process

1. Initial Review (<=1 week):
   * Technical feasibility
   * Alignment with project goals
   * Impact assessment and Design considerations

2. Feedback Integration:
   * Address reviewer comments
   * Clarify design decisions
   * Update technical details

3. Final Approval:
   * Sign-off from required reviewers
   * Merge OEP document
   * Begin implementation phase

### Code Reviews

All submissions, including submissions by project members, require a code review. To make the review process as smooth as possible, please:

1. Keep your changes as concise as possible. If your pull request involves multiple unrelated changes, consider splitting it into separate pull requests.
2. Respond to all comments within a reasonable time frame. If a comment isn't clear, or you disagree with a suggestion, feel free to ask for clarification or discuss the suggestion.
3. Provide constructive feedback and meaningful comments. Focus on specific improvements and suggestions that can enhance the code quality or functionality. Remember to acknowledge and respect the work the author has already put into the submission.

## Prerequisites

Follow the instructions below to set up your development environment. Once you meet these requirements, you can make changes and [deploy your own version of ome](#deploy-ome).

### Install Requirements

You must install these tools:

1. [`go`](https://golang.org/doc/install): OME controller is written in Go and requires Go 1.22.0+.
2. [`git`](https://help.github.com/articles/set-up-git/): For source control.
3. [`Go Module`](https://blog.golang.org/using-go-modules): Go's dependency management system.
4. [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/): For managing development environments.
5. [`kustomize`](https://github.com/kubernetes-sigs/kustomize/): To customize YAMLs for different environments, requires v5.0.0+.
6. [`yq`](https://github.com/mikefarah/yq): yq is used in the project makefiles to parse and display YAML output, requires yq `4.*`.

### Setup Your Environment

To start your environment, you'll need to set these environment variables (we recommend adding them to your `.bashrc`):

1. `GOPATH`: If you don't have one, simply pick a directory and add `export GOPATH=...`
2. `$GOPATH/bin` on `PATH`: This is so that tooling installed via `go get` will work properly.
3. `GONOPROXY`: (Optional) Set go proxy to pull the dependencies from the any internal repository.
4. `GOPRIVATE`: (Optional) Set go private to pull the dependencies from the any internal repository.
5. `ARCH`: If you are using M1 or M2 MacBook, the value is `linux/arm64`.
6. `REGISTRY`: The docker repository to which developer images should be pushed.

**Note:** Set up a container image repository for pushing images. You can use any container image registry by adjusting the authentication methods and repository paths mentioned in the sections below.
```bash
docker login <your-registry-url> -u <username>
```

### Clone OME Repository

The Go tools require that you clone the repository to the `src/sigs.k8s.io/ome` directory in your [`GOPATH`](https://github.com/golang/go/wiki/SettingGOPATH).

To check out this repository:

1. Create your own [clone this repo](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
2. Clone it to your machine:
```shell
mkdir -p ${GOPATH}/src/sigs.k8s.io/ome
cd ${GOPATH}/src/sigs.k8s.io/ome
git clone https://github.com/ome-projects/ome.git
cd ome
```

Once you reach this point, you are ready to do a full build and deploy as described below.

## Deploy OME

### Deploy OME from the Main Branch

We suggest using [cert manager](https://github.com/cert-manager/cert-manager) for provisioning the certificates for the webhook server. Other solutions should also work as long as they put the certificates in the desired location.

**Note**: If you are using an OCI OKE cluster, it's recommended to use [OCI OKE Add-on](https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contengintroducingclusteraddons.htm) which manages cert manager.

You can follow [the cert manager documentation](https://cert-manager.io/docs/installation/) to install it.

If you don't want to install cert manager, you can set the `ENABLE_SELF_SIGNED_CA` environment variable to true. `ENABLE_SELF_SIGNED_CA` will execute a script to create a self-signed CA and patch it to the webhook config.
```bash
export ENABLE_SELF_SIGNED_CA=true
```

After that, you can run the following command to deploy `OME`. You can skip the above step if cert manager is already installed.
```bash
make install
```

Success "Expected Output"
```bash
$ kubectl get pods -n ome -l control-plane=ome-controller-manager
NAME                                    READY   STATUS    RESTARTS   AGE
ome-controller-manager-fdc857d9-p5znk   2/2     Running   0          16m
```

```bash
$ kubectl get pods -n ome -l control-plane=ome-model-controller
NAME                                    READY   STATUS    RESTARTS   AGE
ome-model-controller-7b5b8c9cf4-5m5zz   1/1     Running   0          3d7h
ome-model-controller-7b5b8c9cf4-89475   1/1     Running   0          3d7h
ome-model-controller-7b5b8c9cf4-9znbv   1/1     Running   0          3d7h
```

```bash
$ kubectl get pods -n ome -l control-plane=ome-model-agent-daemonset
NAME                              READY   STATUS    RESTARTS       AGE
ome-model-agent-daemonset-67qhg   1/1     Running   0              3d6h
ome-model-agent-daemonset-67tcv   1/1     Running   0              3d6h
ome-model-agent-daemonset-9gv6f   1/1     Running   0              3d6h
```

**Note:** By default, it installs to the `ome` namespace with the published images (controller manager, model controller, and model agent) from the main branch.

### Deploy OME with Your Own Version

Run the following command to deploy `OME` controller with your local change.
```bash
make push-manager-image;
make patch-manager-dev;
make install
```

**Note:** `make push-manager-image` builds the image from your local code, publishes to `REGISTRY`. `make patch-manager-dev` patches the `ome-controller-manager` image in the deployment configuration digest to your cluster for testing. `make install` installs the `ome-controller-manager` to your cluster. Please also ensure you are logged in to `REGISTRY` from your client machine.

### Running OME Manager Locally

To run the OME manager locally, you can run the following command:
```bash
make run-ome-manager
```

### Running OME Manager in IDE

Goland IDE is recommended for running the OME manager in the IDE. After cloning the repository, you can run the OME manager in your IDE by following the steps below:

* Right-click on the `main.go` file in the `cmd/ome-controller-manager` directory and select `Run 'go build main.go'` from the context menu.
* You can also create a run configuration by clicking on the `Edit Configurations` option in the top right corner of the IDE and adding a new Go configuration with the following settings:
    * Name: `ome-controller-manager`
    * Run Kind: `File`
    * Files: `${GOPATH}/src/sigs.k8s.io/ome/cmd/manager/main.go`
    * Environment Variables: `KUBECONFIG=<path to kubeconfig file>`
    * Program Arguments: `--zap-encoder console --health-probe-addr 127.0.0.1:8081 --metrics-bind-address 127.0.0.1:8080 --leader-elect`
    * Module: `ome`

This will start the OME manager in the IDE. You can also run the manager in debug mode by adding breakpoints in the code.

However, because the cluster might have both mutating and validating webhooks, you might need to run the manager in the cluster to test the webhook functionality.

If you want to run the manager locally, you need to remove the webhooks from the cluster. The following command can be used to remove the webhooks:
```bash
make delete-webhooks
```

### Running OME Manager in VSCode or Cursor

For VSCode or Cursor, follow these steps to set up the development environment:

1. Install the Go extension for VSCode/Cursor
   * Open VSCode/Cursor
   * Go to Extensions (Ctrl+Shift+X)
   * Search for "Go" and install the official Go extension

2. Install Go tools when prompted, or run the command:
   * Open Command Palette (Ctrl+Shift+P)
   * Type "Go: Install/Update Tools"
   * Select all tools and click OK

3. Configure launch configuration:
   * Create or open `.vscode/launch.json`
   * Add the following configuration:
   ```json
   {
       "name": "OME Manager",
       "type": "go",
       "request": "launch",
       "mode": "debug",
       "program": "${workspaceFolder}/cmd/manager/main.go",
       "env": {
           "KUBECONFIG": "<path-to-your-kubeconfig>"
       },
       "args": [
                "--zap-encoder", "console",
                "--health-probe-addr", "127.0.0.1:8081",
                "--metrics-bind-address", "127.0.0.1:8080",
                "--leader-elect"
      ]
   }
   ```

4. Run/Debug the application:
   * Open the Run and Debug view (Ctrl+Shift+D)
   * Select "OME Manager" from the dropdown
   * Click the Play button or press F5 to start debugging
   * Use F9 to set breakpoints in the code

**Note:** Make sure to replace `<path-to-your-kubeconfig>` with the actual path to your kubeconfig file.

## Iterating

As you make changes to the code-base, there are two special cases to be aware of:

* **If you change an input to generated code**, then you must run `make manifests`. Inputs include:
  * API type definitions in [pkg/apis](https://github.com/ome-projects/ome/tree/main/pkg/apis)
  * Manifests or kustomize patches stored in [config](https://github.com/ome-projects/ome/tree/main/config).

  To generate the OME go clients, you should run `make generate`.

* **If you want to add new dependencies**, then you add the imports and the specific version of the dependency module in `go.mod`. When it encounters an import of a package not provided by any module in `go.mod`, the go command automatically looks up the module containing the package and adds it to `go.mod` using the latest version.

* **If you want to upgrade the dependency**, then you run the go get command, e.g., `go get golang.org/x/text` to upgrade to the latest version, `go get golang.org/x/text@v0.3.0` to upgrade to a specific version.

```shell
make install
```
