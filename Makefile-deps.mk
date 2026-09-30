PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
BIN_DIR ?= $(PROJECT_DIR)/bin
TOOLS_DIR := $(PROJECT_DIR)/hack/internal/tools
DOCTOOLS_DIR := $(PROJECT_DIR)/hack/internal/doctools

ifeq (,$(shell go env GOBIN))
	GOBIN=$(shell go env GOPATH)/bin
else
	GOBIN=$(shell go env GOBIN)
endif
GO_CMD ?= go

# Use go.mod go version as source
GOLANGCI_LINT_VERSION ?= $(shell cd $(TOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' github.com/golangci/golangci-lint)
CONTROLLER_GEN_VERSION ?= $(shell cd $(TOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' sigs.k8s.io/controller-tools)
KUSTOMIZE_VERSION ?= $(shell cd $(TOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' sigs.k8s.io/kustomize/kustomize/v4)
ENVTEST_VERSION ?= $(shell cd $(TOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' sigs.k8s.io/controller-runtime/tools/setup-envtest)
KIND_VERSION ?= $(shell cd $(TOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' sigs.k8s.io/kind)
YQ_VERSION ?= $(shell cd $(TOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' github.com/mikefarah/yq/v4)
HELM_VERSION ?= $(shell cd $(DOCTOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' helm.sh/helm/v3)
HELM_DOCS_VERSION ?= $(shell cd $(DOCTOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' github.com/norwoodj/helm-docs)
HUGO_VERSION ?= $(shell cd $(DOCTOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' github.com/gohugoio/hugo)
MDTOC_VERSION ?= $(shell cd $(DOCTOOLS_DIR) && GO111MODULE=on $(GO_CMD) list -m -f '{{.Version}}' sigs.k8s.io/mdtoc)
GENREF_VERSION ?= v0.28.0

##@ 📦 Tools

.PHONY: fix-tools-gomod
fix-tools-gomod: ## 🔧 Fix go.mod file in tools directory
	@echo "🔧 Fixing tools go.mod file..."
	@if [ -f "$(TOOLS_DIR)/go.mod" ]; then \
		sed -i.bak -e 's/^go 1.23.0/go 1.23/' -e '/^toolchain/d' $(TOOLS_DIR)/go.mod && \
		rm -f $(TOOLS_DIR)/go.mod.bak; \
	fi
	@echo "✅ go.mod fixed"

GOLANGCI_LINT = $(PROJECT_DIR)/bin/golangci-lint
.PHONY: golangci-lint
golangci-lint: fix-tools-gomod ## 🔍 Download golangci-lint locally if necessary
	@echo "🔍 Installing golangci-lint..."
	@# The module proxy drops a download stream now and then; a bounded retry absorbs it.
	@cd $(TOOLS_DIR) && for attempt in 1 2 3; do \
		GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) && break; \
		if [ "$$attempt" = 3 ]; then echo "golangci-lint install failed after 3 attempts" >&2; exit 1; fi; \
		echo "golangci-lint install attempt $$attempt failed; retrying"; sleep 5; \
	done
	@echo "✅ Installation complete"


STATICCHECK = $(PROJECT_DIR)/bin/staticcheck
.PHONY: staticcheck
staticcheck: fix-tools-gomod ## 🔎 Download staticcheck locally if necessary
	@echo "🔎 Installing staticcheck..."
	cd $(TOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install honnef.co/go/tools/cmd/staticcheck@latest
	@echo "✅ Installation complete"

CONTROLLER_GEN = $(PROJECT_DIR)/bin/controller-gen
.PHONY: controller-gen
controller-gen: fix-tools-gomod ## 🎮 Download controller-gen locally if necessary
	@echo "🎮 Installing controller-gen..."
	cd $(TOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
	@echo "✅ Installation complete"

KUSTOMIZE = $(PROJECT_DIR)/bin/kustomize
.PHONY: kustomize
kustomize: fix-tools-gomod ## 🔧 Download kustomize locally if necessary
	@echo "🔧 Installing kustomize..."
	cd $(TOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install sigs.k8s.io/kustomize/kustomize/v4@$(KUSTOMIZE_VERSION)
	@echo "✅ Installation complete"

ENVTEST = $(PROJECT_DIR)/bin/setup-envtest
.PHONY: envtest
envtest: fix-tools-gomod ## 🧪 Download envtest-setup locally if necessary
	@echo "🧪 Installing envtest..."
	cd $(TOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)
	@echo "✅ Installation complete"

KIND = $(PROJECT_DIR)/bin/kind
.PHONY: kind
kind: fix-tools-gomod ## 🐳 Download kind locally if necessary
	@echo "🐳 Installing kind..."
	cd $(TOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install sigs.k8s.io/kind@$(KIND_VERSION)
	@echo "✅ Installation complete"

YQ = $(PROJECT_DIR)/bin/yq
.PHONY: yq
yq: fix-tools-gomod ## 🔧 Download yq locally if necessary
	@echo "🔧 Installing yq..."
	cd $(TOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install github.com/mikefarah/yq/v4@$(YQ_VERSION)
	@echo "✅ Installation complete"

HELM = $(PROJECT_DIR)/bin/helm
.PHONY: helm
helm: fix-tools-gomod ## ⎈ Download helm locally if necessary
	@echo "⎈ Installing helm..."
	cd $(DOCTOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install helm.sh/helm/v3/cmd/helm@$(HELM_VERSION)
	@echo "✅ Installation complete"

HELM_DOCS = $(PROJECT_DIR)/bin/helm-docs
.PHONY: helm-docs
helm-docs: fix-tools-gomod ## 📚 Download helm-docs locally if necessary
	@echo "📚 Installing helm-docs..."
	cd $(DOCTOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install github.com/norwoodj/helm-docs/cmd/helm-docs@$(HELM_DOCS_VERSION)
	@echo "✅ Installation complete"

HUGO = $(PROJECT_DIR)/bin/hugo
.PHONY: hugo
hugo: fix-tools-gomod ## 📝 Download hugo locally if necessary
	@echo "📝 Installing hugo..."
	cd $(DOCTOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin CGO_ENABLED=1 $(GO_CMD) install -tags extended github.com/gohugoio/hugo@$(HUGO_VERSION)
	@echo "✅ Installation complete"

MDTOC = $(PROJECT_DIR)/bin/mdtoc
.PHONY: mdtoc
mdtoc: fix-tools-gomod ## 📑 Download mdtoc locally if necessary
	@echo "📑 Installing mdtoc..."
	cd $(DOCTOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin CGO_ENABLED=1 $(GO_CMD) install sigs.k8s.io/mdtoc@$(MDTOC_VERSION)
	@echo "✅ Installation complete"

GOIMPORTS = $(PROJECT_DIR)/bin/goimports
.PHONY: install-goimports
install-goimports: fix-tools-gomod ## 📦 Install goimports if not present
	@echo "📦 Installing goimports..."
	cd $(TOOLS_DIR) && GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install golang.org/x/tools/cmd/goimports@latest
	@echo "✅ Installation complete"

GENREF = $(PROJECT_DIR)/bin/genref
.PHONY: genref
genref: ## 📚 Download genref locally if necessary
	@echo "📚 Installing genref..."
	@GOBIN=$(PROJECT_DIR)/bin $(GO_CMD) install github.com/kubernetes-sigs/reference-docs/genref@$(GENREF_VERSION)
	@echo "✅ Installation complete"
