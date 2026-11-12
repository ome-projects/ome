.PHONY: build test clean release-darwin-% release-linux-% release

RUST_TARGET ?=
GO_LDFLAGS = -ldflags="-extldflags '-L./'"
# Where cargo writes the release artifacts.
TARGET_DIR = target/$(if $(RUST_TARGET),$(RUST_TARGET)/,)release
# The operating system the archive is built for: the target triple when
# cross-compiling, the host otherwise.
TARGET_OS = $(if $(RUST_TARGET),$(if $(findstring apple-darwin,$(RUST_TARGET)),Darwin,Linux),$(shell uname -s))
# cargo hard-links $(TARGET_DIR)/libxet.a to the copy under deps/; both are
# listed so each copy is clean whether or not the strip tool keeps the link.
XET_ARCHIVES = $(TARGET_DIR)/libxet.a $(TARGET_DIR)/deps/libxet.a

# Default target
all: build

# Build the Rust library and Go bindings
build:
	@echo "Building Rust library..."
	cargo build --release $(if $(RUST_TARGET),--target $(RUST_TARGET))
	@echo "Removing LLVM bitcode from the static library..."
	@# An LTO build keeps a copy of every object's LLVM bitcode in the archive
	@# (ELF sections .llvmbc and .llvmcmd, Mach-O segment __LLVM). The Go
	@# toolchain refuses to link a cgo archive that carries them, so they are
	@# removed from the finished library before Go sees it. The machine code
	@# is complete without them.
	@for lib in $(XET_ARCHIVES); do \
		[ -f "$$lib" ] || continue; \
		if [ "$(TARGET_OS)" = Darwin ]; then \
			if xcrun -f bitcode_strip >/dev/null 2>&1; then \
				{ xcrun bitcode_strip -r "$$lib" -o "$$lib.nolto" && mv "$$lib.nolto" "$$lib"; } || exit 1; \
			else \
				echo "bitcode_strip not found: $$lib keeps its __LLVM segment and Go may refuse to link it"; \
			fi; \
		else \
			command -v objcopy >/dev/null || { echo "objcopy (binutils) is required to remove the LTO sections from $$lib"; exit 1; }; \
			objcopy --remove-section .llvmbc --remove-section .llvmcmd "$$lib" || exit 1; \
		fi; \
	done
	@echo "Copying static library..."
	@if [ -z "$(RUST_TARGET)" ]; then \
		cp target/release/libxet.a . 2>/dev/null || cp target/release/libxet.dylib . 2>/dev/null || true; \
	else \
		cp target/$(RUST_TARGET)/release/libxet.a . 2>/dev/null || true; \
	fi
	@echo "Building Go bindings..."
	go build $(GO_LDFLAGS) .

# Run tests
test: build
	@echo "Running Rust tests..."
	cargo test
	@echo "Running Go tests..."
	go test $(GO_LDFLAGS) -v ./... -count=1

# Build for specific Darwin architecture
release-darwin-%: test
	@echo "Building for darwin-$*..."
	$(MAKE) build RUST_TARGET=$*-apple-darwin
	mkdir -p artifacts/darwin-$*
	cp libxet.a artifacts/darwin-$*/ 2>/dev/null || cp libxet.dylib artifacts/darwin-$*/ 2>/dev/null || true
	cd artifacts/darwin-$* && tar -czf libxet.darwin-$*.tar.gz libxet.*

# Build for Linux using Docker
release-linux-%:
	@echo "Building for linux-$*..."
	docker build --platform linux/$* \
		--build-arg DOCKER_TARGETPLATFORM=linux/$* \
		-f build/Dockerfile . \
		-t xet-build:linux-$*
	docker run --rm -v $(PWD)/artifacts:/artifacts xet-build:linux-$*

# Build all release targets
release: release-darwin-aarch64 release-darwin-x86_64 release-linux-amd64 release-linux-arm64
	@echo "Release artifacts created in ./artifacts/"

# Clean build artifacts
clean:
	cargo clean
	rm -f libxet.a libxet.so libxet.dylib
	rm -rf artifacts/
	go clean

# Install dependencies
deps:
	@echo "Installing Rust dependencies..."
	cargo fetch
	@echo "Installing Go dependencies..."
	go mod download

# Format code
fmt:
	cargo fmt
	go fmt ./...

# Run linters
lint:
	cargo clippy
	golangci-lint run ./...

# Generate C header from Rust (requires cbindgen)
header:
	cbindgen --config cbindgen.toml --crate ome-xet-binding --output xet.h

# Help target
help:
	@echo "Available targets:"
	@echo "  make build    - Build the library and Go bindings"
	@echo "  make test     - Run all tests"
	@echo "  make clean    - Clean build artifacts"
	@echo "  make release  - Build release artifacts for all platforms"
	@echo "  make deps     - Install dependencies"
	@echo "  make fmt      - Format code"
	@echo "  make lint     - Run linters"
	@echo "  make header   - Generate C header file"
