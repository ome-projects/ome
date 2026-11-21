# Configurable base image - must be declared before any FROM statement
# Defaults to Oracle Linux 10 for OCI SDK compatibility
# Can be overridden with --build-arg BASE_IMAGE=ubuntu:24.04
# Note: Ubuntu 22.04 has glibc 2.35, but golang:1.24 requires glibc 2.38+
ARG BASE_IMAGE=oraclelinux:10-slim

# Build the model-agent binary
FROM golang:1.27 AS builder

# Install Rust and Cargo for building the XET library
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Build arguments for cross-compilation
ARG TARGETOS
ARG TARGETARCH

# Set working directory
WORKDIR /workspace

# Copy go mod files
COPY go.mod go.mod
COPY go.sum go.sum

# Download dependencies with Go module cache
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source code
COPY cmd/ cmd/
COPY pkg/ pkg/
COPY internal/ internal/

# Build the XET library first
RUN cd pkg/xet && make build

# Verify static library exists and remove dynamic library to force static linking
RUN ls -lh /workspace/pkg/xet/target/release/libxet.* && \
    rm -f /workspace/pkg/xet/target/release/libxet.so

# Build arguments for version info
ARG VERSION
ARG GIT_TAG
ARG GIT_COMMIT

# Build the ome-agent binary (CGO must be enabled for XET library)
RUN CGO_ENABLED=1 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -a \
    -ldflags "-X sigs.k8s.io/ome/pkg/version.GitVersion=${GIT_TAG} -X sigs.k8s.io/ome/pkg/version.GitCommit=${GIT_COMMIT}" \
    -o ome-agent ./cmd/ome-agent

# Use the base image specified at the top of the file
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# Install/update packages and runtime dependencies based on the base image
RUN if [ -f /usr/bin/microdnf ]; then \
        microdnf update -y && \
        microdnf install -y \
            glibc \
            libgcc \
            libstdc++ \
            openssl-libs && \
        microdnf clean all; \
    elif [ -f /usr/bin/apt-get ]; then \
        apt-get update && \
        apt-get install -y \
            ca-certificates \
            libc6 \
            libgcc-s1 \
            libstdc++6 \
            libssl3 && \
        apt-get upgrade -y && \
        apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi

COPY --from=builder /workspace/ome-agent /
COPY config/ome-agent/ome-agent.yaml /
ENTRYPOINT ["/ome-agent"]
