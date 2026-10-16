FROM golang:1.26 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -o /model-eviction-controller ./cmd/model-eviction-controller

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /model-eviction-controller /model-eviction-controller
USER 65532:65532
ENTRYPOINT ["/model-eviction-controller"]
