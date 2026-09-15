# Test-only packaging of the unmodified, locally cross-compiled binaries.
# This deliberately does not claim to validate production image packaging.
FROM scratch AS manager
COPY manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]

FROM scratch AS alfred
COPY alfred /alfred
COPY alfred-simulator /alfred-simulator
USER 65532:65532
ENTRYPOINT ["/alfred"]

FROM scratch AS scheduler
COPY ome-scheduler /ome-scheduler
USER 65532:65532
ENTRYPOINT ["/ome-scheduler"]
