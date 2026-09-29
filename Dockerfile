# syntax=docker/dockerfile:1

# Cross-compile natively on the build machine; the binary is pure Go (no CGO), so no emulation is needed.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH TARGETVARIANT
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} \
    go build -trimpath -tags timetzdata -ldflags="-s -w" -o /out/bridge-monitor . \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bridge-monitor /bridge-monitor
# Named volumes inherit this ownership, so the nonroot user can write the database.
COPY --from=build --chown=65532:65532 /out/data /data
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
    CMD ["/bridge-monitor", "-healthcheck", "http://127.0.0.1:8080/healthz"]
ENTRYPOINT ["/bridge-monitor", "-config", "/config/config.json", "-data-dir", "/data", "-listen", ":8080"]
