# Multi-stage production build for AnchorDB (Minimal Dependencies, Zero CGO, 100% CPU, non-root, < 15 MB)
# Use the latest 1.27.x patch so the image ships a patched standard library.
FROM golang:1.27-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /anchordb-server ./cmd/server

# Final runtime image
FROM alpine:3.22

WORKDIR /app
RUN apk add --no-cache ca-certificates && mkdir -p /data /app && adduser -D -u 1000 anchordb && chown -R anchordb:anchordb /data /app

COPY --from=builder /anchordb-server /app/anchordb-server

# Standard Dedicated AnchorDB Ports (7447: HTTP/REST, 7448: AnchorWire Binary TCP)
EXPOSE 7447 7448

# Volume mount for persistent *.anchordb binary storage
VOLUME ["/data"]

USER anchordb

# Default Environment Variables (Docker & Podman ready)
# SECURITY: AnchorDB refuses to start when ANCHORDB_AUTH_TOKEN is empty unless
# ANCHORDB_ALLOW_INSECURE=true. Provide a strong token at runtime, e.g.
#   docker run -e ANCHORDB_AUTH_TOKEN="$(openssl rand -hex 32)" ...
ENV ANCHORDB_PORT=7447 \
    ANCHORDB_WIRE_PORT=7448 \
    ANCHORDB_DATA_DIR=/data \
    ANCHORDB_AUTH_TOKEN="" \
    ANCHORDB_ALLOW_INSECURE=false \
    ANCHORDB_MEMORY_LIMIT="" \
    ANCHORDB_CRON_ENABLED=true \
    ANCHORDB_PARTITION_SCHEDULE="23:00" \
    ANCHORDB_PARTITION_RETENTION_DAYS=180 \
    ANCHORDB_SYNC_INTERVAL="5m" \
    ANCHORDB_WORKER_ENABLED=true \
    ANCHORDB_WORKER_CONCURRENCY="" \
    ANCHORDB_WORKER_QUEUE_SIZE=1000

HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:7447/health || exit 1

ENTRYPOINT ["/app/anchordb-server"]
