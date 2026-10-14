# Multi-stage production build for AnchorDB (Minimal Dependencies, Zero CGO, 100% CPU, non-root, < 15 MB)
FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod ./
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /anchordb-server cmd/server/main.go

# Final runtime image
FROM alpine:3.19

WORKDIR /app
RUN mkdir -p /data && adduser -D -u 1000 anchordb && chown -R anchordb:anchordb /data /app

COPY --from=builder /anchordb-server /app/anchordb-server

# Standard Dedicated AnchorDB Port
EXPOSE 7447

# Volume mount for persistent *.anchordb binary storage
VOLUME ["/data"]

USER anchordb

# Default Environment Variables
ENV ANCHORDB_PORT=7447 \
    ANCHORDB_DATA_DIR=/data \
    ANCHORDB_AUTH_TOKEN="" \
    ANCHORDB_MEMORY_LIMIT=""

HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:7447/health || exit 1

ENTRYPOINT ["/app/anchordb-server"]
