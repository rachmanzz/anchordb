# Ultra-lightweight production image for AnchorDB (Docker / Podman / Cloud Host)
FROM alpine:3.19

WORKDIR /app
RUN mkdir -p /data

# Copy pre-compiled static binary (Zero CGO, 100% CPU, < 10 MB)
COPY bin/anchordb-server /app/anchordb-server

# Standard Dedicated AnchorDB Port
EXPOSE 7447

# Volume mount for persistent *.anchordb binary storage
VOLUME ["/data"]

# Default Environment Variables
ENV ANCHORDB_PORT=7447 \
    ANCHORDB_DATA_DIR=/data \
    ANCHORDB_AUTH_TOKEN=""

# Run daemon
ENTRYPOINT ["/app/anchordb-server"]
