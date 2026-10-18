#!/bin/sh
set -e

# Create dedicated system group if not already present
if ! getent group anchordb >/dev/null 2>&1; then
    groupadd --system anchordb
fi

# Create dedicated unprivileged system user if not already present
if ! getent passwd anchordb >/dev/null 2>&1; then
    useradd --system \
        --gid anchordb \
        --no-create-home \
        --home-dir /var/lib/anchordb \
        --shell /usr/sbin/nologin \
        --comment "AnchorDB computational database service account" \
        anchordb
fi

# Prepare persistent data directory with safe permissions
mkdir -p /var/lib/anchordb
chown -R anchordb:anchordb /var/lib/anchordb
chmod 0750 /var/lib/anchordb

# Manage systemd lifecycle if systemd is active
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
    systemctl enable anchordb.service || true

    # Start or restart service cleanly
    if systemctl is-active --quiet anchordb.service; then
        echo "[AnchorDB] Restarting active service..."
        systemctl restart anchordb.service || true
    else
        echo "[AnchorDB] Starting systemd service on port 7447..."
        if ! systemctl start anchordb.service 2>/dev/null; then
            echo "[WARNING] AnchorDB service did not start."
            echo "          Set a strong AUTH_TOKEN in /etc/default/anchordb"
            echo "          (or /etc/sysconfig/anchordb), then restart the service."
            echo "          Check journalctl -u anchordb for other errors, such as a port conflict."
        fi
    fi
fi

exit 0
