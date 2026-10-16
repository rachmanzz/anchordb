#!/bin/sh
set -e

# Stop and disable systemd service cleanly before package uninstallation
if [ -d /run/systemd/system ]; then
    systemctl stop anchordb.service || true
    systemctl disable anchordb.service || true
    systemctl daemon-reload || true
fi

exit 0
