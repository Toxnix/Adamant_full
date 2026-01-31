#!/bin/bash

set -e

# Load environment variables from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../.env" ]; then
    set -a
    source "$SCRIPT_DIR/../.env"
    set +a
elif [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

NEXTCLOUD_HOST="${NEXTCLOUD_HOST:-nextcloud}"
NEXTCLOUD_USER="${NEXTCLOUD_USER:-root}"
REMOTE_DATA_DIR="${REMOTE_DATA_DIR:-/home/${NEXTCLOUD_USER}/data_sorted}"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-./data_sorted}"

mkdir -p "$LOCAL_DATA_DIR"

# Function to sync directories
sync_dirs() {
	rsync -avz --delete "${NEXTCLOUD_USER}@${NEXTCLOUD_HOST}:${REMOTE_DATA_DIR}/" "${LOCAL_DATA_DIR}/"
}

# Initial sync
sync_dirs

#done
while true; do
    sync_dirs
    sleep 1
done
