#!/bin/bash
# Script to download server.db files from Clever Cloud via SSH
set -e

BACKUP_DIR="$(dirname "$0")"
SSH_HOST="sshgateway-clevercloud-customers.services.clever-cloud.com"
APP_ID="app_8a8f8bce-3219-4fbb-ac19-a6b6783eff5b"
SSH_KEY="$HOME/.ssh/id_ed25519"

eval "$(ssh-agent -s)" > /dev/null 2>&1
ssh-add "$SSH_KEY" 2>/dev/null

# Use expect-style approach: pipe commands through interactive ssh
# The gateway requires -t for TTY but we can still capture output

for file in server.db server.db-wal server.db-shm; do
    echo "Downloading $file..."
    
    # Use tar + base64 to safely transfer through TTY
    ssh -o StrictHostKeyChecking=no -t ssh@$SSH_HOST "$APP_ID" \
        "tar czf - -C /app/data $file | base64" 2>/dev/null \
        | tr -d '\r' \
        | grep -E '^[A-Za-z0-9+/=]+$' \
        | base64 -d \
        | tar xzf - -C "$BACKUP_DIR" 2>/dev/null
    
    if [ -f "$BACKUP_DIR/$file" ]; then
        echo "  ✓ Downloaded $file ($(stat -c%s "$BACKUP_DIR/$file") bytes)"
    else
        echo "  ✗ Failed to download $file"
    fi
done

echo "Done."
