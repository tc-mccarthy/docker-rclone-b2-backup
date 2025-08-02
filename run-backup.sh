#!/bin/bash

# Exit on any error
set -e

# Get the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check for .env file
if [ ! -f .env ]; then
  echo "❌ Error: .env file not found in $SCRIPT_DIR"
  echo "Please create one based on .env-sample before running this script."
  exit 1
fi

# Source the .env file
echo "📦 Loading environment variables from .env"
source .env

# Confirm key variables
REQUIRED_VARS=(JOB_NAME B2_BUCKET REMOTE_PATH B2_ACCOUNT_ID B2_ACCOUNT_KEY)
for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "❌ Error: Required variable $VAR is not set in .env"
    exit 1
  fi
done

# Run the backup job using docker compose
echo "🚀 Running backup job for '$JOB_NAME'..."
docker compose run --rm docker-rclone-b2-backup

echo "✅ Backup complete."
