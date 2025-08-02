"""
Daily Docker Directory Backup Script

This script compresses the /backup_source directory into a gzipped tarball,
uploads it to a Backblaze B2 bucket using rclone, and retains only the
most recent backups in both local and remote storage. This script is
intended to run in a containerized environment and takes configuration
from environment variables.

Note: This script does not include a built-in scheduler. It is intended to be
run on an external schedule using cron, systemd timers, Kubernetes CronJobs,
or other task runners.

### Example: Daily Cron Job (runs at 2:30 AM)

```cron
30 2 * * * ./run-backup.sh >> /var/log/docker-backup.log 2>&1
```

You can also use `systemd` timers, GitHub Actions, or `kubectl create cronjob` if deploying in cloud-native environments.

Author: TC (with assistance from ChatGPT)
License: MIT or similar permissive license
"""

import os
import subprocess
import datetime
import logging
import sys
import glob

# --- Environment and Logging Setup ---
# Get the job name from environment (used for naming backups and logs)
JOB_NAME = os.environ.get("JOB_NAME")
if not JOB_NAME:
    print("Error: Missing required environment variable JOB_NAME.")
    sys.exit(1)

# Define directories for logs and backups
LOG_DIR = "/usr/app/storage/logs"
BACKUP_DIR = "/usr/app/storage/backups"
LOGFILE = os.path.join(LOG_DIR, f"{JOB_NAME}.log")

# Ensure log directory exists and configure logging to both file and stdout
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_command(command):
    """
    Run a shell command and raise an error if it fails.
    Used for all system-level operations (tar, rclone, etc).
    """
    logging.info(f"Executing: {command}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        logging.error(f"Command failed with exit code {result.returncode}: {command}")
        raise RuntimeError(f"Command failed: {command}")
    return result

def create_backup(source_dir, backup_name):
    """
    Create a gzipped tarball of the source directory.
    This is the main local backup step before uploading to cloud.

    Args:
        source_dir (str): Directory to back up.
        backup_name (str): Filename of the backup.

    Returns:
        str: Full path to the created tar.gz file.
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    backup_path = os.path.join(BACKUP_DIR, backup_name)
    command = f"tar -czf {backup_path} -C {source_dir} ."
    run_command(command)
    return backup_path

def upload_to_b2(local_path, remote_path):
    """
    Upload the tarball to Backblaze B2 using rclone.
    Handles cloud transfer after local backup is created.

    Args:
        local_path (str): Path to the local tar.gz file.
        remote_path (str): Rclone B2 destination.
    """
    command = f"rclone copy '{local_path}' '{remote_path}'"
    run_command(command)

def prune_old_backups_local(retention_count):
    """
    Prune local backups to retain only the specified number.
    Prevents disk from filling up by deleting oldest backups.

    Args:
        retention_count (int): Number of local backups to retain.
    """
    backups = sorted(glob.glob(os.path.join(BACKUP_DIR, f"{JOB_NAME}-backup-*.tar.gz")))
    old_backups = backups[:-retention_count] if len(backups) > retention_count else []
    for file_path in old_backups:
        os.remove(file_path)
        logging.info(f"Deleted old local backup: {file_path}")

def prune_old_backups_remote(remote_path, retention_count):
    """
    Keep only the latest backups in the B2 bucket.
    Ensures cloud storage doesn't grow unbounded by removing oldest files.

    Args:
        remote_path (str): Rclone B2 destination.
        retention_count (int): Number of remote backups to retain.
    """
    # List files in remote backup directory, sorted by time
    list_command = f"rclone lsf --files-only --sort -time '{remote_path}'"
    logging.info(f"Pruning remote backups in {remote_path}, keeping last {retention_count}.")
    result = subprocess.run(list_command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"Failed to list remote backups: {result.stderr}")
        raise RuntimeError("Failed to list remote backups")

    files = result.stdout.strip().split('\n')
    old_files = files[:-retention_count] if len(files) > retention_count else []

    # Delete each old file from remote
    for file in old_files:
        delete_command = f"rclone delete '{remote_path}/{file}'"
        run_command(delete_command)
        logging.info(f"Deleted old remote backup: {file}")

def validate_b2_credentials():
    """
    Validate B2 credentials using rclone.
    Checks that rclone can access the B2 bucket before proceeding.
    """
    logging.info("Validating Backblaze B2 credentials...")
    test_command = "rclone about b2:"
    result = subprocess.run(test_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logging.error(f"B2 credential validation failed: {result.stderr.decode().strip()}")
        raise RuntimeError("Invalid B2 credentials or configuration.")

def main():
    """
    Main backup routine.
    Orchestrates the full backup workflow: validate, backup, upload, prune.
    """
    # --- Gather configuration from environment variables ---
    source_dir = os.environ.get("BACKUP_SOURCE", "/backup_source")
    b2_bucket = os.environ.get("B2_BUCKET")
    remote_path = os.environ.get("REMOTE_PATH")
    b2_account = os.environ.get("B2_ACCOUNT_ID")
    b2_key = os.environ.get("B2_ACCOUNT_KEY")
    local_retention = int(os.environ.get("LOCAL_RETENTION", 30))
    remote_retention = int(os.environ.get("REMOTE_RETENTION", 30))

    # --- Validate required configuration ---
    if not b2_bucket or not remote_path or not b2_account or not b2_key or not source_dir:
        logging.error("Missing required environment variables. Required: B2_BUCKET, REMOTE_PATH, B2_ACCOUNT_ID, B2_ACCOUNT_KEY, BACKUP_SOURCE")
        sys.exit(1)

    # --- Compose remote path and backup filename ---
    b2_remote = f"b2:{b2_bucket}/{remote_path}".rstrip('/')
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_name = f"{JOB_NAME}-backup-{timestamp}.tar.gz"

    try:
        # Step 1: Validate credentials before doing anything destructive
        validate_b2_credentials()
        # Step 2: Create local backup tarball
        backup_path = create_backup(source_dir, backup_name)
        # Step 3: Upload backup to B2 cloud
        upload_to_b2(backup_path, b2_remote)
        # Step 4: Prune old backups in remote cloud
        prune_old_backups_remote(b2_remote, remote_retention)
        # Step 5: Prune old backups locally
        prune_old_backups_local(local_retention)
        logging.info(f"Backup {backup_name} complete and cleaned up.")
    except Exception as e:
        logging.error(f"Backup process failed: {e}")
        sys.exit(1)

# --- Entry point ---
if __name__ == "__main__":
    main()
