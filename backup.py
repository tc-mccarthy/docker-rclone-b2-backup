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

Author: TC McCarthy (with assistance from ChatGPT)
License: MIT or similar permissive license
"""

import os
import subprocess
import datetime
import logging
import sys
import glob

# Configure logging directory and file
JOB_NAME = os.environ.get("JOB_NAME")
if not JOB_NAME:
    print("Error: Missing required environment variable JOB_NAME.")
    sys.exit(1)

LOG_DIR = "/usr/app/storage/logs"
BACKUP_DIR = "/usr/app/storage/backups"
LOGFILE = os.path.join(LOG_DIR, f"{JOB_NAME}.log")

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

    Args:
        command (str): The shell command to execute.

    Returns:
        subprocess.CompletedProcess: The completed process object.
    """
    logging.info(f"Executing: {command}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        logging.error(f"Command failed with exit code {result.returncode}: {command}")
        raise RuntimeError(f"Command failed: {command}")
    return result

def create_backup(source_dir, backup_name):
    """Create a gzipped tarball of the source directory with progress.

    Args:
        source_dir (str): Directory to back up.
        backup_name (str): Filename of the backup.

    Returns:
        str: Full path to the created tar.gz file.
    """
    import tarfile
    import pathlib
    from tqdm import tqdm

    os.makedirs(BACKUP_DIR, exist_ok=True)
    backup_path = os.path.join(BACKUP_DIR, backup_name)

    logging.info("Gathering files for backup...")
    # Gather all files to back up
    all_files = [f for f in pathlib.Path(source_dir).rglob("*") if f.is_file()]

    with tarfile.open(backup_path, "w:gz") as tar, tqdm(total=len(all_files), desc="Creating backup", unit="file") as pbar:
        for file in all_files:
            arcname = file.relative_to(source_dir)
            try:
                tar.add(file, arcname=arcname)
            except Exception as e:
                logging.warning(f"Skipping file {file} due to error: {e}")
            pbar.update(1)

    return backup_path

def upload_to_b2(local_path, remote_path):
    """Upload the tarball to Backblaze B2 using rclone.

    Args:
        local_path (str): Path to the local tar.gz file.
        remote_path (str): Rclone B2 destination.
    """
    command = f"rclone copy '{local_path}' '{remote_path}'"
    run_command(command)

def prune_old_backups_local(retention_count):
    """
    Delete older local backup files, keeping only the most recent N.

    This function ensures the local disk doesn't fill up by limiting the
    number of retained backup archives in BACKUP_DIR.

    Args:
        retention_count (int): Number of recent backups to retain.
    """
    backups = sorted(glob.glob(os.path.join(BACKUP_DIR, f"{JOB_NAME}-backup-*.tar.gz")))
    old_backups = backups[:-retention_count] if len(backups) > retention_count else []
    for file_path in old_backups:
        os.remove(file_path)
        logging.info(f"Deleted old local backup: {file_path}")

def prune_old_backups_remote(remote_path, retention_count):
    """
    Prune older backups stored in the remote B2 bucket.

    This uses `rclone lsf` to list files and `rclone delete` to remove
    older ones beyond the retention limit.

    Args:
        remote_path (str): Remote path (e.g., B2:bucket/path).
        retention_count (int): Number of recent backups to retain.
    """
    list_command = f"rclone lsf --files-only --sort -time '{remote_path}'"
    logging.info(f"Pruning remote backups in {remote_path}, keeping last {retention_count}.")
    result = subprocess.run(list_command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"Failed to list remote backups: {result.stderr}")
        raise RuntimeError("Failed to list remote backups")

    files = result.stdout.strip().split('\n')
    old_files = files[:-retention_count] if len(files) > retention_count else []

    for file in old_files:
        delete_command = f"rclone delete '{remote_path}/{file}'"
        run_command(delete_command)
        logging.info(f"Deleted old remote backup: {file}")

def validate_b2_credentials(remote_path):
    """
    Validate B2 credentials using rclone by testing access to the remote path.

    Args:
        remote_path (str): Remote path to test (e.g., B2:bucket/path)
    """
    logging.info("Validating Backblaze B2 credentials...")
    test_command = f"rclone lsf '{remote_path}'"
    result = subprocess.run(test_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logging.error(f"B2 credential validation failed: {result.stderr.decode().strip()}")
        raise RuntimeError("Invalid B2 credentials or configuration.")

def main():
    """Main backup routine."""
    source_dir = "/backup_source"  # Fixed value for containerized execution
    b2_bucket = os.environ.get("B2_BUCKET")
    remote_path = os.environ.get("REMOTE_PATH")
    b2_account = os.environ.get("B2_ACCOUNT_ID")
    b2_key = os.environ.get("B2_ACCOUNT_KEY")
    local_retention = int(os.environ.get("LOCAL_RETENTION", 30))
    remote_retention = int(os.environ.get("REMOTE_RETENTION", 30))

    if not b2_bucket or not remote_path or not b2_account or not b2_key:
        logging.error("Missing required environment variables. Required: B2_BUCKET, REMOTE_PATH, B2_ACCOUNT_ID, B2_ACCOUNT_KEY")
        sys.exit(1)

    b2_remote = f"B2:{b2_bucket}/{remote_path}".rstrip('/')
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_name = f"{JOB_NAME}-backup-{timestamp}.tar.gz"

    try:
        validate_b2_credentials(b2_remote)
        backup_path = create_backup(source_dir, backup_name)
        upload_to_b2(backup_path, b2_remote)
        prune_old_backups_remote(b2_remote, remote_retention)
        prune_old_backups_local(local_retention)
        logging.info(f"Backup {backup_name} complete and cleaned up.")
    except Exception as e:
        logging.error(f"Backup process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
