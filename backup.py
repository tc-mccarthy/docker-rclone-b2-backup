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

# updated implementation inserted below

import os
import subprocess
import datetime
import logging
import sys
import glob
import tarfile
import pathlib
from tqdm import tqdm
import requests
import base64

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
    """Run a shell command and raise an error if it fails.

    Args:
        command (str): The shell command to execute.

    Returns:
        subprocess.CompletedProcess: The result of the executed command.

    Raises:
        RuntimeError: If the command returns a non-zero exit code.
    """
    logging.info(f"Executing: {command}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        logging.error(f"Command failed with exit code {result.returncode}: {command}")
        raise RuntimeError(f"Command failed: {command}")
    return result

def create_backup(source_dir, backup_name):
    """Create a gzipped tarball of the source directory with progress bar.

    Args:
        source_dir (str): Directory to back up.
        backup_name (str): Filename of the backup.

    Returns:
        str: Full path to the created tar.gz file.
    """
    # Ensure backup directory exists
    os.makedirs(BACKUP_DIR, exist_ok=True)
    backup_path = os.path.join(BACKUP_DIR, backup_name)

    logging.info("Gathering files for backup...")
    # Recursively collect all files to be archived
    all_files = [f for f in pathlib.Path(source_dir).rglob("*") if f.is_file()]

    # Create tar.gz archive with progress bar
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
    """Prune local backups to retain only the specified number.

    Args:
        retention_count (int): Number of local backups to retain.
    """
    # Find all backup files matching the naming pattern
    backups = sorted(glob.glob(os.path.join(BACKUP_DIR, f"{JOB_NAME}-backup-*.tar.gz")))
    # Determine which backups are old (to delete)
    old_backups = backups[:-retention_count] if len(backups) > retention_count else []
    # Remove each old backup file
    for file_path in old_backups:
        os.remove(file_path)
        logging.info(f"Deleted old local backup: {file_path}")

def get_b2_auth_token(account_id, account_key):
    """Obtain a Backblaze B2 API authorization token.

    Args:
        account_id (str): B2 account ID.
        account_key (str): B2 application key.

    Returns:
        dict: JSON response containing API URL and authorization token.
    """
    auth_str = f"{account_id}:{account_key}"
    encoded_auth = base64.b64encode(auth_str.encode()).decode()
    headers = {"Authorization": f"Basic {encoded_auth}"}
    resp = requests.get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account", headers=headers)
    resp.raise_for_status()
    return resp.json()

def list_b2_files(api_url, auth_token, bucket_id, prefix):
    """List files in a B2 bucket with a given prefix.

    Args:
        api_url (str): B2 API URL.
        auth_token (str): B2 authorization token.
        bucket_id (str): B2 bucket ID.
        prefix (str): File prefix to filter.

    Returns:
        list: List of file metadata dicts.
    """
    url = f"{api_url}/b2api/v2/b2_list_file_names"
    headers = {"Authorization": auth_token}
    data = {
        "bucketId": bucket_id,
        "prefix": prefix,
        "maxFileCount": 1000
    }
    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()
    return resp.json().get("files", [])

def delete_b2_file(api_url, auth_token, file_name, file_id):
    """Delete a file version from B2 bucket.

    Args:
        api_url (str): B2 API URL.
        auth_token (str): B2 authorization token.
        file_name (str): Name of the file to delete.
        file_id (str): File ID to delete.
    """
    url = f"{api_url}/b2api/v2/b2_delete_file_version"
    headers = {"Authorization": auth_token}
    data = {"fileName": file_name, "fileId": file_id}
    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()

def prune_old_backups_remote_b2(bucket_id, prefix, retention_count, account_id, account_key):
    """Prune old backups from B2 bucket using B2 API directly.

    Args:
        bucket_id (str): B2 bucket ID.
        prefix (str): File prefix to filter.
        retention_count (int): Number of remote backups to retain.
        account_id (str): B2 account ID.
        account_key (str): B2 application key.
    """
    # Authenticate and get API endpoint/token
    auth_data = get_b2_auth_token(account_id, account_key)
    api_url = auth_data["apiUrl"]
    auth_token = auth_data["authorizationToken"]

    logging.info(f"Pruning remote B2 backups in {prefix}, keeping last {retention_count}.")
    # List all files with the given prefix
    files = list_b2_files(api_url, auth_token, bucket_id, prefix)
    # Sort files by upload timestamp (oldest first)
    sorted_files = sorted(files, key=lambda f: f["uploadTimestamp"])
    # Determine which files to delete
    old_files = sorted_files[:-retention_count] if len(sorted_files) > retention_count else []

    # Delete each old file
    for f in old_files:
        delete_b2_file(api_url, auth_token, f["fileName"], f["fileId"])
        logging.info(f"Deleted remote B2 backup: {f['fileName']}")

def validate_b2_credentials(remote_path):
    """Validate B2 credentials using rclone by testing access to the remote path.

    Args:
        remote_path (str): Rclone B2 destination path.

    Raises:
        RuntimeError: If credentials are invalid or access fails.
    """
    logging.info("Validating Backblaze B2 credentials...")
    test_command = f"rclone lsf '{remote_path}'"
    result = subprocess.run(test_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logging.error(f"B2 credential validation failed: {result.stderr.decode().strip()}")
        raise RuntimeError("Invalid B2 credentials or configuration.")


def main():
    """Main backup routine orchestrating the full backup workflow.

    Gathers configuration, validates credentials, creates backup, uploads to B2,
    prunes old backups both remotely and locally, and logs the process.
    """
    # --- Gather configuration from environment variables ---
    source_dir = "/backup_source"
    b2_bucket = os.environ.get("B2_BUCKET")
    remote_path = os.environ.get("REMOTE_PATH")
    b2_account = os.environ.get("B2_ACCOUNT_ID")
    b2_key = os.environ.get("B2_ACCOUNT_KEY")
    local_retention = int(os.environ.get("LOCAL_RETENTION", 30))
    remote_retention = int(os.environ.get("REMOTE_RETENTION", 30))

    # --- Validate required configuration ---
    if not b2_bucket or not remote_path or not b2_account or not b2_key:
        logging.error("Missing required environment variables. Required: B2_BUCKET, REMOTE_PATH, B2_ACCOUNT_ID, B2_ACCOUNT_KEY")
        sys.exit(1)

    # --- Compose remote path and backup filename ---
    b2_remote = f"B2:{b2_bucket}/{remote_path}".rstrip('/')
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_name = f"{JOB_NAME}-backup-{timestamp}.tar.gz"

    try:
        # Step 1: Validate credentials before doing anything destructive
        validate_b2_credentials(b2_remote)
        # Step 2: Create local backup tarball
        backup_path = create_backup(source_dir, backup_name)
        # Step 3: Upload backup to B2 cloud
        upload_to_b2(backup_path, b2_remote)
        # Step 4: Prune old backups in remote cloud using B2 API
        prune_old_backups_remote_b2(b2_bucket, remote_path, remote_retention, b2_account, b2_key)
        # Step 5: Prune old backups locally
        prune_old_backups_local(local_retention)
        logging.info(f"Backup {backup_name} complete and cleaned up.")
    except Exception as e:
        logging.error(f"Backup process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
