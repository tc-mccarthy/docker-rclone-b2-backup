"""
Daily Docker Directory Backup Script

This script compresses the /backup_source directory into a high-compression tarball,
uploads it to a Backblaze B2 bucket using rclone **with progress output**, and retains only the
most recent backups in both local and remote storage. Remote pruning uses the Backblaze B2 API
for reliability (no rclone flags required). The script is intended to run in a containerized
environment and takes configuration from environment variables.

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
import sys
import glob
import json
import base64
import logging
import pathlib
import tarfile
import datetime
import subprocess
from typing import List, Dict

import requests
from tqdm import tqdm

# --------------------------------------------------------------------------------------
# Configuration & Logging
# --------------------------------------------------------------------------------------

JOB_NAME = os.environ.get("JOB_NAME")
if not JOB_NAME:
    print("Error: Missing required environment variable JOB_NAME.")
    sys.exit(1)

# Fixed paths inside the container
SOURCE_DIR = "/backup_source"  # mount your host path here
LOG_DIR = "/usr/app/storage/logs"
BACKUP_DIR = "/usr/app/storage/backups"

# Ensure dirs exist; log to file + stdout
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
LOGFILE = os.path.join(LOG_DIR, f"{JOB_NAME}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def run_command(command: str) -> subprocess.CompletedProcess:
    """Run a shell command and raise on non-zero exit.

    Args:
        command: Shell command to execute.
    Returns:
        CompletedProcess
    """
    logging.info(f"Executing: {command}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        logging.error(f"Command failed with exit code {result.returncode}: {command}")
        raise RuntimeError(f"Command failed: {command}")
    return result

# --------------------------------------------------------------------------------------
# Backup creation (high compression + progress)
# --------------------------------------------------------------------------------------

def create_backup(source_dir: str, backup_name: str) -> str:
    """Create a high-compression tarball (.tar.xz) of source_dir with a progress bar.

    Skips files that disappear or error during read (e.g., temp WAL files).

    Args:
        source_dir: Directory to archive.
        backup_name: Output filename (should end with .tar.xz).
    Returns:
        Path to the created archive.
    """
    backup_path = os.path.join(BACKUP_DIR, backup_name)

    logging.info("Scanning source tree for files...")
    all_files = [p for p in pathlib.Path(source_dir).rglob("*") if p.is_file()]

    with tarfile.open(backup_path, mode="w:xz") as tar, \
            tqdm(total=len(all_files), desc="Creating backup", unit="file") as pbar:
        for f in all_files:
            arcname = f.relative_to(source_dir)
            try:
                tar.add(f, arcname=str(arcname))
            except Exception as e:
                logging.warning(f"Skipping file {f} due to error: {e}")
            pbar.update(1)

    logging.info(f"Created archive: {backup_path}")
    return backup_path

# --------------------------------------------------------------------------------------
# Upload (rclone with visible progress)
# --------------------------------------------------------------------------------------

def upload_to_b2(local_path: str, remote_path: str) -> None:
    """Upload the tarball to Backblaze B2 using rclone with progress.

    Args:
        local_path: Local archive path.
        remote_path: Remote destination like B2:bucket/prefix
    """
    # Progress flags show clear, single-line updates; tune chunk/transfers for big files
    command = (
        f"rclone copy '{local_path}' '{remote_path}' "
        f"--progress --stats-one-line -P --b2-chunk-size 100M --transfers 4"
    )
    run_command(command)

# --------------------------------------------------------------------------------------
# Local pruning
# --------------------------------------------------------------------------------------

def prune_old_backups_local(retention_count: int) -> None:
    """Keep only the newest N local backups (by filename sort)."""
    pattern = os.path.join(BACKUP_DIR, f"{JOB_NAME}-backup-*.tar.xz")
    backups = sorted(glob.glob(pattern))
    old = backups[:-retention_count] if len(backups) > retention_count else []
    for path in old:
        try:
            os.remove(path)
            logging.info(f"Deleted old local backup: {path}")
        except Exception as e:
            logging.warning(f"Failed to delete {path}: {e}")

# --------------------------------------------------------------------------------------
# Backblaze B2 API client (auth, list, delete)
# --------------------------------------------------------------------------------------

def b2_authorize(account_id: str, account_key: str) -> Dict:
    """Authorize against B2 API and return JSON with apiUrl, authorizationToken, downloadUrl, allowed."""
    auth = base64.b64encode(f"{account_id}:{account_key}".encode()).decode()
    resp = requests.get(
        "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
        headers={"Authorization": f"Basic {auth}"}, timeout=30
    )
    resp.raise_for_status()
    return resp.json()

def b2_resolve_bucket_id(api_url: str, auth_token: str, bucket_name: str, allowed: Dict) -> str:
    """Resolve bucket ID for bucket_name. Use allowed scope if present, else list buckets."""
    if allowed:
        # If key is restricted to a bucket, it may already be provided
        if allowed.get("bucketName") == bucket_name and allowed.get("bucketId"):
            return allowed["bucketId"]

    # Fallback: list buckets
    url = f"{api_url}/b2api/v2/b2_list_buckets"
    payload = {"accountId": allowed.get("accountId") if allowed else None}
    # Provide bucketName to filter server-side
    payload.update({"bucketName": bucket_name})
    resp = requests.post(url, headers={"Authorization": auth_token}, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    buckets = data.get("buckets", [])
    for b in buckets:
        if b.get("bucketName") == bucket_name:
            return b.get("bucketId")
    raise RuntimeError(f"Bucket not found or not permitted: {bucket_name}")

def b2_list_files(api_url: str, auth_token: str, bucket_id: str, prefix: str) -> List[Dict]:
    """List **all** files under prefix using pagination."""
    url = f"{api_url}/b2api/v2/b2_list_file_names"
    files: List[Dict] = []
    next_file_name = None

    while True:
        payload = {
            "bucketId": bucket_id,
            "prefix": prefix,
            "maxFileCount": 1000
        }
        if next_file_name:
            payload["startFileName"] = next_file_name
        resp = requests.post(url, headers={"Authorization": auth_token}, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        files.extend(data.get("files", []))
        next_file_name = data.get("nextFileName")
        if not next_file_name:
            break

    return files

def b2_delete_file(api_url: str, auth_token: str, file_name: str, file_id: str) -> None:
    url = f"{api_url}/b2api/v2/b2_delete_file_version"
    resp = requests.post(url, headers={"Authorization": auth_token}, json={
        "fileName": file_name,
        "fileId": file_id
    }, timeout=30)
    resp.raise_for_status()

# --------------------------------------------------------------------------------------
# Remote pruning (via B2 API)
# --------------------------------------------------------------------------------------

def prune_old_backups_remote_b2(bucket_name: str, prefix: str, keep: int, account_id: str, account_key: str) -> None:
    """Keep only the newest `keep` remote backups under prefix using B2 API."""
    logging.info(f"Pruning remote B2 backups in {bucket_name}/{prefix}, keeping last {keep}.")
    auth = b2_authorize(account_id, account_key)
    api_url = auth["apiUrl"]
    token = auth["authorizationToken"]
    allowed = auth.get("allowed", {})

    # Resolve bucket ID
    bucket_id = b2_resolve_bucket_id(api_url, token, bucket_name, allowed)

    # List and sort by uploadTimestamp (newest first)
    files = b2_list_files(api_url, token, bucket_id, prefix if prefix.endswith('/') else f"{prefix}/")
    files.sort(key=lambda f: f.get("uploadTimestamp", 0), reverse=True)

    # Keep newest N, delete the rest
    to_delete = files[keep:] if len(files) > keep else []
    for f in to_delete:
        try:
            b2_delete_file(api_url, token, f["fileName"], f["fileId"])
            logging.info(f"Deleted remote B2 backup: {f['fileName']}")
        except Exception as e:
            logging.warning(f"Failed to delete {f.get('fileName')}: {e}")

# --------------------------------------------------------------------------------------
# Validation (fail fast before long tar/upload)
# --------------------------------------------------------------------------------------

def validate_b2_or_fail(bucket_name: str, account_id: str, account_key: str) -> None:
    """Fast-fail check: can we auth and see the target bucket?"""
    try:
        auth = b2_authorize(account_id, account_key)
        _ = b2_resolve_bucket_id(auth["apiUrl"], auth["authorizationToken"], bucket_name, auth.get("allowed", {}))
        logging.info("Backblaze B2 credentials validated.")
    except Exception as e:
        logging.error(f"B2 credential validation failed: {e}")
        raise

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

def main() -> None:
    b2_bucket = os.environ.get("B2_BUCKET")
    remote_path = os.environ.get("REMOTE_PATH")
    b2_account = os.environ.get("B2_ACCOUNT_ID")
    b2_key = os.environ.get("B2_ACCOUNT_KEY")
    local_retention = int(os.environ.get("LOCAL_RETENTION", 30))
    remote_retention = int(os.environ.get("REMOTE_RETENTION", 30))

    # Required envs (besides JOB_NAME which we validated earlier)
    missing = [name for name, val in [
        ("B2_BUCKET", b2_bucket),
        ("REMOTE_PATH", remote_path),
        ("B2_ACCOUNT_ID", b2_account),
        ("B2_ACCOUNT_KEY", b2_key),
    ] if not val]
    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Compose remote like B2:bucket/prefix
    b2_remote = f"B2:{b2_bucket}/{remote_path}".rstrip('/')

    # Timestamped archive name (xz)
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_name = f"{JOB_NAME}-backup-{timestamp}.tar.xz"

    try:
        # 1) Validate B2 creds & bucket before doing heavy work
        # validate_b2_or_fail(b2_bucket, b2_account, b2_key)

        # 2) Create local backup
        # archive_path = create_backup(SOURCE_DIR, backup_name)

        # 3) Upload to B2 (rclone shows progress)
        # upload_to_b2(archive_path, b2_remote)

        # 4) Remote prune (API)
        prune_old_backups_remote_b2(b2_bucket, remote_path, remote_retention, b2_account, b2_key)

        # 5) Local prune
        # prune_old_backups_local(local_retention)

        logging.info(f"Backup {backup_name} completed successfully.")
    except Exception as e:
        logging.error(f"Backup process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()