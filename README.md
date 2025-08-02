# 🐳 Docker RClone B2 Backup

This script compresses the `BACKUP_SOURCE` directory (provided via env var or defaulting to `/backup_source`)
into a gzipped tarball, uploads it to a Backblaze B2 bucket using rclone, and retains only the
most recent backups in both local and remote storage. This script is
intended to run in a containerized environment and takes configuration
from environment variables.

---

## ⚙️ How It Works

1. Reads config from environment variables (see `.env-sample`)
2. Archives the source directory to a `.tar.gz` file
3. Uploads to Backblaze B2 using `rclone`
4. Prunes backups beyond `LOCAL_RETENTION` and `REMOTE_RETENTION` counts
5. Logs every step to `/usr/app/storage/logs/$JOB_NAME.log`

---

## ⏰ Scheduling

> ❗ This script does **not** include a built-in scheduler.

Use cron, systemd timers, Kubernetes CronJobs, GitHub Actions, or another external scheduler.

### Example: Daily Cron Job (runs at 2:30 AM)

```cron
30 2 * * * ./run-backup.sh >> /var/log/docker-backup.log 2>&1
````

---

## 🔧 Required Environment Variables

* `JOB_NAME` – Name for the job (used in logs/filenames)
* `BACKUP_SOURCE` – Host path to the directory to back up
* `B2_BUCKET` – Your B2 bucket name
* `B2_ACCOUNT_ID` – B2 Application Key ID
* `B2_ACCOUNT_KEY` – B2 Application Key
* `REMOTE_PATH` – Folder inside the bucket to store backups

---

## 📦 Optional Environment Variables

* `LOCAL_RETENTION` – Number of local backups to keep (default: 30)
* `REMOTE_RETENTION` – Number of remote B2 backups to keep (default: 30)

---

## 🧪 Example Output

A backup with `JOB_NAME=media-rig` might create:

* `media-rig-backup-20250802-031501.tar.gz`
* Stored in: `/usr/app/storage/backups`
* Uploaded to: `b2://<your-bucket>/<REMOTE_PATH>/`
* Logged in: `/usr/app/storage/logs/media-rig.log`

---

## 🙌 Credit

Created by **TC McCarthy** with assistance from [ChatGPT](https://openai.com/chatgpt)
📜 Licensed under MIT or similar permissive license