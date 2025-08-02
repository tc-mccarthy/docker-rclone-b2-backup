# Use a minimal official Python image
FROM python:3.13.1-slim-bookworm

# Install system dependencies
# rclone is used for uploading backups to Backblaze B2
RUN apt-get update && \
    apt-get install -y rclone tar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory inside the container
WORKDIR /usr/app

# Copy your Python script into the container
COPY backup.py .

# Create mountable directories for logs and backups
RUN mkdir -p /usr/app/storage/logs /usr/app/storage/backups

# Set up the script to run with a default command
# This assumes the environment variables are passed at runtime
CMD ["python", "backup.py"]
