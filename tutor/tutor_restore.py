#!/usr/bin/env python3
import os
import subprocess
import shutil
import sys
import json
import time
import argparse
import tarfile
import hashlib
from datetime import datetime
from dotenv import load_dotenv

# ========= LOAD ENVIRONMENT =========

load_dotenv()

CLIENT_NAME = os.getenv("CLIENT_NAME", "buma")
ENV_TYPE = os.getenv("ENV_TYPE", "prod")

REMOTE_SERVER = os.getenv("REMOTE_SERVER")
REMOTE_PATH = os.getenv("REMOTE_PATH")

SSH_PRIVATE_KEY = os.getenv("SSH_PRIVATE_KEY")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH", "/home/ubuntu/.ssh/backup_id_rsa")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_S3_PREFIX = os.getenv("AWS_S3_PREFIX", "tutor-backup")

GCP_SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

RESTORE_DIR = "/tmp/tutor-restore"

# ========= UTILS =========

def log_time(message, start_time):
    elapsed = time.perf_counter() - start_time
    print(f"[{message}] completed in {elapsed:.2f} seconds.")

def run(cmd, check=True):
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=check)

def install_python_package(pkg):
    subprocess.run([sys.executable, "-m", "pip", "install", pkg], check=True)

def is_installed(command):
    return shutil.which(command) is not None

def install_requirements():
    pkgs = []
    if not is_installed("aws"):
        pkgs.append("awscli")
    if not is_installed("rsync"):
        pkgs.append("rsync")
    if pkgs:
        run(f"sudo apt-get update && sudo apt-get install -y {' '.join(pkgs)}")

    try:
        import boto3
    except ImportError:
        install_python_package("boto3")

    try:
        from google.cloud import storage
    except ImportError:
        install_python_package("google-cloud-storage")

def ensure_ssh_key():
    if os.path.exists(SSH_KEY_PATH):
        return
    os.makedirs(os.path.dirname(SSH_KEY_PATH), exist_ok=True)
    with open(SSH_KEY_PATH, "w") as f:
        f.write(SSH_PRIVATE_KEY.replace("\n", "").strip() + "")
    os.chmod(SSH_KEY_PATH, 0o600)

def get_tutor_value(key):
    return subprocess.check_output(f"tutor config printvalue {key}", shell=True).decode().strip()

def get_tutor_root():
    return subprocess.check_output("tutor config printroot", shell=True).decode().strip()

def verify_checksum(file_path, checksum_file):
    """Verify the checksum of a file against its checksum file."""
    if not os.path.exists(checksum_file):
        print(f"Warning: Checksum file {checksum_file} not found. Skipping verification.")
        return True

    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    calculated_hash = sha256_hash.hexdigest()

    with open(checksum_file, "r") as f:
        checksum_line = f.readline().strip()
        stored_hash = checksum_line.split()[0]

    if calculated_hash == stored_hash:
        print(f"Checksum verified for {os.path.basename(file_path)}")
        return True
    else:
        print(f"Warning: Checksum mismatch for {os.path.basename(file_path)}")
        print(f"Expected: {stored_hash}")
        print(f"Got: {calculated_hash}")
        return False

# ========= BACKUP RETRIEVAL =========

def check_rsync_backup(backup_date=None, folder_name=None):
    """Check if a backup exists on the rsync server and download it."""
    if not REMOTE_SERVER or not REMOTE_PATH:
        print("REMOTE_SERVER or REMOTE_PATH not set. Skipping rsync check.")
        return None

    ensure_ssh_key()

    start_time = time.perf_counter()

    # If specific folder_name is provided, use it directly
    if folder_name:
        remote_folder = folder_name
    else:
        # If date is provided, construct folder name with that date
        if backup_date:
            date_str = backup_date
        else:
            # Default to today's date
            date_str = datetime.now().strftime("%Y%m%d")

        remote_folder = f"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-{date_str}"

    print(f"Checking for rsync backup in folder: {remote_folder}")

    # Create local restore directory
    restore_path = os.path.join(RESTORE_DIR, remote_folder)
    if not os.path.exists(restore_path):
        os.makedirs(restore_path, exist_ok=True)

    # Check if backup exists on remote server
    check_cmd = f'ssh -i {SSH_KEY_PATH} {REMOTE_SERVER} "ls -la {REMOTE_PATH}/{remote_folder}/"'
    try:
        result = subprocess.run(check_cmd, shell=True, check=False, capture_output=True)
        if result.returncode != 0:
            print(f"Rsync backup not found: {remote_folder}")
            return None

        # Check for required backup files
        required_files = ["mysql_dump.tar.gz", "mongodb_dump.tar.gz", "openedx_media.tar.gz"]

        for file in required_files:
            if file not in result.stdout.decode():
                print(f"Required file {file} not found in rsync backup")
                return None

        # Download backup files
        download_cmd = f'rsync -avz -e "ssh -i {SSH_KEY_PATH}" --progress {REMOTE_SERVER}:{REMOTE_PATH}/{remote_folder}/* {restore_path}/'
        run(download_cmd)

        log_time("Rsync backup download", start_time)
        return restore_path

    except Exception as e:
        print(f"Error checking/downloading rsync backup: {e}")
        return None

def download_s3_backup(backup_date=None, folder_name=None):
    """Download backup from AWS S3."""
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_BUCKET_NAME:
        print("AWS credentials or bucket not set. Skipping S3 check.")
        return None

    start_time = time.perf_counter()

    try:
        import boto3
        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3 = session.client('s3')

        # If specific folder_name is provided, use it directly
        if folder_name:
            remote_folder = folder_name
        else:
            # If date is provided, construct folder name with that date
            if backup_date:
                date_str = backup_date
            else:
                # Default to today's date
                date_str = datetime.now().strftime("%Y%m%d")

            remote_folder = f"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-{date_str}"

        print(f"Checking for S3 backup in folder: {remote_folder}")

        # Create local restore directory
        restore_path = os.path.join(RESTORE_DIR, remote_folder)
        if not os.path.exists(restore_path):
            os.makedirs(restore_path, exist_ok=True)

        # List objects in the prefix to check if backup exists
        prefix = f"{AWS_S3_PREFIX}/{remote_folder}/"
        response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)

        if 'Contents' not in response:
            print(f"S3 backup not found: {remote_folder}")
            return None

        # Check for required backup files
        required_files = ["mysql_dump.tar.gz", "mongodb_dump.tar.gz", "openedx_media.tar.gz"]
        found_files = set()

        for obj in response['Contents']:
            file_key = obj['Key']
            file_name = os.path.basename(file_key)

            if file_name in required_files:
                found_files.add(file_name)
                # Download file
                local_file = os.path.join(restore_path, file_name)
                print(f"Downloading {file_key} to {local_file}")
                s3.download_file(AWS_BUCKET_NAME, file_key, local_file)

                # Download checksum file if it exists
                checksum_key = f"{file_key}.sha256"
                try:
                    s3.head_object(Bucket=AWS_BUCKET_NAME, Key=checksum_key)
                    s3.download_file(AWS_BUCKET_NAME, checksum_key, f"{local_file}.sha256")
                except:
                    print(f"Checksum file {checksum_key} not found")

        # Check if all required files were found
        if not all(file in found_files for file in required_files):
            missing = set(required_files) - found_files
            print(f"Some required files were not found in S3 backup: {missing}")
            return None

        log_time("S3 backup download", start_time)
        return restore_path

    except Exception as e:
        print(f"Error downloading S3 backup: {e}")
        return None

def download_gcs_backup(backup_date=None, folder_name=None):
    """Download backup from Google Cloud Storage."""
    if not GCP_SERVICE_ACCOUNT_JSON or not AWS_BUCKET_NAME:
        print("GCP credentials or bucket not set. Skipping GCS check.")
        return None

    start_time = time.perf_counter()

    try:
        from google.cloud import storage
        key_path = "/tmp/gcp_service_account.json"
        with open(key_path, "w") as f:
            json.dump(json.loads(GCP_SERVICE_ACCOUNT_JSON), f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

        storage_client = storage.Client()
        bucket = storage_client.bucket(AWS_BUCKET_NAME)

        # If specific folder_name is provided, use it directly
        if folder_name:
            remote_folder = folder_name
        else:
            # If date is provided, construct folder name with that date
            if backup_date:
                date_str = backup_date
            else:
                # Default to today's date
                date_str = datetime.now().strftime("%Y%m%d")

            remote_folder = f"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-{date_str}"

        print(f"Checking for GCS backup in folder: {remote_folder}")

        # Create local restore directory
        restore_path = os.path.join(RESTORE_DIR, remote_folder)
        if not os.path.exists(restore_path):
            os.makedirs(restore_path, exist_ok=True)

        # List objects in the prefix to check if backup exists
        prefix = f"{AWS_S3_PREFIX}/{remote_folder}/"
        blobs = bucket.list_blobs(prefix=prefix)

        # Check for required backup files
        required_files = ["mysql_dump.tar.gz", "mongodb_dump.tar.gz", "openedx_media.tar.gz"]
        found_files = set()

        for blob in blobs:
            file_name = os.path.basename(blob.name)

            if file_name in required_files:
                found_files.add(file_name)
                # Download file
                local_file = os.path.join(restore_path, file_name)
                print(f"Downloading {blob.name} to {local_file}")
                blob.download_to_filename(local_file)

                # Download checksum file if it exists
                checksum_blob_name = f"{blob.name}.sha256"
                checksum_blob = bucket.blob(checksum_blob_name)
                if checksum_blob.exists():
                    checksum_blob.download_to_filename(f"{local_file}.sha256")
                else:
                    print(f"Checksum file {checksum_blob_name} not found")

        # Check if all required files were found
        if not all(file in found_files for file in required_files):
            missing = set(required_files) - found_files
            print(f"Some required files were not found in GCS backup: {missing}")
            return None

        # Clean up service account file
        os.remove(key_path)

        log_time("GCS backup download", start_time)
        return restore_path

    except Exception as e:
        print(f"Error downloading GCS backup: {e}")
        return None

# ========= RESTORE FUNCTIONS =========

def extract_tar(tar_file, extract_to):
    """Extract a tar file to the specified directory."""
    start_time = time.perf_counter()
    print(f"Extracting {tar_file} to {extract_to}")

    os.makedirs(extract_to, exist_ok=True)

    if not os.path.exists(tar_file):
        print(f"Error: Tar file {tar_file} does not exist.")
        return False

    try:
        with tarfile.open(tar_file) as tar:
            tar.extractall(path=extract_to)
        log_time(f"Extraction of {os.path.basename(tar_file)}", start_time)
        return True
    except Exception as e:
        print(f"Error extracting {tar_file}: {e}")
        # Try with sudo
        try:
            cmd = f"sudo tar -xzf {tar_file} -C {extract_to}"
            run(cmd)
            run(f"sudo chown -R $USER:$USER {extract_to}")
            log_time(f"Extraction of {os.path.basename(tar_file)} with sudo", start_time)
            return True
        except Exception as e2:
            print(f"Fatal error extracting {tar_file} even with sudo: {e2}")
            return False

def restore_mysql(mysql_dump_path):
    """Restore MySQL database from a dump file."""
    start_time = time.perf_counter()

    username = get_tutor_value("MYSQL_ROOT_USERNAME")
    password = get_tutor_value("MYSQL_ROOT_PASSWORD")

    # Path to the actual SQL dump file within the extracted directory
    sql_file = os.path.join(mysql_dump_path, "all-databases.sql")

    if not os.path.exists(sql_file):
        print(f"Error: MySQL dump file {sql_file} not found.")
        return False

    try:
        # Import the SQL dump file
        cmd = (
            f'tutor local exec -e USERNAME="{username}" -e PASSWORD="{password}" -e DUMP_PATH="/var/lib/mysql/all-databases.sql" '
            f'mysql sh -c \'mysql --user=$USERNAME --password=$PASSWORD < $DUMP_PATH\''
        )
        run(cmd)
        log_time("MySQL restore", start_time)
        return True
    except Exception as e:
        print(f"Error restoring MySQL: {e}")
        return False

def restore_mongodb(mongodb_dump_path):
    """Restore MongoDB from a dump directory."""
    start_time = time.perf_counter()

    # The expected path to the MongoDB dump directory
    dump_dir = os.path.join(mongodb_dump_path, "dump.mongodb")

    if not os.path.exists(dump_dir):
        print(f"Error: MongoDB dump directory {dump_dir} not found.")
        return False

    try:
        # Use mongorestore to restore the MongoDB database
        cmd = f"tutor local exec mongodb mongorestore --drop /data/db/dump.mongodb"
        run(cmd)
        log_time("MongoDB restore", start_time)
        return True
    except Exception as e:
        print(f"Error restoring MongoDB: {e}")
        return False

def restore_openedx_media(media_dir):
    """Restore OpenedX media files."""
    start_time = time.perf_counter()

    tutor_root = get_tutor_root()
    target_dir = os.path.join(tutor_root, "data/openedx-media")

    if not os.path.exists(media_dir):
        print(f"Error: OpenedX media directory {media_dir} not found.")
        return False

    try:
        # Stop the Tutor services before restoring media
        run("tutor local stop")

        # Remove existing media directory
        if os.path.exists(target_dir):
            print(f"Removing existing media directory: {target_dir}")
            try:
                shutil.rmtree(target_dir)
            except Exception:
                run(f"sudo rm -rf {target_dir}")

        # Create the target directory
        os.makedirs(target_dir, exist_ok=True)

        # Copy the media files
        if os.path.isdir(os.path.join(media_dir, "openedx-media")):
            # If the extract created a nested directory structure
            source_dir = os.path.join(media_dir, "openedx-media")
            cmd = f"cp -r {source_dir}/* {target_dir}/"
        else:
            # If the extract has the files directly
            cmd = f"cp -r {media_dir}/* {target_dir}/"

        run(cmd)

        # Fix permissions
        run(f"sudo chown -R $USER:$USER {target_dir}")

        # Start Tutor services
        run("tutor local start")

        log_time("OpenedX media restore", start_time)
        return True
    except Exception as e:
        print(f"Error restoring OpenedX media: {e}")
        # Make sure to restart Tutor services even if there was an error
        try:
            run("tutor local start")
        except:
            pass
        return False

# ========= MAIN =========

def main():
    parser = argparse.ArgumentParser(description="Restore OpenedX Tutor backup")
    parser.add_argument("--date", help="Backup date in YYYYMMDD format (defaults to today)")
    parser.add_argument("--folder", help="Specific backup folder name (overrides date parameter)")
    args = parser.parse_args()

    total_start_time = time.perf_counter()

    install_requirements()

    # Try to find a backup to restore, prioritizing rsync, then S3, then GCS
    backup_path = None

    # First try rsync
    backup_path = check_rsync_backup(args.date, args.folder)

    # If rsync backup not found, try S3
    if not backup_path:
        print("Rsync backup not found or not accessible. Trying AWS S3...")
        backup_path = download_s3_backup(args.date, args.folder)

    # If S3 backup not found, try GCS
    if not backup_path:
        print("AWS S3 backup not found or not accessible. Trying Google Cloud Storage...")
        backup_path = download_gcs_backup(args.date, args.folder)

    # If still no backup found, exit
    if not backup_path:
        print("Error: No backup found to restore. Please check backup date or folder name.")
        sys.exit(1)

    print(f"Found backup at: {backup_path}")

    # Extract directories for backup files
    mysql_tar = os.path.join(backup_path, "mysql_dump.tar.gz")
    mongodb_tar = os.path.join(backup_path, "mongodb_dump.tar.gz")
    openedx_media_tar = os.path.join(backup_path, "openedx_media.tar.gz")

    # Verify checksums if available
    for tar_file in [mysql_tar, mongodb_tar, openedx_media_tar]:
        if os.path.exists(tar_file) and os.path.exists(f"{tar_file}.sha256"):
            verify_checksum(tar_file, f"{tar_file}.sha256")

    # Create extraction directories
    mysql_extract_dir = os.path.join(backup_path, "mysql_extract")
    mongodb_extract_dir = os.path.join(backup_path, "mongodb_extract")
    openedx_media_extract_dir = os.path.join(backup_path, "openedx_media_extract")

    # Extract all backup files
    extract_tar(mysql_tar, mysql_extract_dir)
    extract_tar(mongodb_tar, mongodb_extract_dir)
    extract_tar(openedx_media_tar, openedx_media_extract_dir)

    # Perform restore operations
    print("\n=== Starting restore process ===\n")

    # Restore MySQL
    print("\n=== Restoring MySQL database ===\n")
    if restore_mysql(mysql_extract_dir):
        print("MySQL restore completed successfully")
    else:
        print("MySQL restore failed")

    # Restore MongoDB
    print("\n=== Restoring MongoDB database ===\n")
    if restore_mongodb(mongodb_extract_dir):
        print("MongoDB restore completed successfully")
    else:
        print("MongoDB restore failed")

    # Restore OpenedX media
    print("\n=== Restoring OpenedX media files ===\n")
    if restore_openedx_media(openedx_media_extract_dir):
        print("OpenedX media restore completed successfully")
    else:
        print("OpenedX media restore failed")

    # Clean up
    try:
        print("\n=== Cleaning up temporary files ===\n")
        shutil.rmtree(backup_path)
        print(f"Removed temporary restore directory: {backup_path}")
    except Exception as e:
        print(f"Warning: Could not remove restore directory {backup_path}: {e}")
        try:
            run(f"sudo rm -rf {backup_path}")
            print(f"Removed temporary restore directory with sudo: {backup_path}")
        except Exception as e2:
            print(f"Error: Failed to remove restore directory even with sudo: {e2}")

    log_time("Total restore process", total_start_time)
    print("\n=== Restore process completed ===\n")

if __name__ == "__main__":
    main()