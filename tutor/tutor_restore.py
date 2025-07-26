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

dotenv_path = os.environ.get("DOTENV_PATH")
if dotenv_path and os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    # Fall back to default behavior
    load_dotenv()

CLIENT_NAME = os.getenv("CLIENT_NAME", "buma")
ENV_TYPE = os.getenv("ENV_TYPE", "prod")

REMOTE_SERVER = os.getenv("REMOTE_SERVER")
REMOTE_PATH = os.getenv("REMOTE_PATH")

SSH_PRIVATE_KEY = os.getenv("SSH_PRIVATE_KEY")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH", "/home/sambaash/.ssh/backup_id_rsa")

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
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message} completed in {elapsed:.2f} seconds.")
    sys.stdout.flush()


def log_message(message):
    """Log a message with timestamp and immediate flush"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()


def run(cmd, check=True, real_time_output=True):
    """Run a shell command with better error handling and real-time output"""
    log_message(f"Running: {cmd}")

    try:
        if real_time_output:
            # For real-time output, use Popen
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # Print output in real-time
            for line in iter(process.stdout.readline, ''):
                print(line.rstrip())
                sys.stdout.flush()

            process.wait()

            if check and process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, cmd)

            return process
        else:
            # For non-real-time, use the original method
            process = subprocess.run(
                cmd,
                shell=True,
                check=check,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if process.stdout:
                print(f"STDOUT: {process.stdout}")
            return process

    except subprocess.CalledProcessError as e:
        log_message(f"ERROR: Command failed with status {e.returncode}")
        if hasattr(e, 'stdout') and e.stdout:
            print(f"STDOUT: {e.stdout}")
        if hasattr(e, 'stderr') and e.stderr:
            print(f"STDERR: {e.stderr}")
        if check:
            raise


def install_python_package(pkg):
    subprocess.run([sys.executable, "-m", "pip", "install", pkg], check=True)


def is_installed(command):
    return shutil.which(command) is not None


def install_requirements():
    """Install required system packages and Python packages."""
    # System packages that are essential
    essential_pkgs = []
    if not is_installed("rsync"):
        essential_pkgs.append("rsync")

    # Install essential packages
    if essential_pkgs:
        try:
            run(f"sudo apt-get update && sudo apt-get install -y {' '.join(essential_pkgs)}")
        except subprocess.CalledProcessError as e:
            log_message(f"Warning: Failed to install system packages: {e}")
            log_message("Some functionality may be limited.")

    # Install AWS CLI via snap if not available
    if not is_installed("aws"):
        try:
            log_message("AWS CLI not found, attempting to install via snap...")
            run("sudo snap install aws-cli --classic")
        except subprocess.CalledProcessError as e:
            log_message(f"Warning: Could not install AWS CLI via snap: {e}")
            log_message("S3 backup functionality will not be available.")

    # Install Python packages
    try:
        import boto3
    except ImportError:
        try:
            log_message("Installing boto3...")
            install_python_package("boto3")
        except Exception as e:
            log_message(f"Warning: Could not install boto3: {e}")
            log_message("S3 backup functionality will not be available.")

    try:
        from google.cloud import storage
    except ImportError:
        try:
            log_message("Installing google-cloud-storage...")
            install_python_package("google-cloud-storage")
        except Exception as e:
            log_message(f"Warning: Could not install google-cloud-storage: {e}")
            log_message("GCS backup functionality will not be available.")


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
        log_message(f"Warning: Checksum file {checksum_file} not found. Skipping verification.")
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
        log_message(f"Checksum verified for {os.path.basename(file_path)}")
        return True
    else:
        log_message(f"Warning: Checksum mismatch for {os.path.basename(file_path)}")
        log_message(f"Expected: {stored_hash}")
        log_message(f"Got: {calculated_hash}")
        return False


# ========= BACKUP RETRIEVAL =========

def check_local_backup(backup_date=None, folder_name=None):
    """Check if a backup exists locally and return the path directly (no copying)."""
    if not REMOTE_PATH:
        log_message("REMOTE_PATH not set. Skipping local backup check.")
        return None

    start_time = time.perf_counter()

    # If specific folder_name is provided, use it directly
    if folder_name:
        backup_folder = folder_name
    else:
        # If date is provided, construct folder name with that date
        if backup_date:
            date_str = backup_date
        else:
            # Default to today's date
            date_str = datetime.now().strftime("%Y%m%d")

        backup_folder = f"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-{date_str}"

    log_message(f"Checking for local backup in folder: {backup_folder}")

    # The complete path where backups are stored by the rsync in the backup script
    local_backup_path = os.path.join(REMOTE_PATH, backup_folder)

    if not os.path.exists(local_backup_path):
        log_message(f"Local backup not found at: {local_backup_path}")
        return None

    # Check for required backup files
    required_files = ["mysql_dump.tar.gz", "mongodb_dump.tar.gz", "openedx_media.tar.gz"]

    missing_files = []
    for file in required_files:
        file_path = os.path.join(local_backup_path, file)
        if not os.path.exists(file_path):
            missing_files.append(file)

    if missing_files:
        log_message(f"Required files not found in local backup: {', '.join(missing_files)}")
        return None

    log_time("Local backup check", start_time)
    log_message(f"Local backup found and verified at: {local_backup_path}")
    return local_backup_path


def download_s3_backup(backup_date=None, folder_name=None):
    """Download backup from AWS S3."""
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_BUCKET_NAME:
        log_message("AWS credentials or bucket not set. Skipping S3 check.")
        return None

    # Check if boto3 is available
    try:
        import boto3
    except ImportError:
        log_message("boto3 not available. Skipping S3 check.")
        return None

    start_time = time.perf_counter()

    try:
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

        log_message(f"Checking for S3 backup in folder: {remote_folder}")

        # Create local restore directory
        restore_path = os.path.join(RESTORE_DIR, remote_folder)
        if not os.path.exists(restore_path):
            os.makedirs(restore_path, exist_ok=True)

        # List objects in the prefix to check if backup exists
        prefix = f"{AWS_S3_PREFIX}/{remote_folder}/"
        response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)

        if 'Contents' not in response:
            log_message(f"S3 backup not found: {remote_folder}")
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
                log_message(f"Downloading {file_key} to {local_file}")
                s3.download_file(AWS_BUCKET_NAME, file_key, local_file)

                # Download checksum file if it exists
                checksum_key = f"{file_key}.sha256"
                try:
                    s3.head_object(Bucket=AWS_BUCKET_NAME, Key=checksum_key)
                    s3.download_file(AWS_BUCKET_NAME, checksum_key, f"{local_file}.sha256")
                except:
                    log_message(f"Checksum file {checksum_key} not found")

        # Check if all required files were found
        if not all(file in found_files for file in required_files):
            missing = set(required_files) - found_files
            log_message(f"Some required files were not found in S3 backup: {missing}")
            return None

        log_time("S3 backup download", start_time)
        return restore_path

    except Exception as e:
        log_message(f"Error downloading S3 backup: {e}")
        return None


def download_gcs_backup(backup_date=None, folder_name=None):
    """Download backup from Google Cloud Storage."""
    if not GCP_SERVICE_ACCOUNT_JSON or not AWS_BUCKET_NAME:
        log_message("GCP credentials or bucket not set. Skipping GCS check.")
        return None

    # Check if google-cloud-storage is available
    try:
        from google.cloud import storage
    except ImportError:
        log_message("google-cloud-storage not available. Skipping GCS check.")
        return None

    start_time = time.perf_counter()

    try:
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

        log_message(f"Checking for GCS backup in folder: {remote_folder}")

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
                log_message(f"Downloading {blob.name} to {local_file}")
                blob.download_to_filename(local_file)

                # Download checksum file if it exists
                checksum_blob_name = f"{blob.name}.sha256"
                checksum_blob = bucket.blob(checksum_blob_name)
                if checksum_blob.exists():
                    checksum_blob.download_to_filename(f"{local_file}.sha256")
                else:
                    log_message(f"Checksum file {checksum_blob_name} not found")

        # Check if all required files were found
        if not all(file in found_files for file in required_files):
            missing = set(required_files) - found_files
            log_message(f"Some required files were not found in GCS backup: {missing}")
            return None

        # Clean up service account file
        os.remove(key_path)

        log_time("GCS backup download", start_time)
        return restore_path

    except Exception as e:
        log_message(f"Error downloading GCS backup: {e}")
        return None


# ========= RESTORE FUNCTIONS =========

def extract_tar(tar_file, extract_to):
    """Extract a tar file to the specified directory."""
    start_time = time.perf_counter()
    log_message(f"Extracting {tar_file} to {extract_to}")

    os.makedirs(extract_to, exist_ok=True)

    if not os.path.exists(tar_file):
        log_message(f"Error: Tar file {tar_file} does not exist.")
        return False

    try:
        with tarfile.open(tar_file) as tar:
            tar.extractall(path=extract_to)
        log_time(f"Extraction of {os.path.basename(tar_file)}", start_time)
        return True
    except Exception as e:
        log_message(f"Error extracting {tar_file}: {e}")
        # Try with sudo
        try:
            cmd = f"sudo tar -xzf {tar_file} -C {extract_to}"
            run(cmd)
            run(f"sudo chown -R $USER:$USER {extract_to}")
            log_time(f"Extraction of {os.path.basename(tar_file)} with sudo", start_time)
            return True
        except Exception as e2:
            log_message(f"Fatal error extracting {tar_file} even with sudo: {e2}")
            return False


def restore_mysql(mysql_dump_path):
    """Restore MySQL database from a dump file using the tutor local exec command."""
    start_time = time.perf_counter()
    log_message("Starting MySQL restore process...")

    username = get_tutor_value("MYSQL_ROOT_USERNAME")
    password = get_tutor_value("MYSQL_ROOT_PASSWORD")

    # Path to the actual SQL dump file within the extracted directory
    sql_file = os.path.join(mysql_dump_path, "all-databases.sql")

    if not os.path.exists(sql_file):
        log_message(f"Error: MySQL dump file {sql_file} not found.")
        return False

    try:
        # Copy the SQL file to a location accessible by the MySQL container
        tutor_root = get_tutor_root()
        mysql_data_dir = os.path.join(tutor_root, "data/mysql")
        target_sql_file = os.path.join(mysql_data_dir, "all-databases.sql")

        # Make sure the target directory exists
        os.makedirs(os.path.dirname(target_sql_file), exist_ok=True)

        log_message(f"Copying SQL file from {sql_file} to {target_sql_file}")
        shutil.copy2(sql_file, target_sql_file)

        # Use the tutor local exec command as you specified
        log_message("Executing MySQL restore command...")
        cmd = f'tutor local exec -e USERNAME="{username}" -e PASSWORD="{password}" mysql sh -c \'mysql --user=$USERNAME --password=$PASSWORD < /var/lib/mysql/all-databases.sql\''

        run(cmd, real_time_output=True)

        # Clean up
        log_message("Cleaning up temporary SQL file...")
        os.remove(target_sql_file)

        log_time("MySQL restore", start_time)
        log_message("MySQL restore completed successfully")
        return True
    except Exception as e:
        log_message(f"Error restoring MySQL: {e}")
        import traceback
        traceback.print_exc()
        return False


def restore_mongodb(mongodb_dump_path):
    """Restore MongoDB from a dump directory using direct docker exec command."""
    start_time = time.perf_counter()
    log_message("Starting MongoDB restore process...")

    # The expected path to the MongoDB dump directory
    dump_dir = os.path.join(mongodb_dump_path, "dump.mongodb")

    if not os.path.exists(dump_dir):
        log_message(f"Error: MongoDB dump directory {dump_dir} not found.")
        return False

    try:
        # Copy the dump directory to a location accessible by the MongoDB container
        tutor_root = get_tutor_root()
        mongodb_data_dir = os.path.join(tutor_root, "data/mongodb")
        target_dump_dir = os.path.join(mongodb_data_dir, "dump.mongodb")

        # Remove existing dump directory if it exists
        if os.path.exists(target_dump_dir):
            try:
                shutil.rmtree(target_dump_dir)
            except Exception:
                run(f"sudo rm -rf {target_dump_dir}")

        log_message(f"Copying MongoDB dump from {dump_dir} to {target_dump_dir}")
        shutil.copytree(dump_dir, target_dump_dir)

        # First, drop all existing databases
        log_message("Dropping all existing MongoDB databases...")

        # Get list of databases and drop them (except admin, local, and config)
        list_dbs_cmd = 'docker exec tutor_local_mongodb_1 mongo --eval "db.adminCommand(\'listDatabases\').databases.forEach(function(d) { if (d.name !== \'admin\' && d.name !== \'local\' && d.name !== \'config\') { print(d.name); } })"'

        try:
            result = run(list_dbs_cmd, real_time_output=False)
            if result.stdout:
                databases = [db.strip() for db in result.stdout.strip().split('\n') if
                             db.strip() and not db.startswith('MongoDB')]

                for db_name in databases:
                    if db_name:  # Make sure it's not empty
                        log_message(f"Dropping database: {db_name}")
                        drop_cmd = f'docker exec tutor_local_mongodb_1 mongo {db_name} --eval "db.dropDatabase()"'
                        run(drop_cmd, real_time_output=True)
        except Exception as e:
            log_message(f"Warning: Could not drop existing databases: {e}")
            log_message("Continuing with restore...")

        # Use the direct docker exec command for MongoDB restore
        log_message("Executing MongoDB restore command...")

        # Create a log file for the restore process
        restore_log = "/tmp/mongorestore.log"

        # Use the command you specified but with real-time monitoring
        restore_cmd = f'docker exec tutor_local_mongodb_1 mongorestore --drop /data/db/dump.mongodb'

        log_message("Starting MongoDB restore (this may take a while for large databases)...")
        run(restore_cmd, real_time_output=True)

        # Clean up
        log_message("Cleaning up temporary dump directory...")
        try:
            shutil.rmtree(target_dump_dir)
        except Exception:
            run(f"sudo rm -rf {target_dump_dir}")

        log_time("MongoDB restore", start_time)
        log_message("MongoDB restore completed successfully")
        return True

    except Exception as e:
        log_message(f"Error restoring MongoDB: {e}")
        import traceback
        traceback.print_exc()
        return False

def restore_openedx_media(media_dir):
    """Restore OpenedX media files."""
    start_time = time.perf_counter()
    log_message("Starting OpenedX media restore process...")

    tutor_root = get_tutor_root()
    target_dir = os.path.join(tutor_root, "data/openedx-media")

    if not os.path.exists(media_dir):
        log_message(f"Error: OpenedX media directory {media_dir} not found.")
        return False

    try:
        # Stop the Tutor services before restoring media
        log_message("Stopping Tutor services...")
        run("tutor local stop")

        # Remove existing media directory
        if os.path.exists(target_dir):
            log_message(f"Removing existing media directory: {target_dir}")
            try:
                shutil.rmtree(target_dir)
            except Exception:
                run(f"sudo rm -rf {target_dir}")

        # Create the target directory
        os.makedirs(target_dir, exist_ok=True)

        # Copy the media files
        source_dir = media_dir
        if os.path.isdir(os.path.join(media_dir, "openedx-media")):
            # If the extract created a nested directory structure
            source_dir = os.path.join(media_dir, "openedx-media")

        log_message(f"Copying media files from {source_dir} to {target_dir}")
        # Use cp command for better handling of large directories
        run(f"cp -r {source_dir}/* {target_dir}/")

        # Fix permissions
        log_message("Fixing permissions...")
        run(f"sudo chown -R $USER:$USER {target_dir}")

        # Start Tutor services
        log_message("Starting Tutor services...")
        run("tutor local start")

        log_time("OpenedX media restore", start_time)
        log_message("OpenedX media restore completed successfully")
        return True
    except Exception as e:
        log_message(f"Error restoring OpenedX media: {e}")
        import traceback
        traceback.print_exc()
        # Make sure to restart Tutor services even if there was an error
        try:
            log_message("Attempting to restart Tutor services after error...")
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

    try:
        log_message("Starting restore process...")
        log_message(f"Current working directory: {os.getcwd()}")
        log_message(f"Arguments: date={args.date}, folder={args.folder}")

        install_requirements()
        log_message("Requirements installation completed")

        # Try to find a backup to restore, prioritizing local backup, then S3, then GCS
        backup_path = None

        # First try local backup (what was previously stored via rsync)
        log_message("Checking for local backup...")
        backup_path = check_local_backup(args.date, args.folder)

        # If local backup not found, try S3
        if not backup_path:
            log_message("Local backup not found or not accessible. Trying AWS S3...")
            backup_path = download_s3_backup(args.date, args.folder)

        # If S3 backup not found, try GCS
        if not backup_path:
            log_message("AWS S3 backup not found or not accessible. Trying Google Cloud Storage...")
            backup_path = download_gcs_backup(args.date, args.folder)

        # If still no backup found, exit
        if not backup_path:
            log_message("Error: No backup found to restore. Please check backup date or folder name.")
            log_message(f"Searched for:")
            log_message(f"  - Date: {args.date or 'today'}")
            log_message(f"  - Folder: {args.folder or 'auto-generated'}")
            log_message(f"  - REMOTE_PATH: {REMOTE_PATH}")
            log_message(f"  - AWS_BUCKET_NAME: {AWS_BUCKET_NAME}")
            sys.exit(1)

        log_message(f"Found backup at: {backup_path}")

        # Extract directories for backup files
        mysql_tar = os.path.join(backup_path, "mysql_dump.tar.gz")
        mongodb_tar = os.path.join(backup_path, "mongodb_dump.tar.gz")
        openedx_media_tar = os.path.join(backup_path, "openedx_media.tar.gz")

        log_message(f"Looking for backup files:")
        log_message(f"  - MySQL: {mysql_tar} (exists: {os.path.exists(mysql_tar)})")
        log_message(f"  - MongoDB: {mongodb_tar} (exists: {os.path.exists(mongodb_tar)})")
        log_message(f"  - Media: {openedx_media_tar} (exists: {os.path.exists(openedx_media_tar)})")

        # Verify checksums if available
        for tar_file in [mysql_tar, mongodb_tar, openedx_media_tar]:
            if os.path.exists(tar_file) and os.path.exists(f"{tar_file}.sha256"):
                verify_checksum(tar_file, f"{tar_file}.sha256")

        # Create extraction directories
        mysql_extract_dir = os.path.join(backup_path, "mysql_extract")
        mongodb_extract_dir = os.path.join(backup_path, "mongodb_extract")
        openedx_media_extract_dir = os.path.join(backup_path, "openedx_media_extract")

        # Extract all backup files
        log_message("Extracting backup files...")
        extract_tar(mysql_tar, mysql_extract_dir)
        extract_tar(mongodb_tar, mongodb_extract_dir)
        extract_tar(openedx_media_tar, openedx_media_extract_dir)

        # Perform restore operations
        log_message("\n=== Starting restore process ===\n")

        # Restore MySQL
        log_message("\n=== Restoring MySQL database ===\n")
        if restore_mysql(mysql_extract_dir):
            log_message("MySQL restore completed successfully")
        else:
            log_message("MySQL restore failed")

        # Restore MongoDB
        log_message("\n=== Restoring MongoDB database ===\n")
        if restore_mongodb(mongodb_extract_dir):
            log_message("MongoDB restore completed successfully")
        else:
            log_message("MongoDB restore failed")

        # Restore OpenedX media
        log_message("\n=== Restoring OpenedX media files ===\n")
        if restore_openedx_media(openedx_media_extract_dir):
            log_message("OpenedX media restore completed successfully")
        else:
            log_message("OpenedX media restore failed")

        # Clean up only if backup was downloaded (not local)
        if backup_path.startswith(RESTORE_DIR):
            try:
                log_message("\n=== Cleaning up temporary files ===\n")
                shutil.rmtree(backup_path)
                log_message(f"Removed temporary restore directory: {backup_path}")
            except Exception as e:
                log_message(f"Warning: Could not remove restore directory {backup_path}: {e}")
                try:
                    run(f"sudo rm -rf {backup_path}")
                    log_message(f"Removed temporary restore directory with sudo: {backup_path}")
                except Exception as e2:
                    log_message(f"Error: Failed to remove restore directory even with sudo: {e2}")
        else:
            log_message("Local backup used - skipping cleanup of extraction directories")

        log_time("Total restore process", total_start_time)
        log_message("\n=== Restore process completed ===\n")

    except Exception as e:
        log_message(f"FATAL ERROR in restore process: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()