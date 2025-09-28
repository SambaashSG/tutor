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

# Force unbuffered output for immediate logging (Python 3 compatible)
class UnbufferedOutput:
    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def writelines(self, lines):
        self.stream.writelines(lines)
        self.stream.flush()

    def __getattr__(self, name):
        return getattr(self.stream, name)

# Apply unbuffered output
sys.stdout = UnbufferedOutput(sys.stdout)
sys.stderr = UnbufferedOutput(sys.stderr)

# Alternative: Set environment variable for unbuffered output
os.environ['PYTHONUNBUFFERED'] = '1'

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

def log(message):
    """Print message with immediate flush to ensure real-time output"""
    print(message)
    sys.stdout.flush()


def log_time(message, start_time):
    elapsed = time.perf_counter() - start_time
    log(f"[{message}] completed in {elapsed:.2f} seconds.")


def run(cmd, check=True):
    """Run a shell command with better error handling"""
    log(f"Running: {cmd}")
    try:
        process = subprocess.run(
            cmd,
            shell=True,
            check=check,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=0  # Unbuffered
        )
        if process.stdout:
            log(f"STDOUT: {process.stdout}")
        return process
    except subprocess.CalledProcessError as e:
        log(f"ERROR: Command failed with status {e.returncode}")
        if e.stdout:
            log(f"STDOUT: {e.stdout}")
        if e.stderr:
            log(f"STDERR: {e.stderr}")
        if check:
            raise


def install_python_package(pkg):
    log(f"Installing Python package: {pkg}")
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
            log(f"Installing system packages: {' '.join(essential_pkgs)}")
            run(f"sudo apt-get update && sudo apt-get install -y {' '.join(essential_pkgs)}")
        except subprocess.CalledProcessError as e:
            log(f"Warning: Failed to install system packages: {e}")
            log("Some functionality may be limited.")

    # Install AWS CLI via snap if not available
    if not is_installed("aws"):
        try:
            log("AWS CLI not found, attempting to install via snap...")
            run("sudo snap install aws-cli --classic")
        except subprocess.CalledProcessError as e:
            log(f"Warning: Could not install AWS CLI via snap: {e}")
            log("S3 backup functionality will not be available.")

    # Install Python packages
    try:
        import boto3
    except ImportError:
        try:
            log("Installing boto3...")
            install_python_package("boto3")
        except Exception as e:
            log(f"Warning: Could not install boto3: {e}")
            log("S3 backup functionality will not be available.")

    try:
        from google.cloud import storage
    except ImportError:
        try:
            log("Installing google-cloud-storage...")
            install_python_package("google-cloud-storage")
        except Exception as e:
            log(f"Warning: Could not install google-cloud-storage: {e}")
            log("GCS backup functionality will not be available.")


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
        log(f"Warning: Checksum file {checksum_file} not found. Skipping verification.")
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
        log(f"Checksum verified for {os.path.basename(file_path)}")
        return True
    else:
        log(f"Warning: Checksum mismatch for {os.path.basename(file_path)}")
        log(f"Expected: {stored_hash}")
        log(f"Got: {calculated_hash}")
        return False


# ========= BACKUP RETRIEVAL =========

def check_extracted_backup(backup_date=None, folder_name=None):
    """Check if extracted backup already exists in the restore directory."""
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

    log(f"Checking for existing extracted backup: {backup_folder}")

    # Check if the backup directory already exists in RESTORE_DIR
    backup_path = os.path.join(RESTORE_DIR, backup_folder)
    
    if not os.path.exists(backup_path):
        log(f"Extracted backup directory not found at: {backup_path}")
        return None

    # Check for extracted directories
    mysql_extract_dir = os.path.join(backup_path, "mysql_extract")
    mongodb_extract_dir = os.path.join(backup_path, "mongodb_extract")
    openedx_media_extract_dir = os.path.join(backup_path, "openedx_media_extract")

    # Check if all required extracted directories exist and have content
    required_extracts = {
        "mysql_extract": mysql_extract_dir,
        "mongodb_extract": mongodb_extract_dir,
        "openedx_media_extract": openedx_media_extract_dir
    }

    missing_extracts = []
    for extract_name, extract_path in required_extracts.items():
        if not os.path.exists(extract_path):
            missing_extracts.append(extract_name)
        else:
            # Check if directory has content
            if not os.listdir(extract_path):
                missing_extracts.append(f"{extract_name} (empty)")

    if missing_extracts:
        log(f"Required extracted directories not found or empty: {', '.join(missing_extracts)}")
        
        # Check if tar files exist instead
        mysql_tar = os.path.join(backup_path, "mysql_dump.tar.gz")
        mongodb_tar = os.path.join(backup_path, "mongodb_dump.tar.gz")
        openedx_media_tar = os.path.join(backup_path, "openedx_media.tar.gz")
        
        tar_files_exist = all(os.path.exists(f) for f in [mysql_tar, mongodb_tar, openedx_media_tar])
        
        if tar_files_exist:
            log("Found tar files that need extraction")
            return backup_path  # Return path for extraction
        else:
            return None

    # Verify the extracted content is valid
    mysql_sql_file = os.path.join(mysql_extract_dir, "all-databases.sql")
    mongodb_dump_dir = os.path.join(mongodb_extract_dir, "dump.mongodb")

    if not os.path.exists(mysql_sql_file):
        log(f"MySQL dump file not found: {mysql_sql_file}")
        return None

    if not os.path.exists(mongodb_dump_dir):
        log(f"MongoDB dump directory not found: {mongodb_dump_dir}")
        return None

    log(f"Found valid extracted backup at: {backup_path}")
    return backup_path


def check_local_backup(backup_date=None, folder_name=None):
    """Check if a backup exists locally in the same path where rsync stores backups."""
    if not REMOTE_PATH:
        log("REMOTE_PATH not set. Skipping local backup check.")
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

    log(f"Checking for local backup in folder: {backup_folder}")

    # The complete path where backups are stored by the rsync in the backup script
    local_backup_path = os.path.join(REMOTE_PATH, backup_folder)

    if not os.path.exists(local_backup_path):
        log(f"Local backup not found at: {local_backup_path}")
        return None

    # Check for required backup files
    required_files = ["mysql_dump.tar.gz", "mongodb_dump.tar.gz", "openedx_media.tar.gz"]

    missing_files = []
    for file in required_files:
        file_path = os.path.join(local_backup_path, file)
        if not os.path.exists(file_path):
            missing_files.append(file)

    if missing_files:
        log(f"Required files not found in local backup: {', '.join(missing_files)}")
        return None

    # Create restore directory
    restore_path = os.path.join(RESTORE_DIR, backup_folder)
    if not os.path.exists(restore_path):
        os.makedirs(restore_path, exist_ok=True)

    # Copy backup files to restore directory
    for file in required_files:
        src_file = os.path.join(local_backup_path, file)
        dst_file = os.path.join(restore_path, file)
        log(f"Copying {src_file} to {dst_file}")
        shutil.copy2(src_file, dst_file)

        # Copy checksum file if it exists
        checksum_file = f"{src_file}.sha256"
        if os.path.exists(checksum_file):
            shutil.copy2(checksum_file, f"{dst_file}.sha256")

    log_time("Local backup copy", start_time)
    return restore_path


def download_s3_backup(backup_date=None, folder_name=None):
    """Download backup from AWS S3."""
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_BUCKET_NAME:
        log("AWS credentials or bucket not set. Skipping S3 check.")
        return None

    # Check if boto3 is available
    try:
        import boto3
    except ImportError:
        log("boto3 not available. Skipping S3 check.")
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

        log(f"Checking for S3 backup in folder: {remote_folder}")

        # Create local restore directory
        restore_path = os.path.join(RESTORE_DIR, remote_folder)
        if not os.path.exists(restore_path):
            os.makedirs(restore_path, exist_ok=True)

        # List objects in the prefix to check if backup exists
        prefix = f"{AWS_S3_PREFIX}/{remote_folder}/"
        response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)

        if 'Contents' not in response:
            log(f"S3 backup not found: {remote_folder}")
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
                log(f"Downloading {file_key} to {local_file}")
                s3.download_file(AWS_BUCKET_NAME, file_key, local_file)

                # Download checksum file if it exists
                checksum_key = f"{file_key}.sha256"
                try:
                    s3.head_object(Bucket=AWS_BUCKET_NAME, Key=checksum_key)
                    s3.download_file(AWS_BUCKET_NAME, checksum_key, f"{local_file}.sha256")
                except:
                    log(f"Checksum file {checksum_key} not found")

        # Check if all required files were found
        if not all(file in found_files for file in required_files):
            missing = set(required_files) - found_files
            log(f"Some required files were not found in S3 backup: {missing}")
            return None

        log_time("S3 backup download", start_time)
        return restore_path

    except Exception as e:
        log(f"Error downloading S3 backup: {e}")
        return None


def download_gcs_backup(backup_date=None, folder_name=None):
    """Download backup from Google Cloud Storage."""
    if not GCP_SERVICE_ACCOUNT_JSON or not AWS_BUCKET_NAME:
        log("GCP credentials or bucket not set. Skipping GCS check.")
        return None

    # Check if google-cloud-storage is available
    try:
        from google.cloud import storage
    except ImportError:
        log("google-cloud-storage not available. Skipping GCS check.")
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

        log(f"Checking for GCS backup in folder: {remote_folder}")

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
                log(f"Downloading {blob.name} to {local_file}")
                blob.download_to_filename(local_file)

                # Download checksum file if it exists
                checksum_blob_name = f"{blob.name}.sha256"
                checksum_blob = bucket.blob(checksum_blob_name)
                if checksum_blob.exists():
                    checksum_blob.download_to_filename(f"{local_file}.sha256")
                else:
                    log(f"Checksum file {checksum_blob_name} not found")

        # Check if all required files were found
        if not all(file in found_files for file in required_files):
            missing = set(required_files) - found_files
            log(f"Some required files were not found in GCS backup: {missing}")
            return None

        # Clean up service account file
        os.remove(key_path)

        log_time("GCS backup download", start_time)
        return restore_path

    except Exception as e:
        log(f"Error downloading GCS backup: {e}")
        return None


# ========= RESTORE FUNCTIONS =========

def extract_tar(tar_file, extract_to):
    """Extract a tar file to the specified directory."""
    start_time = time.perf_counter()
    log(f"Extracting {tar_file} to {extract_to}")

    os.makedirs(extract_to, exist_ok=True)

    if not os.path.exists(tar_file):
        log(f"Error: Tar file {tar_file} does not exist.")
        return False

    try:
        with tarfile.open(tar_file) as tar:
            tar.extractall(path=extract_to)
        log_time(f"Extraction of {os.path.basename(tar_file)}", start_time)
        return True
    except Exception as e:
        log(f"Error extracting {tar_file}: {e}")
        # Try with sudo
        try:
            cmd = f"sudo tar -xzf {tar_file} -C {extract_to}"
            run(cmd)
            run(f"sudo chown -R $USER:$USER {extract_to}")
            log_time(f"Extraction of {os.path.basename(tar_file)} with sudo", start_time)
            return True
        except Exception as e2:
            log(f"Fatal error extracting {tar_file} even with sudo: {e2}")
            return False


def restore_mysql(mysql_dump_path):
    """Restore MySQL database from a dump file."""
    start_time = time.perf_counter()
    log("Starting MySQL restore...")

    username = get_tutor_value("MYSQL_ROOT_USERNAME")
    password = get_tutor_value("MYSQL_ROOT_PASSWORD")

    # Path to the actual SQL dump file within the extracted directory
    sql_file = os.path.join(mysql_dump_path, "all-databases.sql")

    if not os.path.exists(sql_file):
        log(f"Error: MySQL dump file {sql_file} not found.")
        return False

    try:
        # Copy the SQL file to a location accessible by the MySQL container
        tutor_root = get_tutor_root()
        mysql_data_dir = os.path.join(tutor_root, "data/mysql")
        target_sql_file = os.path.join(mysql_data_dir, "all-databases.sql")

        # Make sure the target directory exists
        os.makedirs(os.path.dirname(target_sql_file), exist_ok=True)

        # Copy the SQL file with sudo if needed
        log(f"Copying SQL file to MySQL container accessible location...")
        try:
            shutil.copy2(sql_file, target_sql_file)
        except PermissionError:
            log("Permission denied, trying with sudo...")
            run(f"sudo cp {sql_file} {target_sql_file}")

        # Use docker-compose directly with -T flag to avoid TTY issues
        compose_files = f"-f {tutor_root}/env/local/docker-compose.yml -f {tutor_root}/env/local/docker-compose.prod.yml -f {tutor_root}/env/local/docker-compose.tmp.yml"
        project_name = "tutor_local"

        log("Executing MySQL restore command...")
        cmd = f"docker-compose {compose_files} --project-name {project_name} exec -T -e USERNAME={username} -e PASSWORD={password} mysql sh -c \"mysql --user=\\$USERNAME --password=\\$PASSWORD < /var/lib/mysql/all-databases.sql\""
        run(cmd)

        # Clean up
        try:
            os.remove(target_sql_file)
        except PermissionError:
            run(f"sudo rm {target_sql_file}")

        log_time("MySQL restore", start_time)
        return True
    except Exception as e:
        log(f"Error restoring MySQL: {e}")
        return False


def restore_mongodb(mongodb_dump_path):
    """Restore MongoDB from a dump directory using direct mongorestore."""
    start_time = time.perf_counter()
    log("Starting MongoDB restore...")

    # The expected path to the MongoDB dump directory
    dump_dir = os.path.join(mongodb_dump_path, "dump.mongodb")

    if not os.path.exists(dump_dir):
        log(f"Error: MongoDB dump directory {dump_dir} not found.")
        return False

    try:
        # Use the dump directory directly - no need to copy to another temp location
        log(f"Using dump directory directly: {dump_dir}")
        
        # Get MongoDB connection details
        try:
            mongodb_host = get_tutor_value("MONGODB_HOST") or "localhost"
            mongodb_port = get_tutor_value("MONGODB_PORT") or "27017"
        except:
            mongodb_host = "localhost"
            mongodb_port = "27017"

        # Try to find MongoDB container
        mongodb_container = run("docker ps --filter name=mongodb --format '{{.Names}}'", check=False)
        
        if mongodb_container.returncode == 0 and mongodb_container.stdout.strip():
            container_name = mongodb_container.stdout.strip().split('\n')[0]
            log(f"Found MongoDB container: {container_name}")
            
            # Get the network name from the container
            network_cmd = f"docker inspect {container_name} --format '{{{{range $key, $value := .NetworkSettings.Networks}}}}{{{{$key}}}}{{{{end}}}}'"
            network_result = run(network_cmd, check=False)
            
            if network_result.returncode == 0 and network_result.stdout.strip():
                network_name = network_result.stdout.strip().split('\n')[0]
                log(f"Using network: {network_name}")
                
                # Run mongorestore using a separate container connected to the same network
                # Mount the dump directory directly
                cmd = f"docker run --rm --network {network_name} -v {dump_dir}:/dump mongo:4.4 mongorestore --host {container_name}:27017 --drop /dump"
                run(cmd)
            else:
                # Fallback to host network
                log("Using host network for MongoDB restore...")
                cmd = f"docker run --rm --network host -v {dump_dir}:/dump mongo:4.4 mongorestore --host {mongodb_host}:{mongodb_port} --drop /dump"
                run(cmd)
        else:
            # No container found, try direct connection
            log("MongoDB container not found. Trying direct connection...")
            cmd = f"docker run --rm --network host -v {dump_dir}:/dump mongo:4.4 mongorestore --host {mongodb_host}:{mongodb_port} --drop /dump"
            run(cmd)

        log_time("MongoDB restore", start_time)
        return True
        
    except Exception as e:
        log(f"Error restoring MongoDB: {e}")
        return False


def restore_openedx_media(media_dir):
    """Restore OpenedX media files."""
    start_time = time.perf_counter()
    log("Starting OpenedX media restore...")

    tutor_root = get_tutor_root()
    target_dir = os.path.join(tutor_root, "data/openedx-media")

    if not os.path.exists(media_dir):
        log(f"Error: OpenedX media directory {media_dir} not found.")
        return False

    try:
        # Stop the Tutor services before restoring media
        log("Stopping Tutor services...")
        run("tutor local stop")

        # Remove existing media directory
        if os.path.exists(target_dir):
            log(f"Removing existing media directory: {target_dir}")
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

        # Use cp command for better handling of large directories
        log(f"Copying media files from {source_dir} to {target_dir}...")
        run(f"cp -r {source_dir}/* {target_dir}/")

        # Fix permissions
        log("Fixing permissions...")
        run(f"sudo chown -R $USER:$USER {target_dir}")

        # Start Tutor services
        log("Starting Tutor services...")
        run("tutor local start")

        log_time("OpenedX media restore", start_time)
        return True
    except Exception as e:
        log(f"Error restoring OpenedX media: {e}")
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
    parser.add_argument("--skip-download", action="store_true", help="Skip download and extraction, use existing extracted backup")
    args = parser.parse_args()

    total_start_time = time.perf_counter()

    try:
        log("Starting restore process...")
        log(f"Current working directory: {os.getcwd()}")
        log(f"Arguments: date={args.date}, folder={args.folder}, skip_download={args.skip_download}")

        install_requirements()
        log("Requirements installation completed")

        # First check if extracted backup already exists
        backup_path = None
        
        log("Checking for existing extracted backup...")
        backup_path = check_extracted_backup(args.date, args.folder)
        
        if backup_path and not args.skip_download:
            # Check if we have extracted directories or just tar files
            mysql_extract_dir = os.path.join(backup_path, "mysql_extract")
            if os.path.exists(mysql_extract_dir) and os.listdir(mysql_extract_dir):
                log("Found existing extracted backup, skipping download and extraction")
                args.skip_download = True
            else:
                log("Found tar files but not extracted, will extract them")

        # If no extracted backup found or skip_download is False, try to get backup
        if not backup_path or not args.skip_download:
            if not args.skip_download:
                # Try to find a backup to restore, prioritizing local backup, then S3, then GCS
                log("Checking for local backup...")
                backup_path = check_local_backup(args.date, args.folder)

                # If local backup not found, try S3
                if not backup_path:
                    log("Local backup not found or not accessible. Trying AWS S3...")
                    backup_path = download_s3_backup(args.date, args.folder)

                # If S3 backup not found, try GCS
                if not backup_path:
                    log("AWS S3 backup not found or not accessible. Trying Google Cloud Storage...")
                    backup_path = download_gcs_backup(args.date, args.folder)

        # If still no backup found, exit
        if not backup_path:
            log("Error: No backup found to restore. Please check backup date or folder name.")
            log(f"Searched for:")
            log(f"  - Date: {args.date or 'today'}")
            log(f"  - Folder: {args.folder or 'auto-generated'}")
            log(f"  - REMOTE_PATH: {REMOTE_PATH}")
            log(f"  - AWS_BUCKET_NAME: {AWS_BUCKET_NAME}")
            sys.exit(1)

        log(f"Using backup at: {backup_path}")

        # Create extraction directories
        mysql_extract_dir = os.path.join(backup_path, "mysql_extract")
        mongodb_extract_dir = os.path.join(backup_path, "mongodb_extract")
        openedx_media_extract_dir = os.path.join(backup_path, "openedx_media_extract")

        # Check if we need to extract or if already extracted
        need_extraction = False
        if not (os.path.exists(mysql_extract_dir) and os.listdir(mysql_extract_dir) and
                os.path.exists(mongodb_extract_dir) and os.listdir(mongodb_extract_dir) and
                os.path.exists(openedx_media_extract_dir) and os.listdir(openedx_media_extract_dir)):
            
            need_extraction = True
            
            # Extract directories for backup files
            mysql_tar = os.path.join(backup_path, "mysql_dump.tar.gz")
            mongodb_tar = os.path.join(backup_path, "mongodb_dump.tar.gz")
            openedx_media_tar = os.path.join(backup_path, "openedx_media.tar.gz")

            log(f"Looking for backup files:")
            log(f"  - MySQL: {mysql_tar} (exists: {os.path.exists(mysql_tar)})")
            log(f"  - MongoDB: {mongodb_tar} (exists: {os.path.exists(mongodb_tar)})")
            log(f"  - Media: {openedx_media_tar} (exists: {os.path.exists(openedx_media_tar)})")

            # Verify checksums if available
            for tar_file in [mysql_tar, mongodb_tar, openedx_media_tar]:
                if os.path.exists(tar_file) and os.path.exists(f"{tar_file}.sha256"):
                    verify_checksum(tar_file, f"{tar_file}.sha256")

            # Extract all backup files
            log("Extracting backup files...")
            extract_tar(mysql_tar, mysql_extract_dir)
            extract_tar(mongodb_tar, mongodb_extract_dir)
            extract_tar(openedx_media_tar, openedx_media_extract_dir)
        else:
            log("Using existing extracted backup files...")

        # Perform restore operations
        log("\n=== Starting restore process ===\n")

        # Restore MySQL
        log("\n=== Restoring MySQL database ===\n")
        if restore_mysql(mysql_extract_dir):
            log("MySQL restore completed successfully")
        else:
            log("MySQL restore failed")

        # Restore MongoDB
        log("\n=== Restoring MongoDB database ===\n")
        if restore_mongodb(mongodb_extract_dir):
            log("MongoDB restore completed successfully")
        else:
            log("MongoDB restore failed")

        # Restore OpenedX media
        log("\n=== Restoring OpenedX media files ===\n")
        if restore_openedx_media(openedx_media_extract_dir):
            log("OpenedX media restore completed successfully")
        else:
            log("OpenedX media restore failed")

        # Clean up only if we don't want to keep the extracted files
        if not args.skip_download:
            try:
                log("\n=== Cleaning up temporary files ===\n")
                shutil.rmtree(backup_path)
                log(f"Removed temporary restore directory: {backup_path}")
            except Exception as e:
                log(f"Warning: Could not remove restore directory {backup_path}: {e}")
                try:
                    run(f"sudo rm -rf {backup_path}")
                    log(f"Removed temporary restore directory with sudo: {backup_path}")
                except Exception as e2:
                    log(f"Error: Failed to remove restore directory even with sudo: {e2}")
        else:
            log(f"Keeping extracted backup at: {backup_path}")

        log_time("Total restore process", total_start_time)
        log("\n=== Restore process completed ===\n")

    except Exception as e:
        log(f"FATAL ERROR in restore process: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
