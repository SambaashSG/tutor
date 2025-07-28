#!/usr/bin/env python3
import os
import subprocess
import shutil
import sys
import json
import time
import multiprocessing
import tarfile
import hashlib
import concurrent.futures
import importlib.util
from datetime import datetime, timedelta
from dotenv import load_dotenv
from functools import partial
import re
import requests

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
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH", "/home/ubuntu/.ssh/backup_id_rsa")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_S3_PREFIX = os.getenv("AWS_S3_PREFIX", "tutor-backup")

GCP_SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

# Backup retention configuration
BACKUP_RETAIN_DAILY_DAYS = int(os.getenv("BACKUP_RETAIN_DAILY_DAYS", "2"))  # Keep last N days
BACKUP_RETAIN_WEEKLY_INTERVAL = int(os.getenv("BACKUP_RETAIN_WEEKLY_INTERVAL", "7"))  # Weekly backup interval
BACKUP_RETAIN_WEEKLY_COUNT = int(os.getenv("BACKUP_RETAIN_WEEKLY_COUNT", "52"))  # Keep N weekly backups (52 = 1 year)


BACKUP_DIR = "/tmp/tutor-backup"

# Number of workers for parallel operations
NUM_WORKERS = multiprocessing.cpu_count()

# Compression settings (1-9, where 1 is fastest but largest, 9 is slowest but smallest)
# For backups, a moderate level like 3-5 is often a good balance
COMPRESSION_LEVEL = os.getenv("COMPRESSION_LEVEL", "3")
# Set to True to use faster external compression tools if available (pigz, pbzip2)
USE_FAST_COMPRESSION = os.getenv("USE_FAST_COMPRESSION", "true").lower() in ("true", "yes", "1")

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


def get_dates_to_keep():
    """Generate list of dates to keep based on retention policy from environment variables"""
    now = datetime.now()
    dates_to_keep = set()

    # Keep last N days (daily backups)
    for i in range(BACKUP_RETAIN_DAILY_DAYS):
        date = now - timedelta(days=i)
        dates_to_keep.add(date.strftime("%Y%m%d"))

    # Keep every Nth day going back (weekly backups)
    # Calculate total days to go back based on weekly count and interval
    total_days_back = BACKUP_RETAIN_WEEKLY_COUNT * BACKUP_RETAIN_WEEKLY_INTERVAL
    for i in range(0, total_days_back, BACKUP_RETAIN_WEEKLY_INTERVAL):
        date = now - timedelta(days=i)
        dates_to_keep.add(date.strftime("%Y%m%d"))

    log_message(
        f"Retention policy: Daily={BACKUP_RETAIN_DAILY_DAYS} days, Weekly=every {BACKUP_RETAIN_WEEKLY_INTERVAL} days for {BACKUP_RETAIN_WEEKLY_COUNT} weeks")
    log_message(f"Total dates to keep: {len(dates_to_keep)}")

    return dates_to_keep


def extract_date_from_folder_name(folder_name):
    """Extract date from backup folder name"""
    # Pattern: CLIENT_NAME-ENV_TYPE-tutor-backup-YYYYMMDD
    pattern = rf"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-(\d{{8}})"
    match = re.search(pattern, folder_name)
    return match.group(1) if match else None


def run(cmd, check=True):
    """Run a shell command with better error handling"""
    log_message(f"Running: {cmd}")
    try:
        process = subprocess.run(
            cmd,
            shell=True,
            check=check,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True  # Get string output instead of bytes
        )
        if process.stdout:
            log_message(f"STDOUT: {process.stdout}")
        return process
    except subprocess.CalledProcessError as e:
        log_message(f"ERROR: Command failed with status {e.returncode}")
        if e.stdout:
            log_message(f"STDOUT: {e.stdout}")
        if e.stderr:
            log_message(f"STDERR: {e.stderr}")
        if check:
            raise


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
    if USE_FAST_COMPRESSION:
        if not is_installed("pigz"):
            pkgs.append("pigz")
        if not is_installed("pbzip2"):
            pkgs.append("pbzip2")
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


# ========= BACKUP RETENTION =========

def cleanup_rsync_backups():
    """Clean up old backups on rsync server"""
    start_time = time.perf_counter()
    try:
        dates_to_keep = get_dates_to_keep()
        ssh_opts = f'-i {SSH_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

        # List remote directories
        result = run(f'ssh {ssh_opts} {REMOTE_SERVER} "ls -1 {REMOTE_PATH}/"', check=False)
        if result.returncode != 0:
            log_message("No existing backups found on rsync server or error listing directories")
            return True

        remote_folders = result.stdout.strip().split('\n') if result.stdout.strip() else []

        folders_to_delete = []
        for folder in remote_folders:
            folder = folder.strip()
            if not folder:
                continue

            date_str = extract_date_from_folder_name(folder)
            if date_str and date_str not in dates_to_keep:
                folders_to_delete.append(folder)

        if folders_to_delete:
            log_message(f"Deleting {len(folders_to_delete)} old backup folders from rsync server")
            for folder in folders_to_delete:
                run(f'ssh {ssh_opts} {REMOTE_SERVER} "rm -rf {REMOTE_PATH}/{folder}"')
                log_message(f"Deleted rsync folder: {folder}")
        else:
            log_message("No old backup folders to delete from rsync server")

        log_time("Rsync cleanup", start_time)
        return True
    except Exception as e:
        log_message(f"WARNING: rsync cleanup failed: {e}")
        return False


def cleanup_s3_backups():
    """Clean up old backups on S3"""
    start_time = time.perf_counter()
    try:
        import boto3
        dates_to_keep = get_dates_to_keep()

        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3 = session.client('s3')

        # List objects with the prefix to find folders to delete
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix=AWS_S3_PREFIX + '/', Delimiter='/')

        folders_to_delete = set()

        # Get all folder prefixes (CommonPrefixes)
        for page in pages:
            if 'CommonPrefixes' in page:
                for prefix_info in page['CommonPrefixes']:
                    prefix = prefix_info['Prefix']
                    # Extract folder name from prefix (format: prefix/folder_name/)
                    parts = prefix.rstrip('/').split('/')
                    if len(parts) >= 2:
                        folder_name = parts[1]  # Second part is the folder name
                        date_str = extract_date_from_folder_name(folder_name)
                        if date_str and date_str not in dates_to_keep:
                            folders_to_delete.add(folder_name)

        if folders_to_delete:
            log_message(f"Deleting {len(folders_to_delete)} old backup folders from S3")

            for folder_name in folders_to_delete:
                # List all objects in this folder
                folder_prefix = f"{AWS_S3_PREFIX}/{folder_name}/"
                folder_paginator = s3.get_paginator('list_objects_v2')
                folder_pages = folder_paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix=folder_prefix)

                objects_to_delete = []
                for page in folder_pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            objects_to_delete.append({'Key': obj['Key']})

                if objects_to_delete:
                    # Delete in batches of 1000 (S3 limit)
                    for i in range(0, len(objects_to_delete), 1000):
                        batch = objects_to_delete[i:i + 1000]
                        s3.delete_objects(
                            Bucket=AWS_BUCKET_NAME,
                            Delete={'Objects': batch}
                        )
                    log_message(f"Deleted S3 folder: {folder_name} ({len(objects_to_delete)} objects)")
                else:
                    log_message(f"S3 folder {folder_name} was already empty")
        else:
            log_message("No old backup folders to delete from S3")

        log_time("S3 cleanup", start_time)
        return True
    except Exception as e:
        log_message(f"WARNING: S3 cleanup failed: {e}")
        return False


def cleanup_gcs_backups():
    """Clean up old backups on Google Cloud Storage"""
    start_time = time.perf_counter()
    try:
        from google.cloud import storage
        dates_to_keep = get_dates_to_keep()

        key_path = "/tmp/gcp_service_account.json"
        with open(key_path, "w") as f:
            json.dump(json.loads(GCP_SERVICE_ACCOUNT_JSON), f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

        storage_client = storage.Client()
        bucket = storage_client.bucket(AWS_BUCKET_NAME)

        # Get all blobs with the prefix to identify folders to delete
        blobs = bucket.list_blobs(prefix=AWS_S3_PREFIX + '/')

        folders_to_delete = set()
        blobs_by_folder = {}

        for blob in blobs:
            # Extract folder name from blob name (format: prefix/folder_name/file)
            parts = blob.name.split('/')
            if len(parts) >= 2:
                folder_name = parts[1]  # Second part is the folder name
                date_str = extract_date_from_folder_name(folder_name)
                if date_str and date_str not in dates_to_keep:
                    folders_to_delete.add(folder_name)
                    if folder_name not in blobs_by_folder:
                        blobs_by_folder[folder_name] = []
                    blobs_by_folder[folder_name].append(blob)

        if folders_to_delete:
            log_message(f"Deleting {len(folders_to_delete)} old backup folders from GCS")

            for folder_name in folders_to_delete:
                blobs_to_delete = blobs_by_folder.get(folder_name, [])
                if blobs_to_delete:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        def delete_blob(blob):
                            blob.delete()
                            return blob.name

                        list(executor.map(delete_blob, blobs_to_delete))
                    log_message(f"Deleted GCS folder: {folder_name} ({len(blobs_to_delete)} blobs)")
                else:
                    log_message(f"GCS folder {folder_name} was already empty")
        else:
            log_message("No old backup folders to delete from GCS")

        os.remove(key_path)
        log_time("GCS cleanup", start_time)
        return True
    except Exception as e:
        log_message(f"WARNING: GCS cleanup failed: {e}")
        return False


def cleanup_old_backups(targets):
    """Clean up old backups from all configured targets"""
    log_message("Starting cleanup of old backups...")

    cleanup_tasks = []
    if "rsync" in targets:
        cleanup_tasks.append(("rsync", cleanup_rsync_backups))
    if "s3" in targets:
        cleanup_tasks.append(("s3", cleanup_s3_backups))
    if "gcs" in targets:
        cleanup_tasks.append(("gcs", cleanup_gcs_backups))

    # Execute all cleanup tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(cleanup_tasks)) as executor:
        futures = {executor.submit(func): name for name, func in cleanup_tasks}
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                success = future.result()
                if success:
                    log_message(f"{name.upper()} cleanup completed successfully")
                else:
                    log_message(f"{name.upper()} cleanup failed")
            except Exception as e:
                log_message(f"{name.upper()} cleanup raised an exception: {e}")


# ========= BACKUP FUNCTIONS =========


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


def mysql_dump():
    """Execute MySQL dump directly to the dump directory"""
    start_time = time.perf_counter()
    username = get_tutor_value("MYSQL_ROOT_USERNAME")
    password = get_tutor_value("MYSQL_ROOT_PASSWORD")

    # Create a directory for MySQL dump similar to MongoDB
    tutor_root = get_tutor_root()

    # Determine the path inside the container that maps to our mysql_dump_dir
    # This is typically /var/lib/mysql/dump.mysql
    container_dump_path = "/var/lib/mysql"

    compose_files = f"-f {tutor_root}/env/local/docker-compose.yml -f {tutor_root}/env/local/docker-compose.prod.yml -f {tutor_root}/env/local/docker-compose.tmp.yml"
    project_name = "tutor_local"

    # Generate the MySQL dump directly to the directory
    # Use single quotes for the outer shell and double quotes for the inner variables
    # to avoid nesting issues
    cmd = f"docker-compose {compose_files} --project-name {project_name} exec -T -e USERNAME={username} -e PASSWORD={password} mysql sh -c \"mysqldump --all-databases --user=\\$USERNAME --password=\\$PASSWORD > {container_dump_path}/all-databases.sql\""

    # Add retry logic for more reliability
    max_attempts = 1
    for attempt in range(1, max_attempts + 1):
        try:
            log_message(f"MySQL dump attempt {attempt}/{max_attempts}")
            run(cmd)
            break  # If successful, break out of the retry loop
        except Exception as e:
            log_message(f"MySQL dump attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                log_message("All MySQL dump attempts failed")
                raise  # Re-raise the last exception after all attempts fail
            else:
                log_message(f"Waiting 30 seconds before retry...")
                time.sleep(30)  # Wait before retry

    dump_file = os.path.join(get_tutor_root(), "data/mysql/all-databases.sql")

    # Verify that the dump was created
    if not os.path.exists(dump_file):
        raise FileNotFoundError(f"MySQL dump file was not created at {dump_file}")

    # Check if the file size is reasonable (at least 1KB)
    if os.path.getsize(dump_file) < 1024:
        raise ValueError(f"MySQL dump file is too small ({os.path.getsize(dump_file)} bytes). Dump may have failed.")

    log_time("MySQL dump", start_time)
    return dump_file


def mongodb_dump():
    start_time = time.perf_counter()

    tutor_root = get_tutor_root()
    compose_files = f"-f {tutor_root}/env/local/docker-compose.yml -f {tutor_root}/env/local/docker-compose.prod.yml -f {tutor_root}/env/local/docker-compose.tmp.yml"
    project_name = "tutor_local"

    # Add retry logic for more reliability
    max_attempts = 1
    for attempt in range(1, max_attempts + 1):
        try:
            log_message(f"MongoDB dump attempt {attempt}/{max_attempts}")
            cmd = f"docker-compose {compose_files} --project-name {project_name} exec -T mongodb mongodump --out=/data/db/dump.mongodb"
            run(cmd)
            break  # If successful, break out of the retry loop
        except Exception as e:
            log_message(f"MongoDB dump attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                log_message("All MongoDB dump attempts failed")
                raise  # Re-raise the last exception after all attempts fail
            else:
                log_message(f"Waiting 30 seconds before retry...")
                time.sleep(30)  # Wait before retry

    dump_path = os.path.join(get_tutor_root(), "data/mongodb/dump.mongodb")

    # Verify that the dump directory was created
    if not os.path.exists(dump_path):
        raise FileNotFoundError(f"MongoDB dump directory was not created at {dump_path}")

    # Check if the directory has content
    if not os.listdir(dump_path):
        raise ValueError(f"MongoDB dump directory is empty. Dump may have failed.")

    log_time("MongoDB dump", start_time)
    return dump_path


# ========= COMPRESSION =========

def is_small_file(file_path):
    """Check if file is small enough to use simple compression"""
    try:
        if os.path.isfile(file_path):
            # For files under 100MB, use simple compression
            return os.path.getsize(file_path) < 100 * 1024 * 1024
        elif os.path.isdir(file_path):
            # Check total size of directory
            total_size = 0
            for dirpath, _, filenames in os.walk(file_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    if os.path.isfile(fp):
                        total_size += os.path.getsize(fp)
                    if total_size >= 100 * 1024 * 1024:
                        return False
            return True
        return True
    except Exception:
        # If we can't determine size, assume it's large
        return False


def compress_tar_py(directory, output_file):
    """Use Python's tarfile module for small files or when external tools aren't available"""
    with tarfile.open(output_file, f"w:gz", compresslevel=int(COMPRESSION_LEVEL)) as tar:
        tar.add(directory, arcname=os.path.basename(directory))
    return output_file


def compress_tar_fast(directory, output_file):
    """Use faster external compression tools (pigz or pbzip2) for tar creation"""
    if is_installed("pigz"):
        # pigz is a parallel implementation of gzip
        cores = max(1, multiprocessing.cpu_count() - 1)
        cmd = f"tar -cf - -C {os.path.dirname(directory)} {os.path.basename(directory)} | pigz -p {cores} -{COMPRESSION_LEVEL} > {output_file}"
        run(cmd)
    elif is_installed("pbzip2"):
        # pbzip2 is a parallel implementation of bzip2 - slower than pigz but better compression
        cores = max(1, multiprocessing.cpu_count() - 1)
        cmd = f"tar -cf - -C {os.path.dirname(directory)} {os.path.basename(directory)} | pbzip2 -p{cores} -{COMPRESSION_LEVEL} > {output_file.replace('.tar.gz', '.tar.bz2')}"
        run(cmd)
        # Rename if needed
        if output_file.endswith('.tar.gz') and os.path.exists(output_file.replace('.tar.gz', '.tar.bz2')):
            os.rename(output_file.replace('.tar.gz', '.tar.bz2'), output_file)
    else:
        # Fallback to standard tar with gzip
        cmd = f"tar -czf {output_file} -C {os.path.dirname(directory)} {os.path.basename(directory)}"
        run(cmd)
    return output_file


def compress_tar_exclude(args):
    """Compress directory with exclusions"""
    directory, output_file, exclude_dirs = args
    start_time = time.perf_counter()
    log_message(f"Compressing {directory} to {output_file} (excluding: {', '.join(exclude_dirs) if exclude_dirs else 'none'})")

    # Check if directory exists
    if not os.path.exists(directory):
        log_message(f"Warning: Directory {directory} does not exist. Skipping compression.")
        return None

    try:
        # Build exclude options for tar
        exclude_opts = ""
        if exclude_dirs:
            for exclude_dir in exclude_dirs:
                exclude_opts += f" --exclude='{exclude_dir}'"

        # Use tar command with exclude options
        cmd = f"tar -czf {output_file} -C {os.path.dirname(directory)} {exclude_opts} {os.path.basename(directory)}"
        run(cmd)

    except Exception as e:
        log_message(f"Warning: Error compressing {directory}: {e}")
        # Try with sudo
        try:
            exclude_opts = ""
            if exclude_dirs:
                for exclude_dir in exclude_dirs:
                    exclude_opts += f" --exclude='{exclude_dir}'"

            cmd = f"sudo tar -czf {output_file} -C {os.path.dirname(directory)} {exclude_opts} {os.path.basename(directory)}"
            run(cmd)
            run(f"sudo chown $USER:$USER {output_file}")
        except Exception as e2:
            log_message(f"Fatal error compressing {directory}: {e2}")
            raise

    log_time(f"Compression of {os.path.basename(output_file)}", start_time)
    return output_file


def compress_tar(args):
    directory, output_file = args
    start_time = time.perf_counter()
    log_message(f"Compressing {directory} to {output_file}")

    # Check if directory exists
    if not os.path.exists(directory):
        log_message(f"Warning: Directory {directory} does not exist. Skipping compression.")
        return None

    # Handle permission issues
    try:
        # First try with fast external tools if enabled
        if USE_FAST_COMPRESSION and not is_small_file(directory):
            try:
                compress_tar_fast(directory, output_file)
            except Exception as e:
                log_message(f"Warning: Fast compression failed, falling back to Python implementation: {e}")
                compress_tar_py(directory, output_file)
        else:
            # For small files, use Python's implementation
            compress_tar_py(directory, output_file)
    except Exception as e:
        log_message(f"Warning: Error compressing {directory}: {e}")
        # Try with sudo
        try:
            cmd = f"sudo tar -czf {output_file} -C {os.path.dirname(directory)} {os.path.basename(directory)}"
            run(cmd)
            run(f"sudo chown $USER:$USER {output_file}")
        except Exception as e2:
            log_message(f"Fatal error compressing {directory}: {e2}")
            raise

    log_time(f"Compression of {os.path.basename(output_file)}", start_time)
    return output_file


def generate_checksum(file_path):
    start_time = time.perf_counter()
    checksum_file = file_path + ".sha256"
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    with open(checksum_file, "w") as f:
        f.write(f"{sha256_hash.hexdigest()}  {os.path.basename(file_path)}\n")
    log_time(f"Checksum generation for {os.path.basename(file_path)}", start_time)
    return checksum_file


# ========= TRANSFER & CLEANUP =========


def rsync_transfer(file_list, remote_folder):
    start_time = time.perf_counter()
    try:
        # SSH options to disable host key checking
        ssh_opts = f'-i {SSH_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

        # Ensure remote directory exists
        run(f'ssh {ssh_opts} {REMOTE_SERVER} "mkdir -p {REMOTE_PATH}/{remote_folder}"')
        run(f'rsync -avz -e "ssh {ssh_opts}" --progress {file_list} {REMOTE_SERVER}:{REMOTE_PATH}/{remote_folder}/')
        log_time("Rsync transfer", start_time)
        return True
    except Exception as e:
        log_message(f"WARNING: rsync failed: {e}")
        return False


def s3_transfer(files_list, remote_folder):
    start_time = time.perf_counter()
    try:
        import boto3
        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3 = session.client('s3')

        def upload_file(upload_info):
            file_path, is_checksum = upload_info
            filename = os.path.basename(file_path)
            s3_key = os.path.join(AWS_S3_PREFIX, remote_folder, filename)
            s3.upload_file(file_path, AWS_BUCKET_NAME, s3_key)
            return file_path

        upload_tasks = []
        for file in files_list:
            upload_tasks.append((file, False))
            upload_tasks.append((file + ".sha256", True))

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(upload_tasks), 10)) as executor:
            list(executor.map(upload_file, upload_tasks))

        log_time("S3 transfer", start_time)
        return True
    except Exception as e:
        log_message(f"WARNING: S3 upload failed: {e}")
        return False


def gcs_transfer(files_list, remote_folder):
    start_time = time.perf_counter()
    try:
        from google.cloud import storage
        key_path = "/tmp/gcp_service_account.json"
        with open(key_path, "w") as f:
            json.dump(json.loads(GCP_SERVICE_ACCOUNT_JSON), f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

        storage_client = storage.Client()
        bucket = storage_client.bucket(AWS_BUCKET_NAME)

        def upload_to_gcs(upload_info):
            file_path, is_checksum = upload_info
            filename = os.path.basename(file_path)
            blob_name = f"{AWS_S3_PREFIX}/{remote_folder}/{filename}"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file_path)
            return file_path

        upload_tasks = []
        for file in files_list:
            upload_tasks.append((file, False))
            upload_tasks.append((file + ".sha256", True))

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(upload_tasks), 10)) as executor:
            list(executor.map(upload_to_gcs, upload_tasks))

        os.remove(key_path)
        log_time("GCS transfer", start_time)
        return True
    except Exception as e:
        log_message(f"WARNING: GCS upload failed: {e}")
        return False


def transfer_files(files_to_transfer, remote_folder, targets):
    # Clean up dump directories after compression
    tutor_root = get_tutor_root()
    mongodb_dump_dir = os.path.join(tutor_root, "data/mongodb/dump.mongodb")
    mysql_dump_dir = os.path.join(tutor_root, "data/mysql/all-databases.sql")

    for dump_dir in [mongodb_dump_dir, mysql_dump_dir]:
        if os.path.exists(dump_dir):
            try:
                run(f'sudo rm -rf {dump_dir}')
            except Exception as e:
                log_message(f"WARNING: Failed to remove dump directory {dump_dir}: {e}")

    # Filter out None values from files_to_transfer
    files_to_transfer = [f for f in files_to_transfer if f is not None]
    if not files_to_transfer:
        log_message("WARNING: No files to transfer!")
        return

    # Generate checksums in parallel
    start_time = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(files_to_transfer), NUM_WORKERS)) as executor:
        list(executor.map(generate_checksum, files_to_transfer))
    log_time("Generating all checksums", start_time)

    # Prepare file list for rsync
    file_list = " ".join([file + " " + file + ".sha256" for file in files_to_transfer])

    # Execute transfers in parallel
    transfer_tasks = []

    if "rsync" in targets:
        transfer_tasks.append(("rsync", partial(rsync_transfer, file_list, remote_folder)))

    if "s3" in targets:
        transfer_tasks.append(("s3", partial(s3_transfer, files_to_transfer, remote_folder)))

    if "gcs" in targets:
        transfer_tasks.append(("gcs", partial(gcs_transfer, files_to_transfer, remote_folder)))

    # Execute all transfer tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(transfer_tasks)) as executor:
        futures = {executor.submit(func): name for name, func in transfer_tasks}
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                success = future.result()
                if success:
                    log_message(f"{name.upper()} transfer completed successfully")
                else:
                    log_message(f"{name.upper()} transfer failed")
            except Exception as e:
                log_message(f"{name.upper()} transfer raised an exception: {e}")

    # Clean up files after transfer
    for file in files_to_transfer:
        os.remove(file)
        os.remove(file + ".sha256")

    # Get the directory containing the backup files
    backup_dir = os.path.dirname(files_to_transfer[0]) if files_to_transfer else None

    log_message("Cleanup complete.")

    return backup_dir  # Return the backup directory path for cleanup in main


# ========= AZURE VM OPERATIONS =========

def start_azure_vm():
    """Start the Azure VM for restore operations"""
    log_message("Starting Azure VM for restore operations...")
    
    # Get Azure configuration from environment variables
    subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
    resource_group = os.environ.get("AZURE_RESOURCE_GROUP")
    vm_name = os.environ.get("AZURE_VM_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id = os.environ.get("AZURE_TENANT_ID")
    
    # Check if all required Azure configuration is present
    if not all([subscription_id, resource_group, vm_name, client_id, client_secret, tenant_id]):
        log_message("WARNING: Azure configuration is incomplete. Skipping Azure VM operations.")
        return False
    
    # Azure Resource Manager API endpoint for VM start operation
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Compute/virtualMachines/{vm_name}/start?api-version=2023-03-01"
    
    # Get Azure authentication token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://management.azure.com/'
    }
    
    try:
        # Get authentication token
        token_response = requests.post(token_url, data=token_data)
        token_response.raise_for_status()
        access_token = token_response.json().get('access_token')
        
        # Start the VM
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        
        # VM start is asynchronous, so we need to wait for it to complete
        log_message("Azure VM start initiated. Waiting for VM to be ready...")
        
        # Wait for VM to be in running state
        status_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Compute/virtualMachines/{vm_name}?api-version=2023-03-01"
        max_retries = 30
        retry_interval = 10  # seconds
        
        for i in range(max_retries):
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            vm_status = status_response.json().get('properties', {}).get('instanceView', {}).get('statuses', [{}])[-1].get('code', '')
            
            if 'running' in vm_status.lower():
                log_message(f"Azure VM is now running after {i * retry_interval} seconds")
                return True
            
            log_message(f"Waiting for Azure VM to start... (Attempt {i+1}/{max_retries})")
            time.sleep(retry_interval)
        
        log_message("WARNING: Timed out waiting for Azure VM to start")
        return False
        
    except Exception as e:
        log_message(f"ERROR: Failed to start Azure VM: {e}")
        return False

def stop_azure_vm():
    """Stop the Azure VM after restore operations"""
    log_message("Stopping Azure VM after restore operations...")
    
    # Get Azure configuration from environment variables
    subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
    resource_group = os.environ.get("AZURE_RESOURCE_GROUP")
    vm_name = os.environ.get("AZURE_VM_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id = os.environ.get("AZURE_TENANT_ID")
    
    # Check if all required Azure configuration is present
    if not all([subscription_id, resource_group, vm_name, client_id, client_secret, tenant_id]):
        log_message("WARNING: Azure configuration is incomplete. Skipping Azure VM operations.")
        return False
    
    # Azure Resource Manager API endpoint for VM deallocate operation
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Compute/virtualMachines/{vm_name}/deallocate?api-version=2023-03-01"
    
    # Get Azure authentication token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://management.azure.com/'
    }
    
    try:
        # Get authentication token
        token_response = requests.post(token_url, data=token_data)
        token_response.raise_for_status()
        access_token = token_response.json().get('access_token')
        
        # Stop the VM
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        
        log_message("Azure VM stop initiated successfully")
        return True
        
    except Exception as e:
        log_message(f"ERROR: Failed to stop Azure VM: {e}")
        return False

def trigger_restore():
    """Trigger the restore process on the Azure VM"""
    log_message("Triggering restore process on Azure VM...")
    
    # Get the path to tutor_restore.py
    tutor_restore_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tutor_restore.py")
    
    if not os.path.exists(tutor_restore_path):
        log_message(f"ERROR: Could not find tutor_restore.py at {tutor_restore_path}")
        return False
    
    try:
        # Import tutor_restore module dynamically
        spec = importlib.util.spec_from_file_location("tutor_restore", tutor_restore_path)
        tutor_restore = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tutor_restore)
        
        # Call the main function from tutor_restore
        log_message("Executing tutor_restore.main()...")
        tutor_restore.main()
        
        log_message("Restore process completed successfully")
        return True
        
    except Exception as e:
        log_message(f"ERROR: Failed to trigger restore process: {e}")
        return False

# ========= MAIN =========


def main():
    total_start_time = time.perf_counter()

    ensure_ssh_key()
    install_requirements()

    targets = ["rsync", "s3", "gcs"]

    # Clean up old backups first before creating new ones
    cleanup_old_backups(targets)

    today_str = datetime.now().strftime("%Y%m%d")
    folder_name = f"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-{today_str}"
    backup_path = os.path.join(BACKUP_DIR, folder_name)

    if os.path.exists(backup_path):
        log_message(f"Backup already exists for today: {backup_path}")
        return

    try:
        os.makedirs(backup_path, exist_ok=True)
    except Exception:
        run(f"sudo mkdir -p {backup_path}")
        run(f"sudo chown $USER:$USER {backup_path}")

    # Run database dumps sequentially to avoid Docker container issues
    log_message("Starting MySQL dump...")
    mysql_dump_dir = mysql_dump()

    log_message("Starting MongoDB dump...")
    mongodb_dump_dir = mongodb_dump()

    # Get tutor root and parent directory for tutor config and plugins
    tutor_root = get_tutor_root()
    tutor_parent = os.path.dirname(tutor_root)
    tutor_plugins_dir = os.path.join(tutor_parent, "tutor-plugins")

    # Set up compression tasks
    mysql_dump_file = os.path.join(backup_path, "mysql_dump.tar.gz")
    mongodb_tar_file = os.path.join(backup_path, "mongodb_dump.tar.gz")
    openedx_media_dir = os.path.join(get_tutor_root(), "data/openedx-media")
    openedx_tar_file = os.path.join(backup_path, "openedx_media.tar.gz")
    tutor_config_tar_file = os.path.join(backup_path, "tutor_config.tar.gz")
    tutor_plugins_tar_file = os.path.join(backup_path, "tutor_plugins.tar.gz")

    compression_tasks = [
        (mysql_dump_dir, mysql_dump_file),
        (mongodb_dump_dir, mongodb_tar_file),
        (openedx_media_dir, openedx_tar_file)
    ]

    # Add tutor config backup (excluding data folder)
    compression_tasks_with_excludes = [
        (tutor_root, tutor_config_tar_file, ["data"])
    ]

    # Add tutor-plugins backup if directory exists
    if os.path.exists(tutor_plugins_dir):
        compression_tasks.append((tutor_plugins_dir, tutor_plugins_tar_file))
        log_message(f"Found tutor-plugins directory: {tutor_plugins_dir}")
    else:
        log_message(f"Warning: tutor-plugins directory not found at {tutor_plugins_dir}")

    # Run compression in parallel with ThreadPoolExecutor
    files_to_transfer = []

    # Handle regular compression tasks
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_task = {executor.submit(compress_tar, task): task for task in compression_tasks}
        for future in concurrent.futures.as_completed(future_to_task):
            try:
                result = future.result()
                if result:
                    files_to_transfer.append(result)
            except Exception as e:
                task = future_to_task[future]
                log_message(f"Compression task failed for {task[0]}: {e}")

    # Handle compression tasks with excludes
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_task = {executor.submit(compress_tar_exclude, task): task for task in compression_tasks_with_excludes}
        for future in concurrent.futures.as_completed(future_to_task):
            try:
                result = future.result()
                if result:
                    files_to_transfer.append(result)
            except Exception as e:
                task = future_to_task[future]
                log_message(f"Compression task with excludes failed for {task[0]}: {e}")

    # Transfer compressed files
    transfer_files(files_to_transfer, folder_name, targets)

    # After backup is complete, start Azure VM and trigger restore
    log_message("Backup completed. Starting Azure VM and triggering restore process...")
    
    # Start Azure VM
    vm_started = start_azure_vm()
    
    # If VM started successfully, trigger restore
    if vm_started:
        restore_success = trigger_restore()
        
        # If restore was successful, stop the VM
        if restore_success:
            stop_azure_vm()
        else:
            log_message("WARNING: Restore process failed. Azure VM will remain running.")
    else:
        log_message("WARNING: Failed to start Azure VM. Skipping restore process.")

    # Remove the backup folder after successful transfer
    if os.path.exists(backup_path):
        try:
            shutil.rmtree(backup_path)
            log_message(f"Removed temporary backup folder: {backup_path}")
        except Exception as e:
            log_message(f"Warning: Could not remove backup folder {backup_path}: {e}")
            try:
                run(f"sudo rm -rf {backup_path}")
                log_message(f"Removed temporary backup folder with sudo: {backup_path}")
            except Exception as e2:
                log_message(f"Error: Failed to remove backup folder even with sudo: {e2}")

    log_time("Total backup process", total_start_time)


if __name__ == "__main__":
    main()