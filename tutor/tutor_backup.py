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
from datetime import datetime
from dotenv import load_dotenv
from functools import partial

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

    # Generate the MySQL dump directly to the directory
    cmd = (
        f'tutor local exec -e USERNAME="{username}" -e PASSWORD="{password}" '
        f'mysql sh -c \'mysqldump --all-databases --user=$USERNAME --password=$PASSWORD > {container_dump_path}/all-databases.sql\''
    )
    run(cmd)

    dump_file = os.path.join(get_tutor_root(), "data/mysql/all-databases.sql")

    log_time("MySQL dump", start_time)
    return dump_file


def mongodb_dump():
    start_time = time.perf_counter()
    cmd = "tutor local exec mongodb mongodump --out=/data/db/dump.mongodb"
    run(cmd)
    dump_path = os.path.join(get_tutor_root(), "data/mongodb/dump.mongodb")
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


def compress_tar(args):
    directory, output_file = args
    start_time = time.perf_counter()
    print(f"Compressing {directory} to {output_file}")

    # Check if directory exists
    if not os.path.exists(directory):
        print(f"Warning: Directory {directory} does not exist. Skipping compression.")
        return None

    # Handle permission issues
    try:
        # First try with fast external tools if enabled
        if USE_FAST_COMPRESSION and not is_small_file(directory):
            try:
                compress_tar_fast(directory, output_file)
            except Exception as e:
                print(f"Warning: Fast compression failed, falling back to Python implementation: {e}")
                compress_tar_py(directory, output_file)
        else:
            # For small files, use Python's implementation
            compress_tar_py(directory, output_file)
    except Exception as e:
        print(f"Warning: Error compressing {directory}: {e}")
        # Try with sudo
        try:
            cmd = f"sudo tar -czf {output_file} -C {os.path.dirname(directory)} {os.path.basename(directory)}"
            run(cmd)
            run(f"sudo chown $USER:$USER {output_file}")
        except Exception as e2:
            print(f"Fatal error compressing {directory}: {e2}")
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
        # Ensure remote directory exists
        run(f'ssh -i {SSH_KEY_PATH} {REMOTE_SERVER} "mkdir -p {REMOTE_PATH}/{remote_folder}"')
        run(f'rsync -avz -e "ssh -i {SSH_KEY_PATH}" --progress {file_list} {REMOTE_SERVER}:{REMOTE_PATH}/{remote_folder}/')
        log_time("Rsync transfer", start_time)
        return True
    except Exception as e:
        print(f"WARNING: rsync failed: {e}")
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
        print(f"WARNING: S3 upload failed: {e}")
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
        print(f"WARNING: GCS upload failed: {e}")
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
                print(f"WARNING: Failed to remove dump directory {dump_dir}: {e}")

    # Filter out None values from files_to_transfer
    files_to_transfer = [f for f in files_to_transfer if f is not None]
    if not files_to_transfer:
        print("WARNING: No files to transfer!")
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
                    print(f"{name.upper()} transfer completed successfully")
                else:
                    print(f"{name.upper()} transfer failed")
            except Exception as e:
                print(f"{name.upper()} transfer raised an exception: {e}")

    # Clean up files after transfer
    for file in files_to_transfer:
        os.remove(file)
        os.remove(file + ".sha256")

    # Get the directory containing the backup files
    backup_dir = os.path.dirname(files_to_transfer[0]) if files_to_transfer else None

    print("Cleanup complete.")

    return backup_dir  # Return the backup directory path for cleanup in main


# ========= MAIN =========


def main():
    total_start_time = time.perf_counter()

    ensure_ssh_key()
    install_requirements()

    targets = ["rsync", "s3", "gcs"]

    today_str = datetime.now().strftime("%Y%m%d")
    folder_name = f"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-{today_str}"
    backup_path = os.path.join(BACKUP_DIR, folder_name)

    if os.path.exists(backup_path):
        print(f"Backup already exists for today: {backup_path}")
        return

    try:
        os.makedirs(backup_path, exist_ok=True)
    except Exception:
        run(f"sudo mkdir -p {backup_path}")
        run(f"sudo chown $USER:$USER {backup_path}")

    # Run database dumps sequentially to avoid Docker container issues
    print("Starting MySQL dump...")
    mysql_dump_dir = mysql_dump()

    print("Starting MongoDB dump...")
    mongodb_dump_dir = mongodb_dump()

    # Set up compression tasks
    mysql_dump_file = os.path.join(backup_path, "mysql_dump.tar.gz")
    mongodb_tar_file = os.path.join(backup_path, "mongodb_dump.tar.gz")
    openedx_media_dir = os.path.join(get_tutor_root(), "data/openedx-media")
    openedx_tar_file = os.path.join(backup_path, "openedx_media.tar.gz")

    compression_tasks = [
        (mysql_dump_dir, mysql_dump_file),
        (mongodb_dump_dir, mongodb_tar_file),
        (openedx_media_dir, openedx_tar_file)
    ]

    # Run compression in parallel with ThreadPoolExecutor
    files_to_transfer = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_task = {executor.submit(compress_tar, task): task for task in compression_tasks}
        for future in concurrent.futures.as_completed(future_to_task):
            try:
                result = future.result()
                if result:
                    files_to_transfer.append(result)
            except Exception as e:
                task = future_to_task[future]
                print(f"Compression task failed for {task[0]}: {e}")

    # Transfer compressed files
    transfer_files(files_to_transfer, folder_name, targets)

    # Remove the backup folder after successful transfer
    if os.path.exists(backup_path):
        try:
            shutil.rmtree(backup_path)
            print(f"Removed temporary backup folder: {backup_path}")
        except Exception as e:
            print(f"Warning: Could not remove backup folder {backup_path}: {e}")
            try:
                run(f"sudo rm -rf {backup_path}")
                print(f"Removed temporary backup folder with sudo: {backup_path}")
            except Exception as e2:
                print(f"Error: Failed to remove backup folder even with sudo: {e2}")

    log_time("Total backup process", total_start_time)


if __name__ == "__main__":
    main()