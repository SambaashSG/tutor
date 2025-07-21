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


def mysql_dump(dump_path):
    """Execute MySQL dump using sudo to avoid permission issues"""
    start_time = time.perf_counter()
    username = get_tutor_value("MYSQL_ROOT_USERNAME")
    password = get_tutor_value("MYSQL_ROOT_PASSWORD")
    tutor_root = get_tutor_root()

    # Use a unique temp file name to avoid conflicts
    timestamp = int(time.time())
    temp_dump_file = f"/tmp/mysql_dump_{timestamp}.sql"

    cmd = (
        f'tutor local exec -e USERNAME="{username}" -e PASSWORD="{password}" '
        f'mysql sh -c \'mysqldump --all-databases --user=$USERNAME --password=$PASSWORD > /var/lib/mysql/dump.sql\''
    )
    run(cmd)

    src = os.path.join(tutor_root, "data/mysql/dump.sql")
    dest = os.path.join(dump_path, "mysql_dump.sql")

    # First copy to a temporary location with sudo to handle permissions
    try:
        run(f"sudo cp {src} {temp_dump_file}")
        run(f"sudo chown $USER:$USER {temp_dump_file}")
        # Now we can safely copy from temp to final destination
        shutil.copy(temp_dump_file, dest)
        # Clean up temp file
        os.remove(temp_dump_file)
    except Exception as e:
        print(f"Warning: Error handling MySQL dump file: {e}")
        # Try direct copy as fallback
        try:
            run(f"sudo cp {src} {dest}")
            run(f"sudo chown $USER:$USER {dest}")
        except Exception as e2:
            print(f"Fatal error: Could not copy MySQL dump: {e2}")
            raise

    log_time("MySQL dump", start_time)
    return dest


def mongodb_dump():
    start_time = time.perf_counter()
    cmd = "tutor local exec mongodb mongodump --out=/data/db/dump.mongodb"
    run(cmd)
    dump_path = os.path.join(get_tutor_root(), "data/mongodb/dump.mongodb")
    log_time("MongoDB dump", start_time)
    return dump_path


# ========= COMPRESSION =========


def compress_tar(args):
    directory, output_file = args
    start_time = time.perf_counter()
    print(f"Compressing {directory} to {output_file}")
    with tarfile.open(output_file, "w:gz") as tar:
        tar.add(directory, arcname=os.path.basename(directory))
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
    # Clean up MongoDB dump directory after compression
    mongodb_dump_dir = os.path.join(get_tutor_root(), "data/mongodb/dump.mongodb")
    if os.path.exists(mongodb_dump_dir):
        try:
            run(f'sudo rm -rf {mongodb_dump_dir}')
        except Exception as e:
            print(f"WARNING: Failed to remove MongoDB dump directory with sudo: {e}")

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
    print("Cleanup complete.")


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

    os.makedirs(backup_path, exist_ok=True)

    # Run database dumps in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
        mysql_future = executor.submit(mysql_dump, backup_path)
        mongodb_future = executor.submit(mongodb_dump)

        # Wait for database dumps to complete
        mysql_dump_sql = mysql_future.result()
        mongodb_dump_dir = mongodb_future.result()

    # Set up compression tasks
    mysql_dump_file = os.path.join(backup_path, "mysql_dump.tar.gz")
    mongodb_tar_file = os.path.join(backup_path, "mongodb_dump.tar.gz")
    openedx_media_dir = os.path.join(get_tutor_root(), "data/openedx-media")
    openedx_tar_file = os.path.join(backup_path, "openedx_media.tar.gz")

    compression_tasks = [
        (mysql_dump_sql, mysql_dump_file),
        (mongodb_dump_dir, mongodb_tar_file),
        (openedx_media_dir, openedx_tar_file)
    ]

    # Run compression in parallel
    files_to_transfer = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for result in executor.map(compress_tar, compression_tasks):
            files_to_transfer.append(result)

    # Transfer compressed files
    transfer_files(files_to_transfer, folder_name, targets)

    log_time("Total backup process", total_start_time)


if __name__ == "__main__":
    main()