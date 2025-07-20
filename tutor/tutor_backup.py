import os
import subprocess
import shutil
import sys
import json
import time
import multiprocessing
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

BACKUP_DIR = "/tmp/tutor-backup"

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
    username = get_tutor_value("MYSQL_ROOT_USERNAME")
    password = get_tutor_value("MYSQL_ROOT_PASSWORD")
    cmd = (
        f'tutor local exec -e USERNAME="{username}" -e PASSWORD="{password}" '
        f'mysql sh -c \'mysqldump --all-databases --user=$USERNAME --password=$PASSWORD > /var/lib/mysql/dump.sql\''
    )
    run(cmd)
    src = os.path.join(get_tutor_root(), "data/mysql/dump.sql")
    dest = os.path.join(dump_path, "mysql_dump.sql")
    shutil.copy(src, dest)
    os.remove(src)


def mongodb_dump():
    cmd = "tutor local exec mongodb mongodump --out=/data/db/dump.mongodb"
    run(cmd)
    dump_path = os.path.join(get_tutor_root(), "data/mongodb/dump.mongodb")
    return dump_path


# ========= COMPRESSION =========


def compress_tar(directory, output_file):
    with tarfile.open(output_file, "w:gz") as tar:
        tar.add(directory, arcname=os.path.basename(directory))


def generate_checksum(file_path):
    checksum_file = file_path + ".sha256"
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    with open(checksum_file, "w") as f:
        f.write(f"{sha256_hash.hexdigest()}  {os.path.basename(file_path)}\n")
    return checksum_file


# ========= TRANSFER & CLEANUP =========


def transfer_files(files_to_transfer, remote_folder, targets):
    # Clean up MongoDB dump directory after compression
    mongodb_dump_dir = os.path.join(get_tutor_root(), "data/mongodb/dump.mongodb")
    if os.path.exists(mongodb_dump_dir):
        try:
            run(f'sudo rm -rf {mongodb_dump_dir}')
        except Exception as e:
            print(f"WARNING: Failed to remove MongoDB dump directory with sudo: {e}")
    for file in files_to_transfer:
        generate_checksum(file)

    if "rsync" in targets:
        try:
            # Ensure remote directory exists
            run(f'ssh -i {SSH_KEY_PATH} {REMOTE_SERVER} "mkdir -p {REMOTE_PATH}/{remote_folder}"')
            file_list = " ".join([file + " " + file + ".sha256" for file in files_to_transfer])
            run(f'rsync -avz -e "ssh -i {SSH_KEY_PATH}" --progress {file_list} {REMOTE_SERVER}:{REMOTE_PATH}/{remote_folder}/')
        except Exception as e:
            print(f"WARNING: rsync failed: {e}")

    if "s3" in targets:
        try:
            import boto3
            session = boto3.session.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
            s3 = session.client('s3')
            for file in files_to_transfer:
                for upload_file in [file, file + ".sha256"]:
                    filename = os.path.basename(upload_file)
                    s3_key = os.path.join(AWS_S3_PREFIX, remote_folder, filename)
                    s3.upload_file(upload_file, AWS_BUCKET_NAME, s3_key)
        except Exception as e:
            print(f"WARNING: S3 upload failed: {e}")

    if "gcs" in targets:
        try:
            from google.cloud import storage
            key_path = "/tmp/gcp_service_account.json"
            with open(key_path, "w") as f:
                json.dump(json.loads(GCP_SERVICE_ACCOUNT_JSON), f)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

            storage_client = storage.Client()
            bucket = storage_client.bucket(AWS_BUCKET_NAME)
            for file in files_to_transfer:
                for upload_file in [file, file + ".sha256"]:
                    filename = os.path.basename(upload_file)
                    blob_name = f"{AWS_S3_PREFIX}/{remote_folder}/{filename}"
                    blob = bucket.blob(blob_name)
                    blob.upload_from_filename(upload_file)
            os.remove(key_path)
        except Exception as e:
            print(f"WARNING: GCS upload failed: {e}")

    for file in files_to_transfer:
        os.remove(file)
        os.remove(file + ".sha256")
    print("Cleanup complete.")


# ========= MAIN =========


def main():
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

    mysql_dump_file = os.path.join(backup_path, "mysql_dump.tar.gz")
    mongodb_dump_dir = mongodb_dump()
    mongodb_tar_file = os.path.join(backup_path, "mongodb_dump.tar.gz")
    openedx_media_dir = os.path.join(get_tutor_root(), "data/openedx-media")
    openedx_tar_file = os.path.join(backup_path, "openedx_media.tar.gz")

    tasks = [
        (os.path.join(backup_path, "mysql_dump.sql"), mysql_dump_file),
        (mongodb_dump_dir, mongodb_tar_file),
        (openedx_media_dir, openedx_tar_file)
    ]

    pool = multiprocessing.Pool()
    for src, output in tasks:
        pool.apply_async(compress_tar, (src, output))
    pool.close()
    pool.join()

    files_to_transfer = [mysql_dump_file, mongodb_tar_file, openedx_tar_file]

    transfer_files(files_to_transfer, folder_name, targets)


if __name__ == "__main__":
    main()
