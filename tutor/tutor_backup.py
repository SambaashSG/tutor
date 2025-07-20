import os
import subprocess
import shutil
import sys
import json
import time
from datetime import datetime

# ========= CONFIGURATION =========

CLIENT_NAME = "buma"
ENV_TYPE = "prod"  # Options: prod, uat, demo, stg, test, dev.

REMOTE_SERVER = "sambaash@172.188.10.221"
REMOTE_PATH = "/home/sambaash/.local/share/tutor/backup"

# Embedded SSH Private Key
SSH_PRIVATE_KEY = """
"""

SSH_PUBLIC_KEY = """
"""


SSH_KEY_PATH = "/home/ubuntu/.ssh/backup_id_rsa"

# AWS S3 Config
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_REGION = "ap-southeast-1"
AWS_BUCKET_NAME = "sambaash-backup"
AWS_S3_PREFIX = "tutor-backup"

# GCP Service Account JSON (embed full JSON)
GCP_SERVICE_ACCOUNT_JSON = """
"""

BACKUP_DIR = "/tmp/tutor-backup"
CRON_SCHEDULE = "0 2 * * *"

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

def install_system_packages():
    start = time.perf_counter()
    pkgs = []
    if not is_installed("aws"):
        pkgs.append("awscli")
    if not is_installed("rsync"):
        pkgs.append("rsync")
    if pkgs:
        run(f"sudo apt-get update && sudo apt-get install -y {' '.join(pkgs)}")
    log_time("System package installation", start)

def install_python_packages():
    start = time.perf_counter()
    try:
        import boto3
    except ImportError:
        install_python_package("boto3")
    try:
        from google.cloud import storage
    except ImportError:
        install_python_package("google-cloud-storage")
    log_time("Python package installation", start)

def get_tutor_value(key):
    return subprocess.check_output(f"tutor config printvalue {key}", shell=True).decode().strip()

def get_tutor_root():
    return subprocess.check_output("tutor config printroot", shell=True).decode().strip()

def ensure_ssh_key():
    if os.path.exists(SSH_KEY_PATH):
        print(f"SSH key already exists at {SSH_KEY_PATH}. Skipping write.")
        return
    os.makedirs(os.path.dirname(SSH_KEY_PATH), exist_ok=True)
    print(f"Writing SSH key to {SSH_KEY_PATH}")
    with open(SSH_KEY_PATH, "w") as f:
        f.write(SSH_PRIVATE_KEY.strip() + "\n")
    os.chmod(SSH_KEY_PATH, 0o600)

# ========= BACKUP FUNCTIONS =========

def mysql_dump(dump_path):
    start = time.perf_counter()
    username = get_tutor_value("MYSQL_ROOT_USERNAME")
    password = get_tutor_value("MYSQL_ROOT_PASSWORD")
    cmd = (
        f'tutor local exec -e USERNAME="{username}" -e PASSWORD="{password}" '
        f'mysql sh -c \'mysqldump --all-databases --user=$USERNAME --password=$PASSWORD > /var/lib/mysql/dump.sql\''
    )
    run(cmd)
    src = os.path.join(get_tutor_root(), "data/mysql/dump.sql")
    shutil.copy(src, os.path.join(dump_path, "mysql_dump.sql"))
    log_time("MySQL dump", start)

def mongodb_dump(dump_path):
    start = time.perf_counter()
    cmd = "tutor local exec mongodb mongodump --out=/data/db/dump.mongodb"
    run(cmd)
    src = os.path.join(get_tutor_root(), "data/mongodb/dump.mongodb")
    shutil.make_archive(os.path.join(dump_path, "mongodb_dump"), 'zip', src)
    log_time("MongoDB dump", start)

def media_backup(dump_path):
    start = time.perf_counter()
    src = os.path.join(get_tutor_root(), "data/openedx-media")
    shutil.make_archive(os.path.join(dump_path, "openedx-media"), 'zip', src)
    log_time("Media backup", start)

# ========= TRANSFER FUNCTIONS =========

def rsync_transfer(archive_path):
    start = time.perf_counter()
    try:
        cmd = f'rsync -avz -e "ssh -i {SSH_KEY_PATH}" --progress {archive_path} {REMOTE_SERVER}:{REMOTE_PATH}/'
        run(cmd)
    except Exception as e:
        print(f"WARNING: rsync transfer failed: {e}")
    log_time("Rsync transfer", start)

def s3_transfer(archive_path):
    start = time.perf_counter()
    try:
        import boto3
        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3 = session.client('s3')
        filename = os.path.basename(archive_path)
        s3_key = os.path.join(AWS_S3_PREFIX, datetime.now().strftime('%Y%m%d'), filename)
        print(f"Uploading {filename} to S3 bucket {AWS_BUCKET_NAME}/{s3_key}")
        s3.upload_file(archive_path, AWS_BUCKET_NAME, s3_key)
    except Exception as e:
        print(f"WARNING: S3 upload failed: {e}")
    log_time("S3 transfer", start)

def gcs_transfer(archive_path):
    start = time.perf_counter()
    try:
        from google.cloud import storage
        key_path = "/tmp/gcp_service_account.json"
        with open(key_path, "w") as f:
            json.dump(json.loads(GCP_SERVICE_ACCOUNT_JSON), f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

        storage_client = storage.Client()
        bucket = storage_client.bucket(AWS_BUCKET_NAME)
        filename = os.path.basename(archive_path)
        blob_name = f"{AWS_S3_PREFIX}/{datetime.now().strftime('%Y%m%d')}/{filename}"
        blob = bucket.blob(blob_name)
        print(f"Uploading {filename} to GCS bucket {AWS_BUCKET_NAME}/{blob_name}")
        blob.upload_from_filename(archive_path)
        os.remove(key_path)
    except Exception as e:
        print(f"WARNING: GCS upload failed: {e}")
    log_time("GCS transfer", start)

# ========= CRON SETUP =========

def setup_cron(script_path):
    cron_entry = f"{CRON_SCHEDULE} nohup python3 {script_path} >> /home/ubuntu/sync.log 2>&1"
    try:
        existing_cron = subprocess.check_output("crontab -l", shell=True, text=True)
    except subprocess.CalledProcessError:
        existing_cron = ""

    if cron_entry in existing_cron:
        print("[tutor-backup] Cron job already exists.")
    else:
        new_cron = existing_cron.strip() + "\n" + cron_entry + "\n"
        subprocess.run(f'echo "{new_cron}" | crontab -', shell=True, check=True)
        print("[tutor-backup] Cron job installed.")

# ========= MAIN =========

def main():
    total_start = time.perf_counter()

    ensure_ssh_key()
    install_system_packages()
    install_python_packages()

    today_str = datetime.now().strftime("%Y%m%d")
    archive_name = f"{CLIENT_NAME}-{ENV_TYPE}-tutor-backup-{today_str}.tar.gz"
    archive_path = os.path.join(BACKUP_DIR, archive_name)

    if os.path.exists(archive_path):
        print(f"Backup for today already exists: {archive_path}. Skipping backup creation.")
    else:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        dump_path = os.path.join(BACKUP_DIR, timestamp)
        os.makedirs(dump_path, exist_ok=True)

        try:
            mysql_dump(dump_path)
            mongodb_dump(dump_path)
            media_backup(dump_path)

            start = time.perf_counter()
            print(f"Creating compressed archive: {archive_path}")
            shutil.make_archive(archive_path.replace('.tar.gz', ''), 'gztar', dump_path)
            log_time("Compression", start)

        except Exception as e:
            print(f"ERROR during backup creation: {e}")
        finally:
            shutil.rmtree(dump_path)

    print("Starting transfers...")

    rsync_transfer(archive_path)
    s3_transfer(archive_path)
    gcs_transfer(archive_path)

    print("All transfer attempts complete.")

    setup_cron(os.path.abspath(__file__))

    log_time("Total backup process", total_start)

if __name__ == "__main__":
    main()