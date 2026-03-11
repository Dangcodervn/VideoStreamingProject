import os
import time
import logging
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

CHECKPOINT_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "upload_checkpoint.txt")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Load biến từ file .env ở root project
_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_ROOT, ".env"))

ACCOUNT_NAME = os.environ["ADLS_ACCOUNT_NAME"]
ACCOUNT_KEY  = os.environ["ADLS_ACCOUNT_KEY"]
CONTAINER    = os.environ["ADLS_CONTAINER"]
ADLS_PREFIX  = "video_streaming"

LOCAL_CLEANED = os.path.normpath(
    os.path.join(_ROOT, "data", "cleaned", "method1_clean")
)

def load_checkpoint() -> set:
    """Đọc danh sách partition đã upload thành công."""
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())


def save_checkpoint(dt_folder: str):
    """Ghi thêm 1 partition vào checkpoint sau khi upload thành công."""
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(dt_folder + "\n")


def get_service_client(account_name: str, account_key: str) -> DataLakeServiceClient:
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key,
    )


def upload_partition(fs_client, local_dir: str, dt_folder: str, retries: int = 3):
    """Upload the single CSV file inside one dt=yyyymmdd folder to ADLS.
    
    - Streaming upload (không đọc toàn bộ file vào RAM)
    - Retry tối đa `retries` lần nếu lỗi mạng
    """
    uploaded = 0
    for fname in os.listdir(local_dir):
        if not fname.endswith(".csv"):
            continue

        local_file = os.path.join(local_dir, fname)
        adls_path  = f"{ADLS_PREFIX}/{dt_folder}/data.csv"
        fc = fs_client.get_file_client(adls_path)

        for attempt in range(1, retries + 1):
            try:
                # Streaming: truyền file object trực tiếp, không f.read()
                with open(local_file, "rb") as f:
                    fc.upload_data(f, overwrite=True)
                logger.info("Uploaded: %s → %s", dt_folder, adls_path)
                save_checkpoint(dt_folder)
                uploaded += 1
                break
            except Exception as e:
                logger.warning("Attempt %d/%d failed for %s: %s", attempt, retries, dt_folder, e)
                if attempt < retries:
                    time.sleep(2 ** attempt)  # exponential backoff: 2s, 4s, 8s
                else:
                    logger.error("FAILED after %d retries: %s", retries, dt_folder)

    if uploaded == 0:
        logger.warning("SKIP %s — no CSV file found", dt_folder)

def main():
    if not os.path.isdir(LOCAL_CLEANED):
        logger.error("Local cleaned folder not found: %s", LOCAL_CLEANED)
        return

    # Checkpoint: bỏ qua partition đã upload thành công
    done = load_checkpoint()
    if done:
        logger.info("Checkpoint found — skipping %d already-uploaded partition(s).", len(done))

    service = get_service_client(ACCOUNT_NAME, ACCOUNT_KEY)
    fs      = service.get_file_system_client(CONTAINER)

    dt_folders = sorted([
        d for d in os.listdir(LOCAL_CLEANED)
        if os.path.isdir(os.path.join(LOCAL_CLEANED, d)) and d.startswith("dt=")
    ])

    pending = [d for d in dt_folders if d not in done]

    if not pending:
        logger.info("All partitions already uploaded. Delete upload_checkpoint.txt to re-upload.")
        return

    logger.info("Uploading %d/%d partition(s) to: %s/%s/", len(pending), len(dt_folders), CONTAINER, ADLS_PREFIX)

    for dt_folder in pending:
        local_dir = os.path.join(LOCAL_CLEANED, dt_folder)
        upload_partition(fs, local_dir, dt_folder)
        time.sleep(0.5)  # nghỉ nhẹ giữa mỗi file, giảm tải CPU/mạng

    logger.info("DONE — finished uploading %d partition(s).", len(pending))


if __name__ == "__main__":
    main()
