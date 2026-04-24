import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

GOLD_FILE = Path("data/gold.duckdb")
REMOTE_PATH = "gold/gold.duckdb"


def download_gold():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container = os.getenv("AZURE_STORAGE_CONTAINER")

    client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key,
    )
    fs = client.get_file_system_client(container)

    GOLD_FILE.parent.mkdir(parents=True, exist_ok=True)

    print("Downloading gold.duckdb from ADLS ...")
    file_client = fs.get_file_client(REMOTE_PATH)
    with open(GOLD_FILE, "wb") as f:
        file_client.download_file().readinto(f)

    size_mb = GOLD_FILE.stat().st_size / 1024 / 1024
    print(f"Done. {size_mb:.1f} MB saved to data/gold.duckdb")


if __name__ == "__main__":
    download_gold()
