import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

GOLD_FILE = Path("data/gold.duckdb")
REMOTE_PATH = "gold/gold.duckdb"


def upload_gold():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container = os.getenv("AZURE_STORAGE_CONTAINER")

    client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key,
    )
    fs = client.get_file_system_client(container)

    size_mb = GOLD_FILE.stat().st_size / 1024 / 1024
    print(f"Uploading gold.duckdb ({size_mb:.1f} MB) to ADLS ...")

    file_client = fs.get_file_client(REMOTE_PATH)
    with open(GOLD_FILE, "rb") as f:
        file_client.upload_data(f, length=GOLD_FILE.stat().st_size, overwrite=True)

    print("Done.")


if __name__ == "__main__":
    upload_gold()
