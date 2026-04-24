import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container = os.getenv("AZURE_STORAGE_CONTAINER")

LOCAL_SILVER = Path("data/silver")


def get_client():
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key,
    )


def load_silver():
    client = get_client()
    fs = client.get_file_system_client(container)

    all_files = list(LOCAL_SILVER.rglob("*.parquet"))
    total = len(all_files)
    print(f"Found {total} Parquet files to upload")

    # Collect unique Hive partition directories being written this run
    partitions = set()
    for local_path in all_files:
        relative = local_path.relative_to(LOCAL_SILVER)
        partitions.add(f"silver/{relative.parent.as_posix()}")

    # Delete stale Parquet files in each affected partition before uploading.
    # This prevents duplicate rows from accumulating across incremental runs
    # (each run writes new UUID-named files; without deletion, old files remain).
    print(f"Replacing {len(partitions)} partition(s) in ADLS ...")
    for partition_dir in sorted(partitions):
        try:
            deleted = 0
            for path_item in fs.get_paths(path=partition_dir):
                if not path_item.is_directory and path_item.name.endswith(".parquet"):
                    fs.get_file_client(path_item.name).delete_file()
                    deleted += 1
            if deleted:
                print(f"  Cleared {deleted} file(s) from {partition_dir}")
        except Exception:
            pass  # partition doesn't exist yet on first run

    uploaded = 0
    errors = 0

    for i, local_path in enumerate(all_files, 1):
        try:
            relative = local_path.relative_to(LOCAL_SILVER)
            remote_path = f"silver/{relative.as_posix()}"

            file_client = fs.get_file_client(remote_path)
            with open(local_path, "rb") as f:
                file_client.upload_data(f.read(), overwrite=True)
            uploaded += 1

            if i % 50 == 0:
                print(f"  Uploaded {i}/{total} ...")

        except Exception as e:
            print(f"  ERROR {local_path}: {e}")
            errors += 1

    print(f"\nSilver load complete.")
    print(f"  Uploaded: {uploaded}")
    print(f"  Errors:   {errors}")


if __name__ == "__main__":
    load_silver()
