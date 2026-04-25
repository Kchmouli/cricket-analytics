import os
import argparse
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


def purge_remote_silver(fs):
    """Delete the entire ADLS silver directory before a full-load upload."""
    print("Purging ADLS silver directory...")
    try:
        dir_client = fs.get_directory_client("silver")
        dir_client.delete_directory()
        print("  ADLS silver directory deleted")
    except Exception as e:
        print(f"  WARNING: could not delete ADLS silver directory: {e}")


def load_silver(full_mode: bool = False):
    client = get_client()
    fs = client.get_file_system_client(container)

    if full_mode:
        purge_remote_silver(fs)

    all_files = list(LOCAL_SILVER.rglob("*.parquet"))
    total = len(all_files)
    print(f"Found {total} Parquet files to upload")

    # Append-only upload: new UUID-named parquet files are uploaded without
    # deleting existing ones. Duplicate match_ids across runs are deduplicated
    # by QUALIFY in the dbt staging models (keeping highest revision).
    # Use --full to purge the remote directory before uploading (full load only).
    uploaded = 0
    errors = 0

    for i, local_path in enumerate(all_files, 1):
        try:
            relative = local_path.relative_to(LOCAL_SILVER)
            remote_path = f"silver/{relative.as_posix()}"

            file_client = fs.get_file_client(remote_path)
            file_size = local_path.stat().st_size
            with open(local_path, "rb") as f:
                file_client.upload_data(f, length=file_size, overwrite=True)
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full", action="store_true",
        help="Delete ADLS silver directory before uploading (use with full load only)",
    )
    args = parser.parse_args()
    load_silver(full_mode=args.full)
