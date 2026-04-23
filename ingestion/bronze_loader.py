import os
import json
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError

load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container = os.getenv("AZURE_STORAGE_CONTAINER")

LOCAL_RAW = Path("data/raw")


def get_client():
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )


def get_local_revision(local_path: Path) -> int:
    data = json.loads(local_path.read_bytes())
    return data.get("meta", {}).get("revision", 1)


def get_remote_revision(file_client) -> int:
    """Returns stored revision from ADLS metadata; -1 if file does not exist."""
    try:
        props = file_client.get_file_properties()
        metadata = props.metadata or {}
        return int(metadata.get("revision", 0))
    except ResourceNotFoundError:
        return -1


def upload_file(file_client, local_path: Path, revision: int):
    with open(local_path, "rb") as f:
        data = f.read()
    file_client.upload_data(data, overwrite=True)
    file_client.set_metadata({"revision": str(revision)})


def load_bronze():
    client = get_client()
    fs = client.get_file_system_client(container)

    all_files = list(LOCAL_RAW.rglob("*.json"))
    total_files = len(all_files)
    print(f"Found {total_files} local JSON files to process")

    new_count = 0
    updated_count = 0
    skipped_count = 0
    errors = 0

    for i, local_path in enumerate(all_files, 1):
        try:
            relative = local_path.relative_to(LOCAL_RAW)
            remote_path = f"bronze/{relative.as_posix()}"

            local_revision = get_local_revision(local_path)
            file_client = fs.get_file_client(remote_path)
            remote_revision = get_remote_revision(file_client)

            if remote_revision == -1:
                upload_file(file_client, local_path, local_revision)
                new_count += 1
            elif local_revision > remote_revision:
                upload_file(file_client, local_path, local_revision)
                updated_count += 1
            else:
                skipped_count += 1

            if i % 500 == 0:
                print(
                    f"  Processed {i}/{total_files} ... "
                    f"new={new_count}, updated={updated_count}, skipped={skipped_count}"
                )

        except Exception as e:
            print(f"  ERROR {local_path.name}: {e}")
            errors += 1

    print(f"\nBronze load complete.")
    print(f"  New:     {new_count}")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count} (same or lower revision)")
    print(f"  Errors:  {errors}")


if __name__ == "__main__":
    load_bronze()
