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

# Stored at bronze/_revision_index.json — maps relative path → revision int.
# One ADLS read at startup instead of one get_file_properties() call per file.
REVISION_INDEX_PATH = "bronze/_revision_index.json"


def get_client():
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )


def get_local_revision(local_path: Path) -> int:
    data = json.loads(local_path.read_bytes())
    return data.get("meta", {}).get("revision", 1)


def load_revision_index(fs) -> dict:
    """Download the revision index from ADLS in one API call. Returns {} if absent."""
    try:
        file_client = fs.get_file_client(REVISION_INDEX_PATH)
        data = file_client.download_file().readall()
        return json.loads(data)
    except (ResourceNotFoundError, Exception):
        return {}


def save_revision_index(fs, index: dict):
    """Write the updated revision index back to ADLS in one API call."""
    file_client = fs.get_file_client(REVISION_INDEX_PATH)
    payload = json.dumps(index, separators=(",", ":")).encode()
    file_client.upload_data(payload, overwrite=True)


def upload_file(fs, remote_path: str, local_path: Path, revision: int):
    file_client = fs.get_file_client(remote_path)
    file_size = local_path.stat().st_size
    with open(local_path, "rb") as f:
        file_client.upload_data(f, length=file_size, overwrite=True)
    file_client.set_metadata({"revision": str(revision)})


def load_bronze():
    client = get_client()
    fs = client.get_file_system_client(container)

    # Load all known remote revisions in one API call rather than one call per file.
    print("Loading revision index from ADLS...")
    remote_index = load_revision_index(fs)
    print(f"  Index contains {len(remote_index)} known files")

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
            index_key = relative.as_posix()          # e.g. "match_type/year/id.json"
            remote_path = f"bronze/{index_key}"

            local_revision = get_local_revision(local_path)
            remote_revision = remote_index.get(index_key, -1)

            if remote_revision == -1:
                upload_file(fs, remote_path, local_path, local_revision)
                remote_index[index_key] = local_revision
                new_count += 1
            elif local_revision > remote_revision:
                upload_file(fs, remote_path, local_path, local_revision)
                remote_index[index_key] = local_revision
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

    print("\nSaving updated revision index to ADLS...")
    save_revision_index(fs, remote_index)

    print(f"\nBronze load complete.")
    print(f"  New:     {new_count}")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count} (same or lower revision)")
    print(f"  Errors:  {errors}")


if __name__ == "__main__":
    load_bronze()
