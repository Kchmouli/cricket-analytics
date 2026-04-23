import os
import zipfile
import io
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

CRICSHEET_BASE = "https://cricsheet.org/downloads/"
FULL_LOAD_URL = f"{CRICSHEET_BASE}all_male_json.zip"
INCREMENTAL_URL = f"{CRICSHEET_BASE}recently_added_2_male_json.zip"

LOCAL_RAW = Path("data/raw")


def get_match_type(data: dict) -> str:
    return data.get("info", {}).get("match_type", "unknown").lower()


def get_year(data: dict) -> str:
    dates = data.get("info", {}).get("dates", [])
    if dates:
        return str(dates[0])[:4]
    return "unknown"


def get_revision(data: dict) -> int:
    return data.get("meta", {}).get("revision", 1)


def download_and_extract(url: str, mode: str):
    print(f"Mode: {mode}")
    print(f"Downloading {url} ...")

    resp = requests.get(url, timeout=300)
    resp.raise_for_status()

    size_mb = len(resp.content) / 1024 / 1024
    print(f"  Downloaded {size_mb:.1f} MB")

    partition_counts = {}
    new_count = 0
    updated_count = 0
    skipped_count = 0
    errors = 0

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        json_files = [
            f for f in zf.namelist()
            if f.endswith(".json") and not f.startswith("__")
        ]
        print(f"  Found {len(json_files)} JSON files in ZIP")

        for name in json_files:
            try:
                raw = zf.read(name)
                data = json.loads(raw)

                match_type = get_match_type(data)
                year = get_year(data)
                new_revision = get_revision(data)

                dest_dir = LOCAL_RAW / f"match_type={match_type}" / f"year={year}"
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest_file = dest_dir / Path(name).name

                if dest_file.exists():
                    existing_revision = get_revision(json.loads(dest_file.read_bytes()))
                    if new_revision <= existing_revision:
                        skipped_count += 1
                        continue
                    print(f"  UPDATED {name}: rev {existing_revision} -> {new_revision}")
                    updated_count += 1
                else:
                    new_count += 1

                dest_file.write_bytes(raw)

                key = f"{match_type}/{year}"
                partition_counts[key] = partition_counts.get(key, 0) + 1

            except Exception as e:
                print(f"  ERROR processing {name}: {e}")
                errors += 1

    print(f"\nExtraction complete.")
    print(f"  New:     {new_count}")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count} (same or lower revision)")
    print(f"  Errors:  {errors}")
    if partition_counts:
        print(f"\nWritten by match_type/year:")
        for key in sorted(partition_counts):
            print(f"  {key}: {partition_counts[key]} files")

    return partition_counts


def main():
    import sys

    LOCAL_RAW.mkdir(parents=True, exist_ok=True)

    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    url = INCREMENTAL_URL if mode == "incremental" else FULL_LOAD_URL

    download_and_extract(url, mode)


if __name__ == "__main__":
    main()
