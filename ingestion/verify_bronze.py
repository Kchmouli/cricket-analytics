import os
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from collections import defaultdict

load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container = os.getenv("AZURE_STORAGE_CONTAINER")

client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

fs = client.get_file_system_client(container)

print("Scanning bronze layer...\n")

counts = defaultdict(int)
total = 0

paths = fs.get_paths("bronze", recursive=True)
for path in paths:
    if not path.is_directory:
        total += 1
        parts = path.name.split("/")
        if len(parts) >= 3:
            match_type = parts[1].replace("match_type=", "")
            year = parts[2].replace("year=", "")
            counts[f"{match_type}/{year}"] += 1

print(f"Total files in bronze: {total}")
print(f"\nBreakdown by match_type/year:")
for key in sorted(counts):
    print(f"  {key}: {counts[key]} files")