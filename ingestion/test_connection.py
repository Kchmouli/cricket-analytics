import os
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container = os.getenv("AZURE_STORAGE_CONTAINER")

client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

fs = client.get_file_system_client(container)
paths = list(fs.get_paths("/"))

print(f"Connected to: {account_name}")
print(f"Container: {container}")
print("Folders found:")
for p in paths:
    print(f"  /{p.name}")