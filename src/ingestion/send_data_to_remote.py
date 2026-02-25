from databricks.sdk import WorkspaceClient
import os

def read_tmp_folder():
    return [
        f for f in os.listdir("/tmp")
        if f.endswith(".zip") or f.endswith(".csv")
    ]

def upload_files_to_dbfs():
    w = WorkspaceClient()
    tmp_files = read_tmp_folder()
    if not tmp_files:
        print("No files found in the tmp folder.")
        return
    print(f"Found {len(tmp_files)} file(s) to upload.")
    for file in tmp_files:
        print(f"Uploading {file} to DBFS...")
        local_path = f"/tmp/{file}"
        volume_path = f"/Volumes/rfb/staging/staging/{file}"
        try:
            with open(local_path, "rb") as f:
                w.files.upload(volume_path, f, overwrite=True)
            os.remove(local_path)
            print(f"{file} sent to {volume_path}")
        except Exception as e:
            print(f"Failed to send {file}: {e}")

upload_files_to_dbfs()