from databricks.sdk import WorkspaceClient
import os
import zipfile
import logging

logger = logging.getLogger(__name__)

def read_tmp_folder():
    return [
        f for f in os.listdir("/tmp")
        if f.endswith(".zip") or f.endswith(".csv")
    ]

def extract_zip_files():
    tmp_files = read_tmp_folder()

    for file in tmp_files:
        if not file.endswith(".zip"):
            continue

        zip_path = f"/tmp/{file}"

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                file_type = zip_path.split("/")[-1].upper().replace(".ZIP", "")
                file_name = file_type + '.csv'
                zip_ref.extract(file, "/tmp")
                os.rename(f"/tmp/{file}", f"/tmp/{file_name}")
        os.remove(zip_path)

def upload_files_to_dbfs():
    w = WorkspaceClient()
    logger.info('Extracting zip files in /tmp...')
    extract_zip_files()
    logger.info("Extraction complete.")
    tmp_files = read_tmp_folder()
    if not tmp_files:
        logger.warning("No files found in the tmp folder.")
        return
    logger.info(f"Found {len(tmp_files)} file(s) to upload.")
    for file in tmp_files:
        if file.endswith(".csv"):
            logger.info(f"Uploading {file} to DBFS...")
            local_path = f"/tmp/{file}"

            if 'ESTABELECIMENTO' in file.upper():
                volume_path = f"/Volumes/rfb/transient/transient/estabelecimento/{file}"
            elif 'IBGE' in file.upper():
                volume_path = f"/Volumes/rfb/transient/transient/ibge/{file}"
            elif 'CNAES' in file.upper():
                volume_path = f"/Volumes/rfb/transient/transient/cnae/{file}"
            elif 'EMPRESAS' in file.upper():
                volume_path = f"/Volumes/rfb/transient/transient/empresa/{file}"
            else:
                volume_path = f"/Volumes/rfb/transient/transient/municipios/{file}"
            
            try:
                with open(local_path, "rb") as f:
                    w.files.upload(volume_path, f, overwrite=True)
                os.remove(local_path)
                logger.info(f"{file} sent to {volume_path}")
            except Exception as e:
                logger.warning(f"Failed to send {file}: {e}")
                
    os.remove('/tmp/rfb_files.json') # remove the dictionary file, preparing for the next run
if __name__ == '__main__':
    upload_files_to_dbfs()