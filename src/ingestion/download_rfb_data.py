import requests
import json
from src.ingestion.dto.rfb_files_dto import RfbFilesDto
from requests.auth import HTTPBasicAuth
import logging

logger = logging.getLogger(__name__)

def get_json_data(input_path: str) -> dict:
    with open(input_path) as r:
         return json.load(r)

def json_parser(input_file_path: str):
    data_list = []
    dict_data = get_json_data(input_path=input_file_path)
    try:
        for i in dict_data:
            rfb_data = RfbFilesDto(package_download_url=i['package_download_url'], file_name=i['file_name'], auth_token=i['auth_token'])
            data_list.append(rfb_data)
        return data_list
    
    except Exception as e:
        logger.warning(e)

def download_rfb_files():
    input_file_path = '/tmp/rfb_files.json'
    logger.info(f"Reading JSON data from {input_file_path}")
    rfb_data_list = json_parser(input_file_path)
    logger.info(f"Found {len(rfb_data_list)} RFB files to download")
    for data in rfb_data_list:
        logger.info(f"Downloading {data.file_name} from {data.package_download_url}")
        path = f'/tmp/{data.file_name}'
        basic_auth = HTTPBasicAuth(data.auth_token,"")
        res = requests.get(data.package_download_url, auth=basic_auth)
        if res.status_code == 200:
            with requests.get(data.package_download_url, auth=basic_auth, stream=True) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
        else:
            logger.warning(f"Error {res.status_code}")

if __name__ == "__main__":
    download_rfb_files()