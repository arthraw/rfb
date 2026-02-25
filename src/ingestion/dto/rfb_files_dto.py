from dataclasses import dataclass

@dataclass
class RfbFilesDto:
    package_download_url : str
    file_name: str
    auth_token : str