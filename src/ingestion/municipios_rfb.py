import pandas as pd
import requests

def download_municipios():
    # Download data from https://www.gov.br/receitafederal/dados/municipios.csv
    url = "https://www.gov.br/receitafederal/dados/municipios.csv"
    response = requests.get(url)
    with open("/tmp/municipios_rfb.csv", "wb") as f:
        f.write(response.content)

if __name__ == "__main__":
    download_municipios()