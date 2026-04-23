import scrapy
import base64
from scrapy.http import Request
from lxml import etree
from datetime import datetime
import logging

class RfbSpider(scrapy.Spider):
    name = "rfb_spider"
    allowed_domains = ["arquivos.receitafederal.gov.br"]
    custom_settings = {"ROBOTSTXT_OBEY": False}
    data_list = []
    logger = logging.getLogger(name)

    token = "YggdBLfdninEJX9" # public RFB token, can be used for authentication in PROPFIND requests
    base_url = "https://arquivos.receitafederal.gov.br/public.php/webdav/"
    
    def start_requests(self):
        req = self.propfind(self.base_url, callback=self.parse_propfind)
        yield req

    def propfind(self, url, callback):
        auth_string = base64.b64encode(f"{self.token}:".encode()).decode()
        headers = {
            "Depth": "1",
            "Authorization": f"Basic {auth_string}"
        }

        return Request(
            url=url,
            method="PROPFIND",
            headers=headers,
            callback=callback
        )

    def parse_propfind(self, response):
        tree = etree.fromstring(response.body)
        ns = {"d": "DAV:"}

        current_month = datetime.now().strftime("%Y-%m")
        for resp in tree.findall("d:response", ns):

            href = resp.find("d:href", ns).text
            full_url = response.urljoin(href)

            if href.endswith("/webdav/"):
                continue

            if href.endswith("/"):
                yield self.propfind(full_url, callback=self.parse_propfind)
                continue

            targets = ["Estabelecimentos0", "Estabelecimentos1", "Estabelecimentos2", "Estabelecimentos3", "Estabelecimentos4",
                       "Estabelecimentos5", "Estabelecimentos6", "Estabelecimentos7", "Estabelecimentos8", "Estabelecimentos9",
                       "Empresas0", "Empresas1", "Empresas2", "Empresas3", "Empresas4",
                       "Empresas5", "Empresas6", "Empresas7", "Empresas8", "Empresas9", "Cnaes"]

            for target in targets:
                if current_month in href and target in href:
                    result = {
                        'package_download_url': full_url,
                        'file_name': href.split("/")[-1],
                        'auth_token': self.token
                    }
                    self.data_list.append(result)
                    break
    
    def closed(self, reason):
        import json
        print("Spider finish, generating JSON file.")
        with open('/tmp/rfb_files.json', 'w') as f:
            json.dump(self.data_list, f, indent=4)
        self.logger.info(f"Json file saved with {len(self.data_list)} entries")