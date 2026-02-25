FROM astrocrpublic.azurecr.io/runtime:3.1-13
COPY requirements.txt .
COPY scrapy.cfg .

RUN pip install --no-cache-dir -r requirements.txt