FROM astrocrpublic.azurecr.io/runtime:3.1-13
COPY requirements.txt .
COPY scrapy.cfg .

USER root
RUN pip install --no-cache-dir -r requirements.txt && \
    pip uninstall -y openlineage-integration-common openlineage-python
USER astro