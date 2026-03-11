import logging
import os

import azure.functions as func
from services.blob_downloader import download_blob
from services.blob_url_parser import parse_blob_url
from services.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def handle(event: func.EventGridEvent):
    logger.info(f"Event Grid trigger - Subject: {event.subject}, Event Type: {event.event_type}")

    # L'evento Event Grid contiene la URL del blob che ha scatenato il trigger
    data = event.get_json()
    blob_url = data.get("url", "")
    logger.info(f"Blob URL: {blob_url}")

    conn_str = os.environ.get("DATA_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ConfigurationError("DATA_STORAGE_CONNECTION_STRING is not configured")

    # Parsa la URL per estrarre container e nome blob, poi scarica il contenuto
    container_name, blob_name = parse_blob_url(blob_url)
    content = download_blob(conn_str, container_name, blob_name)
    logger.info(f"Blob content:\n{content}")
