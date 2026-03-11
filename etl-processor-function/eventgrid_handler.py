import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def handle(event: func.EventGridEvent):
    logger.info(f"Event Grid trigger - Subject: {event.subject}, Event Type: {event.event_type}")
    logger.info(f"Event data:\n{event.get_json()}")

    data = event.get_json()
    blob_url = data.get("url", "")
    logger.info(f"Blob URL: {blob_url}")

    conn_str = os.environ.get("DATA_STORAGE_CONNECTION_STRING")
    if not conn_str or not blob_url:
        logger.warning("Missing connection string or blob URL, skipping blob read")
        return

    # URL format: https://<account>.blob.core.windows.net/<container>/<blob>
    parts = blob_url.split(".blob.core.windows.net/", 1)
    if len(parts) < 2:
        logger.warning(f"Cannot parse blob URL: {blob_url}")
        return

    path = parts[1]
    container_name = path.split("/", 1)[0]
    blob_name = path.split("/", 1)[1]

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    content = blob_client.download_blob().readall().decode("utf-8")
    logger.info(f"Blob content:\n{content}")
