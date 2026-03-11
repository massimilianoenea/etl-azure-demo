import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import os


def handle(event: func.EventGridEvent):
    logging.info(f"Event Grid trigger - Subject: {event.subject}, Event Type: {event.event_type}")
    logging.info(f"Event data:\n{event.get_json()}")

    data = event.get_json()
    blob_url = data.get("url", "")
    logging.info(f"Blob URL: {blob_url}")

    conn_str = os.environ.get("DATA_STORAGE_CONNECTION_STRING")
    if not conn_str or not blob_url:
        logging.warning("Missing connection string or blob URL, skipping blob read")
        return

    # URL format: https://<account>.blob.core.windows.net/<container>/<blob>
    parts = blob_url.split(".blob.core.windows.net/", 1)
    if len(parts) < 2:
        logging.warning(f"Cannot parse blob URL: {blob_url}")
        return

    path = parts[1]
    container_name = path.split("/", 1)[0]
    blob_name = path.split("/", 1)[1]

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    content = blob_client.download_blob().readall().decode("utf-8")
    logging.info(f"Blob content:\n{content}")
