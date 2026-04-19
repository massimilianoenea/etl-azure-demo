import logging

from azure.storage.blob import BlobServiceClient
from services.exceptions import ConfigurationError

logger = logging.getLogger("etl-processor-function")


def upload_blob(connection_string: str, container_name: str, blob_name: str, data: str) -> None:
    """Upload a string as a blob to Azure Storage (overwrite)."""
    if not connection_string:
        raise ConfigurationError("DATA_STORAGE_CONNECTION_STRING is not configured")

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    logger.info(f"Uploaded blob: {container_name}/{blob_name} ({len(data)} bytes)")
