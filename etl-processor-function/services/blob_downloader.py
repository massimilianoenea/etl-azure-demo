import logging

from azure.storage.blob import BlobServiceClient
from services.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def download_blob(connection_string: str, container_name: str, blob_name: str) -> str:
    """Download a blob and return its content as a string."""
    if not connection_string:
        raise ConfigurationError("DATA_STORAGE_CONNECTION_STRING is not configured")

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    content = blob_client.download_blob().readall().decode("utf-8")
    logger.info(f"Downloaded blob: {container_name}/{blob_name} ({len(content)} bytes)")
    return content
