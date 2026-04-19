import logging
import os

import azure.functions as func
from services.blob_uploader import upload_blob
from services.exceptions import ConfigurationError

logger = logging.getLogger("etl-processor-function")


def handle(myblob: func.InputStream):
    logger.info(f"Blob trigger - Name: {myblob.name}, Size: {myblob.length} bytes")
    content = myblob.read().decode("utf-8")
    logger.info(f"Blob content:\n{content}")

    conn_str = os.environ.get("DATA_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ConfigurationError("DATA_STORAGE_CONNECTION_STRING is not configured")

    basename = os.path.basename(myblob.name)
    output = (
        "-----------------------------\n"
        "QUESTO FILE E' STATO PROCESSATO DAL BLOB_TRIGGER\n"
        "-----------------------------\n"
        f"{content}"
    )
    upload_blob(conn_str, "etl-output", f"blob-trigger-{basename}", output)
