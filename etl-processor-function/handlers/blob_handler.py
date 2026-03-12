import logging

import azure.functions as func

logger = logging.getLogger("etl-processor-function")


def handle(myblob: func.InputStream):
    logger.info(f"Blob trigger - Name: {myblob.name}, Size: {myblob.length} bytes")
    content = myblob.read().decode("utf-8")
    logger.info(f"Blob content:\n{content}")
