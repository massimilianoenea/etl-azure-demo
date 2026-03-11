import azure.functions as func
import logging


def handle(myblob: func.InputStream):
    logging.info(f"Blob trigger - Name: {myblob.name}, Size: {myblob.length} bytes")
    content = myblob.read().decode("utf-8")
    logging.info(f"Blob content:\n{content}")
