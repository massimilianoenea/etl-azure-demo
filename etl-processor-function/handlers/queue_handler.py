import json
import logging
import os

import azure.functions as func
from opentelemetry import trace
from services.blob_downloader import download_blob
from services.blob_url_parser import parse_blob_url
from services.exceptions import ConfigurationError
from services.servicebus_sender import send_to_queue
from services.tracing import extract_context, inject_context

logger = logging.getLogger("etl-processor-function")
tracer = trace.get_tracer("etl-processor-function")


def handle(msg: str, fn_context: func.Context):
    # Ripristina il trace context dal runtime Azure Functions (propagato dal Service Bus)
    parent_ctx = extract_context(fn_context)
    logger.info(f"Trace context - traceparent: {fn_context.trace_context.trace_parent}")

    # Il messaggio arriva da Event Grid via Service Bus: contiene url e subject del blob
    event_data = json.loads(msg)
    logger.info(f"Queue trigger - BlobCreated event, message: {json.dumps(event_data)}")
    data = event_data.get("data", {})
    blob_url = data.get("url", "")
    subject = event_data.get("subject", "")
    logger.info(f"Queue trigger - BlobCreated event, subject: {subject}")

    conn_str = os.environ.get("DATA_STORAGE_CONNECTION_STRING")
    sb_conn_str = os.environ.get("SERVICE_BUS_CONNECTION_STRING")
    if not conn_str:
        raise ConfigurationError("DATA_STORAGE_CONNECTION_STRING is not configured")
    if not sb_conn_str:
        raise ConfigurationError("SERVICE_BUS_CONNECTION_STRING is not configured")

    container_name, blob_name = parse_blob_url(blob_url)

    # Step 1: Scarica il contenuto del blob dallo storage
    with tracer.start_as_current_span("download-blob", context=parent_ctx, attributes={
        "blob.container": container_name,
        "blob.name": blob_name,
    }) as span:
        content = download_blob(conn_str, container_name, blob_name)
        span.set_attribute("blob.size_bytes", len(content))

    # Step 2: Rimuovi le righe che contengono solo numeri
    with tracer.start_as_current_span("filter-numeric-lines") as span:
        original_lines = content.splitlines()
        filtered_lines = [line for line in original_lines if not line.strip().isdigit()]
        removed = len(original_lines) - len(filtered_lines)
        content = "\n".join(filtered_lines)
        span.set_attribute("lines.original", len(original_lines))
        span.set_attribute("lines.removed", removed)
        logger.info(f"Filtered out {removed} numeric-only lines")

    # Step 3: Inoltra il contenuto filtrato alla coda "etl-function-queue"
    # inject_context() propaga il traceparent nelle application_properties del messaggio
    with tracer.start_as_current_span("send-to-etl-function-queue", attributes={
        "servicebus.queue": "etl-function-queue",
        "blob.name": blob_name,
    }):
        carrier = inject_context()
        logger.info(f"Propagating trace context: {carrier}")

        send_to_queue(
            connection_string=sb_conn_str,
            queue_name="etl-function-queue",
            body={
                "content": content,
                "source_blob_name": blob_name,
                "source_container": container_name,
            },
            application_properties=carrier,
        )
        logger.info(f"Sent content of {blob_name} to etl-function-queue")
