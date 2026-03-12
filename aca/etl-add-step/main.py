import json
import logging
import os

from azure.servicebus import ServiceBusClient
from azure.storage.blob import BlobServiceClient
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace, context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Solo i messaggi che iniziano con questo prefisso vengono processati
REQUIRED_PREFIX = "meetup"

# Configura l'export delle telemetrie verso Application Insights
configure_azure_monitor(logger_name="etl-processor-aca")
logger = logging.getLogger("etl-processor-aca")
tracer = trace.get_tracer("etl-processor-aca")
propagator = TraceContextTextMapPropagator()


def main():
    sb_conn_str = os.environ["SERVICE_BUS_CONNECTION_STRING"]
    storage_conn_str = os.environ["DATA_STORAGE_CONNECTION_STRING"]
    queue_name = os.environ.get("QUEUE_NAME", "etl-function-queue")

    with ServiceBusClient.from_connection_string(sb_conn_str) as sb_client:
        receiver = sb_client.get_queue_receiver(queue_name, max_wait_time=5)
        with receiver:
            messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)

            if not messages:
                logger.info("No messages in queue, exiting")
                return

            for msg in messages:
                # Ricostruisce il trace context dal messaggio per collegare le trace distribuite
                parent_ctx = extract_parent_context(msg.application_properties)
                try:
                    process_message(str(msg), parent_ctx, storage_conn_str)
                    receiver.complete_message(msg)
                    logger.info("Message completed")
                except ValueError as e:
                    # Messaggio non valido: manda in dead-letter invece di ritentare
                    logger.warning(f"Message aborted: {e}")
                    receiver.dead_letter_message(msg, reason="ValidationFailed", error_description=str(e))


# ── Tracing ──────────────────────────────────────────────────────────

def extract_parent_context(application_properties: dict) -> context.Context:
    """Estrae il W3C traceparent dalle application_properties del messaggio Service Bus.
    Questo collega lo span del Container App Job alla trace iniziata dalla Function."""
    carrier = {}
    if application_properties:
        for key in ("traceparent", "tracestate"):
            if key in application_properties:
                carrier[key] = application_properties[key]
    if carrier:
        logger.info(f"Extracted trace context: {carrier}")
        return propagator.extract(carrier)
    logger.info("No trace context found, using current context")
    return context.get_current()


# ── Validation ───────────────────────────────────────────────────────

def validate_content(content: str) -> None:
    """Rifiuta il messaggio se il contenuto non inizia con il prefisso richiesto."""
    if not content.lower().startswith(REQUIRED_PREFIX):
        raise ValueError(
            f"Message rejected: content must start with '{REQUIRED_PREFIX}', "
            f"got '{content[:30]}...'"
        )


# ── Transform ────────────────────────────────────────────────────────

def transform_content(content: str) -> str:
    """Trasforma il contenuto: uppercase + numero di riga all'inizio."""
    lines = content.splitlines()
    return "\n".join(f"{i}. {line.upper()}" for i, line in enumerate(lines, start=1))


# ── Upload ───────────────────────────────────────────────────────────

def upload_blob(connection_string: str, container: str, blob_name: str, data: str) -> None:
    """Salva il risultato della trasformazione come blob su Azure Storage."""
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    logger.info(f"Uploaded result to {container}/{blob_name}")


# ── Processing pipeline ─────────────────────────────────────────────

def process_message(msg_body: str, parent_ctx: context.Context, storage_conn_str: str) -> None:
    """Pipeline completa: valida -> trasforma -> salva su blob storage."""
    with tracer.start_as_current_span("process-etl-message", context=parent_ctx):
        body = json.loads(msg_body)
        content = body.get("content", "")
        source_blob_name = body.get("source_blob_name", "unknown")
        source_container = body.get("source_container", "unknown")
        logger.info(f"Processing blob: {source_blob_name} from {source_container}")

        # Step 1: Verifica che il contenuto inizi con "meetup"
        with tracer.start_as_current_span("validate-content"):
            validate_content(content)
            logger.info("Content validation passed")

        # Step 2: Uppercase + numeri di riga
        with tracer.start_as_current_span("transform-content", attributes={
            "blob.name": source_blob_name,
            "content.lines": len(content.splitlines()),
        }):
            transformed = transform_content(content)
            logger.info(f"Transformed {len(content.splitlines())} lines")

        # Step 3: Salva il risultato nel container "etl-output"
        output_blob_name = os.path.splitext(source_blob_name)[0] + "-processed.txt"

        with tracer.start_as_current_span("upload-result", attributes={
            "blob.container": "etl-output",
            "blob.name": output_blob_name,
            "blob.size_bytes": len(transformed.encode("utf-8")),
        }):
            upload_blob(storage_conn_str, "etl-output", output_blob_name, transformed)


if __name__ == "__main__":
    main()
