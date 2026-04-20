import json
import logging
import os

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace, context
from opentelemetry.propagate import extract
from azure.servicebus import ServiceBusClient
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger("etl-processor-aca")
tracer = trace.get_tracer("etl-processor-aca")

# configure_azure_monitor DEVE essere chiamato PRIMA di importare le librerie Azure SDK,
# altrimenti l'auto-instrumentation non si aggancia
configure_azure_monitor(logger_name="etl-processor-aca")

# Solo i messaggi che iniziano con questo prefisso vengono processati
REQUIRED_PREFIX = "meetup"

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

                    # Alternativa: receiver.abandon_message(msg)
                    # - Rilascia il lock del messaggio, che torna immediatamente disponibile in coda.
                    # - Il delivery count viene incrementato: dopo MaxDeliveryCount tentativi
                    #   falliti, Service Bus sposta automaticamente il messaggio in DLQ.
                    # - Utile per errori TRANSIENTI (rete, storage temporaneamente down),
                    #   non per errori di validazione come questo, dove il retry non cambierà l'esito.
                    # receiver.abandon_message(msg)

                    receiver.dead_letter_message(msg, reason="ValidationFailed", error_description=str(e))

                    # Per far risultare il Container App Job in stato "Failed" invece di "Completed",
                    # occorre terminare il processo con exit code != 0. Decommentare la riga sotto
                    # per simulare un'uscita con errore dopo aver mandato il messaggio in DLQ.
                    # ⚠️ Attenzione: così il job fallisce anche se altri messaggi del batch
                    # sono stati completati correttamente, e ACA potrebbe ri-schedulare il job.
                    # raise e


# ── Tracing ──────────────────────────────────────────────────────────

def extract_parent_context(application_properties: dict) -> context.Context:
    """Estrae il W3C traceparent dalle application_properties del messaggio Service Bus.
    Questo collega lo span del Container App Job alla trace iniziata dalla Function."""
    try:
        # Filtra solo quelli con chiave che inizia per b'tra'
        filtered = {k.decode(): v.decode() for k, v in application_properties.items() if k.startswith(b'tra')}
        # Extract message body
        carrier = {
            "traceparent": filtered.get("traceparent", ""),
            "tracestate": filtered.get("tracestate", ""),
        }
        return extract(carrier)
    except Exception as e:
        logger.error(f"Error extracting trace context: {e}")
        raise e


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
    with tracer.start_as_current_span("process-etl-message", context=parent_ctx) as parentspan:
        parentspan.set_attribute("msg_body", msg_body)
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
        with tracer.start_as_current_span("transform-content") as span:
            span.set_attribute("blob.name", source_blob_name)
            span.set_attribute("content.lines", len(content.splitlines()))
            transformed = transform_content(content)
            logger.info(f"Transformed {len(content.splitlines())} lines")

        # Step 3: Salva il risultato nel container "etl-output"
        output_blob_name = os.path.splitext(source_blob_name)[0] + "-processed.txt"

        with tracer.start_as_current_span("upload-result") as span:
            span.set_attribute("blob.container", "etl-output")
            span.set_attribute("blob.name", output_blob_name)
            span.set_attribute("blob.size_bytes", len(transformed.encode("utf-8")))
            upload_blob(storage_conn_str, "etl-output", output_blob_name, transformed)


if __name__ == "__main__":
    main()
