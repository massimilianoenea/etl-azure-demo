import azure.functions as func
from handlers import blob_handler, eventgrid_handler, queue_handler

app = func.FunctionApp()

# Blob trigger con source EventGrid: il file viene caricato in memoria automaticamente.
# Non funziona con il polling standard (LogsAndContainerScan) su Flex Consumption.
@app.function_name(name="BlobTriggerFunction")
@app.blob_trigger(arg_name="myblob", path="blob-trigger-container/{name}",
                  connection="DATA_STORAGE_CONNECTION_STRING",
                  source="EventGrid")
def blob_trigger_function(myblob: func.InputStream):
    blob_handler.handle(myblob)

# Event Grid trigger: riceve l'evento direttamente (senza caricare il blob in memoria).
@app.function_name(name="EventGridTriggerFunction")
@app.event_grid_trigger(arg_name="event")
def event_grid_trigger_function(event: func.EventGridEvent):
    eventgrid_handler.handle(event)

# Service Bus trigger: ascolta la coda "blob-queue" dove Event Grid manda le notifiche.
# Scarica il blob, filtra le righe numeriche, e inoltra il contenuto a "etl-function-queue".
# context fornisce il trace_context per il distributed tracing.
@app.function_name(name="QueueBlobProcessorFunction")
@app.service_bus_queue_trigger(arg_name="msg", queue_name="blob-queue",
                               connection="SERVICE_BUS_CONNECTION_STRING")
def queue_blob_processor_function(msg: func.ServiceBusMessage, context: func.Context):
    queue_handler.handle(msg.get_body().decode("utf-8"), context)
