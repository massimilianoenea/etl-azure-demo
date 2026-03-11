import logging
import azure.functions as func
import blob_handler
import eventgrid_handler

app = func.FunctionApp()

#With standard source LogsAndContainerScan which uses the standard polling mechanism to detect changes in the container. Not works with Consumption Plan
#File automatically loaded in memory
@app.function_name(name="BlobTriggerFunction")
@app.blob_trigger(arg_name="myblob", path="blob-trigger-container/{name}",
                  connection="DATA_STORAGE_CONNECTION_STRING",
                  source="EventGrid")
def blob_trigger_function(myblob: func.InputStream):
    blob_handler.handle(myblob)

@app.function_name(name="EventGridTriggerFunction")
@app.event_grid_trigger(arg_name="event")
def event_grid_trigger_function(event: func.EventGridEvent):
    eventgrid_handler.handle(event)
