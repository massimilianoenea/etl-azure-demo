# etl-azure-demo

Esempi di codice a supporto dello speech su ETL in Azure.

Il progetto mostra una pipeline ETL event-driven costruita con:

- **Azure Function** (Python, Flex Consumption) per ingestione e pre-processing dei blob
- **Azure Container App Job** (Python) per validazione, trasformazione e scrittura del risultato
- **Azure Service Bus** come bus di messaggi fra i due componenti
- **Event Grid** per notifiche di blob creati
- **Application Insights** + **OpenTelemetry** per il distributed tracing end-to-end

## Architettura

```
        ┌────────────┐    BlobCreated    ┌───────────┐
Upload →│  Storage   │──────────────────►│ EventGrid │
        │ (container │                   └─────┬─────┘
        │ blob-...)  │                         │
        └────────────┘                         ▼
                                      ┌──────────────────┐
                                      │ Service Bus queue│
                                      │   blob-queue     │
                                      └────────┬─────────┘
                                               │
                                               ▼
                                ┌────────────────────────────────┐
                                │ Azure Function                 │
                                │ etl-processor-function         │
                                │  • download blob               │
                                │  • filtra righe numeriche      │
                                │  • inoltra il contenuto        │
                                └────────┬───────────────────────┘
                                         │
                                         ▼
                              ┌───────────────────────┐
                              │ Service Bus queue     │
                              │   etl-function-queue  │
                              └──────────┬────────────┘
                                         │
                                         ▼
                     ┌──────────────────────────────────────────┐
                     │ Azure Container App Job                  │
                     │ aca/etl-add-step                         │
                     │  • valida prefisso "meetup"              │
                     │  • uppercase + numerazione righe         │
                     │  • upload in container "etl-output"      │
                     └──────────────────────────────────────────┘
```

Il trace context W3C (`traceparent`) viene propagato attraverso i messaggi Service Bus
tramite `application_properties`, così le trace su Application Insights risultano
collegate da Function a Container App Job.

## Struttura del repository

```
etl-azure-demo/
├── etl-processor-function/      # Azure Function (Python v2 programming model)
│   ├── function_app.py           # Registrazione dei trigger
│   ├── handlers/                 # Logica dei 3 trigger
│   │   ├── blob_handler.py        # BlobTrigger (source=EventGrid)
│   │   ├── eventgrid_handler.py   # EventGridTrigger
│   │   └── queue_handler.py       # ServiceBusQueueTrigger su blob-queue
│   ├── services/                 # Client Storage, Service Bus, tracing
│   ├── host.json
│   ├── local.settings.json
│   └── requirements.txt
│
└── aca/
    └── etl-add-step/             # Container App Job
        ├── main.py                # Consumer di etl-function-queue
        ├── Dockerfile
        ├── build-and-push.sh      # Build + push immagine Docker Hub
        └── requirements.txt
```

## Componenti

### `etl-processor-function`

Azure Function in Python che espone tre trigger didattici:

| Function | Trigger | Sorgente |
|----------|---------|----------|
| `BlobTriggerFunction` | `@blob_trigger(source="EventGrid")` | container `blob-trigger-container` |
| `EventGridTriggerFunction` | `@event_grid_trigger` | sottoscrizione Event Grid |
| `QueueBlobProcessorFunction` | `@service_bus_queue_trigger` | coda `blob-queue` |

Il flusso produttivo è quello della coda: scarica il blob, rimuove le righe che
contengono solo cifre e inoltra il contenuto filtrato a `etl-function-queue`
includendo il trace context negli attributi del messaggio.

### `aca/etl-add-step`

Container App Job (esecuzione on-demand o schedulata) che:

1. Legge fino a 10 messaggi da `etl-function-queue`
2. Ricostruisce il parent span dal `traceparent` nelle `application_properties`
3. Valida che il contenuto inizi con il prefisso `meetup` — altrimenti
   manda il messaggio in dead-letter con `reason=ValidationFailed`
4. Applica la trasformazione: `"i. LINEA"` (uppercase + numero di riga)
5. Scrive il risultato in `etl-output/<nome>-processed.txt`

## Variabili d'ambiente

Entrambi i componenti richiedono:

| Variabile | Uso |
|-----------|-----|
| `DATA_STORAGE_CONNECTION_STRING` | Account Storage dei blob |
| `SERVICE_BUS_CONNECTION_STRING` | Namespace Service Bus |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | App Insights (auto-instrumentation OTel) |
| `QUEUE_NAME` *(solo ACA, opz.)* | default `etl-function-queue` |

Per la Function in locale popolare `etl-processor-function/local.settings.json`.

## Esecuzione in locale

### Function

```bash
cd etl-processor-function
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
func start
```

### Container App Job

```bash
cd aca/etl-add-step
pip install -r requirements.txt
export DATA_STORAGE_CONNECTION_STRING=...
export SERVICE_BUS_CONNECTION_STRING=...
export APPLICATIONINSIGHTS_CONNECTION_STRING=...
python main.py
```

## Observability

- `configure_azure_monitor(...)` viene invocato **prima** di importare gli SDK Azure
  per garantire che l'auto-instrumentation si agganci correttamente
- Ogni step della pipeline apre uno span dedicato (`download-blob`,
  `filter-numeric-lines`, `validate-content`, `transform-content`, `upload-result`)
- Il `traceparent` viaggia nelle `application_properties` del messaggio Service Bus,
  collegando la trace della Function a quella del Container App Job

## Risorse Azure attese

- 1 Storage Account con i container `blob-trigger-container` e `etl-output`
- 1 Service Bus namespace con le code `blob-queue` e `etl-function-queue`
- 1 Event Grid System Topic sullo Storage Account con subscription verso `blob-queue`
- 1 Azure Function (Flex Consumption)
- 1 Container App Environment + Container App Job
- 1 Application Insights condiviso
