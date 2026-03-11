import azure.functions as func
from opentelemetry import context as otel_context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Usa il propagatore W3C Trace Context esplicitamente (non il generico da opentelemetry.propagate)
propagator = TraceContextTextMapPropagator()


def extract_context(fn_context: func.Context) -> otel_context.Context:
    """Estrae il trace context W3C dal runtime di Azure Functions.
    Il runtime lo riceve automaticamente dal messaggio Service Bus."""
    carrier = {"traceparent": fn_context.trace_context.trace_parent}
    if fn_context.trace_context.trace_state:
        carrier["tracestate"] = fn_context.trace_context.trace_state
    return propagator.extract(carrier)


def inject_context() -> dict:
    """Inietta il trace context corrente in un dizionario carrier.
    Il carrier va passato come application_properties del messaggio Service Bus in uscita."""
    carrier = {}
    propagator.inject(carrier, otel_context.get_current())
    return carrier
