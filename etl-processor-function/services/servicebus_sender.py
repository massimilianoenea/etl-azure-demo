import json
import logging

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from services.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def send_to_queue(
    connection_string: str,
    queue_name: str,
    body: dict,
    application_properties: dict | None = None,
) -> None:
    """Invia un messaggio JSON a una coda Service Bus.
    Le application_properties vengono usate per propagare il trace context."""
    if not connection_string:
        raise ConfigurationError("SERVICE_BUS_CONNECTION_STRING is not configured")

    sb_client = ServiceBusClient.from_connection_string(connection_string)
    with sb_client:
        sender = sb_client.get_queue_sender(queue_name)
        with sender:
            message = ServiceBusMessage(
                body=json.dumps(body),
                application_properties=application_properties,
            )
            sender.send_messages(message)
            logger.info(f"Sent message to queue '{queue_name}'")
