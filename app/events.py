import asyncio
import json
from typing import List, Dict, Any, Coroutine
from fastapi import Request
import aio_pika
from aio_pika.abc import AbstractRobustConnection
from starlette.responses import StreamingResponse
import structlog

from app.core.config import settings

log = structlog.get_logger(__name__)

# RabbitMQ Exchange for broadcasting order events
ORDERS_EXCHANGE_NAME = "orders_events_exchange"

class SSEManager:
    """
    Manages Server-Sent Events connections.
    Allows broadcasting messages to all connected clients.
    """
    def __init__(self):
        self.connections: List[asyncio.Queue] = []
        log.info("SSEManager initialized.")

    async def add_connection(self, queue: asyncio.Queue):
        """Adds a new client connection queue."""
        self.connections.append(queue)
        log.info("SSE client connected.", total_connections=len(self.connections))

    def remove_connection(self, queue: asyncio.Queue):
        """Removes a client connection queue."""
        try:
            self.connections.remove(queue)
            log.info("SSE client disconnected.", total_connections=len(self.connections))
        except ValueError:
            log.warning("Attempted to remove a non-existent SSE connection.")

    async def broadcast(self, event: str, data: Dict[str, Any]):
        """Broadcasts an event and data to all connected clients."""
        message = f"event: {event}\ndata: {json.dumps(data)}\n\n"
        if not self.connections:
            log.debug("SSE broadcast skipped: no clients connected.", event_name=event)
            return

        log.info(f"Broadcasting SSE event '{event}' to {len(self.connections)} clients.")
        # Create a list of tasks to put the message into each client's queue
        tasks = [conn.put(message) for conn in self.connections]
        await asyncio.gather(*tasks, return_exceptions=True)

# Create a single global instance of the SSEManager
sse_manager = SSEManager()

async def sse_event_stream(request: Request):
    """
    Generator function for the SSE streaming response.
    """
    queue = asyncio.Queue()
    await sse_manager.add_connection(queue)
    try:
        # Send initial connection message
        yield "event: connected\ndata: {\"message\": \"SSE connection established\"}\n\n"
        
        # Create a heartbeat task
        async def heartbeat():
            while True:
                await asyncio.sleep(30)  # Send heartbeat every 30 seconds
                try:
                    await queue.put("event: heartbeat\ndata: {\"timestamp\": \"" + str(asyncio.get_event_loop().time()) + "\"}\n\n")
                except:
                    break
        
        heartbeat_task = asyncio.create_task(heartbeat())
        
        try:
            while True:
                # Wait for a message from the broadcast
                message = await queue.get()
                # Check if client is still connected before sending
                if await request.is_disconnected():
                    log.warning("SSE client disconnected before message could be sent.")
                    break
                yield message
        finally:
            heartbeat_task.cancel()
            
    except asyncio.CancelledError:
        log.info("SSE stream cancelled.")
    finally:
        sse_manager.remove_connection(queue)


async def rabbitmq_consumer(connection: AbstractRobustConnection):
    """
    Connects to RabbitMQ, declares a fanout exchange and a unique queue,
    and listens for messages to broadcast via SSE.
    """
    log.info("Starting RabbitMQ consumer for SSE...")
    try:
        async with connection.channel() as channel:
            # Declare a fanout exchange (broadcasts to all queues)
            exchange = await channel.declare_exchange(
                ORDERS_EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True
            )

            # Declare an exclusive, auto-deleting queue for this API instance
            queue = await channel.declare_queue(exclusive=True, auto_delete=True)

            # Bind the queue to the exchange
            await queue.bind(exchange)
            log.info(
                "RabbitMQ consumer ready. Exchange and queue declared and bound.",
                exchange=ORDERS_EXCHANGE_NAME,
                queue_name=queue.name,
            )

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        try:
                            data = json.loads(message.body.decode())
                            event_type = data.get("event_type", "message")
                            payload = data.get("payload", {})
                            log.info("Received message from RabbitMQ for SSE broadcast.", event_type=event_type)
                            await sse_manager.broadcast(event=event_type, data=payload)
                        except json.JSONDecodeError:
                            log.error("Failed to decode RabbitMQ message body.", body=message.body)
                        except Exception:
                            log.error("Error processing message in SSE consumer.", exc_info=True)
    except Exception:
        log.error("RabbitMQ consumer for SSE failed.", exc_info=True)
        # In a real app, you might want to add retry logic here.


async def publish_order_event(
    event_type: str,
    payload: Dict[str, Any],
    connection: AbstractRobustConnection
):
    """
    Publishes an order event to the RabbitMQ fanout exchange.
    """
    log.info("Attempting to publish order event to RabbitMQ.", event_type=event_type)
    try:
        async with connection.channel() as channel:
            # Declare the exchange (same as consumer) to ensure it exists
            exchange = await channel.declare_exchange(
                ORDERS_EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True
            )
            message_body = json.dumps({"event_type": event_type, "payload": payload})
            message = aio_pika.Message(
                body=message_body.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
            )
            await exchange.publish(message, routing_key="") # routing_key is ignored for fanout
            log.info("Successfully published order event.", event_type=event_type)
    except Exception:
        log.error("Failed to publish order event to RabbitMQ.", event_type=event_type, exc_info=True)


def publish_order_event_sync(
    event_type: str,
    payload: Dict[str, Any]
):
    """
    Synchronous wrapper for publishing order events.
    Creates its own RabbitMQ connection and publishes the event.
    Safe to call from synchronous contexts like DIMSE handlers.
    """
    async def _publish():
        log.info("Attempting to publish order event to RabbitMQ (sync).", event_type=event_type)
        try:
            connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
            async with connection:
                async with connection.channel() as channel:
                    # Declare the exchange (same as consumer) to ensure it exists
                    exchange = await channel.declare_exchange(
                        ORDERS_EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True
                    )
                    message_body = json.dumps({"event_type": event_type, "payload": payload})
                    message = aio_pika.Message(
                        body=message_body.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        content_type="application/json",
                    )
                    await exchange.publish(message, routing_key="") # routing_key is ignored for fanout
                    log.info("Successfully published order event (sync).", event_type=event_type)
        except Exception:
            log.error("Failed to publish order event to RabbitMQ (sync).", event_type=event_type, exc_info=True)
    
    try:
        # Get the current event loop if one exists
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, schedule the task
            asyncio.create_task(_publish())
        else:
            # If no loop is running, create a new one
            asyncio.run(_publish())
    except RuntimeError:
        # No event loop, create a new one
        asyncio.run(_publish())
