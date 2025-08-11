#!/usr/bin/env python3
"""
Test script that runs inside the Docker container to publish an SSE event
"""
import asyncio
import json
import aio_pika
from aio_pika import ExchangeType

async def send_test_event():
    try:
        # Connect to RabbitMQ using internal Docker network
        connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq:5672/")
        
        async with connection:
            # Create channel
            channel = await connection.channel()
            
            # Declare the exchange (should match what the consumer expects)
            exchange = await channel.declare_exchange(
                "orders_events_exchange", 
                ExchangeType.FANOUT,
                durable=True
            )
            
            # Create test event data
            test_event = {
                "event_type": "order_created",
                "data": {
                    "id": 999,
                    "patient_first_name": "Test",
                    "patient_last_name": "Patient",
                    "patient_mrn": "TEST999",
                    "accession_number": "TEST-ACC-999",
                    "modality": "CT",
                    "priority": "routine",
                    "examination_description": "Test SSE Event"
                }
            }
            
            # Publish the event
            message = aio_pika.Message(
                json.dumps(test_event).encode(),
                content_type="application/json"
            )
            
            await exchange.publish(message, routing_key="")
            print("✅ Test SSE event published to RabbitMQ!")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(send_test_event())
