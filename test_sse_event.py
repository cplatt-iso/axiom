#!/usr/bin/env python3
"""
Simple test script to trigger an SSE event for testing
"""

import asyncio
import aio_pika
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings

async def send_test_event():
    """Send a test event to the SSE system"""
    try:
        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        
        async with connection.channel() as channel:
            # Declare the exchange
            exchange = await channel.declare_exchange(
                "orders_events_exchange", aio_pika.ExchangeType.FANOUT, durable=True
            )
            
            # Create test event
            test_payload = {
                "id": 999,
                "patient_name": "Test Patient",
                "accession_number": "TEST_ACC_001",
                "modality": "CT",
                "status": "SCHEDULED"
            }
            
            message_body = json.dumps({
                "event_type": "order_created", 
                "payload": test_payload
            })
            
            message = aio_pika.Message(
                body=message_body.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
            )
            
            await exchange.publish(message, routing_key="")
            print("✅ Test event sent successfully!")
            print(f"Event Type: order_created")
            print(f"Payload: {json.dumps(test_payload, indent=2)}")
            
        await connection.close()
        
    except Exception as e:
        print(f"❌ Error sending test event: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🧪 Sending test SSE event...")
    asyncio.run(send_test_event())
