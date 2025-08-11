#!/bin/bash
# SSE Test Script

echo "🧪 Starting comprehensive SSE test..."

# Start SSE connection in background
echo "📡 Starting SSE connection..."
(
    timeout 60s curl -N -s http://localhost:8001/api/v1/orders/events | while IFS= read -r line; do
        echo "📨 SSE: $line"
    done
) &

SSE_PID=$!
echo "📋 SSE connection started with PID: $SSE_PID"

# Wait a bit for connection to establish
sleep 2

# Send test events
echo "🚀 Sending test event 1..."
docker compose exec -T api python -c "
import asyncio, json, aio_pika
from aio_pika import ExchangeType

async def send():
    connection = await aio_pika.connect_robust('amqp://guest:guest@rabbitmq:5672/')
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange('orders_events_exchange', ExchangeType.FANOUT, durable=True)
        event = {'event_type': 'order_created', 'data': {'id': 100, 'patient_first_name': 'Test', 'patient_last_name': 'Patient1'}}
        message = aio_pika.Message(json.dumps(event).encode(), content_type='application/json')
        await exchange.publish(message, routing_key='')
        print('✅ Event 1 sent!')

asyncio.run(send())
"

sleep 3

echo "🚀 Sending test event 2..."
docker compose exec -T api python -c "
import asyncio, json, aio_pika
from aio_pika import ExchangeType

async def send():
    connection = await aio_pika.connect_robust('amqp://guest:guest@rabbitmq:5672/')
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange('orders_events_exchange', ExchangeType.FANOUT, durable=True)
        event = {'event_type': 'order_updated', 'data': {'id': 101, 'patient_first_name': 'Updated', 'patient_last_name': 'Patient2'}}
        message = aio_pika.Message(json.dumps(event).encode(), content_type='application/json')
        await exchange.publish(message, routing_key='')
        print('✅ Event 2 sent!')

asyncio.run(send())
"

sleep 3

echo "🛑 Stopping SSE connection..."
kill $SSE_PID 2>/dev/null

echo "✅ SSE test completed!"
