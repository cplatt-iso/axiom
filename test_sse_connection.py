#!/usr/bin/env python3
"""
Simple test to connect to SSE endpoint without authentication
"""
import asyncio
import aiohttp
import sys

async def test_sse_connection():
    url = "http://localhost:8001/api/v1/orders/events"
    
    try:
        async with aiohttp.ClientSession() as session:
            print(f"🔗 Connecting to SSE endpoint: {url}")
            async with session.get(url) as resp:
                print(f"📡 Response status: {resp.status}")
                print(f"📋 Response headers: {dict(resp.headers)}")
                
                if resp.status == 200:
                    print("✅ Connected! Listening for events...")
                    async for line in resp.content:
                        line_str = line.decode('utf-8').strip()
                        if line_str:
                            print(f"📨 Received: {line_str}")
                else:
                    text = await resp.text()
                    print(f"❌ Connection failed: {text}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_sse_connection())
