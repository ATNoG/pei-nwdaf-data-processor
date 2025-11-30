from contextlib import asynccontextmanager
import json
import os
import asyncio
from utils.kmw import PyKafBridge

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC      = os.getenv("KAFKA_TOPIC", "raw-data")


async def main():
    kafka_bridge = None
    try:
        kafka_bridge = PyKafBridge(TOPIC, hostname=KAFKA_HOST, port=KAFKA_PORT)
        kafka_bridge.add_topic(TOPIC)

        await kafka_bridge.start_consumer()
        
        if not getattr(kafka_bridge, 'consumer', None):
            print("Failed to start consumer: topic may not exist")
            return
            
        print(f"Kafka bridge consumer started, listening to topic: {TOPIC}")
        print("Press Ctrl+C to stop...")

        try:
            while True:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                messages = kafka_bridge.get_topic(TOPIC)
                if messages:
                    print(f"\nConsumed {len(messages)} messages so far:")
                    for msg in messages[-5:]:  # Show last 5 messages (for testing purposes)
                        print(f"  Offset {msg['offset']}: {msg['content'][:100]}")
                        
        except KeyboardInterrupt:
            print("\nShutdown signal received...")
            
    except Exception as e:
        print(f"Failed to start Kafka bridge consumer: {e}")
    finally:
        if kafka_bridge is not None:
            await kafka_bridge.close()
            print("Kafka bridge consumer closed")

if __name__ == "__main__":
    asyncio.run(main())