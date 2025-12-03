from contextlib import asynccontextmanager
import json
import os
import asyncio
from utils.kmw import PyKafBridge

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC      = os.getenv("KAFKA_TOPIC", "raw-data")

def process_message(msg: dict) -> dict:
    """
    Callback function that processes each message as it arrives.
    This is bound to the Kafka topic and called automatically by PyKafBridge.
    """
    try:
        # Process the CSV line 
        csv_line = msg['content']
        offset = msg['offset']
        
        print(f"[Offset {offset}] Processing: {csv_line[:80]}...")
        
        # Add data processing logic  
        
    except Exception as e:
        print(f"Error processing message at offset {msg['offset']}: {e}")
    
    return msg


async def main():
    kafka_bridge = None
    shutdown_event = asyncio.Event()

    try:
        kafka_bridge = PyKafBridge(TOPIC, hostname=KAFKA_HOST, port=KAFKA_PORT)
        
        # Bind the processing function to the topic BEFORE starting the consumer
        kafka_bridge.bind_topic(TOPIC, process_message)
        
        # Optional: consume from beginning instead of only new messages
        # kafka_bridge._consumer_config['auto.offset.reset'] = 'earliest'

        await kafka_bridge.start_consumer()
        
        if not getattr(kafka_bridge, 'consumer', None):
            print("Failed to start consumer: topic may not exist")
            return
            
        print(f"Kafka bridge consumer started, listening to topic: {TOPIC}")
        print("Press Ctrl+C to stop...")

        await shutdown_event.wait()
    
    except KeyboardInterrupt:
        print("Shutdown signal received...")
        shutdown_event.set() 

    except Exception as e:
        print(f"Failed to start Kafka bridge consumer: {e}")
    finally:
        if kafka_bridge is not None:
            await kafka_bridge.close()
            print("Kafka bridge consumer closed")

if __name__ == "__main__":
    asyncio.run(main())