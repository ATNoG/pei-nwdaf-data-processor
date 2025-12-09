import json
import os
import asyncio
import logging
from utils.kmw import PyKafBridge
from src.time_window_manager import TimeWindowManager
from src.profiles.latency_profile import LatencyProfile
from src.empty_window_strategy import EmptyWindowStrategy, SkipStrategy, ZeroFillStrategy, ForwardFillStrategy
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC      = os.getenv("KAFKA_TOPIC", "network.data.ingested")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "network.data.processed")

# Time window setup, in seconds
WINDOW_DURATION = int(os.getenv("WINDOW_DURATION", "10"))
ALLOWED_LATENESS = int(os.getenv("ALLOWED_LATENESS", "10"))
EMPTY_WINDOW_STRATEGY_NAME = os.getenv("EMPTY_WINDOW_STRATEGY", "SKIP")

# Map strategy name to strategy instance
STRATEGY_MAP = {
    "SKIP": SkipStrategy(),
    "ZERO_FILL": ZeroFillStrategy(),
    "FORWARD_FILL": ForwardFillStrategy(),
}

EMPTY_WINDOW_STRATEGY = STRATEGY_MAP.get(EMPTY_WINDOW_STRATEGY_NAME.upper(), SkipStrategy())


window_manager:None|TimeWindowManager  = None
kafka_bridge  :PyKafBridge

async def watermark_task(manager: TimeWindowManager, interval: float = 1.0):
    """
    Periodically advances watermark and closes windows based on event time.
    """
    while True:
        now_ts = int(time.time())
        logger.info("Advancing watermark")

        # Always advance watermark to current time
        manager.advance_watermark(now_ts)

        await asyncio.sleep(interval)



def on_window_complete(data: dict):
    """Callback triggered when a window is completed for a cell"""

    if data is None:
        logger.error("Did not receive data")

    """
    cell_id = data["cell_index"]
    logger.info("=" * 80)
    logger.info(f"WINDOW COMPLETE for Cell {data["cell_index"]}")
    logger.info(f"  Time range: {data["window_start"]} -> {data["window_end"]}")

    logger.info(f"  Processed {data["sample_count"]} samples")
    logger.info(f"  Result keys: {list(data.keys())}")

    logger.info("=" * 80)

    logger.info(f"Processed data {cell_id}: {json.dumps(data, indent=4, default=str)}")
    """
    # Send to output Kafka topic
    if kafka_bridge:
        try:
            message = json.dumps(data, default=str)
            kafka_bridge.produce(OUTPUT_TOPIC, message)
            logger.info(f"Produced processed data to topic '{OUTPUT_TOPIC}'")
        except Exception as e:
            logger.error(f"Failed to produce processed data to topic '{OUTPUT_TOPIC}': {e}")


def process_message(msg: dict):
    """
    Callback function that processes each message as it arrives.
    This is bound to the Kafka topic and called automatically by PyKafBridge.
    """
    try:
        if window_manager is None:
            return
        # Process the CSV line
        csv_line = msg['content']
        offset = msg['offset']

        #print(f"[Offset {offset}] Processing: {csv_line[:80]}...")

        if isinstance(csv_line, str):
            data = json.loads(csv_line)
        else:
            data = csv_line

        # Add to time window manager
        window_manager.add_measurement(data)

        # Log
        cell_id = data.get('cell_index', 'unknown')
        timestamp = data.get('timestamp', 'unknown')
        logger.debug(f"[Offset {offset}] Cell {cell_id} at timestamp {timestamp}")

    except Exception as e:
        print(f"Error processing message at offset {msg['offset']}: {e}")

    return msg


async def main():
    global window_manager, kafka_bridge
    shutdown_event = asyncio.Event()

    try:
        # Initialize time window manager
        window_manager = TimeWindowManager(
            window_size=WINDOW_DURATION,
            on_window_complete=on_window_complete,
            processing_profiles=[LatencyProfile()],
            empty_window_strategy=EMPTY_WINDOW_STRATEGY,
            allowed_lateness_seconds=ALLOWED_LATENESS,
        )
        logger.info("Time window manager initialized with LatencyProfile")
        logger.info(f"  Window duration: {WINDOW_DURATION}s")
        logger.info(f"  Allowed lateness: {ALLOWED_LATENESS}s")
        logger.info(f"  Empty window strategy: {EMPTY_WINDOW_STRATEGY_NAME}")

        watermark_task_handle = asyncio.create_task(watermark_task(window_manager, interval=WINDOW_DURATION))
        logger.info("Watermark task started, advancing every 1s")

        kafka_bridge = PyKafBridge(TOPIC, hostname=KAFKA_HOST, port=KAFKA_PORT)

        # Bind the processing function to the topic before starting the consumer
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
        if watermark_task_handle:
            watermark_task_handle.cancel()
            await asyncio.gather(watermark_task_handle, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
