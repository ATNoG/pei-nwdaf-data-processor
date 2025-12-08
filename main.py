import json
import os
import asyncio
import logging
from utils.kmw import PyKafBridge
from src.time_window_manager import TimeWindowManager
from src.profiles.latency_profile import LatencyProfile
from src.profiles.processing_profile import EmptyWindowStrategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC      = os.getenv("KAFKA_TOPIC", "raw-data")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "processed-data")

# Time window setup, in seconds
WINDOW_DURATION = int(os.getenv("WINDOW_DURATION", "10")) # 10 seconds for testing purposes, change later
EMPTY_WINDOW_STRATEGY = EmptyWindowStrategy[os.getenv("EMPTY_WINDOW_STRATEGY", "SKIP")]

def on_window_complete(cell_id: str, processed_data: dict, window_start: int, window_end: int):
    """Callback triggered when a window is completed for a cell"""

    logger.info("=" * 80)
    logger.info(f"WINDOW COMPLETE for Cell {cell_id}")
    logger.info(f"  Time range: {window_start} -> {window_end}")

    if processed_data:
        # Override start_time and end_time with window boundaries
        processed_data['start_time'] = window_start
        processed_data['end_time'] = window_end
        processed_data['window_duration'] = WINDOW_DURATION

        is_empty = processed_data.get('is_empty_window', False)
        num_samples = processed_data.get('sample_count', 0)

        if is_empty:
            logger.info(f"  Empty window (handled with strategy: {EMPTY_WINDOW_STRATEGY.value})")
        else:
            logger.info(f"  Processed {num_samples} samples")
            logger.info(f"  Result keys: {list(processed_data.keys())}")

    logger.info("=" * 80)

    logger.info(f"Processed data for Cell {cell_id}: {json.dumps(processed_data, indent=4, default=str)}")

    # Send to output Kafka topic
    if kafka_bridge:
        try:
            message = json.dumps(processed_data, default=str)
            kafka_bridge.produce(OUTPUT_TOPIC, message)
            logger.info(f"Produced processed data to topic '{OUTPUT_TOPIC}'")
        except Exception as e:
            logger.error(f"Failed to produce processed data to topic '{OUTPUT_TOPIC}': {e}")


def process_message(msg: dict) -> dict:
    """
    Callback function that processes each message as it arrives.
    This is bound to the Kafka topic and called automatically by PyKafBridge.
    """
    try:
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
    window_manager = None
    kafka_bridge = None
    shutdown_event = asyncio.Event()

    try:
        # Initialize time window manager
        window_manager = TimeWindowManager(
            window_duration_seconds=WINDOW_DURATION,
            on_window_complete=on_window_complete,
            processing_profile=LatencyProfile,
            empty_window_strategy=EMPTY_WINDOW_STRATEGY
        )
        logger.info("Time window manager initialized with LatencyProfile")
        logger.info(f"  Window duration: {WINDOW_DURATION}s")
        logger.info(f"  Empty window strategy: {EMPTY_WINDOW_STRATEGY.value}")

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
        # Force close any remaining windows before shutdown
        if window_manager:
            print("Forcing close of all active windows...")
            completed_windows = window_manager.force_close_all_windows()
            print(f"Closed {len(completed_windows)} windows during shutdown")

        if kafka_bridge is not None:
            await kafka_bridge.close()
            print("Kafka bridge consumer closed")

if __name__ == "__main__":
    asyncio.run(main())
