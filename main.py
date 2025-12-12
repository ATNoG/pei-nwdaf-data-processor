import json
import os
import asyncio
import logging
from utils.kmw import PyKafBridge
from src.time_window_manager import TimeWindowManager
from src.profiles.latency_profile import LatencyProfile
from src.empty_window_strategy import SkipStrategy, ZeroFillStrategy, ForwardFillStrategy
import time

#logging.getLogger("asyncio").setLevel(logging.WARNING)
#logging.getLogger("urllib3").setLevel(logging.WARNING)
#logging.getLogger("requests").setLevel(logging.WARNING)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
OUTPUT_TOPIC = "network.data.processed"

# Time window setup, in seconds
WINDOW_DURATION = int(os.getenv("WINDOW_DURATION", "60"))
ALLOWED_LATENESS = int(os.getenv("ALLOWED_LATENESS", "10"))
EMPTY_WINDOW_STRATEGY_NAME = os.getenv("EMPTY_WINDOW_STRATEGY", "ZERO_FILL")

# Data storage api
DATA_STORAGE_API_URL= os.getenv("DATA_STORAGE_API_URL",None)

if DATA_STORAGE_API_URL is None:
    logger.error("No data storage api provided")
    exit(1)

current_time:int
START_TIME = os.getenv("START_TIME",None)

if START_TIME is None:
    current_time = -1
else:
    try:
        current_time = int(START_TIME)
        assert current_time > 0
    except Exception:
        logger.error(f"Cannot convert {START_TIME} to a valid number")
        exit(1)

class STORAGE:
    url = f"{DATA_STORAGE_API_URL}/api/v1/"
    class endpoint:
        cell = "cell/"
        raw = "raw/"


# Map strategy name to strategy instance
STRATEGY_MAP = {
    "SKIP": SkipStrategy(),
    "ZERO_FILL": ZeroFillStrategy(),
    "FORWARD_FILL": ForwardFillStrategy(),
}

EMPTY_WINDOW_STRATEGY = STRATEGY_MAP.get(EMPTY_WINDOW_STRATEGY_NAME.upper(), ZeroFillStrategy())

logger.info(f"""
----------------------------------------------
Data processord started:
allowed_lateness      : {ALLOWED_LATENESS}
window_duration       : {WINDOW_DURATION}
empty_window_strategy : {EMPTY_WINDOW_STRATEGY.__class__.__name__}
----------------------------------------------
""")

kafka_bridge  :PyKafBridge

def on_window_complete(data: dict):
    """Callback triggered when a window is completed for a cell"""
    # Send to output Kafka topic
    logger.info(f"Processed window {data['window_start']} - {data['window_end']} for cell {data.get('cell_id')}")

    if kafka_bridge:
        try:
            message = json.dumps(data, default=str)
            kafka_bridge.produce(OUTPUT_TOPIC, message)
            logger.info(f"Produced processed data to topic '{OUTPUT_TOPIC}'")
        except Exception as e:
            logger.error(f"Failed to produce processed data to topic '{OUTPUT_TOPIC}': {e}")

    else:
        logger.warning("Kafka bridge not initialized")

async def watermark_task(window_manager:TimeWindowManager) -> None:
    global current_time

    if current_time != -1:
        # form past windows
        check_time = time.time()
        while current_time + WINDOW_DURATION + ALLOWED_LATENESS <= check_time:
            current_time += WINDOW_DURATION
            await window_manager.advance_watermark(current_time)
    else:
        current_time = int(time.time())
        window_manager.set_initial_watermark(current_time)
    # sleep to match allowed lateness
    await asyncio.sleep(ALLOWED_LATENESS)

    while True:
        # make windows
        current_time += WINDOW_DURATION
        start = time.time()
        await window_manager.advance_watermark(current_time)
        end = time.time()
        await asyncio.sleep(max(0, WINDOW_DURATION - (end - start))) # already spent some time

async def main():
    global kafka_bridge
    shutdown_event = asyncio.Event()

    try:
        kafka_bridge = PyKafBridge("", hostname=KAFKA_HOST, port=KAFKA_PORT)

        # Initialize time window manager
        window_manager = TimeWindowManager(
            window_size = WINDOW_DURATION,
            on_window_complete=on_window_complete,
            processing_profiles=[LatencyProfile()],
            empty_window_strategy=EMPTY_WINDOW_STRATEGY,
            storage_struct = STORAGE
        )

        watermark_task_handle = asyncio.create_task(watermark_task(window_manager))

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
