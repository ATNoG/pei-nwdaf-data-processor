import asyncio
import json
import os
import logging
import time

from confluent_kafka import Consumer, Producer, KafkaError
from src.time_window_manager import TimeWindowManager
from src.empty_window_strategy import SkipStrategy, ZeroFillStrategy

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
INPUT_TOPIC = "network.data.ingested"
OUTPUT_TOPIC = "network.data.processed"

WINDOW_DURATION = int(os.getenv("WINDOW_DURATION", "60"))
SLIDE_INTERVAL = int(os.getenv("SLIDE_INTERVAL", str(WINDOW_DURATION)))
ALLOWED_LATENESS = int(os.getenv("ALLOWED_LATENESS", "10"))
EMPTY_WINDOW_STRATEGY_NAME = os.getenv("EMPTY_WINDOW_STRATEGY", "SKIP")
_group_ttl_env = os.getenv("GROUP_TTL_SECONDS") or None
GROUP_TTL: int | None = int(_group_ttl_env) if _group_ttl_env is not None else None

START_TIME = os.getenv("START_TIME", None)
current_time: int

if START_TIME is None:
    current_time = -1
else:
    try:
        current_time = int(START_TIME)
        assert current_time > 0
    except Exception:
        logger.error(f"Cannot convert {START_TIME} to a valid number")
        exit(1)

STRATEGY_MAP = {
    "SKIP": SkipStrategy(),
    "ZERO_FILL": ZeroFillStrategy(),
}
EMPTY_WINDOW_STRATEGY = STRATEGY_MAP.get(EMPTY_WINDOW_STRATEGY_NAME.upper(), SkipStrategy())

logger.info(f"""
----------------------------------------------
Data processor started:
allowed_lateness      : {ALLOWED_LATENESS}
window_duration       : {WINDOW_DURATION}
slide_interval        : {SLIDE_INTERVAL}
empty_window_strategy : {EMPTY_WINDOW_STRATEGY.__class__.__name__}
----------------------------------------------
""")

kafka_producer: Producer | None = None


def on_window_complete(data: dict):
    tags = data.get("tags", {})
    group = f"{tags.get('snssai_sst')}/{tags.get('dnn')}/{tags.get('event')}"
    logger.info(
        f"● Window {data['window_start']}-{data['window_end']} "
        f"group={group} samples={data.get('sample_count', 0)} "
        f"metrics={list(data.get('metrics', {}).keys())}"
    )

    if kafka_producer:
        try:
            kafka_producer.produce(OUTPUT_TOPIC, json.dumps(data, default=str).encode("utf-8"))
            kafka_producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to produce to '{OUTPUT_TOPIC}': {e}")
    else:
        logger.warning("Kafka producer not initialized")


async def watermark_task(window_manager: TimeWindowManager) -> None:
    global current_time

    if current_time != -1:
        check_time = time.time()
        while current_time + SLIDE_INTERVAL + ALLOWED_LATENESS <= check_time:
            current_time += SLIDE_INTERVAL
            await window_manager.advance_watermark(current_time)
    else:
        current_time = int(time.time())
        window_manager.set_initial_watermark(current_time)

    await asyncio.sleep(ALLOWED_LATENESS)

    while True:
        current_time += SLIDE_INTERVAL
        start = time.time()
        await window_manager.advance_watermark(current_time)
        end = time.time()
        await asyncio.sleep(max(0, SLIDE_INTERVAL - (end - start)))


async def consume_messages(window_manager: TimeWindowManager) -> None:
    loop = asyncio.get_running_loop()
    consumer = Consumer({
        "bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
        "group.id": "data-processor",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([INPUT_TOPIC])
    logger.info(f"Subscribed to '{INPUT_TOPIC}'")

    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                records = payload if isinstance(payload, list) else [payload]
                for record in records:
                    window_manager.ingest(record)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except asyncio.CancelledError:
        pass
    finally:
        consumer.close()


async def main():
    global kafka_producer
    tasks = []

    try:
        kafka_producer = Producer({"bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}"})

        window_manager = TimeWindowManager(
            window_size=WINDOW_DURATION,
            slide_interval=SLIDE_INTERVAL,
            on_window_complete=on_window_complete,
            empty_window_strategy=EMPTY_WINDOW_STRATEGY,
            group_ttl=GROUP_TTL,
        )

        tasks = [
            asyncio.create_task(consume_messages(window_manager)),
            asyncio.create_task(watermark_task(window_manager)),
        ]

        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for t in done:
            if t.exception():
                logger.error(f"Task failed: {t.exception()}")

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown signal received")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if kafka_producer is not None:
            kafka_producer.flush()


if __name__ == "__main__":
    asyncio.run(main())
