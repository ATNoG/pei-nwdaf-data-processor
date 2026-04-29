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

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service:8788")
POLICY_COMPONENT_ID = os.getenv("POLICY_COMPONENT_ID", "data-processor")
POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"

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
policy_enabled        : {POLICY_ENABLED}
----------------------------------------------
""")

kafka_producer: Producer | None = None
_window_queue: asyncio.Queue = asyncio.Queue()


def on_window_complete(data: dict):
    tags = data.get("tags", {})
    group = f"{tags.get('snssai_sst')}/{tags.get('dnn')}/{tags.get('event')}"
    logger.info(
        f"● Window {data['window_start']}-{data['window_end']} "
        f"group={group} samples={data.get('sample_count', 0)} "
        f"metrics={list(data.get('metrics', {}).keys())}"
    )
    _window_queue.put_nowait(data)


async def policy_produce_task(policy_client) -> None:
    loop = asyncio.get_running_loop()
    while True:
        data = await _window_queue.get()
        try:
            if policy_client is not None:
                result = await policy_client.process_data(
                    source_id=POLICY_COMPONENT_ID,
                    sink_id="kafka",
                    data={**data.get("tags", {}), **data.get("metrics", {})},
                    action="write",
                )
                if not result.allowed:
                    logger.warning(f"Window blocked by policy: {result.reason}")
                    continue
                tag_keys = data.get("tags", {}).keys()
                metric_keys = data.get("metrics", {}).keys()
                data = {
                    **data,
                    "tags": {k: result.data[k] for k in tag_keys if k in result.data},
                    "metrics": {k: result.data[k] for k in metric_keys if k in result.data},
                }

            if kafka_producer:
                await loop.run_in_executor(
                    None,
                    lambda d=data: (
                        kafka_producer.produce(OUTPUT_TOPIC, json.dumps(d, default=str).encode("utf-8")),
                        kafka_producer.poll(0),
                    ),
                )
            else:
                logger.warning("Kafka producer not initialized")
        except Exception as e:
            logger.error(f"Failed to produce window: {e}")
        finally:
            _window_queue.task_done()


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
    policy_client = None

    try:
        if POLICY_ENABLED:
            from policy_client import PolicyClient
            policy_client = PolicyClient(
                service_url=POLICY_SERVICE_URL,
                component_id=POLICY_COMPONENT_ID,
                heartbeat_interval=30,
                enable_policy=True,
            )
            try:
                await policy_client.register_component(
                    component_type="processor",
                    role=os.getenv("POLICY_ROLENAME", "Processor"),
                    auto_create_attributes=True,
                )
                await policy_client.start_heartbeat()
                logger.info("Registered with Policy Service")
            except Exception as e:
                logger.warning(f"Failed to register with Policy Service: {e}")
                policy_client = None

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
            asyncio.create_task(policy_produce_task(policy_client)),
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
        if policy_client is not None:
            try:
                await policy_client.stop_heartbeat()
            except Exception:
                pass
        if kafka_producer is not None:
            kafka_producer.flush()


if __name__ == "__main__":
    asyncio.run(main())
