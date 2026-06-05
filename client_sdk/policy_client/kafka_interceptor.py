"""
Kafka Middleware - Policy enforcement for Kafka messages using PyKafBridge.

This module provides policy enforcement on Kafka messages using the existing
PyKafBridge class from the Comms/kafka component.
"""
import json
import logging
import asyncio
from typing import Optional, Any, Callable, Iterator

from .client import PolicyClient

logger = logging.getLogger(__name__)


class PKBMiddleware:
    """
    Kafka middleware for policy enforcement on PyKafBridge messages.

    Wraps a PyKafBridge instance to apply policy enforcement on consumed messages.

    Messages that are denied by policy are silently skipped.
    Messages that pass policy are optionally transformed.

    This integrates with the existing PyKafBridge from PEI/main/Comms/kafka/src/kmw.py
    """

    def __init__(
        self,
        kafka_bridge,
        policy_client: PolicyClient,
        component_id: str,
        source_component: str,
        enable_policy: bool = False,
        transform_function: Optional[Callable] = None
    ):
        """
        Initialize the PKB Middleware for PyKafBridge.

        Args:
            kafka_bridge: PyKafBridge instance from Comms/kafka
            policy_client: Policy Client instance
            component_id: This component's ID (sink)
            source_component: Source component ID (message producer)
            enable_policy: Whether to enforce policy (non-blocking if False)
            transform_function: Optional function to transform consumed data
        """
        self.kafka_bridge = kafka_bridge
        self.policy_client = policy_client
        self.component_id = component_id
        self.source_component = source_component
        self.enable_policy = enable_policy
        self.transform_function = transform_function

        self._messagesDenied = 0
        self._messagesAllowed = 0
        self._processed_messages = []

        logger.info(
            f"PKBMiddleware initialized: "
            f"component={component_id}, source={source_component}, enabled={enable_policy}"
        )

    async def consume_with_policy(self) -> list[dict]:
        """
        Consume messages from PyKafBridge with policy enforcement applied.

        This method processes messages from the consumer_data buffer and applies
        policy checks. Messages that pass policy are returned, others are filtered.

        Returns:
            List of processed messages that passed policy checks
        """
        processed = []

        # Get all topics
        for topic in self.kafka_bridge._topics:
            topic_data = self.kafka_bridge.get_topic(topic)

            for msg in topic_data:
                # Skip already processed messages (track by offset)
                offset = msg.get('offset', -1)
                last_offset = self.kafka_bridge.last_consumed(topic)

                if offset <= last_offset:
                    continue  # Already processed

                # Get message content
                content = msg.get('content', '')
                if not content:
                    continue

                try:
                    # Parse JSON
                    if isinstance(content, str):
                        data = json.loads(content)
                    elif isinstance(content, dict):
                        data = content
                    else:
                        # Not JSON, pass through
                        if self.transform_function:
                            data = self.transform_function(msg)
                        processed.append(data)
                        self._messagesAllowed += 1
                        continue

                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Not JSON, pass through with optional transform
                    if self.transform_function:
                        data = self.transform_function(msg)
                    else:
                        data = msg
                    processed.append(data)
                    self._messagesAllowed += 1
                    continue

                # Apply policy if enabled
                if self.enable_policy:
                    result = await self.policy_client.process_data(
                        source_id=self.source_component,
                        sink_id=self.component_id,
                        data=data
                    )

                    if result.allowed:
                        self._messagesAllowed += 1
                        processed.append(result.data)
                    else:
                        self._messagesDenied += 1
                        logger.debug(f"Message denied by policy: {result.reason}")
                else:
                    # Policyless mode - pass through
                    self._messagesAllowed += 1
                    processed.append(data)

        return processed

    def get_stats(self) -> dict:
        """Get interceptor statistics."""
        return {
            "messages_allowed": self._messagesAllowed,
            "messages_denied": self._messagesDenied,
            "total_messages": self._messagesAllowed + self._messagesDenied
        }


def create_pykafbridge_policy_consumer(
    pykafbridge,
    policy_service_url: str,
    component_id: str,
    source_component: str,
    enable_policy: bool = False,
    transform_function: Optional[Callable] = None
) -> PKBMiddleware:
    """
    Convenience function to create a policy-enabled PyKafBridge consumer.

    Args:
        pykafbridge: Existing PyKafBridge instance
        policy_service_url: URL of Policy Service
        component_id: This component's ID
        source_component: Source component ID
        enable_policy: Whether to enable policy enforcement
        transform_function: Optional function to transform consumed data

    Returns:
        PKBMiddleware instance

    Example:
        from policy_client import PolicyClient
        from policy_client.kafka_interceptor import create_pykafbridge_policy_consumer

        # Your existing PyKafBridge
        kafka_bridge = PyKafBridge('network.data.ingested',
                                   hostname=KAFKA_HOST,
                                   port=KAFKA_PORT)

        # Start consumer
        await kafka_bridge.start_consumer()

        # Wrap with policy middleware
        policy_consumer = create_pykafbridge_policy_consumer(
            pykafbridge=kafka_bridge,
            policy_service_url="http://policy-service:8000",
            component_id="data-processor-60s",
            source_component="ingestion-service",
            enable_policy=True
        )

        # Consume with policy
        messages = await policy_consumer.consume_with_policy()
        for msg in messages:
            process(msg)  # Already filtered/transformed
    """
    # Create policy client
    policy_client = PolicyClient(
        service_url=policy_service_url,
        component_id=component_id,
        enable_policy=enable_policy
    )

    # Create and return middleware
    return PKBMiddleware(
        kafka_bridge=pykafbridge,
        policy_client=policy_client,
        component_id=component_id,
        source_component=source_component,
        enable_policy=enable_policy,
        transform_function=transform_function
    )


def bind_policy_to_topic(
    pykafbridge,
    topic: str,
    policy_service_url: str,
    component_id: str,
    source_component: str,
    enable_policy: bool = False
) -> None:
    """
    Bind policy enforcement to a specific topic on a PyKafBridge instance.

    This adds a policy-enforcing function to the topic's bind functions.

    Args:
        pykafbridge: Existing PyKafBridge instance
        topic: Topic name to bind policy to
        policy_service_url: URL of Policy Service
        component_id: This component's ID
        source_component: Source component ID
        enable_policy: Whether to enable policy enforcement

    Example:
        from policy_client.kafka_interceptor import bind_policy_to_topic

        kafka_bridge = PyKafBridge(hostname=KAFKA_HOST, port=KAFKA_PORT)
        await kafka_bridge.start_consumer()

        # Bind policy to topic
        bind_policy_to_topic(
            pykafbridge=kafka_bridge,
            topic="network.data.ingested",
            policy_service_url="http://policy-service:8000",
            component_id="data-processor-60s",
            source_component="ingestion-service",
            enable_policy=True
        )

        # Now consumed messages will have policy applied via bind
    """
    from policy_client import PolicyClient

    policy_client = PolicyClient(
        service_url=policy_service_url,
        component_id=component_id,
        enable_policy=enable_policy
    )

    async def policy_transform(msg: dict) -> dict:
        """Apply policy to consumed message."""
        content = msg.get('content', '')

        if not content:
            return msg

        try:
            data = json.loads(content) if isinstance(content, str) else content
        except (json.JSONDecodeError, TypeError):
            return msg

        if enable_policy:
            result = await policy_client.process_data(
                source_id=source_component,
                sink_id=component_id,
                data=data
            )

            if result.allowed:
                msg['content'] = json.dumps(result.data)
            else:
                msg['content'] = None  # Signal to skip
                logger.debug(f"Message denied by policy: {result.reason}")

        return msg

    # Bind the policy function to the topic
    pykafbridge.bind_topic(topic, policy_transform)
