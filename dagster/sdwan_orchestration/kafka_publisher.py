"""
Thin wrapper around confluent_kafka.Producer.
Publishes enriched network events to the configured Kafka topic.
The serial_number is used as the message key so that all events for
a given device land on the same partition (useful for ordered processing
in Task 2's Flink job).
"""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer

from sdwan_orchestration.settings import SETTINGS


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": SETTINGS.kafka_bootstrap,
        "acks": "all",
        "retries": 3,
    })


def publish_events(events: list[dict[str, Any]], logger=None) -> int:
    """
    Publish a batch of enriched event dicts to network-events.
    Returns the number of messages delivered.
    Raises RuntimeError if delivery fails for any message.
    """
    producer = _make_producer()
    topic = SETTINGS.network_events_topic
    errors: list[str] = []

    def on_delivery(err, msg):
        if err:
            errors.append(f"Delivery failed for {msg.key()}: {err}")

    for event in events:
        key = event["serial_number"].encode("utf-8")
        value = json.dumps(event).encode("utf-8")
        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)

        # Throttle local queue to avoid unbounded memory for large batches
        if len(producer) > 5000:
            producer.poll(0)

    producer.flush(timeout=60)

    if errors:
        raise RuntimeError(f"{len(errors)} Kafka delivery failures:\n" + "\n".join(errors[:10]))

    if logger:
        logger.info(f"Published {len(events)} events to {topic}")

    return len(events)
