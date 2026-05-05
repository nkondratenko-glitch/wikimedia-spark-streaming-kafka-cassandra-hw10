#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict

import requests
import sseclient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "input")
STREAM_URL = os.getenv(
    "WIKIMEDIA_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/page-create",
)


def wait_for_kafka(retries: int = 30, delay: int = 2) -> KafkaProducer:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(
                    value, ensure_ascii=False
                ).encode("utf-8"),
                key_serializer=lambda key: str(key).encode("utf-8")
                if key is not None
                else None,
                acks="all",
                retries=5,
            )
            print(f"Connected to Kafka at {BOOTSTRAP_SERVERS}", flush=True)
            return producer
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Kafka is unavailable ({attempt}/{retries}). Retrying...", flush=True)
            time.sleep(delay)

    raise RuntimeError("Could not connect to Kafka") from last_error


def event_key(event: Dict[str, Any]) -> str:
    meta = event.get("meta") or {}
    return str(meta.get("id") or event.get("id") or event.get("page_id") or "wikimedia")


def main() -> int:
    producer = wait_for_kafka()
    sent = 0

    while True:
        try:
            print(f"Connecting to Wikimedia stream: {STREAM_URL}", flush=True)
            response = requests.get(STREAM_URL, stream=True, timeout=60)
            response.raise_for_status()

            client = sseclient.SSEClient(response)

            for message in client.events():
                if not message.data or message.data == "[DONE]":
                    continue

                try:
                    event = json.loads(message.data)
                except json.JSONDecodeError:
                    continue

                producer.send(TOPIC, key=event_key(event), value=event)
                sent += 1

                if sent % 25 == 0:
                    producer.flush()
                    print(f"Sent {sent} events to Kafka topic {TOPIC}", flush=True)

        except Exception as exc:
            print(f"Stream error: {exc}. Reconnecting in 5 seconds...", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main())
