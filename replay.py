"""Replay read-only MongoDB SPY bars to Kafka with acknowledged checkpoints."""
from __future__ import annotations

import fcntl
import json
import logging
import math
import os
import signal
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from pymongo import MongoClient

SYMBOL = "SPY"
DATABASE = "unified_trader"
COLLECTION = "bars_1m"
TIMEOUT_SECONDS = 30
POLL_SECONDS = 0.1
LOGGER = logging.getLogger(__name__)


def parse_timestamp(value: str) -> datetime:
    """Interpret dates and naive ISO timestamps as UTC."""
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc)


@dataclass(frozen=True)
class Settings:
    """Validated environment settings."""

    mongo_uri: str
    brokers: str
    topic: str
    interval: float
    batch_size: int
    start: datetime | None
    state: Path

    @classmethod
    def from_environment(cls) -> Settings:
        """Load local defaults without overriding exported environment variables."""
        load_dotenv()
        interval = float(os.getenv("REPLAY_INTERVAL_SECONDS", "5"))
        batch_size = int(os.getenv("REPLAY_BATCH_SIZE", "1"))
        if not math.isfinite(interval) or interval <= 0 or batch_size <= 0:
            raise ValueError("Replay interval and batch size must be positive")
        start = os.getenv("REPLAY_START_DATE", "")
        return cls(os.getenv("MONGO_URI", "mongodb://localhost:27017"),
                   os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                   os.getenv("KAFKA_TOPIC", "udp.spy_bars_1m"), interval,
                   batch_size, parse_timestamp(start) if start else None,
                   Path(os.getenv("REPLAY_STATE_FILE", ".state/spy-bars.json")))


def ensure_topic(settings: Settings) -> None:
    """Create a single-partition local topic, tolerating an existing topic."""
    admin = AdminClient({"bootstrap.servers": settings.brokers})
    future = admin.create_topics([NewTopic(settings.topic, num_partitions=1, replication_factor=1)],
                                 request_timeout=TIMEOUT_SECONDS)[settings.topic]
    try:
        future.result(timeout=TIMEOUT_SECONDS)
    except KafkaException as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def read_checkpoint(settings: Settings) -> datetime | None:
    """Fail on corrupt or mismatched checkpoints instead of silently replaying."""
    if not settings.state.exists():
        return None
    checkpoint = json.loads(settings.state.read_text())
    if checkpoint["topic"] != settings.topic or checkpoint["symbol"] != SYMBOL:
        raise ValueError("Checkpoint belongs to another topic or symbol; use another state file")
    return parse_timestamp(checkpoint["ts"])


def save_checkpoint(settings: Settings, timestamp: datetime) -> None:
    """Atomically replace the checkpoint after a broker acknowledgment."""
    staging_path = settings.state.with_suffix(".pending")
    with staging_path.open("w") as checkpoint_file:
        json.dump({"topic": settings.topic, "symbol": SYMBOL, "ts": timestamp.isoformat()}, checkpoint_file)
        checkpoint_file.flush()
        os.fsync(checkpoint_file.fileno())
    staging_path.replace(settings.state)


def send_bar(producer: Producer, settings: Settings, bar: dict[str, Any]) -> None:
    """Wait for delivery acknowledgment before persisting this bar's timestamp."""
    delivery_errors: list[KafkaError | None] = []

    def delivered(error: KafkaError | None, message: Any) -> None:
        """Capture the delivery outcome from Kafka's polling thread."""
        delivery_errors.append(error)

    payload = {**bar, "_id": str(bar["_id"]), "ts": bar["ts"].isoformat()}
    producer.produce(settings.topic, key=bar["symbol"], value=json.dumps(payload, allow_nan=False), on_delivery=delivered)
    while not delivery_errors:
        producer.poll(POLL_SECONDS)
    if delivery_errors[0] is not None:
        raise KafkaException(delivery_errors[0])
    save_checkpoint(settings, bar["ts"])


def replay(settings: Settings, stopped: threading.Event) -> None:
    """Send ordered batches, stopping between bars and interrupting interval waits."""
    checkpoint = read_checkpoint(settings)
    timestamp_filter: dict[str, datetime] = {}
    if settings.start:
        timestamp_filter["$gte"] = settings.start
    if checkpoint:
        timestamp_filter["$gt"] = checkpoint
    query: dict[str, Any] = {"symbol": SYMBOL}
    if timestamp_filter:
        query["ts"] = timestamp_filter
    ensure_topic(settings)
    producer = Producer({"bootstrap.servers": settings.brokers, "enable.idempotence": True,
                         "delivery.timeout.ms": TIMEOUT_SECONDS * 1000})
    try:
        with MongoClient(settings.mongo_uri, tz_aware=True, serverSelectionTimeoutMS=TIMEOUT_SECONDS * 1000) as client:
            with client[DATABASE][COLLECTION].find(query).sort("ts", 1) as bars:
                exhausted = False
                while not stopped.is_set() and not exhausted:
                    sent_count = 0
                    last_timestamp = None
                    for _ in range(settings.batch_size):
                        if stopped.is_set():
                            break
                        bar = next(bars, None)
                        if bar is None:
                            exhausted = True
                            break
                        send_bar(producer, settings, bar)
                        sent_count += 1
                        last_timestamp = bar["ts"]
                    if sent_count:
                        LOGGER.info("Batch sent symbol=%s ts=%s count=%d", SYMBOL, last_timestamp.isoformat(), sent_count)
                    if not exhausted:
                        stopped.wait(settings.interval)
                if exhausted:
                    LOGGER.info("Reached end of SPY bars")
    finally:
        LOGGER.info("Flushing Kafka producer")
        if producer.flush(TIMEOUT_SECONDS):
            raise RuntimeError("Kafka flush timed out with undelivered messages")


def main() -> None:
    """Lock the checkpoint and install graceful shutdown handlers."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_environment()
    settings.state.parent.mkdir(parents=True, exist_ok=True)
    stopped = threading.Event()

    def request_stop(signum: int, frame: Any) -> None:
        """Finish the current send and flush before exiting."""
        stopped.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    with settings.state.with_suffix(".lock").open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        replay(settings, stopped)


if __name__ == "__main__":
    main()
