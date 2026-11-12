"""Land Kafka bars as durable JSON Lines files before committing offsets."""
from __future__ import annotations

import json
import logging
import math
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)
CONSUMER_GROUP = "udp-spy-bars-file-writer"
POLL_SECONDS = 0.5
TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"


@dataclass(frozen=True)
class Settings:
    """Validated file writer configuration."""

    brokers: str
    topic: str
    batch_size: int
    interval: float
    directory: Path

    @classmethod
    def from_environment(cls) -> Settings:
        """Read settings, giving exported variables precedence over .env."""
        load_dotenv()
        batch_size = int(os.getenv("FILE_WRITER_BATCH_SIZE", "10"))
        interval = float(os.getenv("FILE_WRITER_INTERVAL_SECONDS", "30"))
        if batch_size <= 0 or not math.isfinite(interval) or interval <= 0:
            raise ValueError("File writer batch size and interval must be positive")
        return cls(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                   os.getenv("KAFKA_TOPIC", "udp.spy_bars_1m"), batch_size, interval,
                   Path(os.getenv("FILE_WRITER_DIRECTORY", "landing/spy_bars_1m")))


def timestamp_label(timestamp: str) -> str:
    """Format an ISO timestamp as a UTC filename component."""
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


def write_file(directory: Path, bars: list[dict[str, Any]]) -> Path:
    """Sync contents, atomically rename, and sync the directory before returning."""
    first_timestamp = timestamp_label(bars[0]["ts"])
    last_timestamp = timestamp_label(bars[-1]["ts"])
    # A unique suffix preserves prior files even when a crash replays the same bars.
    filename = f"bars_{first_timestamp}_{last_timestamp}_{uuid4().hex}.jsonl"
    final_path = directory / filename
    staging_path = directory / f".{filename}.pending"
    try:
        with staging_path.open("x") as bar_file:
            for bar in bars:
                bar_file.write(json.dumps(bar, allow_nan=False) + "\n")
            bar_file.flush()
            os.fsync(bar_file.fileno())
        staging_path.rename(final_path)
        directory_descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        staging_path.unlink(missing_ok=True)
    LOGGER.info("File written name=%s count=%d", final_path.name, len(bars))
    return final_path


class BatchWriter:
    """Track only validated bars and their explicit next offsets."""

    def __init__(self, directory: Path) -> None:
        """Initialize an empty batch."""
        self.directory = directory
        self.bars: list[dict[str, Any]] = []
        self.offsets: dict[tuple[str, int], int] = {}
        self.started_at: float | None = None

    def append(self, message: Message) -> None:
        """Validate before advancing tracked offsets; invalid input stops consumption."""
        bar = json.loads(message.value())
        if not isinstance(bar, dict) or bar.get("symbol") != "SPY":
            raise ValueError("Expected a SPY bar JSON object")
        timestamp_label(bar["ts"])
        if not self.bars:
            self.started_at = time.monotonic()
        self.bars.append(bar)
        self.offsets[(message.topic(), message.partition())] = message.offset() + 1

    def flush(self, consumer: Consumer) -> None:
        """Persist all buffered bars before synchronously committing their offsets."""
        if not self.bars:
            return
        write_file(self.directory, self.bars)
        offsets = [TopicPartition(topic, partition, offset)
                   for (topic, partition), offset in self.offsets.items()]
        committed = consumer.commit(offsets=offsets, asynchronous=False)
        for partition in committed:
            if partition.error:
                raise KafkaException(partition.error)
        self.bars.clear()
        self.offsets.clear()
        self.started_at = None

    def revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Persist buffered records before a normal partition reassignment."""
        self.flush(consumer)

    def lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Stop without committing offsets for partitions no longer owned."""
        raise RuntimeError("Kafka partition ownership lost; restart to replay uncommitted bars")


def consume(settings: Settings, stopped: threading.Event) -> None:
    """Flush on batch size, elapsed time since the first bar, or graceful shutdown."""
    settings.directory.mkdir(parents=True, exist_ok=True)
    batch = BatchWriter(settings.directory)
    consumer = Consumer({"bootstrap.servers": settings.brokers, "group.id": CONSUMER_GROUP,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False,
                         "enable.auto.offset.store": False})
    try:
        consumer.subscribe([settings.topic], on_revoke=batch.revoke, on_lost=batch.lost)
        LOGGER.info("Consuming topic=%s group=%s directory=%s", settings.topic, CONSUMER_GROUP, settings.directory)
        while not stopped.is_set():
            timeout = POLL_SECONDS
            if batch.started_at is not None:
                timeout = min(timeout, max(0, settings.interval - (time.monotonic() - batch.started_at)))
            message = consumer.poll(timeout)
            if message is not None:
                if message.error():
                    raise KafkaException(message.error())
                batch.append(message)
            expired = batch.started_at is not None and time.monotonic() - batch.started_at >= settings.interval
            if len(batch.bars) >= settings.batch_size or expired:
                batch.flush(consumer)
        batch.flush(consumer)
    finally:
        # Disable callbacks during close: failures must not retry or commit here.
        consumer.subscribe([settings.topic])
        consumer.close()


def main() -> None:
    """Request a partial-batch flush on Ctrl+C or termination."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_environment()
    stopped = threading.Event()

    def request_stop(signum: int, frame: Any) -> None:
        """Let the polling loop perform an orderly shutdown."""
        stopped.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    consume(settings, stopped)


if __name__ == "__main__":
    main()
