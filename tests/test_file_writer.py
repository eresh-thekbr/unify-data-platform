"""Verify file durability ordering and consumer batch boundaries with synthetic bars."""
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from file_writer import BatchWriter, Settings, consume


def bar_message(offset: int = 0) -> MagicMock:
    """Create a synthetic Kafka record without storing market data."""
    message = MagicMock()
    message.value.return_value = json.dumps({"symbol": "SPY", "ts": f"2000-01-01T00:00:{offset:02d}+00:00"})
    message.topic.return_value = "test-bars"
    message.partition.return_value = 0
    message.offset.return_value = offset
    message.error.return_value = None
    return message


class WriterTests(unittest.TestCase):
    """Exercise acknowledgment ordering, retries, and loop flush triggers."""

    def test_file_precedes_commit_and_retry_preserves_files(self) -> None:
        """A failed commit leaves a complete file and retry never overwrites it."""
        with tempfile.TemporaryDirectory() as directory:
            batch = BatchWriter(Path(directory))
            batch.append(bar_message())
            consumer = MagicMock()

            def fail_commit(**arguments: object) -> None:
                """Verify the complete file exists before simulating commit failure."""
                files = list(Path(directory).glob('*.jsonl'))
                self.assertEqual(len(files), 1)
                self.assertEqual(json.loads(files[0].read_text())["symbol"], "SPY")
                raise RuntimeError("simulated commit failure")

            consumer.commit.side_effect = fail_commit
            with self.assertRaises(RuntimeError):
                batch.flush(consumer)
            self.assertEqual(len(batch.bars), 1)
            consumer.commit.side_effect = None
            consumer.commit.return_value = []
            batch.flush(consumer)
            self.assertEqual(len(list(Path(directory).glob('*.jsonl'))), 2)
            self.assertEqual(consumer.commit.call_args.kwargs['offsets'][0].offset, 1)
            self.assertFalse(batch.bars)

    def test_write_failure_does_not_commit(self) -> None:
        """A disk failure must leave Kafka offsets untouched."""
        batch = BatchWriter(Path('/unused'))
        batch.append(bar_message())
        consumer = MagicMock()
        with patch('file_writer.write_file', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                batch.flush(consumer)
        consumer.commit.assert_not_called()

    def test_size_timer_and_shutdown(self) -> None:
        """Each trigger writes a partial or full batch and closes the consumer."""
        for trigger in ('size', 'timer', 'shutdown'):
            with self.subTest(trigger=trigger), tempfile.TemporaryDirectory() as directory:
                stopped = threading.Event()
                consumer = MagicMock()
                consumer.commit.return_value = []
                poll_count = 0

                def poll(timeout: float) -> MagicMock | None:
                    """Return one record and then request shutdown."""
                    nonlocal poll_count
                    poll_count += 1
                    if poll_count == 1:
                        if trigger == 'shutdown':
                            stopped.set()
                        return bar_message()
                    stopped.set()
                    return None

                consumer.poll.side_effect = poll
                settings = Settings('unused', 'test-bars', 1 if trigger == 'size' else 10, 30, Path(directory))
                with patch('file_writer.Consumer', return_value=consumer), patch('file_writer.time.monotonic', side_effect=[0, 31, 31, 31] if trigger == 'timer' else [0] * 10):
                    consume(settings, stopped)
                self.assertEqual(len(list(Path(directory).glob('*.jsonl'))), 1)
                consumer.commit.assert_called_once()
                consumer.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
