"""Test upload crash recovery and durable local records using synthetic files."""
import hashlib
import io
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from databricks.sdk.errors import NotFound
from upload_bars import Settings, upload_file, upload_pending


class UploadTests(unittest.TestCase):
    """Verify reruns, remote reconciliation, and failure handling."""

    def test_rerun_skips_upload(self) -> None:
        """A recorded file uploads once and temporary files are ignored."""
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / 'synthetic.jsonl').write_text('{}\n')
            (folder / '.incomplete.pending').write_text('incomplete')
            settings = Settings('catalog', 'schema', folder, folder / 'record.sqlite3', 30)
            client = MagicMock()
            client.config.host = 'https://example.invalid'
            client.files.get_metadata.side_effect = NotFound('absent')
            with sqlite3.connect(settings.state) as record:
                record.execute('CREATE TABLE uploads (destination TEXT, filename TEXT, digest TEXT, PRIMARY KEY(destination,filename))')
                upload_pending(client, settings, record, threading.Event())
                upload_pending(client, settings, record, threading.Event())
                self.assertEqual(record.execute('SELECT COUNT(*) FROM uploads').fetchone()[0], 1)
            client.files.upload.assert_called_once()
            self.assertFalse(client.files.upload.call_args.kwargs['overwrite'])

    def test_upload_before_record_crash(self) -> None:
        """An existing matching remote file is verified without reuploading."""
        client = MagicMock()
        client.files.download.return_value.contents = io.BytesIO(b'{}\n')
        uploaded = upload_file(client, '/Volumes/catalog/schema/volume/bar.jsonl', Path('unused'),
                               hashlib.sha256(b'{}\n').hexdigest())
        self.assertFalse(uploaded)
        client.files.upload.assert_not_called()

    def test_remote_conflict_stops(self) -> None:
        """Different remote contents cannot be silently marked successful."""
        client = MagicMock()
        client.files.download.return_value.contents = io.BytesIO(b'other')
        with self.assertRaises(RuntimeError):
            upload_file(client, '/Volumes/catalog/schema/volume/bar.jsonl', Path('unused'), 'expected')
        client.files.upload.assert_not_called()

    def test_failed_upload_is_not_recorded(self) -> None:
        """A network failure leaves the local record eligible for retry."""
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / 'synthetic.jsonl').write_text('{}\n')
            settings = Settings('catalog', 'schema', folder, folder / 'record.sqlite3', 30)
            client = MagicMock()
            client.config.host = 'https://example.invalid'
            client.files.get_metadata.side_effect = NotFound('absent')
            client.files.upload.side_effect = RuntimeError('network failure')
            with sqlite3.connect(settings.state) as record:
                record.execute('CREATE TABLE uploads (destination TEXT, filename TEXT, digest TEXT, PRIMARY KEY(destination,filename))')
                with self.assertRaises(RuntimeError):
                    upload_pending(client, settings, record, threading.Event())
                self.assertEqual(record.execute('SELECT COUNT(*) FROM uploads').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
