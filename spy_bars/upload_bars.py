"""Push completed local bar files to a Unity Catalog Volume without overwriting."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import logging
import math
import os
import signal
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, NotFound
from databricks.sdk.service.catalog import VolumeType
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)
VOLUME_NAME = "spy_bars_landing"
HASH_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class Settings:
    """Local paths and explicit destination settings; credentials stay in .env."""

    catalog: str
    schema: str
    directory: Path
    state: Path
    interval: float

    @classmethod
    def from_environment(cls) -> Settings:
        """Load and validate configuration without exposing credentials."""
        load_dotenv()
        interval = float(os.getenv("UPLOAD_INTERVAL_SECONDS", "30"))
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("UPLOAD_INTERVAL_SECONDS must be positive")
        catalog = os.getenv("DATABRICKS_CATALOG", "")
        schema = os.getenv("DATABRICKS_SCHEMA", "uc1_spy_bars")
        for identifier in (catalog, schema):
            if identifier and not all(character.isalnum() or character == '_' for character in identifier):
                raise ValueError("Use catalog and schema names containing letters, digits and underscores")
        return cls(catalog, schema, Path(os.getenv("FILE_WRITER_DIRECTORY", "landing/spy_bars_1m")),
                   Path(os.getenv("UPLOAD_STATE_FILE", ".state/uploads.sqlite3")), interval)

    @property
    def volume_path(self) -> str:
        """Require an inspected catalog before any remote mutation."""
        if not self.catalog or not self.schema:
            raise ValueError("Inspect catalogs first, then set DATABRICKS_CATALOG and DATABRICKS_SCHEMA in .env")
        return f"/Volumes/{self.catalog}/{self.schema}/{VOLUME_NAME}"


def workspace_client() -> WorkspaceClient:
    """Use an explicit host and either a .env PAT or CLI OAuth credentials."""
    host = os.getenv("DATABRICKS_HOST", "")
    if not host.startswith("https://"):
        raise ValueError("Set DATABRICKS_HOST to your HTTPS workspace URL in .env")
    token = os.getenv("DATABRICKS_TOKEN", "")
    if token:
        return WorkspaceClient(host=host, token=token, auth_type="pat")
    return WorkspaceClient(host=host, auth_type="databricks-cli")


def inspect_catalogs(client: WorkspaceClient) -> None:
    """List actual workspace catalogs and owners without making changes."""
    for catalog in client.catalogs.list():
        print(f"catalog={catalog.name} owner={catalog.owner} type={catalog.catalog_type}")


def create_volume(client: WorkspaceClient, settings: Settings) -> None:
    """Create the project schema and managed Volume in an explicitly selected catalog."""
    volume_path = settings.volume_path
    client.catalogs.get(settings.catalog)
    try:
        client.schemas.get(f"{settings.catalog}.{settings.schema}")
    except NotFound:
        client.schemas.create(name=settings.schema, catalog_name=settings.catalog,
                              comment="Use Case 1 SPY bar ingestion")
    volume_name = f"{settings.catalog}.{settings.schema}.{VOLUME_NAME}"
    try:
        client.volumes.read(volume_name)
    except NotFound:
        client.volumes.create(catalog_name=settings.catalog, schema_name=settings.schema,
                              name=VOLUME_NAME, volume_type=VolumeType.MANAGED)
    LOGGER.info("Volume ready path=%s", volume_path)


def file_digest(filename: Path) -> str:
    """Hash a local file in bounded memory."""
    digest = hashlib.sha256()
    with filename.open("rb") as contents:
        while chunk := contents.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def verify_remote_file(client: WorkspaceClient, remote_path: str, expected_digest: str) -> None:
    """Recover an upload-before-record crash only when remote contents match."""
    download = client.files.download(remote_path)
    if download.contents is None:
        raise RuntimeError("Remote download returned no contents")
    digest = hashlib.sha256()
    with download.contents as contents:
        while chunk := contents.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
    if digest.hexdigest() != expected_digest:
        raise RuntimeError(f"Remote file differs from local file: {Path(remote_path).name}")


def upload_file(client: WorkspaceClient, remote_path: str, filename: Path, digest: str) -> bool:
    """Upload absent files only; reconcile existing files without uploading again."""
    try:
        client.files.get_metadata(remote_path)
    except NotFound:
        try:
            with filename.open("rb") as contents:
                client.files.upload(remote_path, contents, overwrite=False)
            return True
        except AlreadyExists:
            pass
    verify_remote_file(client, remote_path, digest)
    return False


def upload_pending(client: WorkspaceClient, settings: Settings, record: sqlite3.Connection,
                   stopped: threading.Event) -> None:
    """Record each filename only after successful upload or remote verification."""
    destination = client.config.host.rstrip('/') + settings.volume_path
    for filename in sorted(settings.directory.glob("*.jsonl")):
        if stopped.is_set():
            return
        if filename.is_symlink() or not filename.is_file():
            continue
        digest = file_digest(filename)
        previous = record.execute("SELECT digest FROM uploads WHERE destination=? AND filename=?",
                                  (destination, filename.name)).fetchone()
        if previous:
            if previous[0] != digest:
                raise RuntimeError(f"Previously uploaded file was modified: {filename.name}")
            continue
        uploaded = upload_file(client, f"{settings.volume_path}/{filename.name}", filename, digest)
        with record:
            record.execute("INSERT INTO uploads VALUES (?, ?, ?)", (destination, filename.name, digest))
        LOGGER.info("%s file=%s", "Uploaded" if uploaded else "Already present; recorded", filename.name)


def run_uploads(client: WorkspaceClient, settings: Settings, once: bool, stopped: threading.Event) -> None:
    """Serialize uploaders and keep a durable, destination-scoped local record."""
    settings.volume_path
    if not settings.directory.is_dir():
        raise ValueError("Local landing directory does not exist")
    client.volumes.read(f"{settings.catalog}.{settings.schema}.{VOLUME_NAME}")
    settings.state.parent.mkdir(parents=True, exist_ok=True)
    with settings.state.with_suffix('.lock').open('w') as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record = sqlite3.connect(settings.state)
        try:
            record.execute("PRAGMA synchronous=FULL")
            record.execute("CREATE TABLE IF NOT EXISTS uploads (destination TEXT, filename TEXT, digest TEXT, PRIMARY KEY(destination, filename))")
            while not stopped.is_set():
                upload_pending(client, settings, record, stopped)
                if once:
                    break
                stopped.wait(settings.interval)
        finally:
            record.close()


def main() -> None:
    """Inspect, provision, or upload; default to continuously watching landed files."""
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--inspect', action='store_true')
    actions.add_argument('--create-volume', action='store_true')
    actions.add_argument('--once', action='store_true')
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    settings = Settings.from_environment()
    client = workspace_client()
    stopped = threading.Event()

    def request_stop(signum: int, frame: object) -> None:
        """Finish the current upload and record before stopping."""
        stopped.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    if arguments.inspect:
        inspect_catalogs(client)
    elif arguments.create_volume:
        create_volume(client, settings)
    else:
        run_uploads(client, settings, arguments.once, stopped)


if __name__ == '__main__':
    main()
