"""Upload the bronze notebook and run it on Databricks serverless compute."""
from __future__ import annotations

import base64
import logging
import time
from pathlib import Path

from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.jobs import NotebookTask, RunResultState, SubmitTask, TaskDependency
from databricks.sdk.service.workspace import ImportFormat, Language
from dotenv import load_dotenv

from upload_bars import workspace_client

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent
WORKSPACE_DIRECTORY = "/Shared/uc1_spy_bars"
NOTEBOOK_PATH = f"{WORKSPACE_DIRECTORY}/bronze_spy_bars"
CHECKPOINT_VOLUME = "workspace.uc1_spy_bars.spy_bars_checkpoints"
POLL_SECONDS = 15
JOB_TIMEOUT_SECONDS = 1800
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def main() -> None:
    """Provision checkpoint storage, deploy source, and wait for the one-time job."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv(PROJECT_ROOT / ".env")
    client = workspace_client()
    try:
        client.volumes.read(CHECKPOINT_VOLUME)
    except NotFound:
        client.volumes.create(catalog_name="workspace", schema_name="uc1_spy_bars",
                              name="spy_bars_checkpoints", volume_type=VolumeType.MANAGED)
    client.workspace.mkdirs(WORKSPACE_DIRECTORY)
    for notebook_name in ("bronze_spy_bars", "silver_spy_bars"):
        notebook_source = (PROJECT_ROOT / "notebooks" / f"{notebook_name}.py").read_bytes()
        client.workspace.import_(path=f"{WORKSPACE_DIRECTORY}/{notebook_name}",
                                 content=base64.b64encode(notebook_source).decode(),
                                 format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)
    submitted = client.jobs.submit(run_name="uc1-spy-bars-bronze-silver", tasks=[
        SubmitTask(task_key="load_bronze", notebook_task=NotebookTask(notebook_path=NOTEBOOK_PATH),
                   timeout_seconds=JOB_TIMEOUT_SECONDS),
        SubmitTask(task_key="load_silver", depends_on=[TaskDependency(task_key="load_bronze")],
                   notebook_task=NotebookTask(notebook_path=f"{WORKSPACE_DIRECTORY}/silver_spy_bars"),
                   timeout_seconds=JOB_TIMEOUT_SECONDS)])
    run_id = submitted.run_id
    LOGGER.info("Submitted run_id=%s", run_id)
    while True:
        run = client.jobs.get_run(run_id)
        if run.state and run.state.life_cycle_state:
            lifecycle = run.state.life_cycle_state.value
            LOGGER.info("Run %s state=%s", run_id, lifecycle)
            if lifecycle in TERMINAL_STATES:
                for task in run.tasks or []:
                    if task.run_id is not None:
                        output = client.jobs.get_run_output(task.run_id)
                        if output.error:
                            LOGGER.error("Task failed: %s", output.error)
                if run.state.result_state != RunResultState.SUCCESS:
                    raise RuntimeError(f"Bronze/silver job failed: {run.state.state_message}")
                LOGGER.info("Bronze/silver job succeeded")
                return
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
