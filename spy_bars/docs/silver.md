# Step 5: silver and quarantine

`notebooks/silver_spy_bars.py` uses the same pure Python quality and ordering
functions in local tests and Spark UDFs. Required fields are symbol, ts, OHLC,
and volume; tradeDate, _id and audit metadata are not required business fields.
Market hours use America/New_York (including DST), weekdays, 09:30 inclusive
to 16:00 exclusive, assuming bar-start timestamps. Holiday and early-close
calendars are not implemented.

The existing `run_bronze.py` runner now submits two dependent notebook tasks.
Silver uses the bronze format (Iceberg in this workspace). Latest ingestion
wins before validation, with deterministic filename/payload tie breaking.
An invalid latest correction deletes any older silver copy. Quarantine is a
replacement snapshot of all rejected bronze copies, including duplicates and
missing keys; its reason column lists all applicable failures. Reruns do not
accumulate quarantine copies. Run this job serially: the two table writes are
not a cross-table transaction. A failed run can be repaired by rerunning.

Local checks: `../.venv/bin/python -m pip install -r requirements-dev.txt`, then
`../.venv/bin/python -m pytest -q`.

Demo: run `../.venv/bin/python demo_silver.py`. It selects ten distinct valid bars
from existing local landing files and writes the identical batch twice under
unique filenames. Run the uploader and job commands it prints, then execute
the printed SQL in Databricks. Expect ten result rows with silver_copies = 1.
The script prints verification SQL; it does not execute remote SQL itself.
If the bars already exist in bronze, silver should not grow. Bronze grows by
20 when only these new files are uploaded; other pending files add more.

```sql
SELECT 'bronze' AS layer, COUNT(*) AS rows FROM workspace.uc1_spy_bars.bronze_spy_bars_1m
UNION ALL
SELECT 'silver', COUNT(*) FROM workspace.uc1_spy_bars.silver_spy_bars_1m
UNION ALL
SELECT 'quarantine', COUNT(*) FROM workspace.uc1_spy_bars.quarantine_spy_bars_1m;

SELECT symbol, ts, COUNT(*) AS copies
FROM workspace.uc1_spy_bars.silver_spy_bars_1m
GROUP BY symbol, ts HAVING COUNT(*) > 1;
```

Bronze starts at the verified 397 rows; without new ingestion it stays 397.
Silver counts distinct keys whose latest copy passes every rule; quarantine
counts every failing bronze row. Thus silver plus quarantine need not equal
bronze. The duplicate query must return zero rows. Exact quality counts have
not been verified on Databricks for this step.

Databricks provides governed Iceberg tables and dependency-ordered bronze/silver execution with SQL verification.
Its limits here are quota-limited batch compute, full-bronze scans, and no atomic transaction across silver and quarantine.
The workaround is serial availableNow runs with repeatable MERGE/snapshot writes; use an exchange calendar for holiday and early-close precision.
