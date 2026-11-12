# SPY bar replay — Use Case 1, Step 1

Python 3.11+. Reads only `unified_trader.bars_1m` for SPY from MongoDB,
ordered by `ts`. Creates the Kafka topic if absent and emits JSON with the
symbol as the message key. `_id` is a string and `ts` is an ISO UTC timestamp.

## Start

```sh
cd /Users/ereshtayanna/my_workspace/unify-data-platform
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# Only on a fresh setup (keep your existing .env):
cp -n .env.example .env
.venv/bin/python replay.py
```

MongoDB must listen at localhost:27017; Kafka must listen at localhost:9092.
Both were already running during setup. Python packages were installed only
in `.venv`. Configure the interval, batch size, start date, topic, broker,
Mongo URI and checkpoint path in `.env`; exported variables take precedence.
Start dates are inclusive and interpreted as UTC. The saved checkpoint is an
additional exclusive lower bound. Use a separate state file for another topic.

The first batch sends immediately; subsequent batches wait the configured
interval after the previous batch completes. The producer exits at end of input.
Ctrl+C finishes any in-flight delivery and checkpoint, then flushes Kafka.

## Watch in another terminal

```sh
kafka-console-consumer --bootstrap-server localhost:9092 --topic udp.spy_bars_1m --from-beginning --property print.key=true --property key.separator=' | '
```

## Prove restart

1. Run the producer until a `Batch sent` line appears, then press Ctrl+C.
2. Run `cat .state/spy-bars.json` and note `ts`.
3. Restart `.venv/bin/python replay.py`. The first logged timestamp must be
   later than the saved timestamp. Press Ctrl+C again when finished.

Checkpoints are saved atomically after each broker acknowledgment. A process
crash between acknowledgment and checkpoint can produce a duplicate (at-least-once
delivery). Only one producer may hold a given checkpoint lock. Timestamp-based
resume assumes unique, immutable SPY timestamps and no later inserts before the
checkpoint. MongoDB is accessed only with read operations; credentials granting
read-only access can be supplied through MONGO_URI.

Environment files, virtual environments and state are ignored by git. Do not
store market data or credentials in tracked files. The live verification sent
the first two source bars and retained the checkpoint for normal continuation.

## Step 2: Kafka to local JSON Lines

Start the consumer in its own terminal:

```sh
cd /Users/ereshtayanna/my_workspace/unify-data-platform
.venv/bin/python file_writer.py
```

Run `.venv/bin/python replay.py` in another terminal. Watch completed files:

```sh
while true; do
  find landing/spy_bars_1m -name '*.jsonl' -exec wc -l {} +
  sleep 2
done
```

The fixed consumer group is `udp-spy-bars-file-writer`. A new group starts at
Kafka's earliest retained offset; restarts use committed offsets. `.env` settings:

- `FILE_WRITER_BATCH_SIZE=10`
- `FILE_WRITER_INTERVAL_SECONDS=30` (measured from the first buffered bar)
- `FILE_WRITER_DIRECTORY=landing/spy_bars_1m`

The writer also uses `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`. Each filename
contains the first and last timestamps in consumption order plus a unique suffix
so duplicate deliveries cannot overwrite existing files. Only `.jsonl` files
are complete; a hard crash can leave ignored `.pending` files. The writer syncs
file contents, renames in the same directory, syncs the directory, then commits
explicit Kafka offsets synchronously. Ctrl+C writes and commits a partial batch.
A write or commit error stops consumption. Replayed duplicates after a crash are
expected. Keep custom landing directories outside git or add them to `.gitignore`.
Kafka retention must retain unconsumed records, and the landing files must be kept.

### Restart check

1. Stop the producer so the topic has a fixed end offset.
2. Start the consumer, let it write a file, then press Ctrl+C. Note the current
   offset and total line count with:

   ```sh
   kafka-consumer-groups --bootstrap-server localhost:9092 --group udp-spy-bars-file-writer --describe
   find landing/spy_bars_1m -name '*.jsonl' -exec wc -l {} +
   ```

3. Restart the consumer and let it drain the topic. Wait at least 30 seconds
   for the last partial batch, then press Ctrl+C. Repeat the two commands:
   lag should be zero, and the added line count should equal the offset increase
   across this clean restart (the topic has one partition).
4. Restart once more while the producer is stopped: no new files should appear.

To exercise crash recovery, use `kill -9` on the consumer while it has a partial
batch, then restart and drain again. All remaining records should land; duplicates
are acceptable if the crash occurred after a file rename but before offset commit.
Do not delete landing files or reset the group during this check.

Validation: ` .venv/bin/python -m unittest discover -s tests -v` covers size,
timer, shutdown, failed writes, and failed commits. Live verification wrote two
batches across a restart and compared the resulting 20 lines with Kafka offsets
0–19. Both files and the group's committed position are retained locally.
