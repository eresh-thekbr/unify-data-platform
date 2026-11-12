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
