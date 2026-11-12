"""Replay ten existing landing bars twice and print SQL for the demo keys."""
from __future__ import annotations

import json
from datetime import datetime

from notebooks.silver_spy_bars import quality_reason
from pathlib import Path

from file_writer import write_file


def main() -> None:
    """Create two unique files; use the usual uploader and two-task job afterward."""
    directory = Path("landing/spy_bars_1m")
    bars: dict[tuple[str, str], dict] = {}
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            bar = json.loads(line)
            if quality_reason(bar | {"ts": datetime.fromisoformat(bar["ts"].replace("Z", "+00:00"))}):
                continue
            bars.setdefault((bar["symbol"], bar["ts"]), bar)
            if len(bars) == 10:
                break
        if len(bars) == 10:
            break
    if len(bars) != 10:
        raise ValueError("Need at least ten distinct bars in landing/spy_bars_1m")
    selected = list(bars.values())
    for _ in range(2):
        print(write_file(directory, selected))
    print("Run: .venv/bin/python upload_bars.py --once && .venv/bin/python run_bronze.py")
    keys = ",\n".join("('" + bar["symbol"].replace("'", "''") + "', TIMESTAMP '" +
                         bar["ts"].replace("'", "''") + "')" for bar in selected)
    print(f"""WITH demo_keys(symbol, ts) AS (VALUES {keys})
SELECT k.symbol, k.ts, count(s.ts) AS silver_copies
FROM demo_keys k LEFT JOIN workspace.uc1_spy_bars.silver_spy_bars_1m s
ON k.symbol = s.symbol AND k.ts = s.ts
GROUP BY k.symbol, k.ts ORDER BY k.ts;
-- Expect ten rows, each with silver_copies = 1 for valid demo bars.
""")


if __name__ == "__main__":
    main()
