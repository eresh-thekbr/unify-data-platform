from datetime import datetime, timezone

import pytest

from notebooks.silver_spy_bars import deduplicate, quality_reason


def valid_bar() -> dict:
    return dict(symbol="SPY", ts=datetime(2026, 11, 10, 14, 30, tzinfo=timezone.utc),
                open=101.0, high=102.0, low=100.0, close=101.0, volume=0,
                ingestion_timestamp=datetime(2026, 11, 11), source_file_name="a")


@pytest.mark.parametrize("field", ["symbol", "ts", "open", "high", "low", "close", "volume"])
def test_required_fields(field: str) -> None:
    bar = valid_bar()
    bar[field] = None
    assert f"missing_{field}" in quality_reason(bar)


@pytest.mark.parametrize("change,reason", [
    ({"high": 99}, "high_below_low"), ({"open": 103}, "open_out_of_range"),
    ({"close": 99}, "close_out_of_range"), ({"volume": -1}, "negative_volume"),
    ({"open": float("nan")}, "nonfinite_price"),
])
def test_quality_failures(change: dict, reason: str) -> None:
    assert reason in quality_reason(valid_bar() | change)


@pytest.mark.parametrize("stamp,valid", [
    ("2026-11-10T14:30:00+00:00", True), ("2026-11-10T14:29:00+00:00", False),
    ("2026-11-10T20:59:00+00:00", True), ("2026-11-10T21:00:00+00:00", False),
    ("2026-07-07T13:30:00+00:00", True), ("2026-11-14T15:00:00+00:00", False),
])
def test_market_hours(stamp: str, valid: bool) -> None:
    assert (quality_reason(valid_bar() | {"ts": datetime.fromisoformat(stamp)}) == "") == valid


def test_latest_invalid_copy_wins_and_replay_is_idempotent() -> None:
    old = valid_bar()
    new = old | {"volume": -1, "ingestion_timestamp": datetime(2026, 11, 12)}
    assert deduplicate([new, old, new, old]) == [new]
    assert quality_reason(deduplicate([old, new])[0]) == "negative_volume"


def test_ties_are_deterministic_and_keys_remain_distinct() -> None:
    first = valid_bar()
    second = first | {"source_file_name": "b"}
    other = first | {"symbol": "OTHER"}
    assert deduplicate([second, first]) == deduplicate([first, second]) == [second]
    assert len(deduplicate([first, second, other])) == 2
