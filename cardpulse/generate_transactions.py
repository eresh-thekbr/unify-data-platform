"""Generate a fake but realistic daily card transactions CSV with planted bad rows.

Run locally with the standard library only:
    python cardpulse/generate_transactions.py
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

RANDOM_SEED = 20261114
BUSINESS_DATE = datetime(2026, 11, 14, tzinfo=timezone.utc)

CLEAN_ROW_COUNT = 990
DUPLICATE_ROW_COUNT = 10
NEGATIVE_AMOUNT_ROW_COUNT = 8
MISSING_CARD_NUMBER_ROW_COUNT = 6
BAD_TIMESTAMP_ROW_COUNT = 2

CARD_POOL_SIZE = 150
US_STYLE_TIMESTAMP_SHARE = 0.2
ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
US_STYLE_TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M:%S"
BAD_TIMESTAMP_VALUES = ("2026-11-14 25:61:00", "yesterday evening")

CSV_COLUMNS = (
    "transaction_id",
    "card_number",
    "merchant_name",
    "merchant_category",
    "amount",
    "transaction_ts",
)

MERCHANTS_BY_CATEGORY = {
    "groceries": (("Whole Foods Market", "Trader Joe's", "Safeway", "Kroger"), (8.00, 180.00)),
    "dining": (("Chipotle", "Olive Garden", "Shake Shack", "Local Bistro"), (9.00, 120.00)),
    "fuel": (("Shell", "Chevron", "ExxonMobil", "BP"), (25.00, 95.00)),
    "travel": (("Delta Air Lines", "Marriott", "Uber", "Hertz"), (15.00, 950.00)),
    "retail": (("Target", "Best Buy", "Macy's", "Home Depot"), (12.00, 600.00)),
    "entertainment": (("Netflix", "AMC Theatres", "Spotify", "Ticketmaster"), (9.99, 250.00)),
    "health": (("CVS Pharmacy", "Walgreens", "City Dental", "Planet Fitness"), (6.00, 300.00)),
    "utilities": (("PG&E", "Comcast Xfinity", "Verizon Wireless", "City Water Dept"), (40.00, 280.00)),
}

OUTPUT_PATH = (
    Path(__file__).parent / "sample_data" / f"card_transactions_{BUSINESS_DATE:%Y-%m-%d}.csv"
)


def build_card_pool(rng: random.Random) -> list[str]:
    """Return fake 16-digit card numbers, so the same cards repeat across transactions."""
    return ["4" + "".join(rng.choices("0123456789", k=15)) for _ in range(CARD_POOL_SIZE)]


def random_transaction_ts(rng: random.Random) -> datetime:
    """Return a random UTC moment within the business date."""
    return BUSINESS_DATE + timedelta(seconds=rng.randrange(24 * 60 * 60))


def format_transaction_ts(transaction_ts: datetime, rng: random.Random) -> str:
    """Write most timestamps as ISO 8601 and some in US style, as mixed sources would."""
    if rng.random() < US_STYLE_TIMESTAMP_SHARE:
        return transaction_ts.strftime(US_STYLE_TIMESTAMP_FORMAT)
    return transaction_ts.strftime(ISO_TIMESTAMP_FORMAT)


def build_clean_transaction(rng: random.Random, sequence_number: int, card_pool: list[str]) -> dict[str, str]:
    """Return one valid transaction row."""
    merchant_category = rng.choice(list(MERCHANTS_BY_CATEGORY))
    merchant_names, (minimum_amount, maximum_amount) = MERCHANTS_BY_CATEGORY[merchant_category]
    return {
        "transaction_id": f"TXN-{BUSINESS_DATE:%Y%m%d}-{sequence_number:06d}",
        "card_number": rng.choice(card_pool),
        "merchant_name": rng.choice(merchant_names),
        "merchant_category": merchant_category,
        "amount": f"{rng.uniform(minimum_amount, maximum_amount):.2f}",
        "transaction_ts": format_transaction_ts(random_transaction_ts(rng), rng),
    }


def build_clean_transactions(rng: random.Random) -> list[dict[str, str]]:
    """Return the valid rows that every bad row is planted into."""
    card_pool = build_card_pool(rng)
    return [
        build_clean_transaction(rng, sequence_number, card_pool)
        for sequence_number in range(1, CLEAN_ROW_COUNT + 1)
    ]


def plant_bad_values(rng: random.Random, transactions: list[dict[str, str]]) -> set[int]:
    """Break distinct rows in place and return the positions of the broken rows."""
    bad_row_count = NEGATIVE_AMOUNT_ROW_COUNT + MISSING_CARD_NUMBER_ROW_COUNT + BAD_TIMESTAMP_ROW_COUNT
    bad_row_positions = rng.sample(range(len(transactions)), bad_row_count)
    negative_amount_positions = bad_row_positions[:NEGATIVE_AMOUNT_ROW_COUNT]
    missing_card_positions = bad_row_positions[
        NEGATIVE_AMOUNT_ROW_COUNT : NEGATIVE_AMOUNT_ROW_COUNT + MISSING_CARD_NUMBER_ROW_COUNT
    ]
    bad_timestamp_positions = bad_row_positions[NEGATIVE_AMOUNT_ROW_COUNT + MISSING_CARD_NUMBER_ROW_COUNT :]

    for position in negative_amount_positions:
        transactions[position]["amount"] = "-" + transactions[position]["amount"]
    for position in missing_card_positions:
        transactions[position]["card_number"] = ""
    for position, bad_timestamp in zip(bad_timestamp_positions, BAD_TIMESTAMP_VALUES):
        transactions[position]["transaction_ts"] = bad_timestamp
    return set(bad_row_positions)


def build_duplicate_rows(
    rng: random.Random, transactions: list[dict[str, str]], bad_row_positions: set[int]
) -> list[dict[str, str]]:
    """Return exact copies of valid rows, as a source system resending records would produce."""
    valid_row_positions = [position for position in range(len(transactions)) if position not in bad_row_positions]
    duplicated_positions = rng.sample(valid_row_positions, DUPLICATE_ROW_COUNT)
    return [dict(transactions[position]) for position in duplicated_positions]


def write_transactions_csv(transactions: list[dict[str, str]], output_path: Path) -> None:
    """Write the rows with a header line."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(transactions)


def main() -> None:
    rng = random.Random(RANDOM_SEED)
    transactions = build_clean_transactions(rng)
    bad_row_positions = plant_bad_values(rng, transactions)
    all_transactions = transactions + build_duplicate_rows(rng, transactions, bad_row_positions)
    rng.shuffle(all_transactions)
    write_transactions_csv(all_transactions, OUTPUT_PATH)

    expected_rejected_row_count = len(bad_row_positions) + DUPLICATE_ROW_COUNT
    print(f"Wrote {len(all_transactions)} rows to {OUTPUT_PATH}")
    print(f"Expected silver rows: {len(all_transactions) - expected_rejected_row_count}")
    print(f"Expected rejected rows: {expected_rejected_row_count}")


if __name__ == "__main__":
    main()
