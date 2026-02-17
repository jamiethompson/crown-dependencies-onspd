import csv
from pathlib import Path

from scripts.pipeline.temporal import apply_temporal_tracking


def test_temporal_preserves_first_seen_and_updates_last_seen(tmp_path: Path):
    canonical_path = tmp_path / "out.csv"
    with canonical_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["normalised_postcode", "first_seen", "last_seen"])
        writer.writeheader()
        writer.writerow({"normalised_postcode": "JE2 3AB", "first_seen": "2026-01-01", "last_seen": "2026-01-01"})
        writer.writerow({"normalised_postcode": "JE9 9ZZ", "first_seen": "2026-01-01", "last_seen": "2026-01-01"})

    rows = [
        {"normalised_postcode": "JE2 3AB"},
        {"normalised_postcode": "JE1 1AA"},
    ]

    updated, stats = apply_temporal_tracking(
        rows,
        territory_code="JE",
        canonical_output_path=canonical_path,
        state_path=tmp_path / "state.json",
        run_date="2026-02-17",
    )

    by_key = {row["normalised_postcode"]: row for row in updated}
    assert by_key["JE2 3AB"]["first_seen"] == "2026-01-01"
    assert by_key["JE2 3AB"]["last_seen"] == "2026-02-17"
    assert by_key["JE1 1AA"]["first_seen"] == "2026-02-17"
    assert stats["disappeared_count"] == 1
