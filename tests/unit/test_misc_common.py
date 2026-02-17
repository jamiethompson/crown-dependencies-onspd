from pathlib import Path

from scripts.common.deterministic import stable_sorted
from scripts.common.geometry import extract_point_from_geometry
from scripts.common.ids import generate_run_id
from scripts.common.scoring import clamp
from scripts.common.time_utils import parse_run_date


def test_stable_sorted_orders_values():
    assert stable_sorted([{"k": 2}, {"k": 1}], key=lambda item: item["k"]) == [{"k": 1}, {"k": 2}]


def test_extract_point_from_geometry_handles_missing_and_point():
    assert extract_point_from_geometry(None) == (None, None)
    assert extract_point_from_geometry({"x": -2.1, "y": 49.2}) == (49.2, -2.1)


def test_clamp_within_bounds():
    assert clamp(-5, minimum=0, maximum=100) == 0
    assert clamp(50, minimum=0, maximum=100) == 50
    assert clamp(150, minimum=0, maximum=100) == 100


def test_generate_run_id_prefix():
    assert generate_run_id().startswith("run-")


def test_parse_run_date_defaults_and_iso():
    assert parse_run_date("2026-02-17") == "2026-02-17"
    assert len(parse_run_date(None)) == len("2026-02-17")
