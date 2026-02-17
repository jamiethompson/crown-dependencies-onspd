from pathlib import Path

import pytest

from scripts.cli import parse_args, run_command


def _run_once(data_dir: Path, run_id: str) -> None:
    args = parse_args(
        [
            "all",
            "--config-dir",
            "config",
            "--data-dir",
            str(data_dir),
            "--run-date",
            "2026-02-17",
            "--run-id",
            run_id,
        ]
    )
    assert run_command(args) == 0


@pytest.mark.regression
def test_canonical_outputs_are_byte_stable_for_same_inputs(tmp_path: Path):
    first = tmp_path / "first"
    second = tmp_path / "second"

    _run_once(first, "run-a")
    _run_once(second, "run-b")

    first_bytes = (first / "out" / "jersey.csv").read_bytes()
    second_bytes = (second / "out" / "jersey.csv").read_bytes()
    assert first_bytes == second_bytes
