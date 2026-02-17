from pathlib import Path

import pytest

from scripts.cli import parse_args, run_command


@pytest.mark.integration
def test_cli_all_generates_expected_artifacts(tmp_path: Path):
    data_dir = tmp_path / "data"
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
            "run-test",
        ]
    )

    exit_code = run_command(args)

    assert exit_code == 0
    assert (data_dir / "out" / "jersey.csv").exists()
    assert (data_dir / "out" / "guernsey.csv").exists()
    assert (data_dir / "out" / "isle_of_man.csv").exists()
    assert (data_dir / "out" / "jersey_onspd.csv").exists()
    assert (data_dir / "out" / "reports" / "run_summary.json").exists()
