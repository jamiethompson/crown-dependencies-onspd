from __future__ import annotations

from pathlib import Path

import pytest

from scripts.common.errors import StageError
from scripts.harvest import runner


@pytest.mark.integration
def test_harvest_fail_soft_when_one_source_unavailable(monkeypatch, tmp_path: Path):
    def fail_arcgis(*_args, **_kwargs):
        raise RuntimeError("arcgis down")

    def fail_overpass(*_args, **_kwargs):
        raise RuntimeError("overpass down")

    def okay_geofabrik(*_args, **_kwargs):
        return {"row_count": 0, "warnings": ["GEOFABRIK_INPUT_PATH_MISSING"]}

    monkeypatch.setattr(runner, "run_arcgis_harvest", fail_arcgis)
    monkeypatch.setattr(runner, "run_overpass_harvest", fail_overpass)
    monkeypatch.setattr(runner, "run_geofabrik_parse", okay_geofabrik)

    territory_config = {
        "arcgis": {"enabled": True},
        "overpass": {"enabled": True},
        "geofabrik": {"enabled": True},
    }

    result = runner.run_harvest_for_territory(
        "JE",
        territory_config,
        tmp_path,
        run_id="run-5",
        run_date="2026-02-17",
    )

    assert sorted(result["failed_sources"]) == ["arcgis", "overpass"]


@pytest.mark.integration
def test_harvest_fails_when_all_enabled_sources_fail(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(runner, "run_arcgis_harvest", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(runner, "run_overpass_harvest", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(runner, "run_geofabrik_parse", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("x")))

    territory_config = {
        "arcgis": {"enabled": True},
        "overpass": {"enabled": True},
        "geofabrik": {"enabled": True},
    }

    with pytest.raises(StageError):
        runner.run_harvest_for_territory("JE", territory_config, tmp_path, run_id="run-6", run_date="2026-02-17")
