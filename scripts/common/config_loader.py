"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.common.fs import read_yaml
from scripts.common.schema import (
    validate_onspd_columns_config,
    validate_scoring_config,
    validate_territory_config,
)


@dataclass(frozen=True)
class ConfigBundle:
    territories: dict[str, dict]
    onspd_columns: dict
    scoring_rules: dict


def _deep_merge(base: Any, overlay: Any) -> Any:
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            if key in merged:
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged
    return overlay


def _load_yaml_with_overlay(path: Path, overlay_path: Path | None) -> dict:
    base = read_yaml(path)
    if overlay_path is None or not overlay_path.exists():
        return base
    overlay = read_yaml(overlay_path)
    return _deep_merge(base, overlay)


def load_all_configs(
    config_dir: Path,
    *,
    allow_unknown: bool = False,
    overlay_config_dir: Path | None = None,
) -> ConfigBundle:
    territory_files = {
        "JE": config_dir / "jersey.yml",
        "GY": config_dir / "guernsey.yml",
        "IM": config_dir / "isle_of_man.yml",
    }

    territories = {}
    for code, path in territory_files.items():
        overlay_path = None
        if overlay_config_dir is not None:
            overlay_path = overlay_config_dir / path.name
        cfg = _load_yaml_with_overlay(path, overlay_path)
        territories[code] = validate_territory_config(cfg, allow_unknown=allow_unknown)

    onspd = validate_onspd_columns_config(
        _load_yaml_with_overlay(
            config_dir / "onspd_columns.yml",
            (overlay_config_dir / "onspd_columns.yml") if overlay_config_dir is not None else None,
        )
    )
    scoring = validate_scoring_config(
        _load_yaml_with_overlay(
            config_dir / "scoring_rules.yml",
            (overlay_config_dir / "scoring_rules.yml") if overlay_config_dir is not None else None,
        )
    )
    return ConfigBundle(territories=territories, onspd_columns=onspd, scoring_rules=scoring)


def resolve_territories(target: str) -> list[str]:
    if target == "all":
        return ["JE", "GY", "IM"]
    return [target]
