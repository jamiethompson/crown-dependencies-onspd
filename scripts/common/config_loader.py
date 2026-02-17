"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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


def load_all_configs(config_dir: Path, *, allow_unknown: bool = False) -> ConfigBundle:
    territory_files = {
        "JE": config_dir / "jersey.yml",
        "GY": config_dir / "guernsey.yml",
        "IM": config_dir / "isle_of_man.yml",
    }

    territories = {}
    for code, path in territory_files.items():
        cfg = read_yaml(path)
        territories[code] = validate_territory_config(cfg, allow_unknown=allow_unknown)

    onspd = validate_onspd_columns_config(read_yaml(config_dir / "onspd_columns.yml"))
    scoring = validate_scoring_config(read_yaml(config_dir / "scoring_rules.yml"))
    return ConfigBundle(territories=territories, onspd_columns=onspd, scoring_rules=scoring)


def resolve_territories(target: str) -> list[str]:
    if target == "all":
        return ["JE", "GY", "IM"]
    return [target]
