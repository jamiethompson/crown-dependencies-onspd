from pathlib import Path

from scripts.common.config_loader import load_all_configs, resolve_territories


def test_load_all_configs_from_repo_config_dir():
    bundle = load_all_configs(Path("config"))
    assert set(bundle.territories) == {"JE", "GY", "IM"}
    assert bundle.onspd_columns["columns"]
    assert "default" in bundle.scoring_rules["profiles"]


def test_resolve_territories():
    assert resolve_territories("all") == ["JE", "GY", "IM"]
    assert resolve_territories("JE") == ["JE"]
