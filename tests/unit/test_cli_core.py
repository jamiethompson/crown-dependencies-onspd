from scripts.cli import parse_args


def test_parse_args_defaults():
    args = parse_args(["discover"])
    assert args.command == "discover"
    assert args.territory == "all"
    assert args.overlay_config_dir is None
    assert args.strict is False


def test_parse_args_accepts_overlay_config_dir():
    args = parse_args(["all", "--overlay-config-dir", "config/live"])
    assert args.overlay_config_dir == "config/live"
