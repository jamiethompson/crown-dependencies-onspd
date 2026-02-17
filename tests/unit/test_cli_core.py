from scripts.cli import parse_args


def test_parse_args_defaults():
    args = parse_args(["discover"])
    assert args.command == "discover"
    assert args.territory == "all"
    assert args.strict is False
